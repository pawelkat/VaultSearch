//! Filesystem side of the agent: which files sync, hashing, atomic writes.

use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::event::sha256_hex;
use crate::key;

/// Prefix of the agent's own temp files (never synced, never watched).
pub const TMP_PREFIX: &str = ".vault-events-tmp-";
/// Default per-file size cap. Larger files stay on the Drive fallback path.
pub const DEFAULT_MAX_BYTES: u64 = 8 * 1024 * 1024;

/// What is on disk at a vault path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Disk {
    Absent,
    /// Present but not synced (too large, symlink, directory, ignored...).
    Skipped(String),
    Present {
        sha256: String,
        bytes: Vec<u8>,
        mtime_ms: u64,
        size: u64,
    },
}

impl Disk {
    pub fn sha(&self) -> Option<&str> {
        match self {
            Disk::Present { sha256, .. } => Some(sha256),
            _ => None,
        }
    }
}

/// The one dot-directory that does sync: shared agent skills
/// (`.grok/skills/**`, e.g. `SKILL.md`, helper scripts). Everything else
/// under `.grok/` (auth, config, sessions) stays local.
pub const SYNCED_DOT_PREFIX: [&str; 2] = [".grok", "skills"];

/// Single source of truth for "does this vault-relative path sync?", used
/// by the scan, the watcher, publishing, incoming apply, catch-up and the
/// hub. Never synced: anything under a dot-directory or a dotfile
/// (`.obsidian/` is per-device UI state, `.trash/`, `.git/`, sync-tool
/// metadata) except the `.grok/skills/` allow-list, editor/download temp
/// files, and our own temp files. Inside `.grok/skills/` nested dot
/// segments (`.grok/skills/a/.DS_Store`, `.grok/skills/.git/...`) are still
/// excluded.
pub fn is_synced_path(rel: &str) -> bool {
    let comps: Vec<&str> = rel.split('/').collect();
    let allowed = comps.len() > SYNCED_DOT_PREFIX.len() && comps.starts_with(&SYNCED_DOT_PREFIX);
    for (i, comp) in comps.iter().enumerate() {
        if comp.starts_with('.') && !(allowed && i < SYNCED_DOT_PREFIX.len()) {
            return false;
        }
    }
    let name = comps.last().copied().unwrap_or(rel);
    !(name.ends_with('~')
        || name.starts_with("~$")
        || name.starts_with(".#")
        || [".tmp", ".swp", ".swx", ".part", ".crdownload", ".partial"]
            .iter()
            .any(|s| name.ends_with(s)))
}

/// Inverse of [`is_synced_path`].
pub fn is_ignored(rel: &str) -> bool {
    !is_synced_path(rel)
}

/// Whether a directory may hold synced files, so the scan and the watcher
/// descend into it: synced paths, plus the bare ancestors of the allow-list
/// (`.grok`, `.grok/skills`).
pub fn may_contain_synced(rel_dir: &str) -> bool {
    let comps: Vec<&str> = rel_dir.split('/').collect();
    is_synced_path(rel_dir)
        || (comps.len() <= SYNCED_DOT_PREFIX.len() && SYNCED_DOT_PREFIX.starts_with(&comps))
}

pub fn mtime_ms(md: &fs::Metadata) -> u64 {
    md.modified()
        .ok()
        .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

static TMP_SEQ: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, Clone)]
pub struct VaultFs {
    root: PathBuf,
    max_bytes: u64,
}

impl VaultFs {
    pub fn new(root: impl Into<PathBuf>, max_bytes: u64) -> Self {
        VaultFs {
            root: root.into(),
            max_bytes,
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn max_bytes(&self) -> u64 {
        self.max_bytes
    }

    /// Absolute path for a validated relative path.
    pub fn abs(&self, rel: &str) -> io::Result<PathBuf> {
        key::validate_rel(rel)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e.to_string()))?;
        Ok(self.root.join(rel))
    }

    /// Cheap metadata (no hashing): `(mtime_ms, size)` of a regular file.
    pub fn stat(&self, rel: &str) -> Option<(u64, u64)> {
        let md = fs::symlink_metadata(self.abs(rel).ok()?).ok()?;
        md.is_file().then(|| (mtime_ms(&md), md.len()))
    }

    pub fn read(&self, rel: &str) -> io::Result<Disk> {
        if is_ignored(rel) {
            return Ok(Disk::Skipped("ignored path".into()));
        }
        let p = self.abs(rel)?;
        let md = match fs::symlink_metadata(&p) {
            Ok(m) => m,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Disk::Absent),
            Err(e) => return Err(e),
        };
        if md.file_type().is_symlink() {
            return Ok(Disk::Skipped("symlink".into()));
        }
        if !md.is_file() {
            return Ok(Disk::Skipped("not a regular file".into()));
        }
        if md.len() > self.max_bytes {
            return Ok(Disk::Skipped(format!(
                "{} bytes > max {}",
                md.len(),
                self.max_bytes
            )));
        }
        let bytes = match fs::read(&p) {
            Ok(b) => b,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(Disk::Absent),
            Err(e) => return Err(e),
        };
        Ok(Disk::Present {
            sha256: sha256_hex(&bytes),
            size: bytes.len() as u64,
            mtime_ms: mtime_ms(&md),
            bytes,
        })
    }

    /// Write via a temp file in the same directory + rename, so readers
    /// (Obsidian, vault-searchd) never see a half-written note.
    pub fn write_atomic(&self, rel: &str, bytes: &[u8]) -> io::Result<()> {
        let p = self.abs(rel)?;
        if let Ok(md) = fs::symlink_metadata(&p) {
            if !md.is_file() {
                return Err(io::Error::other(format!(
                    "refusing to replace non-file {}",
                    p.display()
                )));
            }
        }
        let dir = p.parent().unwrap_or(&self.root);
        fs::create_dir_all(dir)?;
        let tmp = dir.join(format!(
            "{TMP_PREFIX}{}-{}",
            std::process::id(),
            TMP_SEQ.fetch_add(1, Ordering::Relaxed)
        ));
        let res = (|| {
            let mut f = fs::File::create(&tmp)?;
            f.write_all(bytes)?;
            f.sync_all()?;
            fs::rename(&tmp, &p)
        })();
        if res.is_err() {
            let _ = fs::remove_file(&tmp);
        }
        res
    }

    /// Remove a regular file. Missing is fine. Empty parent dirs are kept.
    pub fn remove(&self, rel: &str) -> io::Result<()> {
        let p = self.abs(rel)?;
        match fs::symlink_metadata(&p) {
            Ok(md) if md.is_file() => fs::remove_file(&p),
            Ok(_) => Err(io::Error::other(format!(
                "refusing to delete non-file {}",
                p.display()
            ))),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Every syncable regular file (relative paths, sorted). Does not
    /// follow symlinks and does not descend into ignored directories.
    pub fn scan(&self) -> io::Result<Vec<String>> {
        let mut out = Vec::new();
        let mut stack = vec![self.root.clone()];
        while let Some(dir) = stack.pop() {
            for entry in fs::read_dir(&dir)? {
                let entry = entry?;
                let ft = entry.file_type()?;
                let abs = entry.path();
                let Ok(rel) = key::rel_from_fs(&self.root, &abs) else {
                    continue;
                };
                if ft.is_dir() {
                    if may_contain_synced(&rel) {
                        stack.push(abs);
                    }
                } else if ft.is_file() && is_synced_path(&rel) {
                    out.push(rel);
                }
            }
        }
        out.sort();
        Ok(out)
    }

    /// Where to store a conflict copy of `bytes`: `base` if free, otherwise
    /// `base` with ` 2`, ` 3`, ... before the extension. Returns `None` when a
    /// candidate already holds exactly these bytes (copy already made,
    /// possibly by another device).
    pub fn conflict_slot(&self, base: &str, bytes: &[u8]) -> io::Result<Option<String>> {
        let want = sha256_hex(bytes);
        for n in 1..1000 {
            let cand = if n == 1 {
                base.to_string()
            } else {
                numbered(base, n)
            };
            match self.read(&cand)? {
                Disk::Absent => return Ok(Some(cand)),
                Disk::Present { sha256, .. } if sha256 == want => return Ok(None),
                _ => continue,
            }
        }
        Err(io::Error::other(format!(
            "no free conflict name for {base}"
        )))
    }
}

fn numbered(base: &str, n: u32) -> String {
    let (dir, file) = match base.rfind('/') {
        Some(i) => (&base[..=i], &base[i + 1..]),
        None => ("", base),
    };
    match file.rfind('.') {
        Some(i) if i > 0 => format!("{dir}{} {n}{}", &file[..i], &file[i..]),
        _ => format!("{dir}{file} {n}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ignore_rules() {
        assert!(is_ignored(".obsidian/workspace.json"));
        assert!(is_ignored("Notes/.trash/x.md"));
        assert!(is_ignored("Notes/.vault-events-tmp-1-2"));
        assert!(is_ignored("Notes/a.md.tmp"));
        assert!(is_ignored("Notes/a.md~"));
        assert!(!is_ignored("Notes/a.md"));
        assert!(!is_ignored("Attachments /p.png"));
        assert!(!is_ignored("Notes/note4.md..conflict-Date1"));
    }

    #[test]
    fn grok_skills_allow_list() {
        assert!(is_synced_path(".grok/skills/a/SKILL.md"));
        assert!(is_synced_path(
            ".grok/skills/panel-daily/scripts/after-sync.sh"
        ));
        assert!(is_synced_path(".grok/skills/vault-sync/scripts/config.env"));
        assert!(!is_synced_path(".grok/auth.json"));
        assert!(!is_synced_path(".grok/config"));
        assert!(!is_synced_path(".grok/sessions/x.md"));
        assert!(!is_synced_path(".grok/skills"));
        assert!(!is_synced_path(".grok"));
        assert!(!is_synced_path(".obsidian/x.md"));
        assert!(!is_synced_path(".grok/skills/a/.hidden"));
        assert!(!is_synced_path(".grok/skills/a/.DS_Store"));
        assert!(!is_synced_path(".grok/skills/.git/config"));
        assert!(!is_synced_path(".grok/skills/a/SKILL.md.swp"));
        assert!(
            !is_synced_path("Notes/.grok/skills/a/SKILL.md"),
            "only at vault root"
        );
        assert!(!is_synced_path(".Grok/skills/a/SKILL.md"));
        assert!(may_contain_synced(".grok"));
        assert!(may_contain_synced(".grok/skills"));
        assert!(may_contain_synced(".grok/skills/a"));
        assert!(!may_contain_synced(".grok/sessions"));
        assert!(!may_contain_synced(".obsidian"));
        assert!(!may_contain_synced(".grok/skills/.git"));
    }

    #[test]
    fn scan_includes_grok_skills_only() {
        let t = tempfile::tempdir().unwrap();
        let v = VaultFs::new(t.path(), 100);
        for p in [
            ".grok/skills/a/SKILL.md",
            ".grok/skills/a/scripts/run.sh",
            ".grok/skills/a/.hidden",
            ".grok/skills/.git/HEAD",
            ".grok/auth.json",
            ".grok/config/x.md",
            ".obsidian/x.md",
            "Notes/n.md",
        ] {
            v.write_atomic(p, b"x").unwrap();
        }
        assert_eq!(
            v.scan().unwrap(),
            vec![
                ".grok/skills/a/SKILL.md".to_string(),
                ".grok/skills/a/scripts/run.sh".to_string(),
                "Notes/n.md".to_string(),
            ]
        );
        assert!(matches!(
            v.read(".grok/auth.json").unwrap(),
            Disk::Skipped(_)
        ));
        assert!(matches!(
            v.read(".grok/skills/a/SKILL.md").unwrap(),
            Disk::Present { .. }
        ));
    }

    #[test]
    fn write_read_remove_scan() {
        let t = tempfile::tempdir().unwrap();
        let v = VaultFs::new(t.path(), 10);
        v.write_atomic("Notes/sub/a.md", b"hello").unwrap();
        v.write_atomic(".obsidian/x.json", b"{}").unwrap();
        assert!(matches!(
            v.read("Notes/sub/a.md").unwrap(),
            Disk::Present { size: 5, .. }
        ));
        v.write_atomic("big.bin", b"01234567890").unwrap();
        assert!(matches!(v.read("big.bin").unwrap(), Disk::Skipped(_)));
        assert_eq!(
            v.scan().unwrap(),
            vec!["Notes/sub/a.md".to_string(), "big.bin".to_string()]
        );
        v.remove("Notes/sub/a.md").unwrap();
        v.remove("Notes/sub/a.md").unwrap();
        assert_eq!(v.read("Notes/sub/a.md").unwrap(), Disk::Absent);
        assert!(v.remove("Notes/sub").is_err(), "never deletes a directory");
        assert!(v.write_atomic("../escape.md", b"x").is_err());
        // no temp files left behind
        assert!(fs::read_dir(t.path().join("Notes/sub"))
            .unwrap()
            .next()
            .is_none());
    }

    #[test]
    fn conflict_slots() {
        let t = tempfile::tempdir().unwrap();
        let v = VaultFs::new(t.path(), 100);
        let base = "a (conflict s24 2026-11-18).md";
        assert_eq!(v.conflict_slot(base, b"x").unwrap().as_deref(), Some(base));
        v.write_atomic(base, b"x").unwrap();
        assert_eq!(v.conflict_slot(base, b"x").unwrap(), None);
        assert_eq!(
            v.conflict_slot(base, b"y").unwrap().as_deref(),
            Some("a (conflict s24 2026-11-18) 2.md")
        );
    }
}
