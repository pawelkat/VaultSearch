//! The hub's per-path provenance history: one JSON line per stored event,
//! append-only, in `<data-dir>/history.jsonl` (rotated to `history.jsonl.1`
//! when it exceeds [`DEFAULT_MAX_BYTES`], so at most ~2x that on disk).
//!
//! Debug data only: nothing here is ever written into a note, and losing
//! the file changes nothing about sync.

use std::fs;
use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::event::{ConflictInfo, NoteEvent, Stamp};

/// Rotate `history.jsonl` past this size (one `.1` kept: ~20 MB total).
pub const DEFAULT_MAX_BYTES: u64 = 10 * 1024 * 1024;
pub const FILE: &str = "history.jsonl";

/// One stored event as the hub saw it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistoryEntry {
    /// Hub wall clock when stored, Unix ms.
    pub stored_ms: u64,
    pub path: String,
    pub version: u64,
    /// Producer's clock in the event (the edit time for local edits).
    pub ts_ms: u64,
    pub origin: String,
    #[serde(default)]
    pub writer: Option<String>,
    #[serde(default)]
    pub cause: Option<String>,
    #[serde(default)]
    pub deleted: bool,
    #[serde(default)]
    pub sha256: String,
    /// What the producer based the edit on.
    #[serde(default)]
    pub prev_sha256: Option<String>,
    /// What the hub stored before this (`None` = path was new to the hub;
    /// `Some("")` = it was a tombstone).
    #[serde(default)]
    pub replaced_sha256: Option<String>,
    #[serde(default)]
    pub replaced_version: Option<u64>,
    #[serde(default)]
    pub size: u64,
    #[serde(default)]
    pub conflict: Option<ConflictInfo>,
}

impl HistoryEntry {
    pub fn new(ev: &NoteEvent, replaced: Option<&Stamp>, stored_ms: u64) -> Self {
        let s = &ev.stamp;
        HistoryEntry {
            stored_ms,
            path: s.path.clone(),
            version: s.version,
            ts_ms: s.ts_ms,
            origin: s.origin.clone(),
            writer: s.writer.clone(),
            cause: s.cause.clone(),
            deleted: s.deleted,
            sha256: s.sha256.clone(),
            prev_sha256: s.prev_sha256.clone(),
            replaced_sha256: replaced.map(|r| r.sha256.clone()),
            replaced_version: replaced.map(|r| r.version),
            size: ev.size(),
            conflict: ev.conflict.clone(),
        }
    }
}

pub struct History {
    file: PathBuf,
    max_bytes: u64,
}

impl History {
    pub fn open(data_dir: &Path) -> Self {
        Self::with_limit(data_dir, DEFAULT_MAX_BYTES)
    }

    pub fn with_limit(data_dir: &Path, max_bytes: u64) -> Self {
        History {
            file: data_dir.join(FILE),
            max_bytes,
        }
    }

    pub fn file(&self) -> &Path {
        &self.file
    }

    fn rotated(&self) -> PathBuf {
        self.file.with_extension("jsonl.1")
    }

    pub fn append(&self, e: &HistoryEntry) -> io::Result<()> {
        if let Some(d) = self.file.parent() {
            fs::create_dir_all(d)?;
        }
        if self.max_bytes > 0 {
            if let Ok(m) = fs::metadata(&self.file) {
                if m.len() > self.max_bytes {
                    let _ = fs::remove_file(self.rotated());
                    fs::rename(&self.file, self.rotated())?;
                }
            }
        }
        let mut line = serde_json::to_vec(e)?;
        line.push(b'\n');
        let mut f = fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.file)?;
        f.write_all(&line)
    }

    /// Entries for `path`, newest first, at most `limit` (0 = all).
    /// Unreadable lines are skipped.
    pub fn query(&self, path: &str, limit: usize) -> io::Result<Vec<HistoryEntry>> {
        let mut out = Vec::new();
        for f in [self.rotated(), self.file.clone()] {
            let file = match fs::File::open(&f) {
                Ok(x) => x,
                Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
                Err(e) => return Err(e),
            };
            for line in io::BufReader::new(file).lines() {
                let line = line?;
                // Cheap pre-filter before parsing every line.
                if !line.contains("\"path\"") {
                    continue;
                }
                if let Ok(e) = serde_json::from_str::<HistoryEntry>(&line) {
                    if e.path == path {
                        out.push(e);
                    }
                }
            }
        }
        out.reverse();
        if limit > 0 {
            out.truncate(limit);
        }
        Ok(out)
    }
}

/// Reply body of the hub's `vault/hub/history/<path>` queryable.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HistoryReply {
    pub format: u32,
    pub hub: String,
    pub path: String,
    /// Newest first.
    pub entries: Vec<HistoryEntry>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::cause;

    #[test]
    fn append_query_newest_first_and_limit() {
        let t = tempfile::tempdir().unwrap();
        let h = History::open(t.path());
        assert!(h.query("a.md", 0).unwrap().is_empty());
        let v1 = NoteEvent::put("a.md", b"1".to_vec(), "box", 1, None, 10)
            .by("box/galanonim", cause::LOCAL_EDIT);
        let other = NoteEvent::put("b.md", b"x".to_vec(), "s24", 1, None, 11);
        let v2 = NoteEvent::put(
            "a.md",
            b"22".to_vec(),
            "s24",
            2,
            Some(v1.stamp.sha256.clone()),
            12,
        )
        .by("s24/vaulthome", cause::LOCAL_EDIT);
        let d = NoteEvent::delete("a.md", "box", 3, Some(v2.stamp.sha256.clone()), 13)
            .by("box/bisync", cause::BISYNC_PULL);
        h.append(&HistoryEntry::new(&v1, None, 100)).unwrap();
        h.append(&HistoryEntry::new(&other, None, 101)).unwrap();
        h.append(&HistoryEntry::new(&v2, Some(&v1.stamp), 102))
            .unwrap();
        h.append(&HistoryEntry::new(&d, Some(&v2.stamp), 103))
            .unwrap();
        let all = h.query("a.md", 0).unwrap();
        assert_eq!(
            all.iter().map(|e| e.version).collect::<Vec<_>>(),
            vec![3, 2, 1]
        );
        assert!(all[0].deleted);
        assert_eq!(all[0].writer.as_deref(), Some("box/bisync"));
        assert_eq!(all[1].size, 2);
        assert_eq!(
            all[1].replaced_sha256.as_deref(),
            Some(v1.stamp.sha256.as_str())
        );
        assert_eq!(all[2].replaced_sha256, None);
        assert_eq!(h.query("a.md", 2).unwrap().len(), 2);
        assert_eq!(h.query("b.md", 0).unwrap()[0].writer, None);
    }

    #[test]
    fn rotates_and_still_reads_both_files() {
        let t = tempfile::tempdir().unwrap();
        let h = History::with_limit(t.path(), 300);
        for v in 1..=20u64 {
            let e = NoteEvent::put("a.md", vec![b'x'; v as usize], "box", v, None, v);
            h.append(&HistoryEntry::new(&e, None, v)).unwrap();
        }
        let total: u64 = [h.file().to_path_buf(), h.rotated()]
            .iter()
            .map(|p| fs::metadata(p).map(|m| m.len()).unwrap_or(0))
            .sum();
        assert!(h.rotated().exists());
        assert!(total < 2 * 300 + 2 * 400, "capped: {total}");
        let got = h.query("a.md", 0).unwrap();
        assert_eq!(got[0].version, 20, "newest first across rotation");
        assert!(got.windows(2).all(|w| w[0].version > w[1].version));
        // Garbage lines are skipped.
        fs::OpenOptions::new()
            .append(true)
            .open(h.file())
            .unwrap()
            .write_all(b"{not json \"path\"\n")
            .unwrap();
        assert_eq!(h.query("a.md", 1).unwrap()[0].version, 20);
    }
}
