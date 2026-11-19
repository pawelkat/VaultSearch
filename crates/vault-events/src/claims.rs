//! Named writers: `vault-events write --as <name>` leaves a claim before it
//! writes a note, and the agent labels the change it then sees on disk as
//! `<device>/<name>` (cause `local-edit` / `delete`) instead of
//! `<device>/external`.
//!
//! A claim is `<agent-state-dir>/claims/<sha256(path)>.json` holding
//! `{path, sha256, writer, ts_ms}` (`sha256` empty = delete). The agent
//! consumes it when the disk content matches; claims older than
//! [`STALE_MS`] are dropped.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::event::sha256_hex;

/// Claims not matched within this time are dropped.
pub const STALE_MS: u64 = 10 * 60 * 1000;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Claim {
    pub path: String,
    /// Expected content hash; empty for a delete.
    pub sha256: String,
    /// Program name, without the device (`galanonim`).
    pub writer: String,
    pub ts_ms: u64,
}

pub fn claims_dir(state_dir: &Path) -> PathBuf {
    state_dir.join("claims")
}

fn file_for(dir: &Path, path: &str) -> PathBuf {
    dir.join(format!("{}.json", sha256_hex(path.as_bytes())))
}

/// Writer names are short labels: `[A-Za-z0-9._-]`, 1..=32 chars.
pub fn valid_writer_name(n: &str) -> bool {
    !n.is_empty()
        && n.len() <= 32
        && n.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

pub fn put(dir: &Path, c: &Claim) -> io::Result<()> {
    fs::create_dir_all(dir)?;
    let f = file_for(dir, &c.path);
    let tmp = f.with_extension(format!("tmp-{}", std::process::id()));
    fs::write(&tmp, serde_json::to_vec(c)?)?;
    fs::rename(&tmp, &f)
}

/// The claim for `path`, if any (without consuming it).
pub fn peek(dir: &Path, path: &str) -> Option<Claim> {
    let c: Claim = serde_json::from_slice(&fs::read(file_for(dir, path)).ok()?).ok()?;
    (c.path == path).then_some(c)
}

/// Consume the claim for `path` if it matches `sha` (`None` = deleted).
/// Returns the writer name. A non-matching claim stays (the write may not
/// have landed yet) unless it is stale.
pub fn take(dir: &Path, path: &str, sha: Option<&str>, now: u64) -> Option<String> {
    let c = peek(dir, path)?;
    let f = file_for(dir, path);
    let want = sha.unwrap_or("");
    if c.sha256 == want {
        let _ = fs::remove_file(&f);
        return Some(c.writer);
    }
    if now.saturating_sub(c.ts_ms) > STALE_MS {
        let _ = fs::remove_file(&f);
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn put_take_and_stale() {
        let t = tempfile::tempdir().unwrap();
        let d = claims_dir(t.path());
        let sha = sha256_hex(b"hello");
        put(
            &d,
            &Claim {
                path: "Notes/a.md".into(),
                sha256: sha.clone(),
                writer: "galanonim".into(),
                ts_ms: 1000,
            },
        )
        .unwrap();
        // Different content (write not landed yet): kept.
        assert_eq!(take(&d, "Notes/a.md", Some("other"), 2000), None);
        assert!(peek(&d, "Notes/a.md").is_some());
        assert_eq!(take(&d, "Notes/b.md", Some(&sha), 2000), None);
        assert_eq!(
            take(&d, "Notes/a.md", Some(&sha), 2000).as_deref(),
            Some("galanonim")
        );
        assert!(peek(&d, "Notes/a.md").is_none(), "consumed once");
        // Delete claim, then stale drop.
        put(
            &d,
            &Claim {
                path: "x.md".into(),
                sha256: String::new(),
                writer: "bot".into(),
                ts_ms: 0,
            },
        )
        .unwrap();
        assert_eq!(take(&d, "x.md", Some("abc"), STALE_MS + 1), None);
        assert!(peek(&d, "x.md").is_none(), "stale claim dropped");
        put(
            &d,
            &Claim {
                path: "x.md".into(),
                sha256: String::new(),
                writer: "bot".into(),
                ts_ms: 5,
            },
        )
        .unwrap();
        assert_eq!(take(&d, "x.md", None, 10).as_deref(), Some("bot"));
    }

    #[test]
    fn writer_names() {
        assert!(valid_writer_name("galanonim"));
        assert!(valid_writer_name("gal-anonim_2.0"));
        assert!(!valid_writer_name(""));
        assert!(!valid_writer_name("a/b"));
        assert!(!valid_writer_name("a b"));
        assert!(!valid_writer_name(&"x".repeat(33)));
    }
}
