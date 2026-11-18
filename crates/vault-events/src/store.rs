//! Hub storage: the winning state of every note, persisted as one JSON file
//! per path under `<data-dir>/store/`.
//!
//! Only stamps live in memory; content is read back from disk when a
//! device asks for it. Tombstones are kept (stage 1 has no expiry) so a
//! device that was offline still learns about deletes.

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use crate::event::{sha256_hex, NoteEvent, Stamp};

pub struct HubStore {
    dir: PathBuf,
    stamps: BTreeMap<String, Stamp>,
}

impl HubStore {
    pub fn open(data_dir: &Path) -> io::Result<Self> {
        let dir = data_dir.join("store");
        fs::create_dir_all(&dir)?;
        let mut stamps = BTreeMap::new();
        for entry in fs::read_dir(&dir)? {
            let p = entry?.path();
            if p.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let bytes = fs::read(&p)?;
            match NoteEvent::decode(&bytes, None) {
                Ok(ev) => {
                    stamps.insert(ev.stamp.path.clone(), ev.stamp);
                }
                Err(e) => tracing::warn!("hub store: skipping unreadable {}: {e}", p.display()),
            }
        }
        Ok(HubStore { dir, stamps })
    }

    fn file_for(&self, path: &str) -> PathBuf {
        self.dir
            .join(format!("{}.json", sha256_hex(path.as_bytes())))
    }

    pub fn len(&self) -> usize {
        self.stamps.len()
    }

    pub fn is_empty(&self) -> bool {
        self.stamps.is_empty()
    }

    pub fn live_count(&self) -> usize {
        self.stamps.values().filter(|s| !s.deleted).count()
    }

    /// Store `ev` if it beats what is stored. Returns `true` if stored.
    pub fn offer(&mut self, ev: &NoteEvent) -> io::Result<bool> {
        if let Some(cur) = self.stamps.get(&ev.stamp.path) {
            if !ev.stamp.beats(cur) {
                return Ok(false);
            }
        }
        let f = self.file_for(&ev.stamp.path);
        let tmp = f.with_extension("json.tmp");
        fs::write(&tmp, ev.encode())?;
        fs::rename(&tmp, &f)?;
        self.stamps.insert(ev.stamp.path.clone(), ev.stamp.clone());
        Ok(true)
    }

    pub fn stamp(&self, path: &str) -> Option<&Stamp> {
        self.stamps.get(path)
    }

    pub fn get(&self, path: &str) -> io::Result<Option<NoteEvent>> {
        if !self.stamps.contains_key(path) {
            return Ok(None);
        }
        let bytes = fs::read(self.file_for(path))?;
        NoteEvent::decode(&bytes, None)
            .map(Some)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e.to_string()))
    }

    pub fn stamps(&self) -> impl Iterator<Item = &Stamp> {
        self.stamps.values()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keeps_winner_and_survives_reopen() {
        let t = tempfile::tempdir().unwrap();
        let mut s = HubStore::open(t.path()).unwrap();
        let v1 = NoteEvent::put("a.md", b"1".to_vec(), "box", 1, None, 1);
        let v2 = NoteEvent::put("a.md", b"2".to_vec(), "s24", 2, None, 1);
        assert!(s.offer(&v2).unwrap());
        assert!(!s.offer(&v1).unwrap(), "older version is not stored");
        assert!(!s.offer(&v2).unwrap(), "same state is not re-stored");
        let d = NoteEvent::delete("b.md", "box", 1, None, 1);
        assert!(s.offer(&d).unwrap());
        drop(s);
        let s = HubStore::open(t.path()).unwrap();
        assert_eq!(s.len(), 2);
        assert_eq!(s.live_count(), 1);
        assert_eq!(s.get("a.md").unwrap().unwrap(), v2);
        assert!(s.get("b.md").unwrap().unwrap().stamp.deleted);
        assert!(s.get("zzz").unwrap().is_none());
    }
}
