//! Hub storage: the winning state of every note, persisted as one JSON file
//! per path under `<data-dir>/store/`.
//!
//! Only stamps live in memory; content is read back from disk when a
//! device asks for it. Tombstones are kept (stage 1 has no expiry) so a
//! device that was offline still learns about deletes.
//!
//! Files above the inline limit are kept once in `<data-dir>/blobs/<sha256>`
//! and stored as a blob reference (no `content`), so catch-up replies stay
//! small and the bytes are served in chunks (see [`crate::blob`]). Inline
//! events of large files from older agents are converted on `offer` and on
//! `open`.

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use crate::blob::{BlobDir, DEFAULT_CHUNK_SIZE, DEFAULT_INLINE_MAX};
use crate::event::{sha256_hex, NoteEvent, Stamp};

pub struct HubStore {
    dir: PathBuf,
    blobs: BlobDir,
    stamps: BTreeMap<String, Stamp>,
    inline_max: u64,
    chunk_size: u64,
}

impl HubStore {
    pub fn open(data_dir: &Path) -> io::Result<Self> {
        Self::open_with(data_dir, DEFAULT_INLINE_MAX, DEFAULT_CHUNK_SIZE)
    }

    /// `inline_max`: stored events with more content than this keep their
    /// bytes in the blob directory instead.
    pub fn open_with(data_dir: &Path, inline_max: u64, chunk_size: u64) -> io::Result<Self> {
        let dir = data_dir.join("store");
        fs::create_dir_all(&dir)?;
        let blobs = BlobDir::open(&data_dir.join("blobs"))?;
        let mut st = HubStore {
            dir: dir.clone(),
            blobs,
            stamps: BTreeMap::new(),
            inline_max,
            chunk_size,
        };
        for entry in fs::read_dir(&dir)? {
            let p = entry?.path();
            if p.extension().and_then(|e| e.to_str()) != Some("json") {
                continue;
            }
            let bytes = fs::read(&p)?;
            match NoteEvent::decode(&bytes, None) {
                Ok(ev) if ev.is_blob_ref() && !st.blobs.has(&ev.stamp.sha256) => {
                    // Unknown to the index, so the device holding it
                    // republishes it on its next catch-up.
                    tracing::warn!(
                        "hub store: blob {} of {} is missing; dropping the entry",
                        ev.stamp.sha256,
                        ev.stamp.path
                    );
                }
                Ok(ev) => {
                    if let Some(r) = ev.to_blob_ref(inline_max, chunk_size) {
                        st.blobs.put(ev.content.as_deref().unwrap_or_default())?;
                        st.write(&r)?;
                        tracing::info!(
                            "hub store: moved {} ({} B) into the blob store",
                            ev.stamp.path,
                            ev.size()
                        );
                    }
                    st.stamps.insert(ev.stamp.path.clone(), ev.stamp);
                }
                Err(e) => tracing::warn!("hub store: skipping unreadable {}: {e}", p.display()),
            }
        }
        Ok(st)
    }

    fn write(&self, ev: &NoteEvent) -> io::Result<()> {
        let f = self.file_for(&ev.stamp.path);
        let tmp = f.with_extension("json.tmp");
        fs::write(&tmp, ev.encode())?;
        fs::rename(&tmp, &f)
    }

    pub fn blobs(&self) -> &BlobDir {
        &self.blobs
    }

    pub fn inline_max(&self) -> u64 {
        self.inline_max
    }

    /// `offer` would store a state with this stamp.
    pub fn would_store(&self, stamp: &Stamp) -> bool {
        self.stamps
            .get(&stamp.path)
            .is_none_or(|cur| stamp.beats(cur))
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
    ///
    /// Content above the inline limit goes to the blob directory and the
    /// entry becomes a blob reference. A blob reference is only accepted
    /// once its bytes are in the blob directory (the hub pulls them first).
    pub fn offer(&mut self, ev: &NoteEvent) -> io::Result<bool> {
        if !self.would_store(&ev.stamp) {
            return Ok(false);
        }
        let to_store = if let Some(r) = ev.to_blob_ref(self.inline_max, self.chunk_size) {
            self.blobs.put(ev.content.as_deref().unwrap_or_default())?;
            r
        } else if ev.is_blob_ref() {
            if !self.blobs.has(&ev.stamp.sha256) {
                return Err(io::Error::other(format!(
                    "blob {} of {} not pulled yet",
                    ev.stamp.sha256, ev.stamp.path
                )));
            }
            ev.clone()
        } else {
            ev.clone()
        };
        self.write(&to_store)?;
        let old = self.stamps.insert(ev.stamp.path.clone(), ev.stamp.clone());
        if let Some(old) = old {
            self.drop_unused_blob(&old.sha256);
        }
        Ok(true)
    }

    /// Remove blob `sha` if no stored stamp refers to it any more.
    fn drop_unused_blob(&self, sha: &str) {
        if sha.is_empty() || !self.blobs.has(sha) || self.stamps.values().any(|s| s.sha256 == sha) {
            return;
        }
        if let Err(e) = self.blobs.remove(sha) {
            tracing::warn!("hub store: removing unused blob {sha} failed: {e}");
        }
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

    #[test]
    fn large_content_goes_to_the_blob_dir() {
        let t = tempfile::tempdir().unwrap();
        let mut s = HubStore::open_with(t.path(), 100, 4096).unwrap();
        let big: Vec<u8> = (0..5000u32).map(|i| (i % 251) as u8).collect();
        let e = NoteEvent::put("a.bin", big.clone(), "box", 1, None, 1);
        assert!(s.offer(&e).unwrap());
        let stored = s.get("a.bin").unwrap().unwrap();
        assert!(stored.is_blob_ref(), "stored as a reference");
        assert_eq!(stored.blob.unwrap().chunks, 2);
        assert_eq!(s.blobs().read(&e.stamp.sha256).unwrap(), big);
        // A reference without bytes is refused; with bytes it is stored.
        let big2 = vec![7u8; 300];
        let r2 = NoteEvent::put("a.bin", big2.clone(), "s24", 2, None, 1)
            .to_blob_ref(100, 4096)
            .unwrap();
        assert!(s.offer(&r2).is_err());
        s.blobs().put(&big2).unwrap();
        assert!(s.offer(&r2).unwrap());
        assert!(!s.blobs().has(&e.stamp.sha256), "replaced blob is removed");
        // Older stores holding inline large content are converted on open.
        drop(s);
        let mut legacy = HubStore::open_with(t.path(), u64::MAX, 4096).unwrap();
        let e3 = NoteEvent::put("b.bin", big.clone(), "box", 1, None, 1);
        assert!(legacy.offer(&e3).unwrap());
        assert!(!legacy.get("b.bin").unwrap().unwrap().is_blob_ref());
        drop(legacy);
        let s = HubStore::open_with(t.path(), 100, 4096).unwrap();
        assert!(s.get("b.bin").unwrap().unwrap().is_blob_ref());
        assert_eq!(s.len(), 2);
        // A reference whose blob vanished is dropped from the index.
        s.blobs().remove(&sha256_hex(&big2)).unwrap();
        drop(s);
        let s = HubStore::open_with(t.path(), 100, 4096).unwrap();
        assert!(s.stamp("a.bin").is_none());
        assert!(s.stamp("b.bin").is_some());
    }
}
