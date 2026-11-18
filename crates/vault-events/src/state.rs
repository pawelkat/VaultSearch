//! The agent's persisted view: last accepted stamp per path.

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::event::Stamp;

pub const STATE_FORMAT: u32 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Entry {
    pub stamp: Stamp,
    /// mtime/size of the file when `stamp` was recorded, so a rescan can
    /// skip hashing unchanged files.
    #[serde(default)]
    pub mtime_ms: Option<u64>,
    #[serde(default)]
    pub size: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AgentState {
    pub format: u32,
    pub device: String,
    pub vault: PathBuf,
    pub entries: BTreeMap<String, Entry>,
}

impl AgentState {
    pub fn new(device: &str, vault: &Path) -> Self {
        AgentState {
            format: STATE_FORMAT,
            device: device.to_string(),
            vault: vault.to_path_buf(),
            entries: BTreeMap::new(),
        }
    }

    /// Load `file`, or start empty if it does not exist. Refuses a state
    /// file written for another vault or device: mixing them would turn
    /// every note into a "local edit".
    pub fn load_or_new(file: &Path, device: &str, vault: &Path) -> io::Result<Self> {
        match fs::read(file) {
            Ok(b) => {
                let s: AgentState = serde_json::from_slice(&b)
                    .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
                if s.format != STATE_FORMAT {
                    return Err(io::Error::other(format!(
                        "{}: unsupported state format {}",
                        file.display(),
                        s.format
                    )));
                }
                if s.vault != vault || s.device != device {
                    return Err(io::Error::other(format!(
                        "{} belongs to device {:?} vault {}, not {:?} {}; use another --state-dir",
                        file.display(),
                        s.device,
                        s.vault.display(),
                        device,
                        vault.display()
                    )));
                }
                Ok(s)
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(Self::new(device, vault)),
            Err(e) => Err(e),
        }
    }

    pub fn save(&self, file: &Path) -> io::Result<()> {
        if let Some(d) = file.parent() {
            fs::create_dir_all(d)?;
        }
        let tmp = file.with_extension("json.tmp");
        fs::write(&tmp, serde_json::to_vec_pretty(self)?)?;
        fs::rename(&tmp, file)
    }

    pub fn stamp(&self, path: &str) -> Option<&Stamp> {
        self.entries.get(path).map(|e| &e.stamp)
    }

    pub fn record(&mut self, stamp: Stamp, stat: Option<(u64, u64)>) {
        let path = stamp.path.clone();
        self.entries.insert(
            path,
            Entry {
                stamp,
                mtime_ms: stat.map(|s| s.0),
                size: stat.map(|s| s.1),
            },
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::NoteEvent;

    #[test]
    fn save_load_and_guard() {
        let t = tempfile::tempdir().unwrap();
        let f = t.path().join("s/agent-state.json");
        let mut s = AgentState::load_or_new(&f, "box", Path::new("/v")).unwrap();
        s.record(
            NoteEvent::put("a.md", b"x".to_vec(), "box", 1, None, 1).stamp,
            Some((5, 1)),
        );
        s.save(&f).unwrap();
        let back = AgentState::load_or_new(&f, "box", Path::new("/v")).unwrap();
        assert_eq!(back, s);
        assert!(AgentState::load_or_new(&f, "s24", Path::new("/v")).is_err());
        assert!(AgentState::load_or_new(&f, "box", Path::new("/w")).is_err());
    }
}
