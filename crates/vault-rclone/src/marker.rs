//! "A bisync is running" marker, so other programs (the `vault-events`
//! agent) can tell files rclone pulled from Drive apart from edits.
//!
//! While [`run_bisync`](crate::sync) runs, `sync.running` next to the bisync
//! workdir (default `~/.local/share/vault-rclone/sync.running`) holds
//! `{"pid":..,"start_ms":..,"run":"sync"}`. When the run ends (also on an
//! early return or a panic, via `Drop`) the file is removed and `sync.last`
//! gets `{"pid":..,"start_ms":..,"end_ms":..,"run":..}`.
//!
//! A crashed run leaves `sync.running` behind; readers check that the pid is
//! still alive (`/proc/<pid>`) and treat a dead pid as "not running".

use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};

use crate::config::Config;

pub const RUNNING_FILE: &str = "sync.running";
pub const LAST_FILE: &str = "sync.last";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MarkerInfo {
    pub pid: u32,
    pub start_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub end_ms: Option<u64>,
    #[serde(default)]
    pub run: String,
}

/// Directory holding the marker files for `cfg`: the parent of the bisync
/// workdir (`~/.local/share/vault-rclone` by default).
pub fn marker_dir_for_workdir(workdir: &Path) -> PathBuf {
    workdir
        .parent()
        .map(Path::to_path_buf)
        .unwrap_or_else(|| PathBuf::from("."))
}

pub fn marker_dir(cfg: &Config) -> PathBuf {
    marker_dir_for_workdir(&cfg.bisync_workdir)
}

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn write_atomic(p: &Path, bytes: &[u8]) -> io::Result<()> {
    if let Some(d) = p.parent() {
        fs::create_dir_all(d)?;
    }
    let tmp = p.with_extension(format!("tmp-{}", std::process::id()));
    fs::write(&tmp, bytes)?;
    fs::rename(&tmp, p)
}

/// RAII guard: creates `sync.running` on [`SyncMarker::start`], removes it and
/// writes `sync.last` on drop. Write errors are ignored (a read-only state
/// dir must never block a sync).
pub struct SyncMarker {
    dir: PathBuf,
    info: MarkerInfo,
}

impl SyncMarker {
    pub fn start(dir: &Path, run: &str) -> Self {
        let info = MarkerInfo {
            pid: std::process::id(),
            start_ms: now_ms(),
            end_ms: None,
            run: run.to_string(),
        };
        if let Ok(b) = serde_json::to_vec(&info) {
            let _ = write_atomic(&dir.join(RUNNING_FILE), &b);
        }
        SyncMarker {
            dir: dir.to_path_buf(),
            info,
        }
    }
}

impl Drop for SyncMarker {
    fn drop(&mut self) {
        let mut last = self.info.clone();
        last.end_ms = Some(now_ms());
        if let Ok(b) = serde_json::to_vec(&last) {
            let _ = write_atomic(&self.dir.join(LAST_FILE), &b);
        }
        let _ = fs::remove_file(self.dir.join(RUNNING_FILE));
    }
}

/// What the marker files say right now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Activity {
    /// A run with a live pid holds `sync.running`.
    Running(MarkerInfo),
    /// The last run ended `ago_ms` ago (only reported within the window).
    Recent {
        info: MarkerInfo,
        ago_ms: u64,
    },
    Idle,
}

impl Activity {
    pub fn is_active(&self) -> bool {
        !matches!(self, Activity::Idle)
    }
}

fn read_info(p: &Path) -> Option<MarkerInfo> {
    serde_json::from_slice(&fs::read(p).ok()?).ok()
}

/// `true` if a process with this pid exists. Without `/proc` (non-Linux)
/// a marker is trusted as is.
pub fn pid_alive(pid: u32) -> bool {
    if pid == 0 {
        return false;
    }
    let proc_root = Path::new("/proc");
    if !proc_root.join("self").exists() {
        return true;
    }
    proc_root.join(pid.to_string()).exists()
}

/// Bisync activity seen from `dir` at `now`: running (live pid), ended less
/// than `window_ms` ago, or idle. A stale `sync.running` (dead pid) counts
/// as ended at its start time, so a crash right before a read still gets
/// the window, but an old crash does not.
pub fn activity(dir: &Path, window_ms: u64, now: u64) -> Activity {
    activity_with(dir, window_ms, now, pid_alive)
}

pub fn activity_with(
    dir: &Path,
    window_ms: u64,
    now: u64,
    alive: impl Fn(u32) -> bool,
) -> Activity {
    if let Some(info) = read_info(&dir.join(RUNNING_FILE)) {
        if alive(info.pid) {
            return Activity::Running(info);
        }
    }
    if let Some(info) = read_info(&dir.join(LAST_FILE)) {
        let end = info.end_ms.unwrap_or(info.start_ms);
        let ago = now.saturating_sub(end);
        if ago <= window_ms {
            return Activity::Recent { info, ago_ms: ago };
        }
    }
    Activity::Idle
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guard_writes_running_then_last() {
        let d = tempfile::tempdir().unwrap();
        {
            let _m = SyncMarker::start(d.path(), "sync");
            assert!(d.path().join(RUNNING_FILE).exists());
            match activity(d.path(), 5000, now_ms()) {
                Activity::Running(i) => {
                    assert_eq!(i.pid, std::process::id());
                    assert_eq!(i.run, "sync");
                }
                a => panic!("{a:?}"),
            }
        }
        assert!(!d.path().join(RUNNING_FILE).exists());
        let last = read_info(&d.path().join(LAST_FILE)).unwrap();
        assert!(last.end_ms.is_some());
        assert!(matches!(
            activity(d.path(), 5000, now_ms()),
            Activity::Recent { .. }
        ));
        // Long after the run: idle.
        assert_eq!(
            activity(d.path(), 5000, last.end_ms.unwrap() + 5001),
            Activity::Idle
        );
    }

    #[test]
    fn stale_running_marker_with_dead_pid_is_not_running() {
        let d = tempfile::tempdir().unwrap();
        let info = MarkerInfo {
            pid: 4_000_000_000,
            start_ms: 1_000,
            end_ms: None,
            run: "sync".into(),
        };
        fs::write(
            d.path().join(RUNNING_FILE),
            serde_json::to_vec(&info).unwrap(),
        )
        .unwrap();
        assert_eq!(
            activity_with(d.path(), 5000, 1_000_000, |_| false),
            Activity::Idle
        );
        assert!(matches!(
            activity_with(d.path(), 5000, 1_000_000, |_| true),
            Activity::Running(_)
        ));
        assert!(!pid_alive(0));
        assert!(pid_alive(std::process::id()));
    }

    #[test]
    fn missing_or_garbage_files_are_idle() {
        let d = tempfile::tempdir().unwrap();
        assert_eq!(activity(d.path(), 5000, now_ms()), Activity::Idle);
        fs::write(d.path().join(RUNNING_FILE), b"not json").unwrap();
        fs::write(d.path().join(LAST_FILE), b"{").unwrap();
        assert_eq!(activity(d.path(), 5000, now_ms()), Activity::Idle);
        assert_eq!(
            marker_dir_for_workdir(Path::new("/h/.local/share/vault-rclone/bisync")),
            PathBuf::from("/h/.local/share/vault-rclone")
        );
    }
}
