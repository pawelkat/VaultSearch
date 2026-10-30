//! Prompt reindex on local vault writes.
//!
//! `serve` watches the vault recursively with [`notify`] (inotify on Linux).
//! Events on indexable notes (`.md` / `.markdown`, no dot-dir anywhere in the
//! path, same rule as `collect_notes` / [`indexable_path`]) and on folders
//! (a renamed or removed folder can carry notes) wake a debouncer thread.
//! The debouncer waits until the vault has been quiet for `quiet` (capped at
//! `max_wait` so a busy writer cannot starve it), then runs the same
//! incremental reindex as the 60s rescan and the socket `reindex` op.
//!
//! * The 60s rescan stays as the fallback for missed events, queue overflow,
//!   network filesystems, or platforms without notify.
//! * If the watcher cannot start (inotify limit, permission, unsupported FS)
//!   serve logs it, records it in `status`, and keeps the 60s rescan only.
//! * Reindex already running (`Ok(false)`): the change may have landed after
//!   that pass walked the vault, so the debouncer retries after `busy_retry`
//!   until it gets its own pass.
//! * Events that arrive while our own pass runs are not lost: they sit in the
//!   channel and trigger one more coalesced pass afterwards.
//! * A failed reindex (embedder down) is reported by the callback and not
//!   retried in a loop; the next event or the 60s rescan tries again.

use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, TryRecvError};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use notify::event::{EventKind, ModifyKind};
use notify::{Event, RecommendedWatcher, RecursiveMode, Watcher};
use vault_search::{indexable_path, IpcWatchStatus};

/// Debounce timings.
#[derive(Debug, Clone, Copy)]
pub struct Debounce {
    /// Fire once no event arrived for this long.
    pub quiet: Duration,
    /// Fire at the latest this long after the first event of a burst.
    pub max_wait: Duration,
    /// Wait this long before retrying when another reindex was running.
    pub busy_retry: Duration,
}

impl Debounce {
    pub fn from_millis(quiet_ms: u64) -> Self {
        let quiet = Duration::from_millis(quiet_ms.max(1));
        Self {
            quiet,
            max_wait: (quiet * 10).max(Duration::from_secs(5)),
            busy_retry: Duration::from_secs(1),
        }
    }
}

/// `true` when `path` (absolute, under `vault`) could change the index: an
/// indexable note, the vault root, or a folder (existing, or gone and
/// extensionless). Anything under a dot-dir, and non-markdown files, is
/// ignored, matching `collect_notes`.
pub fn relevant_path(vault: &Path, path: &Path) -> bool {
    let Ok(rel) = path.strip_prefix(vault) else {
        return false;
    };
    let rel = rel.to_string_lossy().replace('\\', "/");
    if rel.is_empty() {
        return true;
    }
    if rel.split('/').any(|s| s.starts_with('.')) {
        return false;
    }
    if indexable_path(&rel) {
        return true;
    }
    match std::fs::symlink_metadata(path) {
        Ok(meta) => meta.is_dir(),
        Err(_) => Path::new(&rel).extension().is_none(),
    }
}

/// `true` when a notify event should schedule a reindex.
pub fn wants_reindex(vault: &Path, event: &Event) -> bool {
    if event.need_rescan() {
        return true;
    }
    match event.kind {
        EventKind::Access(_) | EventKind::Other => false,
        // Content or rename; metadata covers `touch` (the index keys on mtime).
        EventKind::Modify(ModifyKind::Any | ModifyKind::Data(_) | ModifyKind::Name(_))
        | EventKind::Modify(ModifyKind::Metadata(_) | ModifyKind::Other)
        | EventKind::Create(_)
        | EventKind::Remove(_)
        | EventKind::Any => event.paths.iter().any(|p| relevant_path(vault, p)),
    }
}

/// Coalesce wake-ups from `rx` and call `fire` (the incremental reindex).
/// `fire` returns `Ok(true)` when it ran, `Ok(false)` when another reindex
/// was already running, `Err` when it failed. Returns when every sender is
/// gone (after one last pass if something was pending).
pub fn run_debouncer<F>(rx: Receiver<()>, cfg: Debounce, mut fire: F)
where
    F: FnMut() -> Result<bool, String>,
{
    'idle: loop {
        if rx.recv().is_err() {
            return;
        }
        let mut closed = false;
        loop {
            if !closed {
                closed = wait_quiet(&rx, cfg);
            }
            match fire() {
                Ok(true) | Err(_) => {}
                Ok(false) => {
                    std::thread::sleep(cfg.busy_retry);
                    continue;
                }
            }
            if closed {
                return;
            }
            match drain(&rx) {
                Drained::Some => continue,
                Drained::None => continue 'idle,
                Drained::Closed => return,
            }
        }
    }
}

/// Wait until `quiet` passed without an event or `max_wait` since now.
/// Returns `true` if the channel closed.
fn wait_quiet(rx: &Receiver<()>, cfg: Debounce) -> bool {
    let start = Instant::now();
    loop {
        let elapsed = start.elapsed();
        if elapsed >= cfg.max_wait {
            return false;
        }
        match rx.recv_timeout(cfg.quiet.min(cfg.max_wait - elapsed)) {
            Ok(()) => {}
            Err(RecvTimeoutError::Timeout) => return false,
            Err(RecvTimeoutError::Disconnected) => return true,
        }
    }
}

enum Drained {
    Some,
    None,
    Closed,
}

fn drain(rx: &Receiver<()>) -> Drained {
    let mut any = false;
    loop {
        match rx.try_recv() {
            Ok(()) => any = true,
            Err(TryRecvError::Empty) => return if any { Drained::Some } else { Drained::None },
            Err(TryRecvError::Disconnected) => {
                return if any { Drained::Some } else { Drained::Closed }
            }
        }
    }
}

/// A running watcher. Dropping it stops the watch and, once the debouncer
/// finished any pending pass, its thread.
pub struct Watch {
    _watcher: RecommendedWatcher,
    _thread: std::thread::JoinHandle<()>,
}

impl Watch {
    /// Stop watching and wait for the debouncer to finish (tests).
    #[cfg(test)]
    pub fn stop(self) {
        drop(self._watcher);
        self._thread.join().expect("debouncer thread");
    }
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

/// Watch `vault` recursively and call `fire` (debounced) on relevant
/// changes. `Err` when notify could not start; the caller keeps the 60s
/// rescan. `health` is updated with event and reindex counts.
pub fn spawn<F>(
    vault: &Path,
    cfg: Debounce,
    fire: F,
    health: Arc<Mutex<IpcWatchStatus>>,
) -> Result<Watch, String>
where
    F: FnMut() -> Result<bool, String> + Send + 'static,
{
    let (tx, rx) = mpsc::channel::<()>();
    let root: PathBuf = vault.to_path_buf();
    let events = Arc::clone(&health);
    let mut watcher = notify::recommended_watcher(move |res: notify::Result<Event>| {
        let wake = match res {
            Ok(event) => wants_reindex(&root, &event),
            Err(err) => {
                // Overflow or backend error: we may have missed something.
                eprintln!("vault-searchd: watch: {err}");
                true
            }
        };
        if wake {
            if let Ok(mut h) = events.lock() {
                h.events += 1;
            }
            let _ = tx.send(());
        }
    })
    .map_err(|e| format!("watch {}: {e}", vault.display()))?;
    watcher
        .watch(vault, RecursiveMode::Recursive)
        .map_err(|e| format!("watch {}: {e}", vault.display()))?;
    if let Ok(mut h) = health.lock() {
        h.enabled = true;
        h.ok = true;
        h.error = None;
        h.debounce_ms = cfg.quiet.as_millis() as u64;
    }
    let mut fire = fire;
    let thread = std::thread::Builder::new()
        .name("vault-watch".into())
        .spawn(move || {
            run_debouncer(rx, cfg, move || {
                let ran = fire();
                if let (Ok(true), Ok(mut h)) = (&ran, health.lock()) {
                    h.reindexes += 1;
                    h.last_reindex_unix = Some(now_unix());
                }
                ran
            })
        })
        .map_err(|e| format!("watch thread: {e}"))?;
    Ok(Watch {
        _watcher: watcher,
        _thread: thread,
    })
}

/// Health for a watcher that is off or failed to start.
pub fn off_status(error: Option<String>) -> IpcWatchStatus {
    IpcWatchStatus {
        enabled: error.is_some(),
        ok: false,
        error,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use notify::event::{AccessKind, CreateKind, DataChange, RenameMode};
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn fast() -> Debounce {
        Debounce {
            quiet: Duration::from_millis(50),
            max_wait: Duration::from_millis(400),
            busy_retry: Duration::from_millis(20),
        }
    }

    fn ev(kind: EventKind, paths: &[&Path]) -> Event {
        let mut e = Event::new(kind);
        for p in paths {
            e = e.add_path(p.to_path_buf());
        }
        e
    }

    #[test]
    fn filters_paths_like_collect_notes() {
        let dir = tempfile::tempdir().unwrap();
        let v = dir.path();
        std::fs::create_dir(v.join("Projects")).unwrap();
        std::fs::write(v.join("image.png"), b"x").unwrap();
        assert!(relevant_path(v, &v.join("a.md")));
        assert!(relevant_path(v, &v.join("Projects/b.markdown")));
        assert!(relevant_path(v, &v.join("Projects")), "existing folder");
        assert!(relevant_path(v, &v.join("Gone")), "removed folder");
        assert!(relevant_path(v, v), "vault root");
        assert!(!relevant_path(v, &v.join(".obsidian/workspace.md")));
        assert!(!relevant_path(v, &v.join("Projects/.trash/x.md")));
        assert!(!relevant_path(v, &v.join(".hidden.md")));
        assert!(!relevant_path(v, &v.join("image.png")));
        assert!(!relevant_path(v, &v.join("a.md.partial")));
        assert!(!relevant_path(v, &v.join("a.md~")));
        assert!(!relevant_path(v, Path::new("/elsewhere/a.md")));
    }

    #[test]
    fn filters_event_kinds() {
        let v = Path::new("/vault");
        let note = Path::new("/vault/n.md");
        let data = EventKind::Modify(ModifyKind::Data(DataChange::Content));
        assert!(wants_reindex(v, &ev(data, &[note])));
        assert!(wants_reindex(
            v,
            &ev(EventKind::Create(CreateKind::File), &[note])
        ));
        assert!(wants_reindex(
            v,
            &ev(EventKind::Remove(notify::event::RemoveKind::File), &[note])
        ));
        // Rename from a temp file into a note: one relevant path is enough.
        assert!(wants_reindex(
            v,
            &ev(
                EventKind::Modify(ModifyKind::Name(RenameMode::Both)),
                &[Path::new("/vault/.n.md.tmp"), note]
            )
        ));
        assert!(!wants_reindex(
            v,
            &ev(EventKind::Access(AccessKind::Any), &[note])
        ));
        assert!(!wants_reindex(
            v,
            &ev(data, &[Path::new("/vault/.obsidian/app.json")])
        ));
        let rescan = Event::new(EventKind::Other).set_flag(notify::event::Flag::Rescan);
        assert!(wants_reindex(v, &rescan));
    }

    #[test]
    fn burst_coalesces_into_one_pass() {
        let (tx, rx) = mpsc::channel();
        let n = Arc::new(AtomicUsize::new(0));
        let c = Arc::clone(&n);
        let t = std::thread::spawn(move || {
            run_debouncer(rx, fast(), move || {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(true)
            })
        });
        for _ in 0..20 {
            tx.send(()).unwrap();
            std::thread::sleep(Duration::from_millis(5));
        }
        std::thread::sleep(Duration::from_millis(300));
        assert_eq!(n.load(Ordering::SeqCst), 1);
        drop(tx);
        t.join().unwrap();
        assert_eq!(n.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn max_wait_caps_a_steady_stream() {
        let (tx, rx) = mpsc::channel();
        let n = Arc::new(AtomicUsize::new(0));
        let c = Arc::clone(&n);
        let t = std::thread::spawn(move || {
            run_debouncer(rx, fast(), move || {
                c.fetch_add(1, Ordering::SeqCst);
                Ok(true)
            })
        });
        // Events every 20ms (< quiet) for ~1s: max_wait (400ms) still fires.
        let start = Instant::now();
        while start.elapsed() < Duration::from_millis(1000) {
            tx.send(()).unwrap();
            std::thread::sleep(Duration::from_millis(20));
        }
        assert!(n.load(Ordering::SeqCst) >= 1, "starved");
        drop(tx);
        t.join().unwrap();
    }

    #[test]
    fn busy_retries_until_own_pass_runs() {
        let (tx, rx) = mpsc::channel();
        let calls = Arc::new(AtomicUsize::new(0));
        let c = Arc::clone(&calls);
        let t = std::thread::spawn(move || {
            run_debouncer(rx, fast(), move || {
                // First two attempts: another reindex holds the flag.
                Ok(c.fetch_add(1, Ordering::SeqCst) >= 2)
            })
        });
        tx.send(()).unwrap();
        std::thread::sleep(Duration::from_millis(400));
        assert_eq!(calls.load(Ordering::SeqCst), 3);
        drop(tx);
        t.join().unwrap();
    }

    #[test]
    fn events_during_a_pass_schedule_another() {
        let (tx, rx) = mpsc::channel();
        let calls = Arc::new(AtomicUsize::new(0));
        let c = Arc::clone(&calls);
        let mut inner = Some(tx.clone());
        let t = std::thread::spawn(move || {
            run_debouncer(rx, fast(), move || {
                c.fetch_add(1, Ordering::SeqCst);
                // A note is written while the first pass embeds.
                if let Some(inner) = inner.take() {
                    inner.send(()).unwrap();
                }
                Ok(true)
            })
        });
        tx.send(()).unwrap();
        std::thread::sleep(Duration::from_millis(400));
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        drop(tx);
        t.join().unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn failed_pass_is_not_retried_in_a_loop() {
        let (tx, rx) = mpsc::channel();
        let calls = Arc::new(AtomicUsize::new(0));
        let c = Arc::clone(&calls);
        let t = std::thread::spawn(move || {
            run_debouncer(rx, fast(), move || {
                c.fetch_add(1, Ordering::SeqCst);
                Err("embedder down".into())
            })
        });
        tx.send(()).unwrap();
        std::thread::sleep(Duration::from_millis(300));
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        tx.send(()).unwrap();
        std::thread::sleep(Duration::from_millis(300));
        assert_eq!(calls.load(Ordering::SeqCst), 2, "next event retries");
        drop(tx);
        t.join().unwrap();
    }

    /// Real notify on a temp vault: a note write fires within seconds;
    /// dot-dir and non-markdown writes do not.
    #[test]
    fn watcher_fires_on_note_write_only() {
        let dir = tempfile::tempdir().unwrap();
        let vault = dir.path().to_path_buf();
        std::fs::create_dir(vault.join(".obsidian")).unwrap();
        std::fs::create_dir(vault.join("Daily")).unwrap();
        let (fired_tx, fired_rx) = mpsc::channel::<()>();
        let health = Arc::new(Mutex::new(IpcWatchStatus::default()));
        let fired_tx = Mutex::new(fired_tx);
        let watch = spawn(
            &vault,
            Debounce {
                quiet: Duration::from_millis(200),
                max_wait: Duration::from_secs(2),
                busy_retry: Duration::from_millis(50),
            },
            move || {
                let _ = fired_tx.lock().unwrap().send(());
                Ok(true)
            },
            Arc::clone(&health),
        )
        .expect("notify should start on a local tmp dir");
        assert!(health.lock().unwrap().ok);

        std::fs::write(vault.join(".obsidian/workspace.json"), b"{}").unwrap();
        std::fs::write(vault.join(".obsidian/plugin.md"), b"# x").unwrap();
        std::fs::write(vault.join("pic.png"), b"png").unwrap();
        assert!(
            fired_rx.recv_timeout(Duration::from_millis(1200)).is_err(),
            "ignored paths must not trigger a reindex"
        );

        // New note in a folder that existed before the watch started.
        std::fs::write(vault.join("Daily/2026-10-30.md"), b"# today").unwrap();
        fired_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("note write should trigger a reindex within 5s");

        // New folder created after the watch: recursive watch follows it.
        std::fs::create_dir(vault.join("Later")).unwrap();
        while fired_rx.recv_timeout(Duration::from_millis(600)).is_ok() {}
        std::fs::write(vault.join("Later/n.md"), b"# n").unwrap();
        fired_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("note in a new folder should trigger a reindex");

        let h = health.lock().unwrap().clone();
        assert!(h.reindexes >= 2, "{h:?}");
        assert!(h.events >= 2, "{h:?}");
        assert!(h.last_reindex_unix.is_some());
        watch.stop();
    }

    #[test]
    fn missing_vault_is_an_error_not_a_panic() {
        let dir = tempfile::tempdir().unwrap();
        let gone = dir.path().join("nope");
        let r = spawn(
            &gone,
            fast(),
            || Ok(true),
            Arc::new(Mutex::new(IpcWatchStatus::default())),
        );
        assert!(r.is_err());
        let s = off_status(r.err());
        assert!(s.enabled && !s.ok && s.error.is_some());
    }
}
