//! Optional vault sync owned by `vault-searchd serve --sync-interval N`.
//!
//! The daemon does not reimplement Drive sync. Each cycle calls
//! [`vault_rclone::sync::sync`] (the same guarded incremental bisync as the
//! `vault-rclone sync` CLI: `MIN_FILES` empty-side refuse, `--max-delete`,
//! newer-wins with conflict suffix, vault-only remote, run log) and hands it
//! an in-process [`Reindexer`]. vault-rclone calls that reindexer only after
//! bisync exited 0 and only if `VAULT_REINDEX` is on, so the order stays
//! "sync ok, then reindex", and a failed reindex is reported but never turns
//! a good sync into a failed one. No socket hop to ourselves.
//!
//! A failed cycle (guard refusal, rclone non-zero, spawn error) is logged and
//! recorded in [`IpcSyncStatus`]; the loop keeps running and serve keeps
//! answering queries.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use vault_rclone::commands::{Host, EXIT_FAIL};
use vault_rclone::reindex::{ReindexError, ReindexReply};
use vault_rclone::{CommandRunner, Config, Reindexer};
use vault_search::IpcSyncStatus;

/// Runs one incremental reindex in this process and reports the counts.
/// `indexing: Some(true)` means another reindex was already running.
pub type ReindexFn = dyn Fn() -> Result<ReindexReply, String> + Send + Sync;

/// Everything one sync cycle needs.
pub struct SyncJob {
    pub cfg: Config,
    pub host: Host,
    pub interval: Duration,
}

/// Status before the first run. `enabled` is false when `job` is `None`.
pub fn initial_status(job: Option<&SyncJob>) -> IpcSyncStatus {
    match job {
        None => IpcSyncStatus::default(),
        Some(job) => IpcSyncStatus {
            enabled: true,
            interval_secs: job.interval.as_secs(),
            remote: Some(job.cfg.vault_remote.clone()),
            local: job
                .cfg
                .vault_local
                .as_ref()
                .map(|p| p.display().to_string()),
            ..Default::default()
        },
    }
}

/// Point the sync at serve's `--vault` so there is only one vault root.
/// Returns a warning when `VAULT_LOCAL` named a different folder.
pub fn pin_local(cfg: &mut Config, vault: &Path) -> Option<String> {
    let same = |a: &Path, b: &Path| {
        let canon = |p: &Path| std::fs::canonicalize(p).unwrap_or_else(|_| p.to_path_buf());
        canon(a) == canon(b)
    };
    let warning = match &cfg.vault_local {
        Some(p) if !same(p, vault) => Some(format!(
            "VAULT_LOCAL={} differs from --vault {}; syncing --vault",
            p.display(),
            vault.display()
        )),
        _ => None,
    };
    cfg.vault_local = Some(vault.to_path_buf());
    warning
}

/// [`Reindexer`] that calls the daemon's own reindex instead of its socket.
struct InProcess<'a>(&'a ReindexFn);

impl Reindexer for InProcess<'_> {
    fn reindex(&self, _socket: &Path, _t: Duration) -> Result<ReindexReply, ReindexError> {
        (self.0)().map_err(ReindexError::Failed)
    }
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

/// The useful line of a failed run, already credential-safe in vault-rclone.
fn failure_line(lines: &[&str]) -> String {
    let strip = |l: &str| l.strip_prefix("sync: ").unwrap_or(l).to_string();
    lines
        .iter()
        .rev()
        .find(|l| l.contains("FAILED"))
        .or_else(|| {
            lines
                .iter()
                .rev()
                .find(|l| !l.starts_with("sync: log ") && !l.trim().is_empty())
        })
        .map_or_else(|| "sync failed".to_string(), |l| strip(l))
}

/// One guarded bisync, then (on success) the in-process reindex. Returns
/// the exit code vault-rclone would have exited with. Never panics on a
/// failed sync; the caller just waits for the next cycle.
pub fn run_once(
    job: &SyncJob,
    runner: &dyn CommandRunner,
    reindex: &ReindexFn,
    health: &Mutex<IpcSyncStatus>,
    log: &mut dyn FnMut(&str),
) -> i32 {
    if let Ok(mut h) = health.lock() {
        h.running = true;
    }
    let mut buf = Vec::new();
    let code = vault_rclone::sync::sync(
        &job.cfg,
        &job.host,
        runner,
        &InProcess(reindex),
        false,
        &mut buf,
    )
    .unwrap_or_else(|e| {
        buf.extend_from_slice(format!("sync: FAILED (output: {e})\n").as_bytes());
        EXIT_FAIL
    });
    let text = String::from_utf8_lossy(&buf);
    let lines: Vec<&str> = text.lines().collect();
    for line in &lines {
        log(line);
    }
    let now = now_unix();
    if let Ok(mut h) = health.lock() {
        h.running = false;
        h.runs += 1;
        h.last_run_unix = Some(now);
        h.last_exit = Some(code);
        if code == 0 {
            h.last_ok_unix = Some(now);
            h.last_error = None;
            h.last_reindex = lines
                .iter()
                .find_map(|l| l.strip_prefix("sync: reindex: "))
                .map(|m| format!("reindex: {m}"));
        } else {
            h.failures += 1;
            h.last_error = Some(failure_line(&lines));
        }
    }
    code
}

/// Start the interval loop on its own thread, or `None` when the interval
/// is zero (sync off). The first cycle runs at once, then one cycle per
/// interval after the previous one finished. `stop` ends the loop after the
/// current cycle (tests; serve never sets it).
pub fn spawn_loop<R>(
    job: SyncJob,
    runner: R,
    reindex: Arc<ReindexFn>,
    health: Arc<Mutex<IpcSyncStatus>>,
    stop: Arc<AtomicBool>,
) -> Option<std::thread::JoinHandle<()>>
where
    R: CommandRunner + Send + 'static,
{
    if job.interval.is_zero() {
        return None;
    }
    Some(std::thread::spawn(move || {
        while !stop.load(Ordering::Relaxed) {
            run_once(&job, &runner, reindex.as_ref(), &health, &mut |l| {
                eprintln!("vault-searchd: {l}");
            });
            // Sleep in slices so `stop` is honoured promptly.
            let mut left = job.interval;
            while !left.is_zero() && !stop.load(Ordering::Relaxed) {
                let step = left.min(Duration::from_millis(200));
                std::thread::sleep(step);
                left -= step;
            }
        }
    }))
}

/// `vault-searchd serve` flags → a sync job, or `None` for sync off.
pub fn job_from_flags(
    interval_secs: u64,
    no_sync: bool,
    config_file: Option<&Path>,
    vault: &Path,
) -> Result<Option<SyncJob>, String> {
    if interval_secs == 0 || no_sync {
        return Ok(None);
    }
    let mut cfg = Config::load(config_file).map_err(|e| format!("sync config: {e}"))?;
    if let Some(w) = pin_local(&mut cfg, vault) {
        eprintln!("vault-searchd: sync: {w}");
    }
    Ok(Some(SyncJob {
        cfg,
        host: Host::from_process(),
        interval: Duration::from_secs(interval_secs),
    }))
}

/// For log lines and `status`: where the config came from.
pub fn describe(job: &SyncJob) -> String {
    let files: Vec<PathBuf> = job.cfg.files_read.clone();
    let files = if files.is_empty() {
        "env only".to_string()
    } else {
        files
            .iter()
            .map(|p| p.display().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    };
    format!(
        "sync every {}s: {} <-> {} (config: {files})",
        job.interval.as_secs(),
        job.cfg.vault_remote,
        job.cfg
            .vault_local
            .as_ref()
            .map_or("?".to_string(), |p| p.display().to_string()),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::AtomicUsize;

    use vault_rclone::{Error as RcloneError, RunOutput};

    /// Fake rclone: `size` reports `remote` files, `bisync` exits `code`.
    /// Counts calls through an `Arc` so the loop test can see them.
    #[derive(Clone)]
    struct Fake {
        remote: u64,
        code: i32,
        calls: Arc<Mutex<Vec<Vec<String>>>>,
    }

    impl Fake {
        fn new(remote: u64, code: i32) -> Self {
            Fake {
                remote,
                code,
                calls: Arc::new(Mutex::new(Vec::new())),
            }
        }
        fn bisyncs(&self) -> usize {
            let calls = self.calls.lock().unwrap();
            calls.iter().filter(|c| c[0] == "bisync").count()
        }
    }

    impl CommandRunner for Fake {
        fn run(&self, _p: &Path, args: &[String]) -> Result<RunOutput, RcloneError> {
            let i = args
                .iter()
                .position(|a| a == "size" || a == "bisync")
                .expect("only size and bisync");
            let args = args[i..].to_vec();
            let first = args[0].clone();
            self.calls.lock().unwrap().push(args);
            Ok(match first.as_str() {
                "size" => RunOutput {
                    code: Some(0),
                    stdout: format!(r#"{{"count":{},"bytes":1}}"#, self.remote),
                    stderr: String::new(),
                },
                _ if self.code == 0 => RunOutput {
                    code: Some(0),
                    stdout: String::new(),
                    stderr: "INFO  : Bisync successful\n".into(),
                },
                _ => RunOutput {
                    code: Some(self.code),
                    stdout: String::new(),
                    stderr: "ERROR : Bisync aborted. Must run --resync to recover.\n".into(),
                },
            })
        }
    }

    fn job(files: usize, interval: Duration) -> (tempfile::TempDir, SyncJob) {
        let dir = tempfile::tempdir().unwrap();
        let vault = dir.path().join("Vault");
        std::fs::create_dir_all(&vault).unwrap();
        for i in 0..files {
            std::fs::write(vault.join(format!("{i}.md")), "x").unwrap();
        }
        let host = Host {
            home: Some(dir.path().to_path_buf()),
            path_var: None,
            cache_dir: Some(dir.path().join("cache")),
            state_dir: Some(dir.path().join("state")),
            is_exec: |_| true,
            runtime_dir: None,
            uid: 0,
        };
        let mut cfg = Config::load_from(|_| None, &[], None).unwrap();
        cfg.rclone_bin = Some(PathBuf::from("/fake/rclone"));
        pin_local(&mut cfg, &vault);
        (
            dir,
            SyncJob {
                cfg,
                host,
                interval,
            },
        )
    }

    fn counting_reindex(
        n: Arc<AtomicUsize>,
        result: Result<(), String>,
    ) -> impl Fn() -> Result<ReindexReply, String> + Send + Sync {
        move || {
            n.fetch_add(1, Ordering::SeqCst);
            result.clone().map(|_| ReindexReply {
                ok: true,
                notes: Some(4),
                chunks: Some(11),
                ..Default::default()
            })
        }
    }

    fn quiet(_: &str) {}

    #[test]
    fn interval_zero_starts_no_loop_and_calls_nothing() {
        let (_d, job) = job(5, Duration::ZERO);
        let fake = Fake::new(100, 0);
        let n = Arc::new(AtomicUsize::new(0));
        let handle = spawn_loop(
            job,
            fake.clone(),
            Arc::new(counting_reindex(n.clone(), Ok(()))),
            Arc::new(Mutex::new(IpcSyncStatus::default())),
            Arc::new(AtomicBool::new(false)),
        );
        assert!(handle.is_none());
        assert!(fake.calls.lock().unwrap().is_empty());
        assert_eq!(n.load(Ordering::SeqCst), 0);
        assert!(job_from_flags(0, false, None, Path::new("/nope"))
            .unwrap()
            .is_none());
        assert!(job_from_flags(120, true, None, Path::new("/nope"))
            .unwrap()
            .is_none());
    }

    #[test]
    fn good_sync_then_reindex_records_health() {
        let (_d, job) = job(5, Duration::from_secs(120));
        let fake = Fake::new(100, 0);
        let n = Arc::new(AtomicUsize::new(0));
        let health = Mutex::new(initial_status(Some(&job)));
        let reindex = counting_reindex(n.clone(), Ok(()));
        let mut lines = Vec::new();
        let code = run_once(&job, &fake, &reindex, &health, &mut |l| {
            lines.push(l.to_string())
        });
        assert_eq!(code, 0, "{lines:?}");
        assert_eq!(fake.bisyncs(), 1);
        assert_eq!(n.load(Ordering::SeqCst), 1, "reindex after sync");
        let h = health.lock().unwrap();
        assert!(h.enabled);
        assert_eq!(h.interval_secs, 120);
        assert_eq!((h.runs, h.failures, h.last_exit), (1, 0, Some(0)));
        assert!(h.last_ok_unix.is_some());
        assert!(!h.running);
        assert_eq!(
            h.last_reindex.as_deref(),
            Some("reindex: ok (notes=4 chunks=11)")
        );
        // Guards still ran: size before bisync, --max-delete on bisync.
        let calls = fake.calls.lock().unwrap();
        assert_eq!(calls[0][0], "size");
        assert!(calls[1].windows(2).any(|w| w == ["--max-delete", "50"]));
        assert!(!calls[1].contains(&"--resync".to_string()));
    }

    #[test]
    fn failed_sync_skips_reindex_and_keeps_going() {
        let (_d, job) = job(5, Duration::from_secs(60));
        let fake = Fake::new(100, 7);
        let n = Arc::new(AtomicUsize::new(0));
        let health = Mutex::new(initial_status(Some(&job)));
        let reindex = counting_reindex(n.clone(), Ok(()));
        for _ in 0..2 {
            assert_eq!(run_once(&job, &fake, &reindex, &health, &mut quiet), 7);
        }
        assert_eq!(n.load(Ordering::SeqCst), 0, "no reindex after failed sync");
        let h = health.lock().unwrap();
        assert_eq!((h.runs, h.failures, h.last_exit), (2, 2, Some(7)));
        assert!(h.last_ok_unix.is_none());
        let err = h.last_error.as_deref().unwrap();
        assert!(err.contains("FAILED (rclone exit 7)"), "{err}");
    }

    #[test]
    fn guard_refusal_is_a_failed_cycle_not_a_crash() {
        let (_d, job) = job(5, Duration::from_secs(60));
        let fake = Fake::new(0, 0); // empty remote
        let n = Arc::new(AtomicUsize::new(0));
        let health = Mutex::new(initial_status(Some(&job)));
        let code = run_once(
            &job,
            &fake,
            &counting_reindex(n.clone(), Ok(())),
            &health,
            &mut quiet,
        );
        assert_eq!(code, 1);
        assert_eq!(fake.bisyncs(), 0);
        assert_eq!(n.load(Ordering::SeqCst), 0);
        let h = health.lock().unwrap();
        assert!(
            h.last_error.as_deref().unwrap().contains("below MIN_FILES"),
            "{:?}",
            h.last_error
        );
    }

    #[test]
    fn failed_reindex_after_good_sync_is_not_fatal() {
        let (_d, job) = job(5, Duration::from_secs(60));
        let fake = Fake::new(100, 0);
        let n = Arc::new(AtomicUsize::new(0));
        let health = Mutex::new(initial_status(Some(&job)));
        let reindex = counting_reindex(n.clone(), Err("embedder down".into()));
        let code = run_once(&job, &fake, &reindex, &health, &mut quiet);
        assert_eq!(code, 0, "sync stays successful");
        assert_eq!(n.load(Ordering::SeqCst), 1);
        let h = health.lock().unwrap();
        assert_eq!((h.failures, h.last_error.clone()), (0, None));
        assert_eq!(
            h.last_reindex.as_deref(),
            Some("reindex: FAILED (embedder down)")
        );
    }

    #[test]
    fn loop_runs_repeatedly_on_its_own_thread_and_stops() {
        let (_d, job) = job(5, Duration::from_millis(20));
        let fake = Fake::new(100, 1);
        let n = Arc::new(AtomicUsize::new(0));
        let health = Arc::new(Mutex::new(initial_status(Some(&job))));
        let stop = Arc::new(AtomicBool::new(false));
        let handle = spawn_loop(
            job,
            fake.clone(),
            Arc::new(counting_reindex(n.clone(), Ok(()))),
            health.clone(),
            stop.clone(),
        )
        .expect("loop starts");
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while fake.bisyncs() < 2 && std::time::Instant::now() < deadline {
            std::thread::sleep(Duration::from_millis(10));
        }
        stop.store(true, Ordering::Relaxed);
        handle.join().unwrap();
        assert!(fake.bisyncs() >= 2, "failures do not stop the loop");
        assert!(health.lock().unwrap().failures >= 2);
        assert_eq!(n.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn pin_local_uses_serve_vault_and_warns_on_mismatch() {
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("A");
        let b = dir.path().join("B");
        std::fs::create_dir_all(&a).unwrap();
        std::fs::create_dir_all(&b).unwrap();
        let mut cfg = Config::load_from(|_| None, &[], None).unwrap();
        assert!(pin_local(&mut cfg, &a).is_none());
        assert_eq!(cfg.vault_local.as_deref(), Some(a.as_path()));
        assert!(pin_local(&mut cfg, &a).is_none());
        let w = pin_local(&mut cfg, &b).unwrap();
        assert!(w.contains("differs from --vault"), "{w}");
        assert_eq!(cfg.vault_local.as_deref(), Some(b.as_path()));
    }
}
