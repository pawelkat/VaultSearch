//! `lsf`, `sync`, and `resync`: the guarded bisync between the vault remote
//! and `VAULT_LOCAL`.
//!
//! Guards run before any bisync call:
//! 1. `VAULT_LOCAL` is set and is a directory.
//! 2. Local and remote each hold at least `MIN_FILES` files (remote counted
//!    with `rclone size --json --fast-list`). An empty or nearly empty side
//!    usually means a broken mount, a wrong path, or an auth problem, and
//!    syncing against it (above all with `--resync`) can wipe the other side.
//! 3. `--max-delete` is always passed.
//!
//! After bisync exits 0 (and not on `--dry-run`), a running `vault-searchd`
//! is asked for an incremental reindex over its Unix socket. A missing
//! daemon, a timeout, or a failed reindex is reported but never changes the
//! exit code: the files are already synced.
//!
//! Each run appends start, guard results, a short summary, and the exit code
//! to `<state>/vault-rclone/sync.log`. No tokens, no rclone.conf contents,
//! and no per-file lines go into that log.

use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::commands::{count_local_files, Host, EXIT_FAIL, EXIT_OK};
use crate::config::Config;
use crate::error::Error;
use crate::rclone::{self, BisyncOpts, CommandRunner, RunOutput};
use crate::reindex::{self, Reindexer};

/// `<state_dir>/vault-rclone/sync.log`
pub fn log_path(host: &Host) -> Option<PathBuf> {
    host.state_dir
        .as_ref()
        .map(|d| d.join("vault-rclone").join("sync.log"))
}

/// Append-only run log. Write errors are reported once and then ignored, so
/// a read-only state dir never blocks a sync.
struct RunLog {
    path: Option<PathBuf>,
    run: &'static str,
    failed: bool,
}

impl RunLog {
    fn new(host: &Host, run: &'static str) -> Self {
        RunLog {
            path: log_path(host),
            run,
            failed: false,
        }
    }

    fn line(&mut self, msg: &str, out: &mut dyn Write) -> io::Result<()> {
        let Some(path) = self.path.clone() else {
            return Ok(());
        };
        if self.failed {
            return Ok(());
        }
        let res = (|| -> io::Result<()> {
            if let Some(dir) = path.parent() {
                std::fs::create_dir_all(dir)?;
            }
            let mut f = std::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)?;
            writeln!(f, "{} {} {msg}", utc_now(), self.run)
        })();
        if let Err(e) = res {
            self.failed = true;
            writeln!(out, "warning: cannot write {}: {e}", path.display())?;
        }
        Ok(())
    }
}

/// `YYYY-MM-DDTHH:MM:SSZ` without pulling in a date crate.
fn utc_now() -> String {
    let secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    format_utc(secs)
}

/// Unix seconds to `YYYY-MM-DDTHH:MM:SSZ` (proleptic Gregorian, UTC).
pub fn format_utc(secs: u64) -> String {
    let days = (secs / 86_400) as i64;
    let rem = secs % 86_400;
    // Howard Hinnant's civil_from_days.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = yoe + era * 400 + i64::from(m <= 2);
    format!(
        "{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}Z",
        rem / 3600,
        rem % 3600 / 60,
        rem % 60
    )
}

/// `FILTERS_FILE` if set (must exist), else the first existing of
/// `~/.config/vault-rclone/filters.txt` and `~/.config/vault-sync/filters.txt`.
pub fn resolve_filters(cfg: &Config, host: &Host) -> Result<Option<PathBuf>, Error> {
    if let Some(f) = &cfg.filters_file {
        return if f.is_file() {
            Ok(Some(f.clone()))
        } else {
            Err(Error::MissingFilters(f.clone()))
        };
    }
    let Some(home) = host.home.as_deref() else {
        return Ok(None);
    };
    let base = home.join(".config");
    Ok(["vault-rclone", "vault-sync"]
        .iter()
        .map(|d| base.join(d).join("filters.txt"))
        .find(|p| p.is_file()))
}

/// Exit code for the process from an rclone result: rclone's own code
/// (bisync: 1 retryable, 2 usage, 7 critical / needs `--resync`), or 1 if
/// it was killed by a signal.
pub fn exit_code(o: &RunOutput) -> i32 {
    match o.code {
        Some(c) => c,
        None => EXIT_FAIL,
    }
}

/// The most useful single line of a failed rclone run, credential-safe.
fn error_line(stderr: &str) -> String {
    let pick = stderr
        .lines()
        .rev()
        .find(|l| l.contains("ERROR") || l.contains("CRITICAL") || l.contains("Failed"))
        .or_else(|| stderr.lines().rev().find(|l| !l.trim().is_empty()))
        .unwrap_or("");
    rclone::safe_summary(pick)
}

/// `vault-rclone lsf [--local]`: top-level listing of the remote (or the
/// local folder) through rclone. Prints rclone's stdout as-is.
pub fn lsf(
    cfg: &Config,
    host: &Host,
    runner: &dyn CommandRunner,
    local: bool,
    out: &mut dyn Write,
) -> io::Result<i32> {
    let target = if local {
        match cfg.require_local() {
            Ok(p) => Some(p),
            Err(e) => {
                writeln!(out, "lsf: {e}")?;
                return Ok(EXIT_FAIL);
            }
        }
    } else {
        None
    };
    let bin = match host.locate(cfg) {
        Ok(b) => b,
        Err(e) => {
            writeln!(out, "lsf: {e}")?;
            return Ok(EXIT_FAIL);
        }
    };
    match runner.run(&bin, &rclone::lsf_args(cfg, target)) {
        Ok(o) if o.success() => {
            out.write_all(o.stdout.as_bytes())?;
            Ok(EXIT_OK)
        }
        Ok(o) => {
            writeln!(out, "lsf: FAILED: {}", error_line(&o.stderr))?;
            Ok(exit_code(&o).max(EXIT_FAIL))
        }
        Err(e) => {
            writeln!(out, "lsf: FAILED ({e})")?;
            Ok(EXIT_FAIL)
        }
    }
}

/// `vault-rclone sync`: incremental bisync after the guards.
pub fn sync(
    cfg: &Config,
    host: &Host,
    runner: &dyn CommandRunner,
    reindexer: &dyn Reindexer,
    dry_run: bool,
    out: &mut dyn Write,
) -> io::Result<i32> {
    run_bisync(cfg, host, runner, reindexer, false, dry_run, out)
}

/// `vault-rclone resync`: `bisync --resync --resync-mode newer` after the
/// same guards. Use for the first run, after a critical error (exit 7), or
/// after changing the filters file.
pub fn resync(
    cfg: &Config,
    host: &Host,
    runner: &dyn CommandRunner,
    reindexer: &dyn Reindexer,
    dry_run: bool,
    out: &mut dyn Write,
) -> io::Result<i32> {
    run_bisync(cfg, host, runner, reindexer, true, dry_run, out)
}

fn run_bisync(
    cfg: &Config,
    host: &Host,
    runner: &dyn CommandRunner,
    reindexer: &dyn Reindexer,
    resync: bool,
    dry_run: bool,
    out: &mut dyn Write,
) -> io::Result<i32> {
    let name = if resync { "resync" } else { "sync" };
    let mut log = RunLog::new(host, name);
    let fail = |log: &mut RunLog, out: &mut dyn Write, msg: String| -> io::Result<i32> {
        writeln!(out, "{name}: {msg}")?;
        log.line(&format!("refused: {msg}"), out)?;
        log.line(&format!("end exit={EXIT_FAIL}"), out)?;
        Ok(EXIT_FAIL)
    };

    let local: &Path = match cfg.require_local() {
        Ok(p) => p,
        Err(e) => return fail(&mut log, out, e.to_string()),
    };
    log.line(
        &format!(
            "start remote={} local={}{}",
            cfg.vault_remote,
            local.display(),
            if dry_run { " dry-run" } else { "" }
        ),
        out,
    )?;
    if !local.is_dir() {
        return fail(
            &mut log,
            out,
            format!("{} is missing or not a directory", local.display()),
        );
    }
    let bin = match host.locate(cfg) {
        Ok(b) => b,
        Err(e) => return fail(&mut log, out, e.to_string()),
    };
    let filters = match resolve_filters(cfg, host) {
        Ok(f) => f,
        Err(e) => return fail(&mut log, out, e.to_string()),
    };

    // Guard: empty-side refuse, local first (no network needed).
    let min = u64::from(cfg.min_files);
    let local_n = match count_local_files(local) {
        Ok(n) => n,
        Err(e) => return fail(&mut log, out, format!("cannot count local files: {e}")),
    };
    if local_n < min {
        return fail(
            &mut log,
            out,
            format!(
                "local {} has {local_n} files, below MIN_FILES={min}; refusing so an empty side cannot wipe the other",
                local.display()
            ),
        );
    }
    let remote_n = match runner.run(&bin, &rclone::size_args(cfg)) {
        Ok(o) if o.success() => match rclone::parse_size_count(&o.stdout) {
            Some(n) => n,
            None => return fail(&mut log, out, "remote file count unreadable".into()),
        },
        Ok(o) => {
            return fail(
                &mut log,
                out,
                format!("cannot count remote files: {}", error_line(&o.stderr)),
            )
        }
        Err(e) => return fail(&mut log, out, format!("cannot count remote files: {e}")),
    };
    if remote_n < min {
        return fail(
            &mut log,
            out,
            format!(
                "remote {} has {remote_n} files, below MIN_FILES={min}; refusing so an empty side cannot wipe the other",
                cfg.vault_remote
            ),
        );
    }
    let guards = format!(
        "guards ok: local {local_n} files, remote {remote_n} files, min-files {min}, max-delete {}",
        cfg.max_delete
    );
    writeln!(out, "{name}: {guards}")?;
    log.line(&guards, out)?;
    if let Some(f) = &filters {
        writeln!(out, "{name}: filters {}", f.display())?;
        log.line(&format!("filters {}", f.display()), out)?;
    }

    let opts = BisyncOpts {
        resync,
        dry_run,
        filters_file: filters.as_deref(),
    };
    let args = rclone::bisync_args(cfg, local, &opts);
    let result = runner.run(&bin, &args);
    let code = match result {
        Ok(o) => {
            for s in rclone::bisync_summary(&o.stderr) {
                writeln!(out, "{name}: {s}")?;
                log.line(&s, out)?;
            }
            let code = exit_code(&o);
            if code != EXIT_OK {
                let err = error_line(&o.stderr);
                writeln!(out, "{name}: FAILED (rclone exit {code}): {err}")?;
                log.line(&format!("rclone error: {err}"), out)?;
                if code == 7 {
                    writeln!(
                        out,
                        "{name}: rclone needs a resync; check the vault first, then `vault-rclone resync --dry-run`"
                    )?;
                }
            }
            code
        }
        Err(e) => {
            writeln!(out, "{name}: FAILED ({e})")?;
            log.line(&format!("spawn error: {e}"), out)?;
            EXIT_FAIL
        }
    };
    if code == EXIT_OK && !dry_run {
        // Sync is done and stays done; the reindex result is informational.
        let msg = reindex::request(cfg, host, reindexer).message();
        writeln!(out, "{name}: {msg}")?;
        log.line(&msg, out)?;
    }
    log.line(&format!("end exit={code}"), out)?;
    if let Some(p) = log_path(host) {
        writeln!(out, "{name}: log {}", p.display())?;
    }
    Ok(code)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::RefCell;

    use crate::reindex::{ReindexError, ReindexReply};
    use std::time::Duration;

    const SECRET: &str = "FAKE-SUPER-SECRET-ACCESS";

    /// No daemon running: every reindex request is "not listening".
    struct NoDaemon;
    impl Reindexer for NoDaemon {
        fn reindex(&self, _s: &Path, _t: Duration) -> Result<ReindexReply, ReindexError> {
            Err(ReindexError::NotListening(
                "No such file or directory".into(),
            ))
        }
    }

    /// Fake daemon: records (socket, timeout) and answers with `reply`.
    struct FakeDaemon {
        calls: RefCell<Vec<(PathBuf, Duration)>>,
        reply: Result<ReindexReply, ReindexError>,
    }
    impl FakeDaemon {
        fn answering(reply: Result<ReindexReply, ReindexError>) -> Self {
            FakeDaemon {
                calls: RefCell::new(Vec::new()),
                reply,
            }
        }
        fn ok() -> Self {
            Self::answering(Ok(ReindexReply {
                ok: true,
                notes: Some(412),
                chunks: Some(3001),
                indexing: Some(false),
                ..Default::default()
            }))
        }
        fn count(&self) -> usize {
            self.calls.borrow().len()
        }
    }
    impl Reindexer for FakeDaemon {
        fn reindex(&self, s: &Path, t: Duration) -> Result<ReindexReply, ReindexError> {
            self.calls.borrow_mut().push((s.to_path_buf(), t));
            self.reply.clone()
        }
    }

    /// Fake rclone for bisync tests. Records argv; answers by subcommand.
    struct Fake {
        calls: RefCell<Vec<Vec<String>>>,
        remote_count: u64,
        bisync_code: i32,
        bisync_stderr: String,
        lsf_ok: bool,
    }

    impl Fake {
        fn new() -> Self {
            Fake {
                calls: RefCell::new(Vec::new()),
                remote_count: 120,
                bisync_code: 0,
                bisync_stderr: "INFO  : Path1:    0 changes:    0 new,    0 newer,    0 older,    0 deleted\nINFO  : No changes found\nINFO  : Bisync successful\n".into(),
                lsf_ok: true,
            }
        }
        fn bisync_calls(&self) -> Vec<Vec<String>> {
            self.calls
                .borrow()
                .iter()
                .filter(|c| c.first().map(String::as_str) == Some("bisync"))
                .cloned()
                .collect()
        }
    }

    impl CommandRunner for Fake {
        fn run(&self, _p: &Path, args: &[String]) -> Result<RunOutput, Error> {
            self.calls.borrow_mut().push(args.to_vec());
            let ok = |s: String| RunOutput {
                code: Some(0),
                stdout: s,
                stderr: String::new(),
            };
            Ok(match args[0].as_str() {
                "size" => ok(format!("{{\"count\":{},\"bytes\":1}}", self.remote_count)),
                "bisync" => RunOutput {
                    code: Some(self.bisync_code),
                    stdout: String::new(),
                    stderr: self.bisync_stderr.clone(),
                },
                "lsf" if self.lsf_ok => ok("Daily/\nInbox.md\n".into()),
                "lsf" => RunOutput {
                    code: Some(3),
                    stdout: String::new(),
                    stderr: format!(
                        "ERROR : couldn't fetch token {{\"access_token\":\"{SECRET}\"}}\n"
                    ),
                },
                other => panic!("unexpected rclone call {other:?}"),
            })
        }
    }

    fn setup(files: usize) -> (tempfile::TempDir, Host, Config) {
        let dir = tempfile::tempdir().unwrap();
        let vault = dir.path().join("Vault");
        std::fs::create_dir_all(vault.join("Daily")).unwrap();
        for i in 0..files {
            std::fs::write(vault.join("Daily").join(format!("{i}.md")), "x").unwrap();
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
        cfg.vault_local = Some(vault);
        cfg.rclone_bin = Some(PathBuf::from("/fake/rclone"));
        (dir, host, cfg)
    }

    fn run(f: impl FnOnce(&mut Vec<u8>) -> io::Result<i32>) -> (i32, String) {
        let mut buf = Vec::new();
        let code = f(&mut buf).unwrap();
        (code, String::from_utf8(buf).unwrap())
    }

    fn read_log(host: &Host) -> String {
        std::fs::read_to_string(log_path(host).unwrap()).unwrap_or_default()
    }

    fn assert_no_secrets(s: &str) {
        for needle in [SECRET, "access_token", "refresh_token"] {
            assert!(!s.contains(needle), "leaked {needle:?} in:\n{s}");
        }
    }

    #[test]
    fn sync_happy_path_runs_bisync_without_resync() {
        let (_d, host, cfg) = setup(3);
        let fake = Fake::new();
        let (code, s) = run(|o| sync(&cfg, &host, &fake, &NoDaemon, false, o));
        assert_eq!(code, EXIT_OK, "{s}");
        assert!(
            s.contains("guards ok: local 3 files, remote 120 files"),
            "{s}"
        );
        assert!(s.contains("Bisync successful"), "{s}");
        let b = fake.bisync_calls();
        assert_eq!(b.len(), 1);
        let argv = &b[0];
        assert_eq!(argv[1], "gdrive:Vault");
        assert_eq!(
            argv[2],
            cfg.vault_local.as_ref().unwrap().display().to_string()
        );
        assert!(argv.windows(2).any(|w| w == ["--max-delete", "50"]));
        assert!(argv
            .windows(2)
            .any(|w| w == ["--conflict-resolve", "newer"]));
        assert!(!argv.contains(&"--resync".to_string()));
        assert!(!argv.contains(&"--dry-run".to_string()));
        // Size ran before bisync.
        let calls = fake.calls.borrow();
        assert_eq!(calls[0][0], "size");
        assert_eq!(calls[1][0], "bisync");
    }

    #[test]
    fn resync_happy_path_includes_resync() {
        let (_d, host, cfg) = setup(5);
        let fake = Fake::new();
        let (code, s) = run(|o| resync(&cfg, &host, &fake, &NoDaemon, true, o));
        assert_eq!(code, EXIT_OK, "{s}");
        let b = fake.bisync_calls();
        assert_eq!(b.len(), 1);
        assert!(b[0].contains(&"--resync".to_string()));
        assert!(b[0].windows(2).any(|w| w == ["--resync-mode", "newer"]));
        assert!(b[0].contains(&"--dry-run".to_string()));
        assert!(b[0].windows(2).any(|w| w == ["--max-delete", "50"]));
        assert!(read_log(&host).contains("resync start remote=gdrive:Vault"));
        assert!(read_log(&host).contains(" dry-run"));
    }

    #[test]
    fn empty_local_aborts_before_any_rclone_call() {
        let (_d, host, cfg) = setup(2);
        let fake = Fake::new();
        for f in [sync, resync] {
            let (code, s) = run(|o| f(&cfg, &host, &fake, &NoDaemon, false, o));
            assert_eq!(code, EXIT_FAIL);
            assert!(s.contains("has 2 files, below MIN_FILES=3"), "{s}");
        }
        assert!(fake.calls.borrow().is_empty());
        let log = read_log(&host);
        assert!(log.contains("refused: local"), "{log}");
        assert!(log.contains("end exit=1"), "{log}");
    }

    #[test]
    fn empty_remote_aborts_before_bisync() {
        let (_d, host, cfg) = setup(10);
        let mut fake = Fake::new();
        fake.remote_count = 0;
        for f in [sync, resync] {
            let (code, s) = run(|o| f(&cfg, &host, &fake, &NoDaemon, false, o));
            assert_eq!(code, EXIT_FAIL);
            assert!(s.contains("remote gdrive:Vault has 0 files"), "{s}");
        }
        assert!(fake.bisync_calls().is_empty());
        assert_eq!(fake.calls.borrow().len(), 2); // two `size` calls only
    }

    #[test]
    fn missing_local_dir_and_unset_local_refuse() {
        let (d, host, mut cfg) = setup(3);
        let fake = Fake::new();
        cfg.vault_local = Some(d.path().join("nope"));
        let (code, s) = run(|o| sync(&cfg, &host, &fake, &NoDaemon, false, o));
        assert_eq!(code, EXIT_FAIL);
        assert!(s.contains("missing or not a directory"), "{s}");
        cfg.vault_local = None;
        let (code, s) = run(|o| sync(&cfg, &host, &fake, &NoDaemon, false, o));
        assert_eq!(code, EXIT_FAIL);
        assert!(s.contains("VAULT_LOCAL is not set"), "{s}");
        assert!(fake.calls.borrow().is_empty());
    }

    #[test]
    fn rclone_exit_code_is_passed_through_and_logged() {
        let (_d, host, cfg) = setup(3);
        let mut fake = Fake::new();
        fake.bisync_code = 7;
        fake.bisync_stderr = format!(
            "INFO  : Path1:    2 changes:    0 new,    0 newer,    0 older,    2 deleted\n\
             ERROR : Bisync critical error: couldn't refresh token {{\"refresh_token\":\"{SECRET}\"}}\n\
             ERROR : Bisync aborted. Must run --resync to recover.\n"
        );
        let (code, s) = run(|o| sync(&cfg, &host, &fake, &NoDaemon, false, o));
        assert_eq!(code, 7);
        assert!(s.contains("FAILED (rclone exit 7)"), "{s}");
        assert!(s.contains("resync --dry-run"), "{s}");
        let log = read_log(&host);
        assert!(log.contains("sync start"), "{log}");
        assert!(log.contains("Path1:    2 changes"), "{log}");
        assert!(log.contains("end exit=7"), "{log}");
        assert_no_secrets(&s);
        assert_no_secrets(&log);
    }

    #[test]
    fn log_has_start_summary_end_and_no_file_names() {
        let (_d, host, cfg) = setup(3);
        let mut fake = Fake::new();
        fake.bisync_stderr = "INFO  : - Path1    File is new     - Private/diary.md\n\
             INFO  : Path1:    1 changes:    1 new,    0 newer,    0 older,    0 deleted\n\
             INFO  : Bisync successful\n"
            .into();
        let (code, _s) = run(|o| sync(&cfg, &host, &fake, &NoDaemon, false, o));
        assert_eq!(code, EXIT_OK);
        let log = read_log(&host);
        let lines: Vec<&str> = log.lines().collect();
        assert!(
            lines[0].contains(" sync start remote=gdrive:Vault"),
            "{log}"
        );
        assert!(lines[0].contains("local="), "{log}");
        assert!(log.contains("guards ok"), "{log}");
        assert!(log.contains("Bisync successful"), "{log}");
        assert!(lines.last().unwrap().ends_with("sync end exit=0"), "{log}");
        assert!(!log.contains("diary.md"), "{log}");
        // Appends across runs.
        run(|o| sync(&cfg, &host, &fake, &NoDaemon, false, o));
        assert_eq!(read_log(&host).matches("sync start").count(), 2);
    }

    #[test]
    fn filters_file_explicit_default_and_missing() {
        let (d, host, mut cfg) = setup(3);
        let fake = Fake::new();
        assert_eq!(resolve_filters(&cfg, &host).unwrap(), None);

        let sync_dir = d.path().join(".config/vault-sync");
        std::fs::create_dir_all(&sync_dir).unwrap();
        std::fs::write(sync_dir.join("filters.txt"), "- .obsidian/cache/**\n").unwrap();
        let (code, s) = run(|o| sync(&cfg, &host, &fake, &NoDaemon, false, o));
        assert_eq!(code, EXIT_OK, "{s}");
        let b = fake.bisync_calls();
        let want = sync_dir.join("filters.txt").display().to_string();
        assert!(b[0]
            .windows(2)
            .any(|w| w[0] == "--filters-file" && w[1] == want));

        cfg.filters_file = Some(d.path().join("missing.txt"));
        let (code, s) = run(|o| sync(&cfg, &host, &fake, &NoDaemon, false, o));
        assert_eq!(code, EXIT_FAIL);
        assert!(s.contains("missing.txt does not exist"), "{s}");
        assert_eq!(fake.bisync_calls().len(), 1);
    }

    #[test]
    fn unreadable_remote_count_refuses() {
        struct NoCount;
        impl CommandRunner for NoCount {
            fn run(&self, _p: &Path, a: &[String]) -> Result<RunOutput, Error> {
                assert_eq!(a[0], "size", "bisync must not run");
                Ok(RunOutput {
                    code: Some(1),
                    stdout: String::new(),
                    stderr: "ERROR : dial tcp: no such host\n".into(),
                })
            }
        }
        let (_d, host, cfg) = setup(3);
        let (code, s) = run(|o| sync(&cfg, &host, &NoCount, &NoDaemon, false, o));
        assert_eq!(code, EXIT_FAIL);
        assert!(
            s.contains("cannot count remote files: ERROR : dial tcp"),
            "{s}"
        );
    }

    #[test]
    fn lsf_remote_local_and_failure() {
        let (_d, host, cfg) = setup(3);
        let mut fake = Fake::new();
        let (code, s) = run(|o| lsf(&cfg, &host, &fake, false, o));
        assert_eq!(code, EXIT_OK);
        assert_eq!(s, "Daily/\nInbox.md\n");
        assert_eq!(
            fake.calls.borrow()[0],
            ["lsf", "--max-depth", "1", "--fast-list", "gdrive:Vault"]
        );
        let (code, _) = run(|o| lsf(&cfg, &host, &fake, true, o));
        assert_eq!(code, EXIT_OK);
        assert_eq!(
            fake.calls.borrow()[1].last().unwrap(),
            &cfg.vault_local.as_ref().unwrap().display().to_string()
        );
        fake.lsf_ok = false;
        let (code, s) = run(|o| lsf(&cfg, &host, &fake, false, o));
        assert_eq!(code, 3);
        assert!(s.contains("lsf: FAILED"), "{s}");
        assert_no_secrets(&s);
    }

    #[test]
    fn no_state_dir_means_no_log_but_sync_still_runs() {
        let (_d, mut host, cfg) = setup(3);
        host.state_dir = None;
        let fake = Fake::new();
        let (code, s) = run(|o| sync(&cfg, &host, &fake, &NoDaemon, false, o));
        assert_eq!(code, EXIT_OK, "{s}");
        assert!(!s.contains("log "), "{s}");
    }

    #[test]
    fn utc_formatting() {
        assert_eq!(format_utc(0), "1970-01-01T00:00:00Z");
        assert_eq!(format_utc(1_793_358_300), "2026-10-30T11:05:00Z");
        assert_eq!(format_utc(951_782_400), "2000-02-29T00:00:00Z");
    }

    #[test]
    fn successful_sync_reindexes_once_then_ends() {
        let (_d, mut host, cfg) = setup(3);
        host.runtime_dir = Some(PathBuf::from("/run/user/1000"));
        let fake = Fake::new();
        let daemon = FakeDaemon::ok();
        let (code, s) = run(|o| sync(&cfg, &host, &fake, &daemon, false, o));
        assert_eq!(code, EXIT_OK, "{s}");
        assert_eq!(daemon.count(), 1);
        let (sock, timeout) = daemon.calls.borrow()[0].clone();
        assert_eq!(sock, PathBuf::from("/run/user/1000/vault-search.sock"));
        assert_eq!(timeout, Duration::from_secs(1800));
        assert!(
            s.contains("sync: reindex: ok (notes=412 chunks=3001)"),
            "{s}"
        );
        let log = read_log(&host);
        let lines: Vec<&str> = log.lines().collect();
        let n = lines.len();
        assert!(
            lines[n - 2].ends_with("sync reindex: ok (notes=412 chunks=3001)"),
            "{log}"
        );
        assert!(lines[n - 1].ends_with("sync end exit=0"), "{log}");
    }

    #[test]
    fn resync_also_reindexes_and_honours_socket_and_timeout() {
        let (_d, host, mut cfg) = setup(3);
        cfg.search_socket = Some(PathBuf::from("/x/vs.sock"));
        cfg.reindex_timeout_secs = 90;
        let fake = Fake::new();
        let daemon = FakeDaemon::ok();
        let (code, s) = run(|o| resync(&cfg, &host, &fake, &daemon, false, o));
        assert_eq!(code, EXIT_OK, "{s}");
        assert_eq!(
            daemon.calls.borrow().as_slice(),
            [(PathBuf::from("/x/vs.sock"), Duration::from_secs(90))]
        );
        assert!(s.contains("resync: reindex: ok"), "{s}");
    }

    #[test]
    fn dry_run_never_reindexes() {
        let (_d, host, cfg) = setup(3);
        let fake = Fake::new();
        let daemon = FakeDaemon::ok();
        let (code, s) = run(|o| sync(&cfg, &host, &fake, &daemon, true, o));
        assert_eq!(code, EXIT_OK, "{s}");
        let (code, s2) = run(|o| resync(&cfg, &host, &fake, &daemon, true, o));
        assert_eq!(code, EXIT_OK, "{s2}");
        assert_eq!(daemon.count(), 0);
        assert!(!s.contains("reindex") && !s2.contains("reindex"), "{s}{s2}");
        assert!(!read_log(&host).contains("reindex"));
    }

    #[test]
    fn daemon_not_listening_skips_and_keeps_exit_0() {
        let (_d, host, cfg) = setup(3);
        let fake = Fake::new();
        let (code, s) = run(|o| sync(&cfg, &host, &fake, &NoDaemon, false, o));
        assert_eq!(code, EXIT_OK, "{s}");
        assert!(
            s.contains(
                "sync: reindex: skipped (vault-searchd not listening on /tmp/vault-search-0.sock"
            ),
            "{s}"
        );
        let log = read_log(&host);
        assert!(
            log.contains("reindex: skipped (vault-searchd not listening"),
            "{log}"
        );
        assert!(log.lines().last().unwrap().ends_with("end exit=0"), "{log}");
    }

    #[test]
    fn reindex_ok_false_or_timeout_is_surfaced_but_exit_0() {
        let (_d, host, cfg) = setup(3);
        let fake = Fake::new();
        let daemon = FakeDaemon::answering(Ok(ReindexReply {
            ok: false,
            error: Some("embed: http://127.0.0.1:8081 connection refused".into()),
            ..Default::default()
        }));
        let (code, s) = run(|o| sync(&cfg, &host, &fake, &daemon, false, o));
        assert_eq!(code, EXIT_OK, "{s}");
        assert!(
            s.contains("sync: reindex: FAILED (embed: http://127.0.0.1:8081 connection refused)"),
            "{s}"
        );
        assert!(read_log(&host).contains("reindex: FAILED (embed:"));

        let daemon = FakeDaemon::answering(Err(ReindexError::Failed(
            "no reply within 1800s (raise VAULT_REINDEX_TIMEOUT_SECS)".into(),
        )));
        let (code, s) = run(|o| sync(&cfg, &host, &fake, &daemon, false, o));
        assert_eq!(code, EXIT_OK, "{s}");
        assert!(s.contains("reindex: FAILED (no reply within 1800s"), "{s}");
    }

    #[test]
    fn reindex_disabled_by_config() {
        let (_d, host, mut cfg) = setup(3);
        cfg.reindex = false;
        let fake = Fake::new();
        let daemon = FakeDaemon::ok();
        let (code, s) = run(|o| sync(&cfg, &host, &fake, &daemon, false, o));
        assert_eq!(code, EXIT_OK, "{s}");
        assert_eq!(daemon.count(), 0);
        assert!(s.contains("reindex: disabled (VAULT_REINDEX=0)"), "{s}");
    }

    #[test]
    fn guard_refusal_or_rclone_failure_never_reindexes() {
        // Guard: too few local files.
        let (_d, host, cfg) = setup(1);
        let fake = Fake::new();
        let daemon = FakeDaemon::ok();
        let (code, _) = run(|o| sync(&cfg, &host, &fake, &daemon, false, o));
        assert_eq!(code, EXIT_FAIL);
        // Guard: too few remote files.
        let (_d, host, cfg) = setup(3);
        let mut fake = Fake::new();
        fake.remote_count = 0;
        let (code, _) = run(|o| resync(&cfg, &host, &fake, &daemon, false, o));
        assert_eq!(code, EXIT_FAIL);
        // rclone bisync itself fails.
        let mut fake = Fake::new();
        fake.bisync_code = 1;
        let (code, s) = run(|o| sync(&cfg, &host, &fake, &daemon, false, o));
        assert_eq!(code, 1, "{s}");
        assert_eq!(daemon.count(), 0);
        assert!(!s.contains("reindex"), "{s}");
    }
}
