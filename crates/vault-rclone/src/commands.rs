//! `status` and `check-config`, plus the shared [`Host`]. `sync`, `resync`,
//! and `lsf` live in [`crate::sync`].
//!
//! Commands write human-readable lines to `out` and return a process exit
//! code: 0 ok, 1 a check failed or a guard refused, 2 bad config. `sync`,
//! `resync`, and `lsf` pass rclone's own non-zero exit code through.

use std::ffi::OsString;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use crate::config::Config;
use crate::error::Error;
use crate::rclone::{self, CommandRunner};

pub const EXIT_OK: i32 = 0;
pub const EXIT_FAIL: i32 = 1;
/// Config could not be loaded (bad `config.env`, bad `VAULT_REMOTE`, ...).
pub const EXIT_CONFIG: i32 = 2;

/// Facts about the machine that commands need, injectable for tests.
pub struct Host {
    pub home: Option<PathBuf>,
    pub path_var: Option<OsString>,
    /// `$XDG_CACHE_HOME`, else `~/.cache`.
    pub cache_dir: Option<PathBuf>,
    /// `$XDG_STATE_HOME`, else `~/.local/state`. The sync log goes under
    /// `<state_dir>/vault-rclone/`. `None` disables the log.
    pub state_dir: Option<PathBuf>,
    pub is_exec: fn(&Path) -> bool,
}

impl Host {
    pub fn from_process() -> Self {
        let home = std::env::var_os("HOME").map(PathBuf::from);
        let cache_dir = std::env::var_os("XDG_CACHE_HOME")
            .filter(|v| !v.is_empty())
            .map(PathBuf::from)
            .or_else(|| home.as_ref().map(|h| h.join(".cache")));
        let state_dir = std::env::var_os("XDG_STATE_HOME")
            .filter(|v| !v.is_empty())
            .map(PathBuf::from)
            .or_else(|| home.as_ref().map(|h| h.join(".local").join("state")));
        Host {
            home,
            path_var: std::env::var_os("PATH"),
            cache_dir,
            state_dir,
            is_exec: rclone::is_executable,
        }
    }

    pub(crate) fn locate(&self, cfg: &Config) -> Result<PathBuf, Error> {
        rclone::locate_rclone(
            cfg.rclone_bin.as_deref(),
            self.path_var.as_deref(),
            self.home.as_deref(),
            self.is_exec,
        )
    }
}

/// Find rclone and read its version. Prints one `rclone:` line.
fn rclone_line(
    cfg: &Config,
    host: &Host,
    runner: &dyn CommandRunner,
    out: &mut dyn Write,
) -> io::Result<Option<PathBuf>> {
    let bin = match host.locate(cfg) {
        Ok(b) => b,
        Err(e) => {
            writeln!(out, "rclone:  MISSING ({e})")?;
            return Ok(None);
        }
    };
    match runner.run(&bin, &rclone::version_args(cfg)) {
        Ok(o) if o.success() => {
            let v = rclone::parse_version(&o.stdout).unwrap_or_else(|| "version unknown".into());
            writeln!(out, "rclone:  {} ({v})", bin.display())?;
            Ok(Some(bin))
        }
        Ok(o) => {
            writeln!(
                out,
                "rclone:  {} FAILED `rclone version`: {}",
                bin.display(),
                rclone::safe_summary(&o.stderr)
            )?;
            Ok(None)
        }
        Err(e) => {
            writeln!(out, "rclone:  FAILED ({e})")?;
            Ok(None)
        }
    }
}

/// Count regular files under `root`, not following symlinks.
pub fn count_local_files(root: &Path) -> Result<u64, Error> {
    let mut n = 0;
    let mut stack = vec![root.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let rd = std::fs::read_dir(&dir).map_err(|source| Error::Io {
            path: dir.clone(),
            source,
        })?;
        for entry in rd {
            let entry = entry.map_err(|source| Error::Io {
                path: dir.clone(),
                source,
            })?;
            let ft = entry.file_type().map_err(|source| Error::Io {
                path: entry.path(),
                source,
            })?;
            if ft.is_dir() {
                stack.push(entry.path());
            } else if ft.is_file() {
                n += 1;
            }
        }
    }
    Ok(n)
}

/// `vault-rclone status`. With `offline`, skips `rclone size` on the remote.
pub fn status(
    cfg: &Config,
    host: &Host,
    runner: &dyn CommandRunner,
    offline: bool,
    out: &mut dyn Write,
) -> io::Result<i32> {
    let mut ok = true;
    let mut warnings = Vec::new();

    match cfg.vault_local.as_deref() {
        None => {
            writeln!(out, "local:   NOT SET ({})", Error::MissingLocal)?;
            ok = false;
        }
        Some(p) if !p.is_dir() => {
            writeln!(out, "local:   {} (MISSING or not a directory)", p.display())?;
            ok = false;
        }
        Some(p) => match count_local_files(p) {
            Ok(n) => {
                writeln!(out, "local:   {} ({n} files)", p.display())?;
                if n < u64::from(cfg.min_files) {
                    warnings.push(format!(
                        "local has {n} files, below MIN_FILES={}; sync would refuse",
                        cfg.min_files
                    ));
                }
            }
            Err(e) => {
                writeln!(out, "local:   {} (unreadable: {e})", p.display())?;
                ok = false;
            }
        },
    }
    writeln!(out, "remote:  {}", cfg.vault_remote)?;
    writeln!(
        out,
        "guards:  min-files {}, max-delete {}",
        cfg.min_files, cfg.max_delete
    )?;
    config_files_line(cfg, out)?;

    let locks = host
        .cache_dir
        .as_deref()
        .map(rclone::bisync_locks)
        .unwrap_or_default();
    if locks.is_empty() {
        writeln!(out, "lock:    none")?;
    } else {
        writeln!(out, "lock:    {} bisync lock file(s):", locks.len())?;
        for l in &locks {
            writeln!(out, "         {}", l.display())?;
        }
        warnings.push("a bisync lock is present; a run may be in progress or crashed".into());
    }

    let bin = rclone_line(cfg, host, runner, out)?;
    ok &= bin.is_some();

    match (&bin, offline) {
        (_, true) => writeln!(out, "reach:   skipped (--offline)")?,
        (None, false) => writeln!(out, "reach:   skipped (no rclone)")?,
        (Some(bin), false) => match runner.run(bin, &rclone::size_args(cfg)) {
            Ok(o) if o.success() => match rclone::parse_size_count(&o.stdout) {
                Some(n) => {
                    writeln!(out, "reach:   ok ({n} remote files)")?;
                    if n < u64::from(cfg.min_files) {
                        warnings.push(format!(
                            "remote has {n} files, below MIN_FILES={}; sync would refuse",
                            cfg.min_files
                        ));
                    }
                }
                None => writeln!(out, "reach:   ok (file count unreadable)")?,
            },
            Ok(o) => {
                writeln!(out, "reach:   FAILED: {}", rclone::safe_summary(&o.stderr))?;
                ok = false;
            }
            Err(e) => {
                writeln!(out, "reach:   FAILED ({e})")?;
                ok = false;
            }
        },
    }

    for w in &warnings {
        writeln!(out, "warning: {w}")?;
    }
    writeln!(out, "result:  {}", if ok { "ok" } else { "problems found" })?;
    Ok(if ok { EXIT_OK } else { EXIT_FAIL })
}

fn config_files_line(cfg: &Config, out: &mut dyn Write) -> io::Result<()> {
    if cfg.files_read.is_empty() {
        writeln!(out, "config:  environment and defaults only")
    } else {
        let list: Vec<String> = cfg
            .files_read
            .iter()
            .map(|p| p.display().to_string())
            .collect();
        writeln!(out, "config:  {}", list.join(", "))
    }
}

/// `vault-rclone check-config`. Checks rclone, rclone.conf, the remote name,
/// and (unless `offline`) `rclone lsd` on the vault. Prints paths and names
/// only: never rclone.conf contents or token values.
pub fn check_config(
    cfg: &Config,
    host: &Host,
    runner: &dyn CommandRunner,
    offline: bool,
    out: &mut dyn Write,
) -> io::Result<i32> {
    let mut ok = true;
    config_files_line(cfg, out)?;
    let Some(bin) = rclone_line(cfg, host, runner, out)? else {
        writeln!(out, "result:  problems found")?;
        return Ok(EXIT_FAIL);
    };
    let name = cfg.remote_name();

    // Where rclone keeps its config. We look at section names only.
    let conf_path = match runner.run(&bin, &rclone::config_file_args(cfg)) {
        Ok(o) if o.success() => rclone::parse_config_file(&o.stdout),
        _ => None,
    };
    match conf_path.as_deref() {
        Some(p) if p.is_file() => {
            writeln!(out, "conf:    {} (exists, contents not shown)", p.display())?;
            match std::fs::read_to_string(p) {
                Ok(text) => {
                    let shape = rclone::conf_shape(&text);
                    let tok = if shape.with_token.iter().any(|s| s == name) {
                        "present (not shown)"
                    } else if shape.sections.iter().any(|s| s == name) {
                        "no token key in section"
                    } else {
                        "section not readable (encrypted config?)"
                    };
                    writeln!(out, "token:   {tok}")?;
                }
                Err(e) => writeln!(out, "token:   unknown (cannot read conf: {})", e.kind())?,
            }
        }
        Some(p) => {
            writeln!(out, "conf:    {} MISSING", p.display())?;
            ok = false;
        }
        None => {
            writeln!(out, "conf:    UNKNOWN (`rclone config file` failed)")?;
            ok = false;
        }
    }

    match runner.run(&bin, &rclone::listremotes_args(cfg)) {
        Ok(o) if o.success() => {
            if rclone::parse_listremotes(&o.stdout)
                .iter()
                .any(|r| r == name)
            {
                writeln!(out, "remote:  {name} listed")?;
            } else {
                writeln!(out, "remote:  {name} NOT LISTED by `rclone listremotes`")?;
                ok = false;
            }
        }
        Ok(o) => {
            writeln!(out, "remote:  FAILED: {}", rclone::safe_summary(&o.stderr))?;
            ok = false;
        }
        Err(e) => {
            writeln!(out, "remote:  FAILED ({e})")?;
            ok = false;
        }
    }

    if offline {
        writeln!(out, "vault:   skipped (--offline)")?;
    } else {
        match runner.run(&bin, &rclone::lsd_args(cfg)) {
            Ok(o) if o.success() => writeln!(out, "vault:   {} reachable", cfg.vault_remote)?,
            Ok(o) => {
                writeln!(
                    out,
                    "vault:   {} FAILED: {}",
                    cfg.vault_remote,
                    rclone::safe_summary(&o.stderr)
                )?;
                ok = false;
            }
            Err(e) => {
                writeln!(out, "vault:   FAILED ({e})")?;
                ok = false;
            }
        }
    }

    writeln!(out, "result:  {}", if ok { "ok" } else { "problems found" })?;
    Ok(if ok { EXIT_OK } else { EXIT_FAIL })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rclone::RunOutput;
    use std::cell::RefCell;

    const SECRET: &str = "FAKE-SUPER-SECRET-ACCESS";
    const REFRESH: &str = "1//REFRESH-SECRET";

    /// Fake rclone keyed on the subcommand. Records every argv.
    struct Fake {
        calls: RefCell<Vec<Vec<String>>>,
        conf: PathBuf,
        remotes: &'static str,
        lsd_ok: bool,
        size_json: &'static str,
    }

    impl CommandRunner for Fake {
        fn run(&self, _p: &Path, args: &[String]) -> Result<RunOutput, Error> {
            self.calls.borrow_mut().push(args.to_vec());
            let ok = |s: String| RunOutput {
                code: Some(0),
                stdout: s,
                stderr: String::new(),
            };
            let sub: Vec<&str> = args
                .iter()
                .map(String::as_str)
                .filter(|a| !a.starts_with('-'))
                .collect();
            Ok(match sub.as_slice() {
                ["version", ..] => ok("rclone v1.75.0\n- os: linux\n".into()),
                ["config", "file", ..] => ok(format!(
                    "Configuration file is stored at:\n{}\n",
                    self.conf.display()
                )),
                ["listremotes", ..] => ok(self.remotes.into()),
                ["lsd", ..] if self.lsd_ok => ok("          -1 2026-10-30 Daily\n".into()),
                ["lsd", ..] => RunOutput {
                    code: Some(1),
                    stdout: String::new(),
                    stderr: format!("Failed to create file system: couldn't fetch token: {{\"access_token\":\"{SECRET}\"}}\n"),
                },
                ["size", ..] => ok(self.size_json.into()),
                other => panic!("unexpected rclone call {other:?}"),
            })
        }
    }

    fn setup() -> (tempfile::TempDir, Host, Config, Fake) {
        let dir = tempfile::tempdir().unwrap();
        let conf = dir.path().join("rclone.conf");
        std::fs::write(
            &conf,
            format!("[gdrive]\ntype = drive\nclient_secret = shh\ntoken = {{\"access_token\":\"{SECRET}\",\"refresh_token\":\"{REFRESH}\"}}\n"),
        )
        .unwrap();
        let vault = dir.path().join("Vault");
        std::fs::create_dir_all(vault.join("Daily")).unwrap();
        for f in ["a.md", "b.md", "Daily/c.md"] {
            std::fs::write(vault.join(f), "x").unwrap();
        }
        let host = Host {
            home: Some(dir.path().to_path_buf()),
            path_var: None,
            cache_dir: Some(dir.path().join("cache")),
            state_dir: Some(dir.path().join("state")),
            is_exec: |_| true,
        };
        let mut cfg = Config::load_from(|_| None, &[], None).unwrap();
        cfg.vault_local = Some(vault);
        cfg.rclone_bin = Some(PathBuf::from("/fake/rclone"));
        let fake = Fake {
            calls: RefCell::new(Vec::new()),
            conf,
            remotes: "gdrive:\n",
            lsd_ok: true,
            size_json: "{\"count\":120,\"bytes\":4096}",
        };
        (dir, host, cfg, fake)
    }

    fn run(f: impl FnOnce(&mut Vec<u8>) -> io::Result<i32>) -> (i32, String) {
        let mut buf = Vec::new();
        let code = f(&mut buf).unwrap();
        (code, String::from_utf8(buf).unwrap())
    }

    fn assert_no_secrets(s: &str) {
        for needle in [SECRET, REFRESH, "shh", "access_token", "refresh_token"] {
            assert!(!s.contains(needle), "leaked {needle:?} in:\n{s}");
        }
    }

    #[test]
    fn check_config_happy_path_hides_secrets() {
        let (_d, host, cfg, fake) = setup();
        let (code, s) = run(|o| check_config(&cfg, &host, &fake, false, o));
        assert_eq!(code, EXIT_OK, "{s}");
        assert!(s.contains("rclone v1.75.0"));
        assert!(s.contains("contents not shown"));
        assert!(s.contains("token:   present (not shown)"));
        assert!(s.contains("gdrive listed"));
        assert!(s.contains("gdrive:Vault reachable"));
        assert_no_secrets(&s);
        let calls = fake.calls.borrow();
        assert!(calls.contains(&vec![
            "lsd".into(),
            "--max-depth".into(),
            "1".into(),
            "gdrive:Vault".into()
        ]));
    }

    #[test]
    fn check_config_failure_output_hides_secrets() {
        let (_d, host, cfg, mut fake) = setup();
        fake.lsd_ok = false;
        let (code, s) = run(|o| check_config(&cfg, &host, &fake, false, o));
        assert_eq!(code, EXIT_FAIL);
        assert!(s.contains("FAILED"));
        assert_no_secrets(&s);
    }

    #[test]
    fn check_config_missing_remote_fails() {
        let (_d, host, cfg, mut fake) = setup();
        fake.remotes = "onedrive:\n";
        let (code, s) = run(|o| check_config(&cfg, &host, &fake, true, o));
        assert_eq!(code, EXIT_FAIL);
        assert!(s.contains("NOT LISTED"));
        assert!(s.contains("skipped (--offline)"));
        assert!(!fake.calls.borrow().iter().any(|c| c[0] == "lsd"));
    }

    #[test]
    fn check_config_missing_conf_fails() {
        let (d, host, cfg, mut fake) = setup();
        fake.conf = d.path().join("nope.conf");
        let (code, s) = run(|o| check_config(&cfg, &host, &fake, true, o));
        assert_eq!(code, EXIT_FAIL);
        assert!(s.contains("MISSING"));
    }

    #[test]
    fn rclone_missing_fails_both_commands() {
        let (_d, mut host, cfg, fake) = setup();
        host.is_exec = |_| false;
        let (code, s) = run(|o| check_config(&cfg, &host, &fake, false, o));
        assert_eq!(code, EXIT_FAIL);
        assert!(s.contains("rclone:  MISSING"));
        let (code, s) = run(|o| status(&cfg, &host, &fake, false, o));
        assert_eq!(code, EXIT_FAIL);
        assert!(s.contains("skipped (no rclone)"));
        assert!(fake.calls.borrow().is_empty());
    }

    #[test]
    fn status_reports_counts_guards_and_reach() {
        let (_d, host, cfg, fake) = setup();
        let (code, s) = run(|o| status(&cfg, &host, &fake, false, o));
        assert_eq!(code, EXIT_OK, "{s}");
        assert!(s.contains("(3 files)"), "{s}");
        assert!(s.contains("remote:  gdrive:Vault"));
        assert!(s.contains("min-files 3, max-delete 50"));
        assert!(s.contains("lock:    none"));
        assert!(s.contains("ok (120 remote files)"));
        assert!(!s.contains("warning"));
        assert_no_secrets(&s);
    }

    #[test]
    fn status_offline_skips_remote_and_warns_on_small_sides() {
        let (_d, host, mut cfg, fake) = setup();
        cfg.min_files = 10;
        let (code, s) = run(|o| status(&cfg, &host, &fake, true, o));
        assert_eq!(code, EXIT_OK);
        assert!(s.contains("skipped (--offline)"));
        assert!(s.contains("below MIN_FILES=10"));
        assert!(!fake.calls.borrow().iter().any(|c| c[0] == "size"));
    }

    #[test]
    fn status_flags_lock_and_missing_local() {
        let (d, host, mut cfg, fake) = setup();
        let b = d.path().join("cache/rclone/bisync");
        std::fs::create_dir_all(&b).unwrap();
        std::fs::write(b.join("x..y.lck"), "").unwrap();
        cfg.vault_local = None;
        let (code, s) = run(|o| status(&cfg, &host, &fake, true, o));
        assert_eq!(code, EXIT_FAIL);
        assert!(s.contains("local:   NOT SET"));
        assert!(s.contains("1 bisync lock file(s)"));
        assert!(s.contains("warning: a bisync lock"));
    }

    #[test]
    fn status_remote_unreachable_fails() {
        struct Down;
        impl CommandRunner for Down {
            fn run(&self, _p: &Path, a: &[String]) -> Result<RunOutput, Error> {
                Ok(if a[0] == "version" {
                    RunOutput {
                        code: Some(0),
                        stdout: "rclone v1.75.0\n".into(),
                        stderr: String::new(),
                    }
                } else {
                    RunOutput {
                        code: Some(1),
                        stdout: String::new(),
                        stderr: "dial tcp: no such host\n".into(),
                    }
                })
            }
        }
        let (_d, host, cfg, _fake) = setup();
        let (code, s) = run(|o| status(&cfg, &host, &Down, false, o));
        assert_eq!(code, EXIT_FAIL);
        assert!(s.contains("reach:   FAILED: dial tcp: no such host"));
    }
}
