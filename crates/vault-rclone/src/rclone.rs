//! Finding and calling the `rclone` binary.
//!
//! Everything that spawns a process goes through [`CommandRunner`], so tests
//! swap in a fake and never touch Drive.

use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use crate::config::Config;
use crate::error::Error;

/// Captured result of one rclone call.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RunOutput {
    /// Exit code, or `None` if killed by a signal.
    pub code: Option<i32>,
    pub stdout: String,
    pub stderr: String,
}

impl RunOutput {
    pub fn success(&self) -> bool {
        self.code == Some(0)
    }
}

/// Runs a program with arguments and captures its output.
pub trait CommandRunner {
    fn run(&self, program: &Path, args: &[String]) -> Result<RunOutput, Error>;
}

/// The real runner: `std::process::Command`, stdin closed, output captured.
#[derive(Debug, Default, Clone, Copy)]
pub struct SystemRunner;

impl CommandRunner for SystemRunner {
    fn run(&self, program: &Path, args: &[String]) -> Result<RunOutput, Error> {
        let out = Command::new(program)
            .args(args)
            .stdin(Stdio::null())
            .output()
            .map_err(|source| Error::Spawn {
                bin: program.to_path_buf(),
                source,
            })?;
        Ok(RunOutput {
            code: out.status.code(),
            stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
        })
    }
}

/// Find rclone: `RCLONE_BIN` (a path, or a bare name looked up on `PATH`),
/// then `rclone` on `PATH`, then `~/.local/bin/rclone`.
///
/// An explicit `RCLONE_BIN` that is not executable is an error rather than a
/// silent fallback, so a typo does not run some other rclone.
pub fn locate_rclone(
    explicit: Option<&Path>,
    path_var: Option<&OsStr>,
    home: Option<&Path>,
    is_exec: impl Fn(&Path) -> bool,
) -> Result<PathBuf, Error> {
    let on_path = |name: &OsStr| -> Option<PathBuf> {
        std::env::split_paths(path_var?)
            .filter(|d| !d.as_os_str().is_empty())
            .map(|d| d.join(name))
            .find(|p| is_exec(p))
    };

    if let Some(bin) = explicit {
        let bare = bin.components().count() == 1 && !bin.is_absolute();
        let hit = if bare {
            on_path(bin.as_os_str())
        } else {
            is_exec(bin).then(|| bin.to_path_buf())
        };
        return hit.ok_or_else(|| Error::RcloneNotFound {
            tried: vec![format!("RCLONE_BIN={}", bin.display())],
        });
    }

    let mut tried = vec!["rclone on PATH".to_string()];
    if let Some(p) = on_path(OsStr::new("rclone")) {
        return Ok(p);
    }
    if let Some(home) = home {
        let p = home.join(".local").join("bin").join("rclone");
        if is_exec(&p) {
            return Ok(p);
        }
        tried.push(p.display().to_string());
    }
    Err(Error::RcloneNotFound { tried })
}

/// Executable regular file (follows symlinks).
pub fn is_executable(p: &Path) -> bool {
    let Ok(meta) = std::fs::metadata(p) else {
        return false;
    };
    if !meta.is_file() {
        return false;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        meta.permissions().mode() & 0o111 != 0
    }
    #[cfg(not(unix))]
    {
        true
    }
}

/// Global flags placed before every subcommand.
pub fn global_args(cfg: &Config) -> Vec<String> {
    match &cfg.rclone_config {
        Some(p) => vec!["--config".into(), p.display().to_string()],
        None => Vec::new(),
    }
}

fn with_globals(cfg: &Config, rest: &[&str]) -> Vec<String> {
    let mut v = global_args(cfg);
    v.extend(rest.iter().map(|s| s.to_string()));
    v
}

/// `rclone version`
pub fn version_args(cfg: &Config) -> Vec<String> {
    with_globals(cfg, &["version"])
}

/// `rclone config file` — prints where rclone.conf lives, not its contents.
pub fn config_file_args(cfg: &Config) -> Vec<String> {
    with_globals(cfg, &["config", "file"])
}

/// `rclone listremotes` — names only.
pub fn listremotes_args(cfg: &Config) -> Vec<String> {
    with_globals(cfg, &["listremotes"])
}

/// `rclone lsd --max-depth 1 <VAULT_REMOTE>` — cheap reachability check.
pub fn lsd_args(cfg: &Config) -> Vec<String> {
    with_globals(cfg, &["lsd", "--max-depth", "1", &cfg.vault_remote])
}

/// `rclone size --json --fast-list <VAULT_REMOTE>` — remote file count.
pub fn size_args(cfg: &Config) -> Vec<String> {
    with_globals(cfg, &["size", "--json", "--fast-list", &cfg.vault_remote])
}

/// `rclone lsf --max-depth 1 [--fast-list] <target>` — top-level listing for
/// smoke checks. `target` is the vault remote by default, or the local path.
/// `--fast-list` only helps (and is only passed) for the remote.
pub fn lsf_args(cfg: &Config, local: Option<&Path>) -> Vec<String> {
    let mut v = with_globals(cfg, &["lsf", "--max-depth", "1"]);
    match local {
        Some(p) => v.push(p.display().to_string()),
        None => {
            v.push("--fast-list".into());
            v.push(cfg.vault_remote.clone());
        }
    }
    v
}

/// Options for one bisync run.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct BisyncOpts<'a> {
    /// First run, or recovery after a critical error / filters change.
    pub resync: bool,
    /// Pass `--dry-run`.
    pub dry_run: bool,
    /// Pass `--filters-file`.
    pub filters_file: Option<&'a Path>,
}

/// `rclone bisync <VAULT_REMOTE> <VAULT_LOCAL> ...`
///
/// Path1 is the remote and Path2 the local folder, the same order as the
/// Nitro setup, so bisync's listing files in `~/.cache/rclone/bisync` keep
/// their names if the bash script and this binary take turns.
///
/// Always passes `--max-delete`, newer-wins conflict resolution with a
/// `*.conflict-…` loser kept, `--fast-list`, and `-v` so the run summary
/// lands on stderr. `--resync` adds `--resync-mode newer`, so a resync also
/// keeps the newer copy instead of rclone's default "Path1 always wins".
pub fn bisync_args(cfg: &Config, local: &Path, opts: &BisyncOpts<'_>) -> Vec<String> {
    let max_delete = cfg.max_delete.to_string();
    let mut v = with_globals(
        cfg,
        &[
            "bisync",
            &cfg.vault_remote,
            &local.display().to_string(),
            "--max-delete",
            &max_delete,
            "--conflict-resolve",
            "newer",
            "--conflict-loser",
            "num",
            "--conflict-suffix",
            &cfg.conflict_suffix,
            "--fast-list",
            "-v",
        ],
    );
    if let Some(f) = opts.filters_file {
        v.push("--filters-file".into());
        v.push(f.display().to_string());
    }
    if opts.resync {
        v.extend(["--resync", "--resync-mode", "newer"].map(String::from));
    }
    if opts.dry_run {
        v.push("--dry-run".into());
    }
    v
}

/// High-level lines from a verbose bisync log: change counts, conflicts, and
/// the final verdict. Per-file lines are dropped so the log stays short and
/// note names do not pile up outside the vault.
pub fn bisync_summary(stderr: &str) -> Vec<String> {
    const KEEP: &[&str] = &[
        "Path1:",
        "Path2:",
        "No changes found",
        "Bisync successful",
        "Bisync critical error",
        "Bisync aborted",
        "Must run --resync",
        "too many deletes",
        "Safety abort",
        "Empty current Path",
        "lock file found",
    ];
    let mut out: Vec<String> = Vec::new();
    let mut conflicts = 0usize;
    for line in stderr.lines() {
        let l = line.trim();
        if l.contains("New or changed in both paths") {
            conflicts += 1;
            continue;
        }
        if KEEP.iter().any(|k| l.contains(k)) {
            // Drop rclone's `2026/10/30 13:05:00 INFO  : ` prefix.
            let msg = l.split_once(" : ").map_or(l, |(_, m)| m).trim();
            let msg = safe_summary(msg);
            if !out.contains(&msg) {
                out.push(msg);
            }
        }
    }
    if conflicts > 0 {
        out.push(format!(
            "{conflicts} conflict(s): newer kept, loser renamed"
        ));
    }
    out
}

/// First line of `rclone version`, e.g. `rclone v1.75.0`.
pub fn parse_version(stdout: &str) -> Option<String> {
    stdout
        .lines()
        .map(str::trim)
        .find(|l| l.starts_with("rclone "))
        .map(str::to_string)
}

/// Path printed by `rclone config file` (last non-empty line).
pub fn parse_config_file(stdout: &str) -> Option<PathBuf> {
    stdout
        .lines()
        .map(str::trim)
        .rfind(|l| !l.is_empty() && !l.ends_with(':'))
        .map(PathBuf::from)
}

/// Remote names from `rclone listremotes` (`gdrive:` -> `gdrive`).
pub fn parse_listremotes(stdout: &str) -> Vec<String> {
    stdout
        .lines()
        .map(str::trim)
        .filter_map(|l| l.strip_suffix(':'))
        .filter(|l| !l.is_empty())
        .map(str::to_string)
        .collect()
}

/// `count` from `rclone size --json` output.
pub fn parse_size_count(stdout: &str) -> Option<u64> {
    let rest = &stdout[stdout.find("\"count\"")? + "\"count\"".len()..];
    let rest = rest.trim_start().strip_prefix(':')?.trim_start();
    let digits: String = rest.chars().take_while(|c| c.is_ascii_digit()).collect();
    digits.parse().ok()
}

/// What `check-config` may learn from rclone.conf: section names, and whether
/// a section has a `token` key. Values are never read into memory beyond the
/// line being scanned.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ConfShape {
    pub sections: Vec<String>,
    pub with_token: Vec<String>,
}

pub fn conf_shape(text: &str) -> ConfShape {
    let mut shape = ConfShape::default();
    let mut current: Option<String> = None;
    for line in text.lines().map(str::trim) {
        if let Some(name) = line.strip_prefix('[').and_then(|l| l.strip_suffix(']')) {
            current = Some(name.trim().to_string());
            shape.sections.push(name.trim().to_string());
        } else if let (Some(sec), Some((key, _))) = (&current, line.split_once('=')) {
            if key.trim() == "token" && !shape.with_token.contains(sec) {
                shape.with_token.push(sec.clone());
            }
        }
    }
    shape
}

/// One short, safe line of rclone stderr for error messages. Anything that
/// smells like a credential is replaced wholesale.
pub fn safe_summary(stderr: &str) -> String {
    const SUSPECT: &[&str] = &[
        "token",
        "secret",
        "password",
        "refresh",
        "access_",
        "bearer",
        "authorization",
        "client_id",
    ];
    let Some(line) = stderr.lines().map(str::trim).find(|l| !l.is_empty()) else {
        return "no error output".into();
    };
    let lower = line.to_ascii_lowercase();
    if SUSPECT.iter().any(|s| lower.contains(s)) {
        return "[rclone error output withheld: may contain credentials]".into();
    }
    let mut s: String = line.chars().take(200).collect();
    if line.chars().count() > 200 {
        s.push('…');
    }
    s
}

/// rclone's bisync lock files (`*.lck`) under `<cache>/rclone/bisync`.
pub fn bisync_locks(cache_dir: &Path) -> Vec<PathBuf> {
    let dir = cache_dir.join("rclone").join("bisync");
    let Ok(rd) = std::fs::read_dir(&dir) else {
        return Vec::new();
    };
    let mut v: Vec<PathBuf> = rd
        .filter_map(Result::ok)
        .map(|e| e.path())
        .filter(|p| p.extension() == Some(OsStr::new("lck")))
        .collect();
    v.sort();
    v
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    fn cfg() -> Config {
        Config::load_from(|_| None, &[], None).unwrap()
    }

    fn exec_set(paths: &[&str]) -> impl Fn(&Path) -> bool {
        let set: HashSet<PathBuf> = paths.iter().map(PathBuf::from).collect();
        move |p: &Path| set.contains(p)
    }

    #[test]
    fn locate_prefers_explicit_then_path_then_local_bin() {
        let path = OsStr::new("/usr/bin:/opt/bin");
        let home = Some(Path::new("/h"));
        let all = exec_set(&["/x/rclone", "/opt/bin/rclone", "/h/.local/bin/rclone"]);

        let got = locate_rclone(Some(Path::new("/x/rclone")), Some(path), home, &all).unwrap();
        assert_eq!(got, PathBuf::from("/x/rclone"));

        let got = locate_rclone(None, Some(path), home, &all).unwrap();
        assert_eq!(got, PathBuf::from("/opt/bin/rclone"));

        let only_local = exec_set(&["/h/.local/bin/rclone"]);
        let got = locate_rclone(None, Some(path), home, &only_local).unwrap();
        assert_eq!(got, PathBuf::from("/h/.local/bin/rclone"));
    }

    #[test]
    fn explicit_bare_name_searches_path() {
        let path = OsStr::new("/a:/b");
        let ex = exec_set(&["/b/rclone-1.75"]);
        let got = locate_rclone(Some(Path::new("rclone-1.75")), Some(path), None, &ex).unwrap();
        assert_eq!(got, PathBuf::from("/b/rclone-1.75"));
    }

    #[test]
    fn explicit_missing_does_not_fall_back() {
        let ex = exec_set(&["/usr/bin/rclone"]);
        let err = locate_rclone(
            Some(Path::new("/typo/rclone")),
            Some(OsStr::new("/usr/bin")),
            None,
            &ex,
        )
        .unwrap_err();
        assert!(err.to_string().contains("RCLONE_BIN=/typo/rclone"), "{err}");
    }

    #[test]
    fn not_found_lists_what_was_tried() {
        let err = locate_rclone(None, None, Some(Path::new("/h")), exec_set(&[])).unwrap_err();
        let s = err.to_string();
        assert!(
            s.contains("PATH") && s.contains("/h/.local/bin/rclone"),
            "{s}"
        );
    }

    #[test]
    fn is_executable_checks_mode() {
        use std::os::unix::fs::PermissionsExt;
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("rclone");
        std::fs::write(&p, "#!/bin/sh\n").unwrap();
        assert!(!is_executable(&p));
        std::fs::set_permissions(&p, std::fs::Permissions::from_mode(0o755)).unwrap();
        assert!(is_executable(&p));
        assert!(!is_executable(dir.path()));
    }

    #[test]
    fn argv_builders() {
        let mut c = cfg();
        assert_eq!(version_args(&c), ["version"]);
        assert_eq!(config_file_args(&c), ["config", "file"]);
        assert_eq!(listremotes_args(&c), ["listremotes"]);
        assert_eq!(lsd_args(&c), ["lsd", "--max-depth", "1", "gdrive:Vault"]);
        assert_eq!(
            size_args(&c),
            ["size", "--json", "--fast-list", "gdrive:Vault"]
        );

        c.rclone_config = Some(PathBuf::from("/h/rclone.conf"));
        assert_eq!(
            lsd_args(&c),
            [
                "--config",
                "/h/rclone.conf",
                "lsd",
                "--max-depth",
                "1",
                "gdrive:Vault"
            ]
        );
    }

    #[test]
    fn lsf_argv() {
        let c = cfg();
        assert_eq!(
            lsf_args(&c, None),
            ["lsf", "--max-depth", "1", "--fast-list", "gdrive:Vault"]
        );
        assert_eq!(
            lsf_args(&c, Some(Path::new("/v"))),
            ["lsf", "--max-depth", "1", "/v"]
        );
    }

    fn flag_value<'a>(argv: &'a [String], flag: &str) -> Option<&'a str> {
        argv.iter()
            .position(|a| a == flag)
            .and_then(|i| argv.get(i + 1))
            .map(String::as_str)
    }

    #[test]
    fn bisync_argv_sync_and_resync() {
        let mut c = cfg();
        c.max_delete = 25;
        let local = Path::new("/home/u/Vault");
        let sync = bisync_args(&c, local, &BisyncOpts::default());
        assert_eq!(&sync[..3], ["bisync", "gdrive:Vault", "/home/u/Vault"]);
        assert_eq!(flag_value(&sync, "--max-delete"), Some("25"));
        assert_eq!(flag_value(&sync, "--conflict-resolve"), Some("newer"));
        assert_eq!(flag_value(&sync, "--conflict-loser"), Some("num"));
        assert_eq!(
            flag_value(&sync, "--conflict-suffix"),
            Some(".conflict-{DateOnly}-")
        );
        assert!(sync.contains(&"--fast-list".to_string()));
        assert!(!sync.contains(&"--resync".to_string()));
        assert!(!sync.contains(&"--resync-mode".to_string()));
        assert!(!sync.contains(&"--dry-run".to_string()));
        assert!(!sync.contains(&"--filters-file".to_string()));

        let f = Path::new("/h/filters.txt");
        let re = bisync_args(
            &c,
            local,
            &BisyncOpts {
                resync: true,
                dry_run: true,
                filters_file: Some(f),
            },
        );
        assert!(re.contains(&"--resync".to_string()));
        assert_eq!(flag_value(&re, "--resync-mode"), Some("newer"));
        assert_eq!(flag_value(&re, "--filters-file"), Some("/h/filters.txt"));
        assert_eq!(flag_value(&re, "--max-delete"), Some("25"));
        assert!(re.contains(&"--dry-run".to_string()));

        c.rclone_config = Some(PathBuf::from("/h/rclone.conf"));
        let g = bisync_args(&c, local, &BisyncOpts::default());
        assert_eq!(&g[..3], ["--config", "/h/rclone.conf", "bisync"]);
    }

    #[test]
    fn bisync_summary_keeps_totals_only() {
        let log = "\
2026/10/30 13:05:00 INFO  : Synching Path1 \"gdrive:Vault/\" with Path2 \"/v/\"
2026/10/30 13:05:01 INFO  : - Path1    File is newer                       - Daily/secret-plans.md
2026/10/30 13:05:01 INFO  : Path1:    1 changes:    0 new,    1 newer,    0 older,    0 deleted
2026/10/30 13:05:01 INFO  : Path2:    1 changes:    0 new,    1 newer,    0 older,    0 deleted
2026/10/30 13:05:01 NOTICE: - WARNING  New or changed in both paths        - Daily/secret-plans.md
2026/10/30 13:05:02 INFO  : Bisync successful
";
        let s = bisync_summary(log);
        assert_eq!(
            s,
            [
                "Path1:    1 changes:    0 new,    1 newer,    0 older,    0 deleted",
                "Path2:    1 changes:    0 new,    1 newer,    0 older,    0 deleted",
                "Bisync successful",
                "1 conflict(s): newer kept, loser renamed",
            ]
        );
        assert!(!s.join("\n").contains("secret-plans"));
        let s = bisync_summary("ERROR : Bisync critical error: oauth2: refresh_token invalid\n");
        assert_eq!(s.len(), 1);
        assert!(!s[0].contains("refresh_token"), "{s:?}");
    }

    #[test]
    fn argv_never_targets_drive_root() {
        let c = cfg();
        for argv in [lsd_args(&c), size_args(&c), lsf_args(&c, None)] {
            let target = argv.last().unwrap();
            assert_eq!(target, "gdrive:Vault");
            assert!(!target.ends_with(':'));
        }
    }

    #[test]
    fn parsers() {
        assert_eq!(
            parse_version("rclone v1.75.0\n- os/version: ubuntu\n").as_deref(),
            Some("rclone v1.75.0")
        );
        assert_eq!(parse_version("garbage"), None);
        assert_eq!(
            parse_config_file("Configuration file is stored at:\n/h/.config/rclone/rclone.conf\n"),
            Some(PathBuf::from("/h/.config/rclone/rclone.conf"))
        );
        assert_eq!(
            parse_listremotes("gdrive:\nlocal2:\n\n"),
            ["gdrive", "local2"]
        );
        assert_eq!(
            parse_size_count("{\"count\":1234,\"bytes\":99}"),
            Some(1234)
        );
        assert_eq!(parse_size_count("{\"count\": 7 }"), Some(7));
        assert_eq!(parse_size_count("{}"), None);
    }

    #[test]
    fn conf_shape_keeps_names_only() {
        let text = "[gdrive]\ntype = drive\ntoken = {\"access_token\":\"FAKE.SECRET\"}\n\n[other]\ntype = local\n";
        let s = conf_shape(text);
        assert_eq!(s.sections, ["gdrive", "other"]);
        assert_eq!(s.with_token, ["gdrive"]);
        assert!(!format!("{s:?}").contains("SECRET"));
    }

    #[test]
    fn safe_summary_withholds_credentials() {
        assert_eq!(
            safe_summary("\n2026/10/30 ERROR : directory not found\nmore"),
            "2026/10/30 ERROR : directory not found"
        );
        let s = safe_summary("couldn't refresh token: {\"refresh_token\":\"1//abc\"}");
        assert!(!s.contains("1//abc"), "{s}");
        assert_eq!(safe_summary(""), "no error output");
        assert!(safe_summary(&"x".repeat(500)).chars().count() <= 201);
    }

    #[test]
    fn finds_bisync_locks() {
        let dir = tempfile::tempdir().unwrap();
        assert!(bisync_locks(dir.path()).is_empty());
        let b = dir.path().join("rclone").join("bisync");
        std::fs::create_dir_all(&b).unwrap();
        std::fs::write(b.join("a..b.lck"), "").unwrap();
        std::fs::write(b.join("a..b.path1.lst"), "").unwrap();
        assert_eq!(bisync_locks(dir.path()), vec![b.join("a..b.lck")]);
    }

    #[test]
    fn run_output_success() {
        assert!(RunOutput {
            code: Some(0),
            ..Default::default()
        }
        .success());
        assert!(!RunOutput {
            code: Some(1),
            ..Default::default()
        }
        .success());
        assert!(!RunOutput::default().success());
    }
}
