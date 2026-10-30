//! Settings from the environment and optional `config.env` files.
//!
//! Precedence, highest first: process environment, then
//! `~/.config/vault-rclone/config.env`, then `~/.config/vault-sync/config.env`
//! (the bash skill's file, read for drop-in reuse), then built-in defaults.
//! Passing `--config FILE` replaces both files with that one file.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::error::Error;

/// Remote used when `VAULT_REMOTE` is unset.
pub const DEFAULT_REMOTE: &str = "gdrive:Vault";
/// `--max-delete` passed to every bisync (PR2).
pub const DEFAULT_MAX_DELETE: u32 = 50;
/// Refuse sync/resync when either side has fewer files than this (PR2).
pub const DEFAULT_MIN_FILES: u32 = 3;
/// `--conflict-suffix` for bisync. rclone puts a `.` before it and a number
/// after it, so a losing `note.md` becomes `note.md..conflict-2026-10-30-1`.
/// `{DateOnly}` is an rclone date macro (Go's `2006-01-02`).
pub const DEFAULT_CONFLICT_SUFFIX: &str = ".conflict-{DateOnly}-";

/// Keys this crate reads. Anything else in a config file is ignored and never printed.
pub const KNOWN_KEYS: &[&str] = &[
    "VAULT_LOCAL",
    "VAULT_REMOTE",
    "RCLONE_BIN",
    "RCLONE_CONFIG",
    "MAX_DELETE",
    "MIN_FILES",
    "FILTERS_FILE",
    "CONFLICT_SUFFIX",
];

/// Resolved settings. Holds paths only, never credentials.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    pub vault_local: Option<PathBuf>,
    pub vault_remote: String,
    /// Explicit `RCLONE_BIN`; `None` means search `PATH` then `~/.local/bin`.
    pub rclone_bin: Option<PathBuf>,
    /// Explicit `RCLONE_CONFIG`; `None` lets rclone pick its default.
    pub rclone_config: Option<PathBuf>,
    pub max_delete: u32,
    pub min_files: u32,
    /// Explicit `FILTERS_FILE` for `bisync --filters-file`. `None` means use
    /// `filters.txt` next to a default config file if one exists.
    pub filters_file: Option<PathBuf>,
    /// `--conflict-suffix` passed to bisync.
    pub conflict_suffix: String,
    /// Config files that existed and were read, lowest precedence first.
    pub files_read: Vec<PathBuf>,
}

impl Config {
    /// Load from the real process environment and the default file locations.
    pub fn load(explicit_file: Option<&Path>) -> Result<Self, Error> {
        let home = std::env::var_os("HOME").map(PathBuf::from);
        let files = match explicit_file {
            Some(f) => vec![f.to_path_buf()],
            None => default_files(home.as_deref()),
        };
        Self::load_from(|k| std::env::var(k).ok(), &files, home.as_deref())
    }

    /// Testable loader: `env` stands in for the process environment.
    /// Missing files are skipped; unreadable or malformed files are errors.
    pub fn load_from(
        env: impl Fn(&str) -> Option<String>,
        files: &[PathBuf],
        home: Option<&Path>,
    ) -> Result<Self, Error> {
        let mut merged: HashMap<String, String> = HashMap::new();
        let mut files_read = Vec::new();
        for path in files {
            let text = match std::fs::read_to_string(path) {
                Ok(t) => t,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => continue,
                Err(source) => {
                    return Err(Error::Io {
                        path: path.clone(),
                        source,
                    })
                }
            };
            merged.extend(parse_env_file(&text, path)?);
            files_read.push(path.clone());
        }
        for key in KNOWN_KEYS {
            if let Some(v) = env(key) {
                merged.insert((*key).to_string(), v);
            }
        }

        let get = |k: &str| merged.get(k).map(|s| s.trim()).filter(|s| !s.is_empty());
        let path_of = |k: &str| get(k).map(|v| expand_home(v, home));

        let vault_remote = get("VAULT_REMOTE").unwrap_or(DEFAULT_REMOTE).to_string();
        validate_remote(&vault_remote)?;
        let conflict_suffix = get("CONFLICT_SUFFIX")
            .unwrap_or(DEFAULT_CONFLICT_SUFFIX)
            .to_string();
        if conflict_suffix.contains('/') || conflict_suffix.contains('\\') {
            return Err(Error::BadConflictSuffix(conflict_suffix));
        }

        Ok(Config {
            vault_local: path_of("VAULT_LOCAL"),
            vault_remote,
            rclone_bin: path_of("RCLONE_BIN"),
            rclone_config: path_of("RCLONE_CONFIG"),
            max_delete: number(get("MAX_DELETE"), "MAX_DELETE", DEFAULT_MAX_DELETE)?,
            min_files: number(get("MIN_FILES"), "MIN_FILES", DEFAULT_MIN_FILES)?,
            filters_file: path_of("FILTERS_FILE"),
            conflict_suffix,
            files_read,
        })
    }

    /// `VAULT_LOCAL`, or [`Error::MissingLocal`].
    pub fn require_local(&self) -> Result<&Path, Error> {
        self.vault_local.as_deref().ok_or(Error::MissingLocal)
    }

    /// The rclone remote name, e.g. `gdrive` for `gdrive:Vault`.
    pub fn remote_name(&self) -> &str {
        remote_name(&self.vault_remote)
    }
}

/// `~/.config/vault-sync/config.env` then `~/.config/vault-rclone/config.env`.
pub fn default_files(home: Option<&Path>) -> Vec<PathBuf> {
    let Some(home) = home else { return Vec::new() };
    let base = home.join(".config");
    vec![
        base.join("vault-sync").join("config.env"),
        base.join("vault-rclone").join("config.env"),
    ]
}

/// Parse `KEY=value` lines. Accepts `export KEY=...`, `#` comments, and
/// single or double quotes. Values are never echoed back in errors.
pub fn parse_env_file(text: &str, path: &Path) -> Result<HashMap<String, String>, Error> {
    let mut out = HashMap::new();
    for (i, raw) in text.lines().enumerate() {
        let line = raw.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let line = line.strip_prefix("export ").unwrap_or(line).trim_start();
        let err = |msg: &str| Error::ConfigLine {
            path: path.to_path_buf(),
            line: i + 1,
            msg: msg.to_string(),
        };
        let (key, value) = line
            .split_once('=')
            .ok_or_else(|| err("expected KEY=value"))?;
        let key = key.trim();
        if key.is_empty() || !key.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
            return Err(err("key must be letters, digits, or _"));
        }
        let value = unquote(value.trim()).ok_or_else(|| err("unterminated quote"))?;
        out.insert(key.to_string(), value);
    }
    Ok(out)
}

fn unquote(v: &str) -> Option<String> {
    for q in ['"', '\''] {
        if let Some(rest) = v.strip_prefix(q) {
            let end = rest.find(q)?;
            return Some(rest[..end].to_string());
        }
    }
    // Unquoted: a ` #` starts a trailing comment, as in shell.
    let v = match v.find(" #") {
        Some(i) => &v[..i],
        None => v,
    };
    Some(v.trim_end().to_string())
}

/// Expand a leading `~`, `$HOME`, or `${HOME}`.
pub fn expand_home(v: &str, home: Option<&Path>) -> PathBuf {
    if let Some(home) = home {
        for prefix in ["~", "$HOME", "${HOME}"] {
            if v == prefix {
                return home.to_path_buf();
            }
            if let Some(rest) = v.strip_prefix(prefix).and_then(|r| r.strip_prefix('/')) {
                return home.join(rest);
            }
        }
    }
    PathBuf::from(v)
}

fn number(v: Option<&str>, key: &'static str, default: u32) -> Result<u32, Error> {
    match v {
        None => Ok(default),
        Some(s) => s.parse().map_err(|_| Error::BadNumber {
            key,
            value: s.to_string(),
        }),
    }
}

/// The part before the first `:`.
pub fn remote_name(remote: &str) -> &str {
    remote.split_once(':').map_or(remote, |(n, _)| n)
}

/// `name:path` with a non-empty name and a folder path. `gdrive:` or
/// `gdrive:/` would be the whole Drive, which the DriveSync policy forbids.
pub fn validate_remote(remote: &str) -> Result<(), Error> {
    let Some((name, path)) = remote.split_once(':') else {
        return Err(Error::BadRemote(remote.to_string()));
    };
    if name.is_empty() || name.contains('/') {
        return Err(Error::BadRemote(remote.to_string()));
    }
    if path.trim_matches('/').is_empty() {
        return Err(Error::WholeRemote(remote.to_string()));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn no_env(_: &str) -> Option<String> {
        None
    }

    fn write(dir: &Path, name: &str, body: &str) -> PathBuf {
        let p = dir.join(name);
        std::fs::File::create(&p)
            .unwrap()
            .write_all(body.as_bytes())
            .unwrap();
        p
    }

    #[test]
    fn guard_defaults_match_policy() {
        assert_eq!(DEFAULT_MAX_DELETE, 50);
        assert_eq!(DEFAULT_MIN_FILES, 3);
        assert_eq!(DEFAULT_REMOTE, "gdrive:Vault");
        let c = Config::load_from(no_env, &[], None).unwrap();
        assert_eq!(c.max_delete, 50);
        assert_eq!(c.min_files, 3);
        assert_eq!(c.vault_remote, "gdrive:Vault");
        assert_eq!(c.remote_name(), "gdrive");
        assert!(c.vault_local.is_none());
        assert!(matches!(c.require_local(), Err(Error::MissingLocal)));
        assert_eq!(c.conflict_suffix, ".conflict-{DateOnly}-");
        assert!(c.filters_file.is_none());
    }

    #[test]
    fn filters_and_conflict_suffix_keys() {
        let env = |k: &str| match k {
            "FILTERS_FILE" => Some("~/.config/vault-sync/filters.txt".to_string()),
            "CONFLICT_SUFFIX" => Some(".conflict-{Date}".to_string()),
            _ => None,
        };
        let c = Config::load_from(env, &[], Some(Path::new("/h"))).unwrap();
        assert_eq!(
            c.filters_file.as_deref(),
            Some(Path::new("/h/.config/vault-sync/filters.txt"))
        );
        assert_eq!(c.conflict_suffix, ".conflict-{Date}");
        let bad = |k: &str| (k == "CONFLICT_SUFFIX").then(|| "a/b".to_string());
        assert!(matches!(
            Config::load_from(bad, &[], None),
            Err(Error::BadConflictSuffix(_))
        ));
    }

    #[test]
    fn parses_env_file_shapes() {
        let dir = tempfile::tempdir().unwrap();
        let f = write(
            dir.path(),
            "config.env",
            "# vault-sync\n\
             export VAULT_LOCAL=\"~/Notes/Vault\"\n\
             VAULT_REMOTE='gdrive:Vault'\n\
             MAX_DELETE=20   # tighter\n\
             \n\
             UNRELATED_SECRET=do-not-print\n",
        );
        let c = Config::load_from(no_env, std::slice::from_ref(&f), Some(Path::new("/home/u")))
            .unwrap();
        assert_eq!(
            c.vault_local.as_deref(),
            Some(Path::new("/home/u/Notes/Vault"))
        );
        assert_eq!(c.vault_remote, "gdrive:Vault");
        assert_eq!(c.max_delete, 20);
        assert_eq!(c.min_files, 3);
        assert_eq!(c.files_read, vec![f]);
        assert!(!format!("{c:?}").contains("do-not-print"));
    }

    #[test]
    fn precedence_env_over_rclone_file_over_sync_file() {
        let dir = tempfile::tempdir().unwrap();
        let sync = write(
            dir.path(),
            "sync.env",
            "VAULT_LOCAL=/a\nMIN_FILES=5\nMAX_DELETE=7\n",
        );
        let rcl = write(dir.path(), "rclone.env", "VAULT_LOCAL=/b\nMIN_FILES=6\n");
        let env = |k: &str| (k == "VAULT_LOCAL").then(|| "$HOME/c".to_string());
        let c = Config::load_from(env, &[sync, rcl], Some(Path::new("/h"))).unwrap();
        assert_eq!(c.vault_local.as_deref(), Some(Path::new("/h/c")));
        assert_eq!(c.min_files, 6);
        assert_eq!(c.max_delete, 7);
    }

    #[test]
    fn missing_files_are_skipped() {
        let c = Config::load_from(no_env, &[PathBuf::from("/nope/config.env")], None).unwrap();
        assert!(c.files_read.is_empty());
    }

    #[test]
    fn default_files_prefer_vault_rclone() {
        let f = default_files(Some(Path::new("/h")));
        assert_eq!(
            f,
            vec![
                PathBuf::from("/h/.config/vault-sync/config.env"),
                PathBuf::from("/h/.config/vault-rclone/config.env"),
            ]
        );
        assert!(default_files(None).is_empty());
    }

    #[test]
    fn bad_lines_report_line_without_value() {
        let dir = tempfile::tempdir().unwrap();
        let f = write(
            dir.path(),
            "c.env",
            "VAULT_LOCAL=/x\nTOKEN_LIKE_garbage_no_equals\n",
        );
        let e = Config::load_from(no_env, &[f], None)
            .unwrap_err()
            .to_string();
        assert!(e.contains(":2:"), "{e}");
        assert!(!e.contains("garbage"), "{e}");

        let f = write(dir.path(), "q.env", "VAULT_LOCAL=\"/x\n");
        assert!(Config::load_from(no_env, &[f], None).is_err());
    }

    #[test]
    fn bad_numbers_rejected() {
        let env = |k: &str| (k == "MAX_DELETE").then(|| "-1".to_string());
        assert!(matches!(
            Config::load_from(env, &[], None),
            Err(Error::BadNumber {
                key: "MAX_DELETE",
                ..
            })
        ));
    }

    #[test]
    fn remote_must_be_a_folder() {
        assert!(validate_remote("gdrive:Vault").is_ok());
        assert!(validate_remote("gdrive:Notes/Vault").is_ok());
        assert!(matches!(
            validate_remote("gdrive:"),
            Err(Error::WholeRemote(_))
        ));
        assert!(matches!(
            validate_remote("gdrive:/"),
            Err(Error::WholeRemote(_))
        ));
        assert!(matches!(
            validate_remote("/local/path"),
            Err(Error::BadRemote(_))
        ));
        assert!(matches!(
            validate_remote(":Vault"),
            Err(Error::BadRemote(_))
        ));
        let env = |k: &str| (k == "VAULT_REMOTE").then(|| "gdrive:".to_string());
        assert!(Config::load_from(env, &[], None).is_err());
    }

    #[test]
    fn expand_home_forms() {
        let h = Some(Path::new("/h"));
        assert_eq!(expand_home("~", h), PathBuf::from("/h"));
        assert_eq!(expand_home("~/v", h), PathBuf::from("/h/v"));
        assert_eq!(expand_home("${HOME}/v", h), PathBuf::from("/h/v"));
        assert_eq!(expand_home("/abs", h), PathBuf::from("/abs"));
        assert_eq!(expand_home("~other/v", h), PathBuf::from("~other/v"));
        assert_eq!(expand_home("~/v", None), PathBuf::from("~/v"));
    }
}
