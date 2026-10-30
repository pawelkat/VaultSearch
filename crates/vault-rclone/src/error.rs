use std::path::PathBuf;

/// Everything that can go wrong before or while calling rclone.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("{path}:{line}: {msg}")]
    ConfigLine {
        path: PathBuf,
        line: usize,
        msg: String,
    },
    #[error("{key} must be a non-negative whole number, got {value:?}")]
    BadNumber { key: &'static str, value: String },
    #[error("VAULT_REMOTE must look like `remote:path` (e.g. gdrive:Vault), got {0:?}")]
    BadRemote(String),
    #[error("VAULT_REMOTE {0:?} points at the remote root; only a vault folder is allowed")]
    WholeRemote(String),
    #[error("CONFLICT_SUFFIX must not contain a path separator, got {0:?}")]
    BadConflictSuffix(String),
    #[error("FILTERS_FILE {0} does not exist")]
    MissingFilters(PathBuf),
    #[error("VAULT_LOCAL is not set (env or ~/.config/vault-sync/config.env)")]
    MissingLocal,
    #[error("rclone not found; tried: {}", tried.join(", "))]
    RcloneNotFound { tried: Vec<String> },
    #[error("could not run {bin}: {source}")]
    Spawn {
        bin: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error("{path}: {source}")]
    Io {
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}
