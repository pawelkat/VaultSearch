use std::path::PathBuf;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("sqlite: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("index dimension is {found}, this index expects {expected}")]
    Dimension { expected: usize, found: usize },
    #[error("index {key} is {found}, this build expects {expected}")]
    Contract {
        key: &'static str,
        expected: String,
        found: String,
    },
    #[error("path is not an indexable vault note: {0}")]
    Path(String),
    #[error("duplicate path in plan: {0}")]
    Duplicate(String),
    #[error("{vectors} vectors for {chunks} chunks")]
    Count { vectors: usize, chunks: usize },
    #[error("vector contains a non-finite value")]
    NonFinite,
    #[error("embedding shorter than {dim} dimensions")]
    ShortVector { dim: usize },
    #[error("index lock poisoned")]
    Poisoned,
    #[error("index file must stay outside the vault: {0}")]
    IndexInsideVault(PathBuf),
}
