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
    #[error("duplicate file in plan: {repo}/{path}")]
    Duplicate { repo: String, path: String },
    #[error("{vectors} vectors for {chunks} chunks")]
    Count { vectors: usize, chunks: usize },
    #[error("chunk identity does not match the file being stored")]
    ChunkMismatch,
    #[error("vector contains a non-finite value")]
    NonFinite,
    #[error("embedding shorter than {dim} dimensions")]
    ShortVector { dim: usize },
    #[error("index lock poisoned")]
    Poisoned,
    #[error("code index must stay outside the projects root: {0}")]
    IndexInsideRoot(PathBuf),
    #[error("refusing to open this file as a code index: {0}")]
    NotCodeIndex(PathBuf),
    #[error("git: {0}")]
    Git(String),
}
