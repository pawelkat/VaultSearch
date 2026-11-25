//! Shared vault search core.
//!
//! Callers supply embedding vectors. This crate chunks Markdown, stores the
//! vectors, and ranks them. It does not load EmbeddingGemma and it does not
//! speak MCP.

mod chunk;
mod error;
mod index;
mod ipc;
mod prefix;

pub use chunk::{
    chunk_markdown, filename_stem, indexable_bytes, indexable_path, snippet, Chunk,
    CHUNKER_VERSION, MAX_CHUNK_CHARS, MAX_FILE_BYTES, PREFIX_VERSION, SNIPPET_CHARS,
};
pub use error::Error;
pub use index::{
    clamp_k, fit_dim, format_hits, is_inside, k_bounds, limit_model_text, model_result_limit,
    require_index_outside_vault, resolve_k_bounds, Hit, Index, Plan, Stamp, DAEMON_DEFAULT_K,
    DAEMON_MAX_K, MCP_DEFAULT_K, MCP_MAX_K, MODEL_RESULT_CHARS,
};
pub use ipc::{
    roundtrip, roundtrip_with_timeout, IpcHit, IpcRequest, IpcResponse, IpcSyncStatus,
    IpcWatchStatus,
};
pub use prefix::{document_prefix, query_prefix};
