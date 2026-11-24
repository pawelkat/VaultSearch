//! Desktop git analyzer for EmbeddingGemma code search.
//!
//! Lists the working tree of tracked files (`git ls-files`), chunks them, and
//! stores vectors the caller computed. It does not load a model, and it does
//! not use the vault Markdown chunker or the vault task prefixes.
//!
//! Query text: `task: code retrieval | query: {query}`
//! Document text: `title: {symbol or filename} | text: {code}`

mod chunk;
mod collect;
mod error;
mod index;

pub use chunk::{
    chunk_code, code_document_prefix, code_query_prefix, language_of, CodeChunk,
    CODE_MAX_CHUNK_CHARS, CODE_OVERLAP_LINES, CODE_WINDOW_LINES,
};
pub use collect::{
    collect_tracked_files, indexable_code_path, read_code_text, TrackedFile, CODE_MAX_FILE_BYTES,
};
pub use error::Error;
pub use index::{
    clamp_k, format_code_hits, require_index_outside_root, CodeHit, CodeIndex, CodePlan, CodeStamp,
    FileId, CODE_CHUNKER_VERSION, CODE_INDEX_KIND, CODE_PREFIX_VERSION,
};
