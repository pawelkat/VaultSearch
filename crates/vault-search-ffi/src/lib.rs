use std::path::Path;
use std::sync::Arc;

use vault_search::{chunk_markdown, document_prefix, query_prefix, Chunk, Index, Stamp};

uniffi::setup_scaffolding!();

#[derive(Debug, thiserror::Error, uniffi::Error)]
pub enum FfiError {
    #[error("{reason}")]
    Search { reason: String },
}

fn map_err(err: impl ToString) -> FfiError {
    FfiError::Search {
        reason: err.to_string(),
    }
}

#[derive(uniffi::Record)]
pub struct ChunkRecord {
    pub title: String,
    pub heading: String,
    pub text: String,
}

#[derive(uniffi::Record)]
pub struct StampRecord {
    pub path: String,
    pub mtime_ns: i64,
}

#[derive(uniffi::Record)]
pub struct PlanRecord {
    pub embed: Vec<String>,
    pub delete: Vec<String>,
}

#[derive(uniffi::Record)]
pub struct HitRecord {
    pub path: String,
    pub heading: String,
    pub title: String,
    pub score: f32,
    pub snippet: String,
    pub text: String,
}

#[uniffi::export]
pub fn search_query_prefix(query: String) -> String {
    query_prefix(&query)
}

#[uniffi::export]
pub fn search_document_prefix(title: String, text: String) -> String {
    document_prefix(&title, &text)
}

#[uniffi::export]
pub fn chunk_note(rel_path: String, source: String) -> Vec<ChunkRecord> {
    chunk_markdown(&rel_path, &source)
        .into_iter()
        .map(|chunk| ChunkRecord {
            title: chunk.title,
            heading: chunk.heading,
            text: chunk.text,
        })
        .collect()
}

#[derive(uniffi::Object)]
pub struct VaultIndex(Index);

#[uniffi::export]
impl VaultIndex {
    #[uniffi::constructor]
    pub fn open(path: String, dim: u32) -> Result<Arc<Self>, FfiError> {
        Index::open(Path::new(&path), dim as usize)
            .map(|index| Arc::new(Self(index)))
            .map_err(map_err)
    }

    pub fn plan(&self, live: Vec<StampRecord>) -> Result<PlanRecord, FfiError> {
        let stamps = live
            .into_iter()
            .map(|stamp| Stamp {
                path: stamp.path,
                mtime_ns: stamp.mtime_ns,
            })
            .collect::<Vec<_>>();
        let plan = self.0.plan(&stamps).map_err(map_err)?;
        Ok(PlanRecord {
            embed: plan.embed,
            delete: plan.delete,
        })
    }

    pub fn upsert(
        &self,
        path: String,
        mtime_ns: i64,
        chunks: Vec<ChunkRecord>,
        vectors: Vec<Vec<f32>>,
    ) -> Result<(), FfiError> {
        let chunks = chunks
            .into_iter()
            .map(|chunk| Chunk {
                title: chunk.title,
                heading: chunk.heading,
                text: chunk.text,
            })
            .collect::<Vec<_>>();
        self.0
            .upsert(&path, mtime_ns, &chunks, &vectors)
            .map_err(map_err)
    }

    pub fn search(&self, vector: Vec<f32>, k: u32) -> Result<Vec<HitRecord>, FfiError> {
        self.0
            .search(&vector, k as usize)
            .map_err(map_err)
            .map(|hits| {
                hits.into_iter()
                    .map(|hit| HitRecord {
                        path: hit.path,
                        heading: hit.heading,
                        title: hit.title,
                        score: hit.score,
                        snippet: hit.snippet,
                        text: hit.text,
                    })
                    .collect()
            })
    }
}
