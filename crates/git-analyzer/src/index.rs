use std::collections::HashMap;
use std::path::Path;
use std::sync::Mutex;

use rusqlite::{params, Connection, OpenFlags};

use crate::chunk::snippet_text;
use crate::error::Error;
use crate::CodeChunk;

pub const CODE_CHUNKER_VERSION: &str = "1";
pub const CODE_PREFIX_VERSION: &str = "code-1";
pub const CODE_INDEX_KIND: &str = "code";

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct FileId {
    pub repo: String,
    pub path: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodeStamp {
    pub repo: String,
    pub path: String,
    pub mtime_ns: i64,
}

impl CodeStamp {
    pub fn id(&self) -> FileId {
        FileId {
            repo: self.repo.clone(),
            path: self.path.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodePlan {
    pub embed: Vec<FileId>,
    pub delete: Vec<FileId>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CodeHit {
    pub repo: String,
    pub path: String,
    pub start_line: u32,
    pub end_line: u32,
    pub language: String,
    pub symbol: Option<String>,
    pub score: f32,
    pub snippet: String,
    pub text: String,
}

pub struct CodeIndex {
    conn: Mutex<Connection>,
    dim: usize,
}

impl CodeIndex {
    /// Open a code index. Refuses a vault note index without changing it.
    pub fn open(path: &Path, dim: usize) -> Result<Self, Error> {
        if dim == 0 || dim > 4096 {
            return Err(Error::Dimension {
                expected: dim,
                found: 0,
            });
        }
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent)?;
            }
        }
        if path.is_file() {
            let readonly = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;
            if is_foreign_index(&readonly)? {
                return Err(Error::NotCodeIndex(path.to_path_buf()));
            }
        }
        let conn = Connection::open(path)?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS files (
                repo TEXT NOT NULL,
                path TEXT NOT NULL,
                mtime_ns INTEGER NOT NULL,
                PRIMARY KEY (repo, path)
            );
            CREATE TABLE IF NOT EXISTS chunks (
                id INTEGER PRIMARY KEY,
                repo TEXT NOT NULL,
                path TEXT NOT NULL,
                start_line INTEGER NOT NULL,
                end_line INTEGER NOT NULL,
                language TEXT NOT NULL,
                symbol TEXT NOT NULL,
                body TEXT NOT NULL,
                vector BLOB NOT NULL,
                FOREIGN KEY (repo, path) REFERENCES files(repo, path) ON DELETE CASCADE
            );
            CREATE INDEX IF NOT EXISTS chunks_file ON chunks(repo, path);",
        )?;
        ensure_contract(&conn, dim)?;
        Ok(Self {
            conn: Mutex::new(conn),
            dim,
        })
    }

    pub fn dim(&self) -> usize {
        self.dim
    }

    pub fn file_count(&self) -> Result<u64, Error> {
        let conn = self.lock()?;
        Ok(conn.query_row("SELECT COUNT(*) FROM files", [], |row| row.get(0))?)
    }

    pub fn chunk_count(&self) -> Result<u64, Error> {
        let conn = self.lock()?;
        Ok(conn.query_row("SELECT COUNT(*) FROM chunks", [], |row| row.get(0))?)
    }

    pub fn plan(&self, live: &[CodeStamp]) -> Result<CodePlan, Error> {
        let mut seen = HashMap::with_capacity(live.len());
        for stamp in live {
            if seen
                .insert((stamp.repo.clone(), stamp.path.clone()), stamp.mtime_ns)
                .is_some()
            {
                return Err(Error::Duplicate {
                    repo: stamp.repo.clone(),
                    path: stamp.path.clone(),
                });
            }
        }
        let conn = self.lock()?;
        let mut stmt = conn.prepare("SELECT repo, path, mtime_ns FROM files")?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })?;
        let mut stored = HashMap::new();
        for row in rows {
            let (repo, path, mtime) = row?;
            stored.insert((repo, path), mtime);
        }
        let mut embed = Vec::new();
        for ((repo, path), mtime) in &seen {
            match stored.get(&(repo.clone(), path.clone())) {
                Some(old) if old == mtime => {}
                _ => embed.push(FileId {
                    repo: repo.clone(),
                    path: path.clone(),
                }),
            }
        }
        let mut delete = Vec::new();
        for (repo, path) in stored.keys() {
            if !seen.contains_key(&(repo.clone(), path.clone())) {
                delete.push(FileId {
                    repo: repo.clone(),
                    path: path.clone(),
                });
            }
        }
        embed.sort();
        delete.sort();
        Ok(CodePlan { embed, delete })
    }

    pub fn upsert(
        &self,
        stamp: &CodeStamp,
        chunks: &[CodeChunk],
        vectors: &[Vec<f32>],
    ) -> Result<(), Error> {
        if chunks.len() != vectors.len() {
            return Err(Error::Count {
                vectors: vectors.len(),
                chunks: chunks.len(),
            });
        }
        for chunk in chunks {
            if chunk.repo != stamp.repo || chunk.path != stamp.path {
                return Err(Error::ChunkMismatch);
            }
        }
        for vector in vectors {
            check_vector(vector, self.dim)?;
        }
        let conn = self.lock()?;
        let tx = conn.unchecked_transaction()?;
        tx.execute(
            "INSERT INTO files(repo, path, mtime_ns) VALUES (?1, ?2, ?3)
             ON CONFLICT(repo, path) DO UPDATE SET mtime_ns = excluded.mtime_ns",
            params![stamp.repo, stamp.path, stamp.mtime_ns],
        )?;
        tx.execute(
            "DELETE FROM chunks WHERE repo = ?1 AND path = ?2",
            params![stamp.repo, stamp.path],
        )?;
        for (chunk, vector) in chunks.iter().zip(vectors) {
            tx.execute(
                "INSERT INTO chunks(repo, path, start_line, end_line, language, symbol, body, vector)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    stamp.repo,
                    stamp.path,
                    chunk.start_line,
                    chunk.end_line,
                    chunk.language,
                    chunk.symbol.clone().unwrap_or_default(),
                    chunk.text,
                    vec_to_blob(vector),
                ],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn delete(&self, files: &[FileId]) -> Result<(), Error> {
        let conn = self.lock()?;
        let tx = conn.unchecked_transaction()?;
        for file in files {
            tx.execute(
                "DELETE FROM files WHERE repo = ?1 AND path = ?2",
                params![file.repo, file.path],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn search(&self, vector: &[f32], k: usize) -> Result<Vec<CodeHit>, Error> {
        check_vector(vector, self.dim)?;
        if k == 0 {
            return Ok(Vec::new());
        }
        let conn = self.lock()?;
        let mut stmt = conn.prepare(
            "SELECT repo, path, start_line, end_line, language, symbol, body, vector
             FROM chunks ORDER BY repo, path, start_line",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
                row.get::<_, i64>(3)?,
                row.get::<_, String>(4)?,
                row.get::<_, String>(5)?,
                row.get::<_, String>(6)?,
                row.get::<_, Vec<u8>>(7)?,
            ))
        })?;
        let mut scored = Vec::new();
        for row in rows {
            let (repo, path, start_line, end_line, language, symbol, text, blob) = row?;
            let stored = blob_to_vec(&blob);
            if stored.len() != self.dim {
                continue;
            }
            let start_line = u32::try_from(start_line).unwrap_or(0);
            let end_line = u32::try_from(end_line).unwrap_or(0);
            scored.push((
                cosine(vector, &stored),
                repo,
                path,
                start_line,
                end_line,
                language,
                symbol,
                text,
            ));
        }
        scored.sort_by(|a, b| {
            b.0.partial_cmp(&a.0)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.1.cmp(&b.1))
                .then_with(|| a.2.cmp(&b.2))
                .then_with(|| a.3.cmp(&b.3))
        });
        scored.truncate(k);
        Ok(scored
            .into_iter()
            .map(
                |(score, repo, path, start_line, end_line, language, symbol, text)| CodeHit {
                    snippet: snippet_text(&text),
                    symbol: if symbol.is_empty() {
                        None
                    } else {
                        Some(symbol)
                    },
                    repo,
                    path,
                    start_line,
                    end_line,
                    language,
                    score,
                    text,
                },
            )
            .collect())
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Connection>, Error> {
        self.conn.lock().map_err(|_| Error::Poisoned)
    }
}

pub fn clamp_k(k: Option<u32>) -> usize {
    k.unwrap_or(6).clamp(1, 12) as usize
}

pub fn require_index_outside_root(root: &Path, index: &Path) -> Result<(), Error> {
    let root = std::path::absolute(root).unwrap_or_else(|_| root.to_path_buf());
    let index = std::path::absolute(index).unwrap_or_else(|_| index.to_path_buf());
    if index.starts_with(&root) {
        Err(Error::IndexInsideRoot(index))
    } else {
        Ok(())
    }
}

pub fn format_code_hits(hits: &[CodeHit]) -> String {
    if hits.is_empty() {
        return "No matching code.".to_string();
    }
    let mut out = String::new();
    for (i, hit) in hits.iter().enumerate() {
        if i > 0 {
            out.push_str("\n---\n");
        }
        out.push_str("repo: ");
        out.push_str(&hit.repo);
        out.push_str("\npath: ");
        out.push_str(&hit.path);
        out.push_str(&format!("\nlines: {}-{}\n", hit.start_line, hit.end_line));
        out.push_str("language: ");
        out.push_str(&hit.language);
        out.push('\n');
        if let Some(symbol) = &hit.symbol {
            out.push_str("symbol: ");
            out.push_str(symbol);
            out.push('\n');
        }
        out.push_str(&format!("score: {:.3}\n", hit.score));
        out.push_str(&hit.text);
        out.push('\n');
    }
    out
}

fn is_foreign_index(conn: &Connection) -> Result<bool, Error> {
    if table_exists(conn, "notes")? {
        return Ok(true);
    }
    let kind = meta_value(conn, "kind")?;
    if let Some(kind) = kind {
        return Ok(kind != CODE_INDEX_KIND);
    }
    Ok(meta_value(conn, "chunker")?.is_some())
}

fn table_exists(conn: &Connection, name: &str) -> Result<bool, Error> {
    let count: i64 = conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
        params![name],
        |row| row.get(0),
    )?;
    Ok(count > 0)
}

fn meta_value(conn: &Connection, key: &str) -> Result<Option<String>, Error> {
    if !table_exists(conn, "meta")? {
        return Ok(None);
    }
    let mut stmt = conn.prepare("SELECT value FROM meta WHERE key = ?1")?;
    let mut rows = stmt.query(params![key])?;
    if let Some(row) = rows.next()? {
        Ok(Some(row.get(0)?))
    } else {
        Ok(None)
    }
}

fn ensure_contract(conn: &Connection, dim: usize) -> Result<(), Error> {
    let mut existing = HashMap::new();
    let mut stmt = conn.prepare("SELECT key, value FROM meta")?;
    let rows = stmt.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
    })?;
    for row in rows {
        let (key, value) = row?;
        existing.insert(key, value);
    }
    drop(stmt);
    let want = [
        ("kind", CODE_INDEX_KIND.to_string()),
        ("dim", dim.to_string()),
        ("chunker", CODE_CHUNKER_VERSION.to_string()),
        ("prefix", CODE_PREFIX_VERSION.to_string()),
    ];
    if existing.is_empty() {
        for (key, value) in want {
            conn.execute(
                "INSERT INTO meta(key, value) VALUES (?1, ?2)",
                params![key, value],
            )?;
        }
        return Ok(());
    }
    for (key, value) in want {
        match existing.get(key).map(String::as_str) {
            Some(found) if found == value => {}
            Some(found) => {
                return Err(Error::Contract {
                    key,
                    expected: value,
                    found: found.to_string(),
                });
            }
            None => {
                return Err(Error::Contract {
                    key,
                    expected: value,
                    found: "missing".to_string(),
                });
            }
        }
    }
    Ok(())
}

fn check_vector(vector: &[f32], dim: usize) -> Result<(), Error> {
    if vector.len() != dim {
        return Err(Error::Dimension {
            expected: dim,
            found: vector.len(),
        });
    }
    if vector.iter().any(|value| !value.is_finite()) {
        return Err(Error::NonFinite);
    }
    Ok(())
}

fn cosine(left: &[f32], right: &[f32]) -> f32 {
    let mut dot = 0.0;
    let mut left_norm = 0.0;
    let mut right_norm = 0.0;
    for (x, y) in left.iter().zip(right) {
        dot += x * y;
        left_norm += x * x;
        right_norm += y * y;
    }
    let denom = left_norm.sqrt() * right_norm.sqrt();
    if denom == 0.0 {
        0.0
    } else {
        dot / denom
    }
}

fn vec_to_blob(vector: &[f32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(vector.len() * 4);
    for value in vector {
        out.extend_from_slice(&value.to_le_bytes());
    }
    out
}

fn blob_to_vec(blob: &[u8]) -> Vec<f32> {
    let (chunks, _) = blob.as_chunks::<4>();
    chunks.iter().copied().map(f32::from_le_bytes).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(repo: &str, path: &str, text: &str) -> CodeChunk {
        CodeChunk {
            repo: repo.to_string(),
            path: path.to_string(),
            start_line: 1,
            end_line: 1,
            language: "rust".to_string(),
            symbol: Some("answer".to_string()),
            text: text.to_string(),
        }
    }

    #[test]
    fn ranks_the_closer_file_and_skips_unchanged_mtime() {
        let dir = tempfile::tempdir().unwrap();
        let index = CodeIndex::open(&dir.path().join("code.db"), 2).unwrap();
        let power = sample("demo", "src/power.rs", "gpio power relay");
        let salad = sample("demo", "src/salad.rs", "chop tomatoes");
        index
            .upsert(
                &CodeStamp {
                    repo: "demo".into(),
                    path: "src/power.rs".into(),
                    mtime_ns: 10,
                },
                &[power],
                &[vec![1.0, 0.0]],
            )
            .unwrap();
        index
            .upsert(
                &CodeStamp {
                    repo: "demo".into(),
                    path: "src/salad.rs".into(),
                    mtime_ns: 10,
                },
                &[salad],
                &[vec![0.0, 1.0]],
            )
            .unwrap();
        let hits = index.search(&[1.0, 0.1], 6).unwrap();
        assert_eq!(hits[0].path, "src/power.rs");
        assert_eq!(hits[0].repo, "demo");
        assert_eq!(hits[0].symbol.as_deref(), Some("answer"));
        assert!(hits[0].score > hits[1].score);
        assert!(format_code_hits(&hits).contains("lines: 1-1"));

        let live = vec![
            CodeStamp {
                repo: "demo".into(),
                path: "src/power.rs".into(),
                mtime_ns: 10,
            },
            CodeStamp {
                repo: "demo".into(),
                path: "src/salad.rs".into(),
                mtime_ns: 11,
            },
        ];
        let plan = index.plan(&live).unwrap();
        assert_eq!(plan.embed.len(), 1);
        assert_eq!(plan.embed[0].path, "src/salad.rs");
        assert!(plan.delete.is_empty());
    }

    #[test]
    fn refuses_a_vault_note_index_and_a_different_dimension() {
        let dir = tempfile::tempdir().unwrap();
        let vault = dir.path().join("vault.db");
        let conn = Connection::open(&vault).unwrap();
        conn.execute_batch(
            "CREATE TABLE notes (path TEXT PRIMARY KEY, mtime_ns INTEGER NOT NULL);
             CREATE TABLE meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
             INSERT INTO meta(key, value) VALUES ('chunker', '1');",
        )
        .unwrap();
        drop(conn);
        let err = match CodeIndex::open(&vault, 4) {
            Err(err) => err,
            Ok(_) => panic!("vault note index should be refused"),
        };
        assert!(err.to_string().contains("refusing"));
        let still = Connection::open(&vault).unwrap();
        let tables: i64 = still
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE name = 'files'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(tables, 0);

        let code = dir.path().join("code.db");
        CodeIndex::open(&code, 4).unwrap();
        let err = match CodeIndex::open(&code, 8) {
            Err(err) => err,
            Ok(_) => panic!("reopen with another dimension should fail"),
        };
        assert!(err.to_string().contains("dim"));
    }

    #[test]
    fn index_must_sit_outside_the_projects_root() {
        let dir = tempfile::tempdir().unwrap();
        let root = dir.path().join("projects");
        std::fs::create_dir_all(&root).unwrap();
        assert!(require_index_outside_root(&root, &root.join("index.db")).is_err());
        assert!(require_index_outside_root(&root, &dir.path().join("code.db")).is_ok());
    }
}
