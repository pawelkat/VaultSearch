use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use rusqlite::{params, Connection};

use crate::chunk::{snippet, CHUNKER_VERSION, PREFIX_VERSION};
use crate::error::Error;
use crate::Chunk;

#[derive(Debug, Clone, PartialEq)]
pub struct Stamp {
    pub path: String,
    pub mtime_ns: i64,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Plan {
    pub embed: Vec<String>,
    pub delete: Vec<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Hit {
    pub path: String,
    pub heading: String,
    pub title: String,
    pub score: f32,
    pub snippet: String,
    pub text: String,
}

pub struct Index {
    conn: Mutex<Connection>,
    dim: usize,
}

impl Index {
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
        let conn = Connection::open(path)?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS notes (
                path TEXT PRIMARY KEY,
                mtime_ns INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS chunks (
                id INTEGER PRIMARY KEY,
                path TEXT NOT NULL REFERENCES notes(path) ON DELETE CASCADE,
                ord INTEGER NOT NULL,
                heading TEXT NOT NULL,
                title TEXT NOT NULL,
                body TEXT NOT NULL,
                vector BLOB NOT NULL
            );
            CREATE INDEX IF NOT EXISTS chunks_path ON chunks(path);",
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

    pub fn note_count(&self) -> Result<u64, Error> {
        let conn = self.lock()?;
        Ok(conn.query_row("SELECT COUNT(*) FROM notes", [], |r| r.get(0))?)
    }

    pub fn chunk_count(&self) -> Result<u64, Error> {
        let conn = self.lock()?;
        Ok(conn.query_row("SELECT COUNT(*) FROM chunks", [], |r| r.get(0))?)
    }

    pub fn plan(&self, live: &[Stamp]) -> Result<Plan, Error> {
        let mut seen = HashMap::with_capacity(live.len());
        for stamp in live {
            if !crate::indexable_path(&stamp.path) {
                return Err(Error::Path(stamp.path.clone()));
            }
            if seen.insert(stamp.path.clone(), stamp.mtime_ns).is_some() {
                return Err(Error::Duplicate(stamp.path.clone()));
            }
        }
        let conn = self.lock()?;
        let mut stmt = conn.prepare("SELECT path, mtime_ns FROM notes")?;
        let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?)))?;
        let mut stored = HashMap::new();
        for row in rows {
            let (path, mtime) = row?;
            stored.insert(path, mtime);
        }
        let mut embed = Vec::new();
        for (path, mtime) in &seen {
            match stored.get(path) {
                Some(old) if old == mtime => {}
                _ => embed.push(path.clone()),
            }
        }
        let mut delete = Vec::new();
        for path in stored.keys() {
            if !seen.contains_key(path) {
                delete.push(path.clone());
            }
        }
        embed.sort();
        delete.sort();
        Ok(Plan { embed, delete })
    }

    pub fn upsert(
        &self,
        path: &str,
        mtime_ns: i64,
        chunks: &[Chunk],
        vectors: &[Vec<f32>],
    ) -> Result<(), Error> {
        if !crate::indexable_path(path) {
            return Err(Error::Path(path.to_string()));
        }
        if chunks.len() != vectors.len() {
            return Err(Error::Count {
                vectors: vectors.len(),
                chunks: chunks.len(),
            });
        }
        for vector in vectors {
            check_vector(vector, self.dim)?;
        }
        let conn = self.lock()?;
        let tx = conn.unchecked_transaction()?;
        tx.execute(
            "INSERT INTO notes(path, mtime_ns) VALUES (?1, ?2)
             ON CONFLICT(path) DO UPDATE SET mtime_ns = excluded.mtime_ns",
            params![path, mtime_ns],
        )?;
        tx.execute("DELETE FROM chunks WHERE path = ?1", params![path])?;
        for (ord, (chunk, vector)) in chunks.iter().zip(vectors).enumerate() {
            tx.execute(
                "INSERT INTO chunks(path, ord, heading, title, body, vector)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
                params![
                    path,
                    ord as i64,
                    chunk.heading,
                    chunk.title,
                    chunk.text,
                    vec_to_blob(vector)
                ],
            )?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn delete(&self, paths: &[String]) -> Result<(), Error> {
        let conn = self.lock()?;
        let tx = conn.unchecked_transaction()?;
        for path in paths {
            tx.execute("DELETE FROM notes WHERE path = ?1", params![path])?;
        }
        tx.commit()?;
        Ok(())
    }

    pub fn search(&self, vector: &[f32], k: usize) -> Result<Vec<Hit>, Error> {
        check_vector(vector, self.dim)?;
        if k == 0 {
            return Ok(Vec::new());
        }
        let conn = self.lock()?;
        let mut stmt = conn.prepare(
            "SELECT path, heading, title, body, vector, ord FROM chunks ORDER BY path, ord",
        )?;
        let rows = stmt.query_map([], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, Vec<u8>>(4)?,
                r.get::<_, i64>(5)?,
            ))
        })?;
        let mut scored = Vec::new();
        for row in rows {
            let (path, heading, title, text, blob, ord) = row?;
            let stored = blob_to_vec(&blob);
            if stored.len() != self.dim {
                continue;
            }
            scored.push((cosine(vector, &stored), ord, path, heading, title, text));
        }
        scored.sort_by(|a, b| {
            b.0.partial_cmp(&a.0)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.2.cmp(&b.2))
                .then_with(|| a.1.cmp(&b.1))
        });
        scored.truncate(k);
        Ok(scored
            .into_iter()
            .map(|(score, _, path, heading, title, text)| Hit {
                snippet: snippet(&text),
                path,
                heading,
                title,
                score,
                text,
            })
            .collect())
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, Connection>, Error> {
        self.conn.lock().map_err(|_| Error::Poisoned)
    }
}

pub fn clamp_k(k: Option<u32>) -> usize {
    k.unwrap_or(6).clamp(1, 12) as usize
}

pub fn fit_dim(mut vector: Vec<f32>, dim: usize) -> Result<Vec<f32>, Error> {
    if vector.len() < dim {
        return Err(Error::ShortVector { dim });
    }
    vector.truncate(dim);
    if vector.iter().any(|v| !v.is_finite()) {
        return Err(Error::NonFinite);
    }
    l2_normalize(&mut vector);
    Ok(vector)
}

pub fn format_hits(hits: &[Hit]) -> String {
    if hits.is_empty() {
        return "No matching notes.".to_string();
    }
    let mut out = String::new();
    for (i, hit) in hits.iter().enumerate() {
        if i > 0 {
            out.push_str("\n---\n");
        }
        out.push_str("path: ");
        out.push_str(&hit.path);
        out.push('\n');
        if !hit.heading.is_empty() {
            out.push_str("heading: ");
            out.push_str(&hit.heading);
            out.push('\n');
        }
        out.push_str(&format!("score: {:.3}\n", hit.score));
        out.push_str(&hit.text);
        out.push('\n');
    }
    out
}

pub fn is_inside(parent: &Path, child: &Path) -> bool {
    let Ok(parent) = std::path::absolute(parent) else {
        return false;
    };
    let Ok(child) = std::path::absolute(child) else {
        return false;
    };
    child.starts_with(parent)
}

pub fn require_index_outside_vault(vault: &Path, index: &Path) -> Result<(), Error> {
    if is_inside(vault, index) {
        Err(Error::IndexInsideVault(PathBuf::from(index)))
    } else {
        Ok(())
    }
}

fn ensure_contract(conn: &Connection, dim: usize) -> Result<(), Error> {
    let mut existing = HashMap::new();
    let mut stmt = conn.prepare("SELECT key, value FROM meta")?;
    let rows = stmt.query_map([], |r| Ok((r.get::<_, String>(0)?, r.get::<_, String>(1)?)))?;
    for row in rows {
        let (k, v) = row?;
        existing.insert(k, v);
    }
    drop(stmt);
    let want = [
        ("dim", dim.to_string()),
        ("chunker", CHUNKER_VERSION.to_string()),
        ("prefix", PREFIX_VERSION.to_string()),
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
    if vector.iter().any(|v| !v.is_finite()) {
        return Err(Error::NonFinite);
    }
    Ok(())
}

fn l2_normalize(vector: &mut [f32]) {
    let norm = vector.iter().map(|v| v * v).sum::<f32>().sqrt();
    if norm > 0.0 {
        for v in vector {
            *v /= norm;
        }
    }
}

fn cosine(a: &[f32], b: &[f32]) -> f32 {
    let mut dot = 0.0;
    let mut na = 0.0;
    let mut nb = 0.0;
    for (x, y) in a.iter().zip(b) {
        dot += x * y;
        na += x * x;
        nb += y * y;
    }
    let denom = na.sqrt() * nb.sqrt();
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
    use crate::Chunk;

    fn temp_index(dim: usize) -> (tempfile::TempDir, Index) {
        let dir = tempfile::tempdir().unwrap();
        let index = Index::open(&dir.path().join("index.db"), dim).unwrap();
        (dir, index)
    }

    #[test]
    fn ranks_the_closer_note_and_skips_unchanged_mtime() {
        let (_dir, index) = temp_index(2);
        let a = Chunk {
            title: "A".into(),
            heading: "A".into(),
            text: "alpha".into(),
        };
        let b = Chunk {
            title: "B".into(),
            heading: "".into(),
            text: "beta".into(),
        };
        index
            .upsert("Notes/A.md", 10, &[a], &[vec![1.0, 0.0]])
            .unwrap();
        index
            .upsert("Notes/B.md", 10, &[b], &[vec![0.0, 1.0]])
            .unwrap();
        let hits = index.search(&[1.0, 0.1], 6).unwrap();
        assert_eq!(hits[0].path, "Notes/A.md");
        assert!(hits[0].score > hits[1].score);
        assert_eq!(hits[0].snippet, "alpha");

        let live = vec![
            Stamp {
                path: "Notes/A.md".into(),
                mtime_ns: 10,
            },
            Stamp {
                path: "Notes/B.md".into(),
                mtime_ns: 11,
            },
        ];
        let plan = index.plan(&live).unwrap();
        assert_eq!(plan.embed, vec!["Notes/B.md".to_string()]);
        assert!(plan.delete.is_empty());
    }

    #[test]
    fn refuses_a_different_dimension_and_an_index_inside_the_vault() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("index.db");
        Index::open(&path, 4).unwrap();
        let err = match Index::open(&path, 8) {
            Err(err) => err,
            Ok(_) => panic!("reopen with another dimension should fail"),
        };
        assert!(err.to_string().contains("dim"));
        let vault = dir.path().join("Vault");
        std::fs::create_dir_all(vault.join("Notes")).unwrap();
        let inside = vault.join("index.db");
        assert!(require_index_outside_vault(&vault, &inside).is_err());
        assert!(require_index_outside_vault(&vault, &dir.path().join("outside.db")).is_ok());
    }

    #[test]
    fn truncates_matryoshka_vectors() {
        let v = fit_dim(vec![3.0, 0.0, 9.0], 2).unwrap();
        assert_eq!(v.len(), 2);
        let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 1e-5);
    }
}
