use std::collections::{HashMap, HashSet};
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
        // k is a note count. A second chunk of the same path must not crowd out another note.
        let mut seen = HashSet::new();
        let mut kept = Vec::new();
        for item in scored {
            if seen.insert(item.2.clone()) {
                kept.push(item);
                if kept.len() == k {
                    break;
                }
            }
        }
        Ok(kept
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

/// `vault-searchd query` when `--k` is omitted. Nitro5.
/// TowerX sets `VAULT_SEARCH_K_DEFAULT` on the daemon.
pub const DAEMON_DEFAULT_K: u32 = 6;
/// Hard cap for `vault-searchd query`. Nitro5.
/// TowerX sets `VAULT_SEARCH_K_MAX` on the daemon.
pub const DAEMON_MAX_K: u32 = 12;
/// MCP `search` and the chat proxy when `k` is omitted. Nitro5.
/// TowerX sets `VAULT_SEARCH_K_DEFAULT` on llama-server and the proxy.
pub const MCP_DEFAULT_K: u32 = 4;
/// MCP `search` and the chat proxy hard cap. Nitro5.
/// TowerX sets `VAULT_SEARCH_K_MAX`.
pub const MCP_MAX_K: u32 = 4;

/// Apply an optional env override. Values below 1 are ignored.
pub fn resolve_k_bounds(
    builtin_default: u32,
    builtin_max: u32,
    default_env: Option<u32>,
    max_env: Option<u32>,
) -> (u32, u32) {
    let max_k = max_env.filter(|n| *n >= 1).unwrap_or(builtin_max).max(1);
    let default_k = default_env
        .filter(|n| *n >= 1)
        .unwrap_or(builtin_default)
        .clamp(1, max_k);
    (default_k, max_k)
}

pub fn k_bounds(builtin_default: u32, builtin_max: u32) -> (u32, u32) {
    resolve_k_bounds(
        builtin_default,
        builtin_max,
        env_u32("VAULT_SEARCH_K_DEFAULT"),
        env_u32("VAULT_SEARCH_K_MAX"),
    )
}

pub fn clamp_k(k: Option<u32>) -> usize {
    let (default_k, max_k) = k_bounds(DAEMON_DEFAULT_K, DAEMON_MAX_K);
    k.unwrap_or(default_k).clamp(1, max_k) as usize
}

fn env_u32(name: &str) -> Option<u32> {
    let raw = std::env::var(name).ok()?;
    let value = raw.trim().parse::<u32>().ok()?;
    (value >= 1).then_some(value)
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

/// Character budget for text pasted into the chat model. About 1,000 tokens,
/// so one tool result cannot fill an 8,192-token window. Nitro5.
/// TowerX sets `VAULT_SEARCH_RESULT_CHARS` on llama-server.
pub const MODEL_RESULT_CHARS: usize = 4_000;

pub fn model_result_limit() -> usize {
    std::env::var("VAULT_SEARCH_RESULT_CHARS")
        .ok()
        .and_then(|raw| raw.trim().parse::<usize>().ok())
        .filter(|n| *n >= 200)
        .unwrap_or(MODEL_RESULT_CHARS)
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

/// Keep whole hits while they fit. A result that is still too long is cut, and
/// the model is told that later matches were left out.
pub fn limit_model_text(text: &str, max_chars: usize) -> String {
    let max_chars = max_chars.max(200);
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    let note = "\n\nFurther matches omitted so this result does not fill the context window.\n";
    let budget = max_chars.saturating_sub(note.chars().count());
    let parts: Vec<&str> = text.split("\n---\n").collect();
    let mut out = String::new();
    let mut used = 0usize;
    let mut shown = 0usize;
    for part in &parts {
        let sep = if shown == 0 { 0 } else { 5 };
        let part_len = part.chars().count();
        if used + sep + part_len <= budget {
            if shown > 0 {
                out.push_str("\n---\n");
            }
            out.push_str(part);
            used += sep + part_len;
            shown += 1;
            continue;
        }
        if shown == 0 {
            let keep = budget.saturating_sub(1);
            let truncated: String = part.chars().take(keep).collect();
            out.push_str(truncated.trim_end());
            out.push('…');
            shown = 1;
        }
        break;
    }
    if shown < parts.len() {
        out.push_str(note);
    }
    if out.chars().count() > max_chars {
        out = out.chars().take(max_chars).collect();
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
        assert_eq!(hits.len(), 2);

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
    fn search_keeps_one_chunk_per_note() {
        let (_dir, index) = temp_index(2);
        let best = Chunk {
            title: "P".into(),
            heading: "A".into(),
            text: "plan a".into(),
        };
        let other = Chunk {
            title: "P".into(),
            heading: "B".into(),
            text: "plan b".into(),
        };
        let phone = Chunk {
            title: "Phone".into(),
            heading: "note4c".into(),
            text: "phone".into(),
        };
        index
            .upsert(
                "Notes/plan.md",
                1,
                &[best, other],
                &[vec![1.0, 0.0], vec![0.9, 0.1]],
            )
            .unwrap();
        index
            .upsert(
                "Inventory/Phones/note4c.md",
                1,
                &[phone],
                &[vec![0.8, 0.2]],
            )
            .unwrap();
        let hits = index.search(&[1.0, 0.0], 2).unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].path, "Notes/plan.md");
        assert_eq!(hits[0].text, "plan a");
        assert_eq!(hits[1].path, "Inventory/Phones/note4c.md");
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
    fn k_bounds_stay_on_the_nitro_defaults_unless_overridden() {
        assert_eq!(
            resolve_k_bounds(DAEMON_DEFAULT_K, DAEMON_MAX_K, None, None),
            (6, 12)
        );
        assert_eq!(resolve_k_bounds(MCP_DEFAULT_K, MCP_MAX_K, None, None), (4, 4));
        assert_eq!(
            resolve_k_bounds(MCP_DEFAULT_K, MCP_MAX_K, Some(10), Some(15)),
            (10, 15)
        );
        assert_eq!(
            resolve_k_bounds(DAEMON_DEFAULT_K, DAEMON_MAX_K, Some(10), Some(15)),
            (10, 15)
        );
        assert_eq!(resolve_k_bounds(6, 12, Some(0), None), (6, 12));
        assert_eq!(clamp_k(None), 6);
        assert_eq!(clamp_k(Some(0)), 1);
        assert_eq!(clamp_k(Some(12)), 12);
        assert_eq!(clamp_k(Some(99)), 12);
        assert_eq!(MODEL_RESULT_CHARS, 4_000);
        assert_eq!(model_result_limit(), 4_000);
    }

    #[test]
    fn model_text_keeps_only_the_hits_that_fit() {
        let hit = |n: &str| format!("path: Notes/{n}.md\nscore: 0.500\n{}\n", "x".repeat(1500));
        let text = format!("{}\n---\n{}\n---\n{}", hit("A"), hit("B"), hit("C"));
        let limited = limit_model_text(&text, 4000);
        assert!(limited.chars().count() <= 4000);
        assert!(limited.contains("Notes/A.md"));
        assert!(limited.contains("Notes/B.md"));
        assert!(!limited.contains("Notes/C.md"));
        assert!(limited.contains("omitted"));
        assert_eq!(limit_model_text("short", 4000), "short");
    }

    #[test]
    fn truncates_matryoshka_vectors() {
        let v = fit_dim(vec![3.0, 0.0, 9.0], 2).unwrap();
        assert_eq!(v.len(), 2);
        let norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((norm - 1.0).abs() < 1e-5);
    }
}
