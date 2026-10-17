mod embed;

use std::collections::HashMap;
use std::io::{BufRead, BufReader, Write};
use std::os::unix::fs::MetadataExt;
use std::os::unix::fs::PermissionsExt;
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, UNIX_EPOCH};

use clap::{Parser, Subcommand};

use vault_search::{
    chunk_markdown, document_prefix, format_hits, indexable_bytes, indexable_path, query_prefix,
    require_index_outside_vault, roundtrip, Chunk, Hit, Index, IpcHit, IpcRequest, IpcResponse,
    Stamp, MAX_FILE_BYTES,
};

use crate::embed::HttpEmbedder;

#[derive(Parser)]
#[command(name = "vault-searchd", about = "Keep a vault embedding index warm")]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Watch the vault, embed changed notes, and serve the unix socket.
    Serve(ServeArgs),
    /// Ask a running daemon one question and print the hits.
    Query(QueryArgs),
}

#[derive(clap::Args)]
struct ServeArgs {
    /// Obsidian vault root. Read-only.
    #[arg(long)]
    vault: PathBuf,
    /// sqlite file. Must not live inside the vault.
    #[arg(long)]
    index: PathBuf,
    #[arg(long, default_value = "http://127.0.0.1:8081/v1/embeddings")]
    embed_url: String,
    /// Stored width. 768 on this PC. VaultHome on the phone stays at 256 in its own file.
    #[arg(long, default_value_t = 768)]
    dim: usize,
    /// Defaults to $XDG_RUNTIME_DIR/vault-search.sock, else /tmp/vault-search-$UID.sock.
    #[arg(long)]
    socket: Option<PathBuf>,
}

#[derive(clap::Args)]
struct QueryArgs {
    query: String,
    #[arg(long)]
    k: Option<u32>,
    #[arg(long)]
    socket: Option<PathBuf>,
}

fn main() {
    let cli = Cli::parse();
    let result = match cli.cmd {
        Cmd::Serve(args) => serve(args),
        Cmd::Query(args) => query(args),
    };
    if let Err(err) = result {
        eprintln!("vault-searchd: {err}");
        std::process::exit(1);
    }
}

fn query(args: QueryArgs) -> Result<(), String> {
    let socket = args.socket.unwrap_or_else(default_socket);
    let response = roundtrip(
        &socket,
        &IpcRequest::Search {
            query: args.query,
            k: args.k,
        },
    )?;
    if !response.ok {
        return Err(response.error.unwrap_or_else(|| "search failed".into()));
    }
    println!("{}", response.text.unwrap_or_default());
    Ok(())
}

struct App {
    vault: PathBuf,
    dim: usize,
    index: Index,
    embedder: HttpEmbedder,
    indexing: AtomicBool,
    last_error: Mutex<Option<String>>,
}

fn serve(args: ServeArgs) -> Result<(), String> {
    let vault = std::path::absolute(&args.vault).map_err(|e| e.to_string())?;
    if !vault.is_dir() {
        return Err(format!("vault is not a directory: {}", vault.display()));
    }
    require_index_outside_vault(&vault, &args.index).map_err(|e| e.to_string())?;
    let index = Index::open(&args.index, args.dim).map_err(|e| e.to_string())?;
    let embedder = HttpEmbedder::new(&args.embed_url)?;
    let socket = args.socket.unwrap_or_else(default_socket);
    if let Some(parent) = socket.parent() {
        if !parent.as_os_str().is_empty() {
            std::fs::create_dir_all(parent).map_err(|e| e.to_string())?;
        }
    }
    prepare_socket(&socket)?;
    let listener =
        UnixListener::bind(&socket).map_err(|e| format!("bind {}: {e}", socket.display()))?;
    std::fs::set_permissions(&socket, std::fs::Permissions::from_mode(0o600))
        .map_err(|e| e.to_string())?;
    let app = Arc::new(App {
        vault,
        dim: args.dim,
        index,
        embedder,
        indexing: AtomicBool::new(false),
        last_error: Mutex::new(None),
    });
    let scanner = Arc::clone(&app);
    std::thread::spawn(move || loop {
        if let Err(err) = reindex(&scanner) {
            eprintln!("vault-searchd: reindex: {err}");
            *scanner.last_error.lock().expect("error lock") = Some(err);
        }
        std::thread::sleep(Duration::from_secs(60));
    });
    eprintln!(
        "vault-searchd listening on {} (dim {})",
        socket.display(),
        args.dim
    );
    for conn in listener.incoming() {
        let stream = match conn {
            Ok(stream) => stream,
            Err(err) => {
                eprintln!("vault-searchd: accept: {err}");
                continue;
            }
        };
        let app = Arc::clone(&app);
        std::thread::spawn(move || {
            if let Err(err) = handle_conn(&app, stream) {
                eprintln!("vault-searchd: {err}");
            }
        });
    }
    Ok(())
}

fn handle_conn(app: &App, stream: UnixStream) -> Result<(), String> {
    let mut reader = BufReader::new(&stream);
    let mut line = String::new();
    reader.read_line(&mut line).map_err(|e| e.to_string())?;
    drop(reader);
    let request: IpcRequest = serde_json::from_str(&line).map_err(|e| e.to_string())?;
    let response = match request {
        IpcRequest::Search { query, k } => search(app, &query, k),
        IpcRequest::Status => Ok(status(app)),
        IpcRequest::Reindex => reindex(app).map(|_| status(app)),
    };
    let response = match response {
        Ok(response) => response,
        Err(err) => IpcResponse {
            ok: false,
            error: Some(err),
            text: None,
            hits: None,
            notes: None,
            chunks: None,
            indexing: Some(app.indexing.load(Ordering::Relaxed)),
            dim: Some(app.dim as u32),
        },
    };
    let mut line = serde_json::to_string(&response).map_err(|e| e.to_string())?;
    line.push('\n');
    (&stream)
        .write_all(line.as_bytes())
        .map_err(|e| e.to_string())?;
    Ok(())
}

fn search(app: &App, query: &str, k: Option<u32>) -> Result<IpcResponse, String> {
    let query = query.trim();
    if query.is_empty() {
        return Err("query is empty".into());
    }
    let k = vault_search::clamp_k(k);
    let vectors = app.embedder.embed(&[query_prefix(query)], app.dim)?;
    let hits = app
        .index
        .search(&vectors[0], k)
        .map_err(|e| e.to_string())?;
    Ok(hits_response(app, hits))
}

fn hits_response(app: &App, hits: Vec<Hit>) -> IpcResponse {
    let ipc: Vec<IpcHit> = hits.iter().map(IpcHit::from).collect();
    let text = format_hits(&hits);
    IpcResponse {
        ok: true,
        error: None,
        text: Some(text),
        hits: Some(ipc),
        notes: app.index.note_count().ok(),
        chunks: app.index.chunk_count().ok(),
        indexing: Some(app.indexing.load(Ordering::Relaxed)),
        dim: Some(app.dim as u32),
    }
}

fn status(app: &App) -> IpcResponse {
    let err = app.last_error.lock().ok().and_then(|e| e.clone());
    IpcResponse {
        ok: err.is_none(),
        error: err,
        text: None,
        hits: None,
        notes: app.index.note_count().ok(),
        chunks: app.index.chunk_count().ok(),
        indexing: Some(app.indexing.load(Ordering::Relaxed)),
        dim: Some(app.dim as u32),
    }
}

fn reindex(app: &App) -> Result<(), String> {
    if app
        .indexing
        .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
        .is_err()
    {
        return Ok(());
    }
    let result = reindex_inner(app);
    app.indexing.store(false, Ordering::Release);
    if result.is_ok() {
        *app.last_error.lock().map_err(|e| e.to_string())? = None;
    }
    result
}

fn reindex_inner(app: &App) -> Result<(), String> {
    let notes = collect_notes(&app.vault)?;
    let stamps = notes
        .iter()
        .map(|(path, mtime, _)| Stamp {
            path: path.clone(),
            mtime_ns: *mtime,
        })
        .collect::<Vec<_>>();
    let plan = app.index.plan(&stamps).map_err(|e| e.to_string())?;
    let by_path: HashMap<_, _> = notes
        .into_iter()
        .map(|(path, mtime, full)| (path, (mtime, full)))
        .collect();
    for path in &plan.embed {
        let (mtime, full) = &by_path[path];
        match load_note(path, full)? {
            None => app
                .index
                .delete(std::slice::from_ref(path))
                .map_err(|e| e.to_string())?,
            Some(chunks) => {
                let inputs = chunks
                    .iter()
                    .map(|chunk| document_prefix(&chunk.title, &chunk.text))
                    .collect::<Vec<_>>();
                let vectors = embed_batched(&app.embedder, &inputs, app.dim)?;
                app.index
                    .upsert(path, *mtime, &chunks, &vectors)
                    .map_err(|e| e.to_string())?;
            }
        }
    }
    app.index.delete(&plan.delete).map_err(|e| e.to_string())?;
    Ok(())
}

fn embed_batched(
    embedder: &crate::embed::HttpEmbedder,
    inputs: &[String],
    dim: usize,
) -> Result<Vec<Vec<f32>>, String> {
    let mut vectors = Vec::with_capacity(inputs.len());
    for batch in inputs.chunks(2) {
        vectors.extend(embedder.embed(batch, dim)?);
    }
    Ok(vectors)
}

fn load_note(rel: &str, full: &Path) -> Result<Option<Vec<Chunk>>, String> {
    let bytes = std::fs::read(full).map_err(|e| format!("read {}: {e}", full.display()))?;
    if !indexable_bytes(&bytes) {
        return Ok(None);
    }
    let Some(text) = std::str::from_utf8(&bytes).ok() else {
        return Ok(None);
    };
    Ok(Some(chunk_markdown(rel, text)))
}

fn collect_notes(vault: &Path) -> Result<Vec<(String, i64, PathBuf)>, String> {
    let mut out = Vec::new();
    walk(vault, vault, &mut out).map_err(|e| e.to_string())?;
    out.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(out)
}

fn walk(vault: &Path, dir: &Path, out: &mut Vec<(String, i64, PathBuf)>) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with('.') {
            continue;
        }
        let file_type = entry.file_type()?;
        if file_type.is_symlink() {
            continue;
        }
        let path = entry.path();
        if file_type.is_dir() {
            walk(vault, &path, out)?;
            continue;
        }
        if !file_type.is_file() {
            continue;
        }
        let Ok(rel) = path.strip_prefix(vault) else {
            continue;
        };
        let rel = rel.to_string_lossy().replace('\\', "/");
        if !indexable_path(&rel) {
            continue;
        }
        let meta = entry.metadata()?;
        if meta.len() > MAX_FILE_BYTES as u64 {
            continue;
        }
        let mtime = meta
            .modified()
            .ok()
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| i64::try_from(d.as_nanos()).unwrap_or(i64::MAX))
            .unwrap_or(0);
        out.push((rel, mtime, path));
    }
    Ok(())
}

fn prepare_socket(socket: &Path) -> Result<(), String> {
    if !socket.exists() {
        return Ok(());
    }
    match UnixStream::connect(socket) {
        Ok(_) => Err(format!(
            "already listening on {}. Stop that vault-searchd first.",
            socket.display()
        )),
        Err(_) => {
            std::fs::remove_file(socket).map_err(|e| e.to_string())?;
            Ok(())
        }
    }
}

fn default_socket() -> PathBuf {
    if let Some(dir) = std::env::var_os("XDG_RUNTIME_DIR") {
        return PathBuf::from(dir).join("vault-search.sock");
    }
    let uid = std::fs::metadata("/proc/self")
        .map(|meta| meta.uid())
        .unwrap_or(0);
    PathBuf::from(format!("/tmp/vault-search-{uid}.sock"))
}
