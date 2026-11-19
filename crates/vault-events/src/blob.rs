//! Chunked transfer of files above the inline limit.
//!
//! A live event for a file larger than [`TransferOpts::inline_max`] carries
//! no `content`, only a [`BlobInfo`] (size, chunk size, chunk count) next to
//! the usual stamp with the file's SHA-256. The bytes are pulled on demand
//! with Zenoh queries on `vault/blob/<sha256>/<index>?cs=<chunk_size>`:
//!
//! - the hub answers from its content-addressed blob directory;
//! - the sending agent answers from a short-lived in-memory cache of the
//!   files it just published, so the hub (and early receivers) can pull
//!   the bytes before the hub has them.
//!
//! Each query names who should answer with a `src` parameter
//! (`?cs=<chunk_size>;src=<who>`): `hub`, the sending device, or `any`.
//! Receivers ask the hub first, then the device that published the file,
//! then anyone, and keep using whichever answered. So one holder sends each
//! chunk and a slow router hop (note4c) never carries the same 256 KiB twice.
//! Queries without `src` (older receivers) are answered by everyone.
//!
//! Chunk queries use priority `DataLow` and `CongestionControl::Block`
//! (replies inherit both): a reply crossing a slow router waits for room
//! instead of being dropped fragment by fragment, which used to cost a 15 s
//! timeout per lost chunk. Transfers are pull-driven with a few chunks in
//! flight, so blocking never queues more than that. Note events keep
//! their normal priority and go ahead of chunk replies. A receiver takes
//! the first reply and does not wait for the query's final message.
//!
//! Receivers assemble into `<partial-dir>/<sha256>.part` (outside the vault,
//! so neither the watcher, Obsidian nor rclone bisync ever see a partial
//! file), verify the SHA-256, and only then hand the full bytes to the
//! normal apply path, which writes them with a same-directory temp file +
//! rename. A hash mismatch discards the partial file and starts over; a
//! partial file is never applied and never becomes a conflict copy.

use std::collections::{BTreeSet, VecDeque};
use std::fs;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

use futures_util::stream::{FuturesUnordered, StreamExt};
use zenoh::qos::{CongestionControl, Priority};
use zenoh::query::{ConsolidationMode, QueryTarget};
use zenoh::sample::Locality;
use zenoh::Session;

use crate::event::{is_sha256_hex, sha256_hex, BlobInfo};

/// Prefix of every chunk key.
pub const BLOB_PREFIX: &str = "vault/blob";
/// Wildcard the hub and agents declare their chunk queryables on.
pub const BLOB_WILDCARD: &str = "vault/blob/**";
/// Files above this travel in chunks (default 1 MiB).
pub const DEFAULT_INLINE_MAX: u64 = 1 << 20;
/// Default chunk size (256 KiB).
pub const DEFAULT_CHUNK_SIZE: u64 = 256 << 10;
pub const MIN_CHUNK_SIZE: u64 = 4 << 10;
/// Chunk queries in flight per transfer (1 MiB at the default chunk size).
pub const DEFAULT_IN_FLIGHT: usize = 4;
pub const MAX_CHUNK_SIZE: u64 = 4 << 20;

/// `ceil(size / chunk_size)`; 0 for an empty file.
pub fn chunk_count(size: u64, chunk_size: u64) -> u64 {
    if chunk_size == 0 {
        return 0;
    }
    size.div_ceil(chunk_size)
}

/// `(offset, len)` of chunk `i`, `None` past the end.
pub fn chunk_range(size: u64, chunk_size: u64, i: u64) -> Option<(u64, u64)> {
    let start = i.checked_mul(chunk_size)?;
    if start >= size {
        return None;
    }
    Some((start, chunk_size.min(size - start)))
}

/// `vault/blob/<sha256>/<i>`
pub fn blob_key(sha: &str, i: u64) -> String {
    format!("{BLOB_PREFIX}/{sha}/{i}")
}

/// `vault/blob/<sha256>/<i>?cs=<chunk_size>`
pub fn blob_selector(sha: &str, i: u64, chunk_size: u64) -> String {
    format!("{}?cs={chunk_size}", blob_key(sha, i))
}

/// `src` value asking only the hub.
pub const SRC_HUB: &str = "hub";
/// `src` value asking every holder.
pub const SRC_ANY: &str = "any";

/// `vault/blob/<sha256>/<i>?cs=<chunk_size>;src=<who>`
pub fn blob_selector_src(sha: &str, i: u64, chunk_size: u64, src: &str) -> String {
    format!("{};src={src}", blob_selector(sha, i, chunk_size))
}

/// Who serves chunks: the hub (blob store) or an agent (its outgoing cache).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Role {
    Hub,
    Agent(String),
}

impl Role {
    /// Answer a query carrying `src`? No `src` (older receivers) and `any`
    /// mean everyone; otherwise only the named server.
    pub fn answers(&self, src: Option<&str>) -> bool {
        match (self, src) {
            (_, None) | (_, Some(SRC_ANY)) => true,
            (Role::Hub, Some(s)) => s == SRC_HUB,
            (Role::Agent(d), Some(s)) => s == d,
        }
    }
}

/// Inverse of [`blob_key`]: `(sha256, index)`.
pub fn parse_blob_key(k: &str) -> Option<(String, u64)> {
    let rest = k.strip_prefix(BLOB_PREFIX)?.strip_prefix('/')?;
    let (sha, idx) = rest.split_once('/')?;
    if !is_sha256_hex(sha) || idx.is_empty() || !idx.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    Some((sha.to_string(), idx.parse().ok()?))
}

/// The `cs` selector parameter, clamped; default [`DEFAULT_CHUNK_SIZE`].
pub fn chunk_size_param(v: Option<&str>) -> u64 {
    v.and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(DEFAULT_CHUNK_SIZE)
        .clamp(MIN_CHUNK_SIZE, MAX_CHUNK_SIZE)
}

/// Transfer tuning shared by hub and agents.
#[derive(Debug, Clone)]
pub struct TransferOpts {
    /// Files above this many bytes travel in chunks.
    pub inline_max: u64,
    /// Chunk size this side asks for / announces.
    pub chunk_size: u64,
    /// Per-chunk query timeout.
    pub chunk_timeout: Duration,
    /// Passes over the missing chunks before giving up (catch-up retries later).
    pub rounds: u32,
    /// Pause between passes (doubles each time, max 8x).
    pub backoff: Duration,
    /// Chunk queries in flight per transfer.
    pub in_flight: usize,
    /// A live blob reference: how long a receiver waits for the hub to
    /// store the file before pulling from the sending device itself. One
    /// copy then crosses a slow router hop instead of one per receiver.
    pub hub_wait: Duration,
}

impl Default for TransferOpts {
    fn default() -> Self {
        TransferOpts {
            inline_max: DEFAULT_INLINE_MAX,
            chunk_size: DEFAULT_CHUNK_SIZE,
            chunk_timeout: Duration::from_secs(15),
            rounds: 8,
            backoff: Duration::from_millis(500),
            in_flight: DEFAULT_IN_FLIGHT,
            hub_wait: Duration::from_secs(30),
        }
    }
}

impl TransferOpts {
    /// Defaults, overridden by `VAULT_EVENTS_INLINE_MAX` /
    /// `VAULT_EVENTS_CHUNK_SIZE` (bytes) and `VAULT_EVENTS_IN_FLIGHT` when set.
    pub fn from_env() -> Self {
        let mut o = TransferOpts::default();
        if let Some(v) = env_u64("VAULT_EVENTS_INLINE_MAX") {
            o.inline_max = v;
        }
        if let Some(v) = env_u64("VAULT_EVENTS_CHUNK_SIZE") {
            o.chunk_size = v;
        }
        if let Some(v) = env_u64("VAULT_EVENTS_IN_FLIGHT") {
            o.in_flight = v as usize;
        }
        o.normalized()
    }

    pub fn normalized(mut self) -> Self {
        self.chunk_size = self.chunk_size.clamp(MIN_CHUNK_SIZE, MAX_CHUNK_SIZE);
        self.in_flight = self.in_flight.clamp(1, 16);
        self
    }
}

fn env_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok().and_then(|v| v.trim().parse().ok())
}

/// Content-addressed blob files `<dir>/<sha256>` (the hub's blob store).
#[derive(Debug, Clone)]
pub struct BlobDir {
    dir: PathBuf,
}

impl BlobDir {
    pub fn open(dir: &Path) -> io::Result<Self> {
        fs::create_dir_all(dir)?;
        Ok(BlobDir {
            dir: dir.to_path_buf(),
        })
    }

    pub fn path(&self, sha: &str) -> PathBuf {
        self.dir.join(sha)
    }

    pub fn has(&self, sha: &str) -> bool {
        is_sha256_hex(sha) && self.path(sha).is_file()
    }

    /// Store `bytes` under their own hash (atomic). Returns the hash.
    pub fn put(&self, bytes: &[u8]) -> io::Result<String> {
        let sha = sha256_hex(bytes);
        let p = self.path(&sha);
        if p.is_file() {
            return Ok(sha);
        }
        let tmp = self.dir.join(format!("{sha}.tmp"));
        let mut f = fs::File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
        fs::rename(&tmp, &p)?;
        Ok(sha)
    }

    pub fn read(&self, sha: &str) -> io::Result<Vec<u8>> {
        fs::read(self.path(sha))
    }

    /// Chunk `i` at `chunk_size`, `None` if the blob is unknown or `i` is
    /// past the end.
    pub fn read_chunk(&self, sha: &str, i: u64, chunk_size: u64) -> io::Result<Option<Vec<u8>>> {
        if !is_sha256_hex(sha) {
            return Ok(None);
        }
        let mut f = match fs::File::open(self.path(sha)) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        let size = f.metadata()?.len();
        let Some((off, len)) = chunk_range(size, chunk_size, i) else {
            return Ok(None);
        };
        f.seek(SeekFrom::Start(off))?;
        let mut buf = vec![0u8; len as usize];
        f.read_exact(&mut buf)?;
        Ok(Some(buf))
    }

    pub fn remove(&self, sha: &str) -> io::Result<()> {
        match fs::remove_file(self.path(sha)) {
            Err(e) if e.kind() != io::ErrorKind::NotFound => Err(e),
            _ => Ok(()),
        }
    }

    /// Hashes of every stored blob.
    pub fn list(&self) -> io::Result<Vec<String>> {
        let mut v = Vec::new();
        for e in fs::read_dir(&self.dir)? {
            let n = e?.file_name().to_string_lossy().to_string();
            if is_sha256_hex(&n) {
                v.push(n);
            }
        }
        Ok(v)
    }
}

/// Files this agent published as blob references, kept in memory for a
/// while so the hub (and other devices) can pull them. Bounded by total
/// bytes and age; the hub keeps its own copy once it has pulled it.
/// `(sha256, bytes, inserted)`.
type CacheEntry = (String, Arc<Vec<u8>>, Instant);

pub struct OutgoingCache {
    inner: Mutex<VecDeque<CacheEntry>>,
    max_bytes: u64,
    ttl: Duration,
}

impl OutgoingCache {
    pub fn new(max_bytes: u64, ttl: Duration) -> Self {
        OutgoingCache {
            inner: Mutex::new(VecDeque::new()),
            max_bytes,
            ttl,
        }
    }

    pub fn insert(&self, sha: &str, bytes: Vec<u8>) {
        let mut q = self.inner.lock().expect("cache lock");
        q.retain(|(s, _, _)| s != sha);
        q.push_back((sha.to_string(), Arc::new(bytes), Instant::now()));
        let mut total: u64 = q.iter().map(|(_, b, _)| b.len() as u64).sum();
        while total > self.max_bytes && q.len() > 1 {
            if let Some((_, b, _)) = q.pop_front() {
                total -= b.len() as u64;
            }
        }
    }

    pub fn get(&self, sha: &str) -> Option<Arc<Vec<u8>>> {
        let mut q = self.inner.lock().expect("cache lock");
        let ttl = self.ttl;
        q.retain(|(_, _, t)| t.elapsed() < ttl);
        q.iter()
            .find(|(s, _, _)| s == sha)
            .map(|(_, b, _)| b.clone())
    }

    pub fn chunk(&self, sha: &str, i: u64, chunk_size: u64) -> Option<Vec<u8>> {
        let b = self.get(sha)?;
        let (off, len) = chunk_range(b.len() as u64, chunk_size, i)?;
        Some(b[off as usize..(off + len) as usize].to_vec())
    }

    pub fn len(&self) -> usize {
        self.inner.lock().expect("cache lock").len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for OutgoingCache {
    /// 256 MiB, 30 minutes.
    fn default() -> Self {
        OutgoingCache::new(256 << 20, Duration::from_secs(30 * 60))
    }
}

/// Reassembles one blob in `<dir>/<sha>.part`, remembering finished chunks
/// in `<dir>/<sha>.have` so a retry (or a restart) asks only for the
/// chunks still missing.
pub struct Assembler {
    part: PathBuf,
    have_file: PathBuf,
    sha: String,
    size: u64,
    chunk_size: u64,
    have: BTreeSet<u64>,
}

impl Assembler {
    pub fn open(dir: &Path, sha: &str, size: u64, chunk_size: u64) -> io::Result<Self> {
        if !is_sha256_hex(sha) || chunk_size == 0 {
            return Err(io::Error::other(format!(
                "bad blob {sha} / chunk size {chunk_size}"
            )));
        }
        fs::create_dir_all(dir)?;
        let mut a = Assembler {
            part: dir.join(format!("{sha}.part")),
            have_file: dir.join(format!("{sha}.have")),
            sha: sha.to_string(),
            size,
            chunk_size,
            have: BTreeSet::new(),
        };
        let header = a.header();
        let resumed = fs::read_to_string(&a.have_file).ok().and_then(|t| {
            let mut lines = t.lines();
            (lines.next() == Some(header.as_str())).then(|| {
                lines
                    .filter_map(|l| l.trim().parse::<u64>().ok())
                    .filter(|i| *i < chunk_count(size, chunk_size))
                    .collect::<BTreeSet<u64>>()
            })
        });
        match resumed {
            Some(h) if fs::metadata(&a.part).is_ok_and(|m| m.len() == size) => a.have = h,
            _ => a.reset()?,
        }
        Ok(a)
    }

    fn header(&self) -> String {
        format!("{} {} {}", self.sha, self.size, self.chunk_size)
    }

    pub fn sha(&self) -> &str {
        &self.sha
    }

    pub fn chunks(&self) -> u64 {
        chunk_count(self.size, self.chunk_size)
    }

    pub fn have(&self) -> usize {
        self.have.len()
    }

    /// Chunk indices still to fetch, in order.
    pub fn missing(&self) -> Vec<u64> {
        (0..self.chunks())
            .filter(|i| !self.have.contains(i))
            .collect()
    }

    /// Store chunk `i`. Rejects a wrong length (`Ok(false)`).
    pub fn put(&mut self, i: u64, bytes: &[u8]) -> io::Result<bool> {
        let Some((off, len)) = chunk_range(self.size, self.chunk_size, i) else {
            return Ok(false);
        };
        if bytes.len() as u64 != len {
            return Ok(false);
        }
        let mut f = fs::OpenOptions::new().write(true).open(&self.part)?;
        f.seek(SeekFrom::Start(off))?;
        f.write_all(bytes)?;
        f.flush()?;
        let mut h = fs::OpenOptions::new().append(true).open(&self.have_file)?;
        writeln!(h, "{i}")?;
        self.have.insert(i);
        Ok(true)
    }

    /// All chunks there: verify. `Ok(Ok(bytes))` and the partial files are
    /// gone; `Ok(Err(got_sha))` on a mismatch, after which the assembler
    /// starts over (every chunk missing).
    pub fn finish(&mut self) -> io::Result<Result<Vec<u8>, String>> {
        if !self.missing().is_empty() {
            return Err(io::Error::other("finish() with chunks missing"));
        }
        let bytes = fs::read(&self.part)?;
        let got = sha256_hex(&bytes);
        if got != self.sha || bytes.len() as u64 != self.size {
            self.reset()?;
            return Ok(Err(got));
        }
        self.discard();
        Ok(Ok(bytes))
    }

    /// Forget every chunk (fresh, zero-filled part file of the right size).
    pub fn reset(&mut self) -> io::Result<()> {
        let f = fs::File::create(&self.part)?;
        f.set_len(self.size)?;
        fs::write(&self.have_file, format!("{}\n", self.header()))?;
        self.have.clear();
        Ok(())
    }

    /// Remove the partial files.
    pub fn discard(&self) {
        let _ = fs::remove_file(&self.part);
        let _ = fs::remove_file(&self.have_file);
    }
}

/// Where chunks come from (Zenoh in production, fakes in tests).
pub trait ChunkSource {
    fn chunk(
        &self,
        sha: &str,
        i: u64,
        chunk_size: u64,
    ) -> impl std::future::Future<Output = Option<Vec<u8>>> + Send;
}

#[derive(Debug)]
pub enum FetchError {
    Io(io::Error),
    /// Gave up with chunks still missing (`have` of `chunks`).
    Incomplete {
        have: u64,
        chunks: u64,
    },
    /// Every complete assembly hashed wrong.
    Mismatch {
        got: String,
    },
}

impl std::fmt::Display for FetchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FetchError::Io(e) => write!(f, "{e}"),
            FetchError::Incomplete { have, chunks } => {
                write!(f, "incomplete: {have}/{chunks} chunks")
            }
            FetchError::Mismatch { got } => write!(f, "sha256 mismatch (got {})", short(got)),
        }
    }
}

impl std::error::Error for FetchError {}

impl From<io::Error> for FetchError {
    fn from(e: io::Error) -> Self {
        FetchError::Io(e)
    }
}

/// Outcome of a successful [`fetch`].
#[derive(Debug)]
pub struct Fetched {
    pub bytes: Vec<u8>,
    pub chunks: u64,
    /// Chunks that were already on disk from an earlier attempt.
    pub resumed: u64,
    /// Chunk requests that came back empty (retried).
    pub retries: u64,
    pub elapsed: Duration,
}

/// Pull blob `sha` (`info`) chunk by chunk from `src` into `partial_dir`,
/// asking again only for missing chunks, and return the verified bytes.
pub async fn fetch<S: ChunkSource>(
    src: &S,
    partial_dir: &Path,
    sha: &str,
    info: &BlobInfo,
    opts: &TransferOpts,
) -> Result<Fetched, FetchError> {
    let t0 = Instant::now();
    let cs = info.chunk_size.clamp(MIN_CHUNK_SIZE, MAX_CHUNK_SIZE);
    let mut asm = Assembler::open(partial_dir, sha, info.size, cs)?;
    let resumed = asm.have() as u64;
    let mut retries = 0u64;
    let mut last_mismatch: Option<String> = None;
    let mut wait = opts.backoff;
    let width = opts.in_flight.max(1);
    for round in 0..opts.rounds.max(1) {
        // Up to `width` chunk queries in flight. After `width` failures in
        // a row the source is gone for now: stop asking, back off, retry.
        let mut todo = asm.missing().into_iter();
        let mut running = FuturesUnordered::new();
        let mut failed_in_a_row = 0usize;
        loop {
            while running.len() < width && failed_in_a_row < width {
                let Some(i) = todo.next() else { break };
                running.push(async move { (i, src.chunk(sha, i, cs).await) });
            }
            let Some((i, got)) = running.next().await else {
                break;
            };
            match got {
                Some(b) if asm.put(i, &b)? => failed_in_a_row = 0,
                _ => {
                    // Nobody answered (or a wrong length): retried next pass.
                    retries += 1;
                    failed_in_a_row += 1;
                }
            }
        }
        if asm.missing().is_empty() {
            match asm.finish()? {
                Ok(bytes) => {
                    return Ok(Fetched {
                        bytes,
                        chunks: asm.chunks(),
                        resumed,
                        retries,
                        elapsed: t0.elapsed(),
                    })
                }
                Err(got) => {
                    tracing::warn!(
                        "blob {}: assembled bytes hash to {}, discarded; refetching",
                        short(sha),
                        short(&got)
                    );
                    last_mismatch = Some(got);
                    continue;
                }
            }
        }
        if round + 1 < opts.rounds.max(1) {
            tokio::time::sleep(wait).await;
            wait = (wait * 2).min(opts.backoff * 8);
        }
    }
    match last_mismatch {
        Some(got) if asm.have() == 0 => Err(FetchError::Mismatch { got }),
        _ => Err(FetchError::Incomplete {
            have: asm.have() as u64,
            chunks: asm.chunks(),
        }),
    }
}

/// [`ChunkSource`] over a Zenoh session. Tries the `src` values in order
/// (e.g. `hub`, the sending device, `any`) and sticks to the first that
/// answers. Takes the first non-empty reply without waiting for the final.
pub struct ZenohSource {
    session: Session,
    timeout: Duration,
    sources: Vec<String>,
    pref: AtomicUsize,
}

impl ZenohSource {
    /// Ask everyone (diagnostics).
    pub fn new(session: Session, opts: &TransferOpts) -> Self {
        Self::with_sources(session, opts, &[SRC_ANY])
    }

    /// An agent fetching a file published by `origin`: hub, then `origin`,
    /// then anyone.
    pub fn for_agent(session: Session, opts: &TransferOpts, origin: &str) -> Self {
        Self::with_sources(session, opts, &[SRC_HUB, origin, SRC_ANY])
    }

    /// The hub pulling a file published by `origin`: `origin`, then anyone.
    pub fn for_hub(session: Session, opts: &TransferOpts, origin: &str) -> Self {
        Self::with_sources(session, opts, &[origin, SRC_ANY])
    }

    fn with_sources(session: Session, opts: &TransferOpts, srcs: &[&str]) -> Self {
        let mut sources: Vec<String> = Vec::new();
        for s in srcs {
            if !s.is_empty() && !sources.iter().any(|x| x == s) {
                sources.push(s.to_string());
            }
        }
        ZenohSource {
            session,
            timeout: opts.chunk_timeout,
            sources,
            pref: AtomicUsize::new(0),
        }
    }

    pub fn sources(&self) -> &[String] {
        &self.sources
    }

    async fn ask(&self, sel: &str) -> Option<Vec<u8>> {
        let (tx, rx) = tokio::sync::oneshot::channel::<Vec<u8>>();
        let tx = Mutex::new(Some(tx));
        self.session
            .get(sel)
            .callback(move |reply| {
                if let Ok(sample) = reply.result() {
                    let b = sample.payload().to_bytes();
                    if !b.is_empty() {
                        if let Some(tx) = tx.lock().ok().and_then(|mut t| t.take()) {
                            let _ = tx.send(b.to_vec());
                        }
                    }
                }
            })
            .target(QueryTarget::All)
            .consolidation(ConsolidationMode::None)
            .allowed_destination(Locality::Remote)
            .congestion_control(CongestionControl::Block)
            .priority(Priority::DataLow)
            .timeout(self.timeout)
            .await
            .ok()?;
        // The sender is dropped when the query ends (final reply or
        // timeout) without data; that wakes us at once.
        tokio::time::timeout(self.timeout + Duration::from_secs(1), rx)
            .await
            .ok()?
            .ok()
    }
}

impl ChunkSource for ZenohSource {
    async fn chunk(&self, sha: &str, i: u64, chunk_size: u64) -> Option<Vec<u8>> {
        // The assembler checks the length of what comes back.
        let n = self.sources.len();
        let start = self.pref.load(AtomicOrdering::Relaxed) % n.max(1);
        for k in 0..n {
            let idx = (start + k) % n;
            let sel = blob_selector_src(sha, i, chunk_size, &self.sources[idx]);
            if let Some(b) = self.ask(&sel).await {
                self.pref.store(idx, AtomicOrdering::Relaxed);
                return Some(b);
            }
        }
        None
    }
}

/// Answer one chunk query with `lookup(sha, index, chunk_size)` if `role`
/// is asked (`src`). Returns `(sha, index)` when a chunk was sent.
pub async fn answer(
    query: &zenoh::query::Query,
    role: &Role,
    lookup: impl FnOnce(&str, u64, u64) -> Option<Vec<u8>>,
) -> Option<(String, u64)> {
    if !role.answers(query.parameters().get("src")) {
        return None;
    }
    let k = query.key_expr().as_str().to_string();
    let (sha, i) = parse_blob_key(&k)?;
    let cs = chunk_size_param(query.parameters().get("cs"));
    let bytes = lookup(&sha, i, cs)?;
    if let Err(e) = query.reply(k.as_str(), bytes).await {
        tracing::debug!("blob reply {} failed: {e}", k);
        return None;
    }
    Some((sha, i))
}

/// The running `vault/blob/**` queryable; dropping it stops serving.
pub struct BlobServer {
    task: tokio::task::JoinHandle<()>,
}

impl Drop for BlobServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Declare the `vault/blob/**` queryable and answer it on its own task,
/// one reply task per query. A busy agent loop (catch-up, a fetch, a
/// rescan) never delays a chunk, and pipelined requests are answered side
/// by side.
pub async fn serve<F>(session: &Session, role: Role, lookup: F) -> Result<BlobServer, zenoh::Error>
where
    F: Fn(&str, u64, u64) -> Option<Vec<u8>> + Send + Sync + 'static,
{
    let q = session.declare_queryable(BLOB_WILDCARD).await?;
    let lookup = Arc::new(lookup);
    let role = Arc::new(role);
    let task = tokio::spawn(async move {
        while let Ok(query) = q.recv_async().await {
            let (lookup, role) = (lookup.clone(), role.clone());
            tokio::spawn(async move {
                if let Some((sha, i)) = answer(&query, &role, |s, i, cs| lookup(s, i, cs)).await {
                    if i == 0 {
                        let from = match role.as_ref() {
                            Role::Hub => "",
                            Role::Agent(_) => " from the outgoing cache",
                        };
                        tracing::info!("serving blob {} (chunk 0){from}", short(&sha));
                    } else {
                        tracing::debug!("served blob {} chunk {i}", short(&sha));
                    }
                }
            });
        }
    });
    Ok(BlobServer { task })
}

fn short(sha: &str) -> &str {
    &sha[..12.min(sha.len())]
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn data(n: usize) -> Vec<u8> {
        (0..n).map(|i| (i * 31 % 251) as u8).collect()
    }

    #[test]
    fn chunk_math_and_keys() {
        assert_eq!(chunk_count(0, 10), 0);
        assert_eq!(chunk_count(10, 10), 1);
        assert_eq!(chunk_count(11, 10), 2);
        assert_eq!(chunk_count(7_000_000, DEFAULT_CHUNK_SIZE), 27);
        assert_eq!(chunk_range(25, 10, 2), Some((20, 5)));
        assert_eq!(chunk_range(25, 10, 3), None);
        let sha = sha256_hex(b"x");
        let k = blob_key(&sha, 12);
        zenoh::key_expr::KeyExpr::try_from(k.clone()).unwrap();
        assert_eq!(parse_blob_key(&k), Some((sha.clone(), 12)));
        assert!(parse_blob_key(&format!("vault/blob/{sha}/x")).is_none());
        assert!(parse_blob_key("vault/blob/abc/1").is_none());
        assert!(parse_blob_key(&format!("vault/notes/{sha}/1")).is_none());
        assert_eq!(chunk_size_param(Some("1")), MIN_CHUNK_SIZE);
        assert_eq!(chunk_size_param(None), DEFAULT_CHUNK_SIZE);
        assert_eq!(chunk_size_param(Some("65536")), 65536);
        assert_eq!(
            blob_selector(&sha, 0, 4096),
            format!("vault/blob/{sha}/0?cs=4096")
        );
    }

    #[test]
    fn blob_dir_serves_chunks() {
        let t = tempfile::tempdir().unwrap();
        let d = BlobDir::open(t.path()).unwrap();
        let b = data(10_000);
        let sha = d.put(&b).unwrap();
        assert!(d.has(&sha));
        assert_eq!(
            d.read_chunk(&sha, 2, 4096).unwrap().unwrap(),
            b[8192..].to_vec()
        );
        assert!(d.read_chunk(&sha, 3, 4096).unwrap().is_none());
        assert!(d
            .read_chunk(&sha256_hex(b"nope"), 0, 4096)
            .unwrap()
            .is_none());
        assert_eq!(d.list().unwrap(), vec![sha.clone()]);
        d.remove(&sha).unwrap();
        assert!(!d.has(&sha));
    }

    #[test]
    fn assembler_out_of_order_resume_and_mismatch() {
        let t = tempfile::tempdir().unwrap();
        let b = data(10_000);
        let sha = sha256_hex(&b);
        let mut a = Assembler::open(t.path(), &sha, b.len() as u64, 4096).unwrap();
        assert_eq!(a.missing(), vec![0, 1, 2]);
        assert!(!a.put(0, &b[..100]).unwrap(), "wrong length rejected");
        assert!(a.put(2, &b[8192..]).unwrap());
        drop(a);
        // Resume: chunk 2 is remembered across a reopen.
        let mut a = Assembler::open(t.path(), &sha, b.len() as u64, 4096).unwrap();
        assert_eq!(a.missing(), vec![0, 1]);
        // A different chunk size does not reuse the old progress.
        drop(a);
        let a2 = Assembler::open(t.path(), &sha, b.len() as u64, 8192).unwrap();
        assert_eq!(a2.missing(), vec![0, 1]);
        drop(a2);
        a = Assembler::open(t.path(), &sha, b.len() as u64, 4096).unwrap();
        assert_eq!(a.missing(), vec![0, 1, 2]);
        // Corrupt chunk: complete, but the hash is wrong -> reset.
        let mut bad = b[4096..8192].to_vec();
        bad[0] ^= 1;
        a.put(0, &b[..4096]).unwrap();
        a.put(1, &bad).unwrap();
        a.put(2, &b[8192..]).unwrap();
        assert!(a.finish().unwrap().is_err());
        assert_eq!(a.missing(), vec![0, 1, 2], "mismatch starts over");
        for i in 0..3u64 {
            let (o, l) = chunk_range(b.len() as u64, 4096, i).unwrap();
            a.put(i, &b[o as usize..(o + l) as usize]).unwrap();
        }
        assert_eq!(a.finish().unwrap().unwrap(), b);
        assert!(
            std::fs::read_dir(t.path()).unwrap().next().is_none(),
            "partial files removed"
        );
    }

    /// Serves from a map; `fail_first` empty answers per chunk; `corrupt`
    /// chunks are served with a flipped byte once.
    struct Fake {
        bytes: Vec<u8>,
        calls: AtomicU64,
        fail: Mutex<HashMap<u64, u32>>,
        corrupt: Mutex<HashMap<u64, u32>>,
        asked: Mutex<Vec<u64>>,
    }

    impl Fake {
        fn new(bytes: Vec<u8>) -> Self {
            Fake {
                bytes,
                calls: AtomicU64::new(0),
                fail: Mutex::new(HashMap::new()),
                corrupt: Mutex::new(HashMap::new()),
                asked: Mutex::new(Vec::new()),
            }
        }
    }

    impl ChunkSource for Fake {
        async fn chunk(&self, _sha: &str, i: u64, cs: u64) -> Option<Vec<u8>> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            self.asked.lock().unwrap().push(i);
            if let Some(n) = self.fail.lock().unwrap().get_mut(&i) {
                if *n > 0 {
                    *n -= 1;
                    return None;
                }
            }
            let (o, l) = chunk_range(self.bytes.len() as u64, cs, i)?;
            let mut c = self.bytes[o as usize..(o + l) as usize].to_vec();
            if let Some(n) = self.corrupt.lock().unwrap().get_mut(&i) {
                if *n > 0 {
                    *n -= 1;
                    c[0] ^= 0xff;
                }
            }
            Some(c)
        }
    }

    fn quick() -> TransferOpts {
        TransferOpts {
            backoff: Duration::from_millis(1),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn fetch_retries_only_missing_chunks() {
        let t = tempfile::tempdir().unwrap();
        let b = data(20_000);
        let sha = sha256_hex(&b);
        let info = BlobInfo::new(b.len() as u64, 4096);
        let src = Fake::new(b.clone());
        src.fail.lock().unwrap().insert(3, 2);
        let f = fetch(&src, t.path(), &sha, &info, &quick()).await.unwrap();
        assert_eq!(f.bytes, b);
        assert_eq!(f.chunks, 5);
        assert_eq!(f.retries, 2);
        // 0,1,2,4 once; 3 three times (the other chunks do not wait for it).
        let asked = src.asked.lock().unwrap().clone();
        assert_eq!(asked, vec![0, 1, 2, 3, 4, 3, 3]);
    }

    #[tokio::test]
    async fn fetch_resumes_from_partial_file() {
        let t = tempfile::tempdir().unwrap();
        let b = data(20_000);
        let sha = sha256_hex(&b);
        let info = BlobInfo::new(b.len() as u64, 4096);
        // First attempt gets everything but chunk 2, which never answers.
        let src = Fake::new(b.clone());
        src.fail.lock().unwrap().insert(2, 1000);
        let opts = TransferOpts {
            rounds: 2,
            ..quick()
        };
        match fetch(&src, t.path(), &sha, &info, &opts).await {
            Err(FetchError::Incomplete { have: 4, chunks: 5 }) => {}
            other => panic!("{other:?}"),
        }
        // Second attempt asks only for 2.
        let src2 = Fake::new(b.clone());
        let f = fetch(&src2, t.path(), &sha, &info, &quick()).await.unwrap();
        assert_eq!(f.bytes, b);
        assert_eq!(f.resumed, 4);
        assert_eq!(src2.asked.lock().unwrap().clone(), vec![2]);
    }

    #[tokio::test]
    async fn fetch_discards_on_hash_mismatch_and_refetches() {
        let t = tempfile::tempdir().unwrap();
        let b = data(20_000);
        let sha = sha256_hex(&b);
        let info = BlobInfo::new(b.len() as u64, 4096);
        let src = Fake::new(b.clone());
        src.corrupt.lock().unwrap().insert(1, 1);
        let f = fetch(&src, t.path(), &sha, &info, &quick()).await.unwrap();
        assert_eq!(f.bytes, b);
        assert_eq!(
            src.calls.load(Ordering::Relaxed),
            10,
            "full refetch after mismatch"
        );
        // A source that always lies never produces bytes.
        let t2 = tempfile::tempdir().unwrap();
        let liar = Fake::new(b.clone());
        liar.corrupt.lock().unwrap().insert(0, u32::MAX);
        let opts = TransferOpts {
            rounds: 3,
            ..quick()
        };
        assert!(matches!(
            fetch(&liar, t2.path(), &sha, &info, &opts).await,
            Err(FetchError::Mismatch { .. })
        ));
    }

    #[test]
    fn src_parameter_picks_one_server() {
        assert_eq!(
            blob_selector_src(&"a".repeat(64), 3, 4096, SRC_HUB),
            format!("vault/blob/{}/3?cs=4096;src=hub", "a".repeat(64))
        );
        let hub = Role::Hub;
        let towerx = Role::Agent("towerx".into());
        // Older receivers send no src: everyone answers, as before.
        assert!(hub.answers(None) && towerx.answers(None));
        assert!(hub.answers(Some("any")) && towerx.answers(Some("any")));
        assert!(hub.answers(Some("hub")) && !towerx.answers(Some("hub")));
        assert!(!hub.answers(Some("towerx")) && towerx.answers(Some("towerx")));
        assert!(!towerx.answers(Some("s24")));
    }

    /// Slow source: each answer takes a while; counts how many run at once.
    struct Slow {
        bytes: Vec<u8>,
        now: AtomicU64,
        peak: AtomicU64,
        gone: bool,
        calls: AtomicU64,
    }

    impl ChunkSource for Slow {
        async fn chunk(&self, _sha: &str, i: u64, cs: u64) -> Option<Vec<u8>> {
            self.calls.fetch_add(1, Ordering::Relaxed);
            let n = self.now.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(n, Ordering::SeqCst);
            tokio::time::sleep(Duration::from_millis(20)).await;
            self.now.fetch_sub(1, Ordering::SeqCst);
            if self.gone {
                return None;
            }
            let (o, l) = chunk_range(self.bytes.len() as u64, cs, i)?;
            Some(self.bytes[o as usize..(o + l) as usize].to_vec())
        }
    }

    #[tokio::test]
    async fn fetch_keeps_chunks_in_flight_and_stops_on_a_dead_source() {
        let b = data(40_000);
        let sha = sha256_hex(&b);
        let info = BlobInfo::new(b.len() as u64, 4096); // 10 chunks
        let t = tempfile::tempdir().unwrap();
        let src = Slow {
            bytes: b.clone(),
            now: AtomicU64::new(0),
            peak: AtomicU64::new(0),
            gone: false,
            calls: AtomicU64::new(0),
        };
        let t0 = Instant::now();
        let f = fetch(&src, t.path(), &sha, &info, &quick()).await.unwrap();
        assert_eq!(f.bytes, b);
        assert_eq!(src.peak.load(Ordering::SeqCst), DEFAULT_IN_FLIGHT as u64);
        assert!(
            t0.elapsed() < Duration::from_millis(150),
            "pipelined: {:?}",
            t0.elapsed()
        );
        // Nobody answers: each pass stops issuing after `in_flight` failures
        // in a row (the 4 in flight plus 3 refills), not after all 10 chunks.
        let t2 = tempfile::tempdir().unwrap();
        let dead = Slow {
            bytes: b.clone(),
            now: AtomicU64::new(0),
            peak: AtomicU64::new(0),
            gone: true,
            calls: AtomicU64::new(0),
        };
        let opts = TransferOpts {
            rounds: 2,
            ..quick()
        };
        assert!(matches!(
            fetch(&dead, t2.path(), &sha, &info, &opts).await,
            Err(FetchError::Incomplete {
                have: 0,
                chunks: 10
            })
        ));
        assert_eq!(
            dead.calls.load(Ordering::Relaxed),
            2 * (2 * DEFAULT_IN_FLIGHT as u64 - 1)
        );
    }

    #[test]
    fn outgoing_cache_bounds() {
        let c = OutgoingCache::new(25_000, Duration::from_secs(60));
        let a = data(10_000);
        let b = data(12_000);
        let d = data(9_000);
        c.insert("a", a.clone());
        c.insert("b", b);
        assert_eq!(c.chunk("a", 1, 4096).unwrap(), a[4096..8192].to_vec());
        c.insert("d", d);
        assert!(c.get("a").is_none(), "oldest evicted over the byte limit");
        assert!(c.get("b").is_some() && c.get("d").is_some());
        let short_lived = OutgoingCache::new(1 << 20, Duration::from_millis(1));
        short_lived.insert("x", vec![1]);
        std::thread::sleep(Duration::from_millis(5));
        assert!(short_lived.get("x").is_none(), "expired");
    }
}
