//! Chunked transfer of files above the inline limit.
//!
//! A live event for a file larger than [`TransferOpts::inline_max`] carries
//! no `content`, only a [`BlobInfo`] (size, chunk size, chunk count) next to
//! the usual stamp with the file's SHA-256. The bytes are pulled on demand
//! with Zenoh queries on `vault/blob/<sha256>/<index>?cs=<chunk_size>`:
//!
//! - the hub answers from its content-addressed blob directory;
//! - the sending agent answers by reading the needed chunk from the file
//!   on disk (after streaming a SHA-256 check that the file still matches),
//!   so a 64 MiB publish never holds the whole file in RAM. A small
//!   in-memory cache is kept only for tests and already-buffered tiny
//!   files.
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

use std::collections::{BTreeSet, HashMap, VecDeque};
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
        Ok(self.read_chunk_sized(sha, i, chunk_size)?.map(|(b, _)| b))
    }

    /// Chunk `i` plus the whole blob's size.
    pub fn read_chunk_sized(
        &self,
        sha: &str,
        i: u64,
        chunk_size: u64,
    ) -> io::Result<Option<(Vec<u8>, u64)>> {
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
        Ok(Some((buf, size)))
    }

    /// Install a verified file as blob `sha` (copy + rename). If `sha`
    /// is already stored, this is a no-op. The file is hashed by streaming
    /// if the caller passes `verify = true`.
    pub fn put_file(&self, sha: &str, src: &Path, verify: bool) -> io::Result<()> {
        if !is_sha256_hex(sha) {
            return Err(io::Error::other(format!("bad blob sha {sha}")));
        }
        let p = self.path(sha);
        if p.is_file() {
            return Ok(());
        }
        if verify {
            let got = crate::event::sha256_file(src)?;
            if got != sha {
                return Err(io::Error::other(format!("blob {sha}: file hash is {got}")));
            }
        }
        let tmp = self.dir.join(format!("{sha}.tmp"));
        let res = (|| {
            let mut inp = fs::File::open(src)?;
            let mut out = fs::File::create(&tmp)?;
            io::copy(&mut inp, &mut out)?;
            out.sync_all()?;
            fs::rename(&tmp, &p)
        })();
        if res.is_err() {
            let _ = fs::remove_file(&tmp);
        }
        res
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

/// How a published blob is served.
enum CacheBody {
    /// Read chunks from this file after a streaming SHA-256 check.
    File {
        path: PathBuf,
        size: u64,
        /// Last metadata used for a successful hash check.
        checked: Option<(u64, std::time::SystemTime)>,
    },
    /// Small already-buffered file (tests / tiny payloads). Bounded by
    /// [`OutgoingCache::max_bytes`].
    Bytes(Arc<Vec<u8>>),
}

impl CacheBody {
    fn mem_bytes(&self) -> u64 {
        match self {
            CacheBody::Bytes(b) => b.len() as u64,
            CacheBody::File { .. } => 0,
        }
    }
}

/// `(sha256, body, inserted)`.
type CacheEntry = (String, CacheBody, Instant);

/// Files this agent published as blob references. The usual entry is a
/// path on disk: chunks are read as 256 KiB ranges after a streaming
/// SHA-256 check that the file still matches. A small in-memory bound
/// covers tests and already-buffered tiny files; a full 64 MiB file is
/// never held in RAM.
pub struct OutgoingCache {
    inner: Mutex<VecDeque<CacheEntry>>,
    /// sha256 -> vault path, for the `served blob` log line.
    names: Mutex<HashMap<String, String>>,
    max_bytes: u64,
    ttl: Duration,
}

/// In-memory part of the cache (not on-disk entries): 8 MiB.
pub const OUTGOING_MEM_MAX: u64 = 8 << 20;

impl OutgoingCache {
    pub fn new(max_bytes: u64, ttl: Duration) -> Self {
        OutgoingCache {
            inner: Mutex::new(VecDeque::new()),
            names: Mutex::new(HashMap::new()),
            max_bytes,
            ttl,
        }
    }

    fn remember_name(&self, sha: &str, vault_path: &str) {
        let live: BTreeSet<String> = self
            .inner
            .lock()
            .expect("cache lock")
            .iter()
            .map(|(s, _, _)| s.clone())
            .collect();
        let mut n = self.names.lock().expect("names lock");
        n.retain(|s, _| live.contains(s));
        n.insert(sha.to_string(), vault_path.to_string());
    }

    fn push(&self, sha: &str, body: CacheBody) {
        let mut q = self.inner.lock().expect("cache lock");
        q.retain(|(s, _, _)| s != sha);
        q.push_back((sha.to_string(), body, Instant::now()));
        let mut total: u64 = q.iter().map(|(_, b, _)| b.mem_bytes()).sum();
        while total > self.max_bytes && q.len() > 1 {
            if let Some((_, b, _)) = q.pop_front() {
                total = total.saturating_sub(b.mem_bytes());
            }
        }
    }

    /// Serve chunks from `path` (a vault file). `vault_path` is the
    /// relative path for log lines.
    pub fn insert_file(&self, sha: &str, path: impl AsRef<Path>, vault_path: &str) {
        let path = path.as_ref().to_path_buf();
        let size = fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
        self.push(
            sha,
            CacheBody::File {
                path,
                size,
                checked: None,
            },
        );
        self.remember_name(sha, vault_path);
    }

    /// Small in-memory insert (tests). Large buffers are not kept: the
    /// caller should use [`Self::insert_file`].
    pub fn insert(&self, sha: &str, bytes: Vec<u8>) {
        self.push(sha, CacheBody::Bytes(Arc::new(bytes)));
    }

    /// [`Self::insert`], remembering the vault path for log lines.
    pub fn insert_named(&self, sha: &str, path: &str, bytes: Vec<u8>) {
        self.insert(sha, bytes);
        self.remember_name(sha, path);
    }

    /// Vault path a cached blob was published for.
    pub fn name(&self, sha: &str) -> Option<String> {
        self.names.lock().expect("names lock").get(sha).cloned()
    }

    /// Refresh TTL filter; return a mutable entry if still live.
    fn get_live<'a>(
        q: &'a mut VecDeque<CacheEntry>,
        ttl: Duration,
        sha: &str,
    ) -> Option<&'a mut CacheEntry> {
        q.retain(|(_, _, t)| t.elapsed() < ttl);
        q.iter_mut().find(|(s, _, _)| s == sha)
    }

    /// Chunk `i` plus the whole blob's size. For a file entry, hashes the
    /// file (streamed) if it was never checked or its mtime/size changed,
    /// and serves only when the SHA-256 still matches.
    pub fn chunk_sized(&self, sha: &str, i: u64, chunk_size: u64) -> Option<(Vec<u8>, u64)> {
        let mut q = self.inner.lock().expect("cache lock");
        let ttl = self.ttl;
        let e = Self::get_live(&mut q, ttl, sha)?;
        match &mut e.1 {
            CacheBody::Bytes(b) => {
                let (off, len) = chunk_range(b.len() as u64, chunk_size, i)?;
                Some((
                    b[off as usize..(off + len) as usize].to_vec(),
                    b.len() as u64,
                ))
            }
            CacheBody::File {
                path,
                size,
                checked,
            } => {
                let md = fs::metadata(&*path).ok()?;
                let mtime = md.modified().ok()?;
                let fresh = md.len() == *size
                    && checked
                        .as_ref()
                        .is_some_and(|(sz, t)| *sz == md.len() && *t == mtime);
                if !fresh {
                    let got = crate::event::sha256_file(path).ok()?;
                    if got != sha {
                        return None;
                    }
                    *size = md.len();
                    *checked = Some((md.len(), mtime));
                }
                let (off, len) = chunk_range(*size, chunk_size, i)?;
                let mut f = fs::File::open(&*path).ok()?;
                f.seek(SeekFrom::Start(off)).ok()?;
                let mut buf = vec![0u8; len as usize];
                f.read_exact(&mut buf).ok()?;
                Some((buf, *size))
            }
        }
    }

    pub fn get(&self, sha: &str) -> Option<Arc<Vec<u8>>> {
        let mut q = self.inner.lock().expect("cache lock");
        let ttl = self.ttl;
        let e = Self::get_live(&mut q, ttl, sha)?;
        match &e.1 {
            CacheBody::Bytes(b) => Some(b.clone()),
            CacheBody::File { .. } => None,
        }
    }

    pub fn chunk(&self, sha: &str, i: u64, chunk_size: u64) -> Option<Vec<u8>> {
        self.chunk_sized(sha, i, chunk_size).map(|(b, _)| b)
    }

    pub fn len(&self) -> usize {
        self.inner.lock().expect("cache lock").len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl Default for OutgoingCache {
    /// 8 MiB in-memory bound, 30 minutes.
    fn default() -> Self {
        OutgoingCache::new(OUTGOING_MEM_MAX, Duration::from_secs(30 * 60))
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

    /// All chunks there: stream-hash the part file. `Ok(Ok(path))` leaves
    /// the part in place for the caller to install; `Ok(Err(got_sha))` on
    /// a mismatch, after which every chunk is missing again.
    pub fn finish_verified(&mut self) -> io::Result<Result<PathBuf, String>> {
        if !self.missing().is_empty() {
            return Err(io::Error::other("finish() with chunks missing"));
        }
        let got = crate::event::sha256_file(&self.part)?;
        let len = fs::metadata(&self.part)?.len();
        if got != self.sha || len != self.size {
            self.reset()?;
            return Ok(Err(got));
        }
        Ok(Ok(self.part.clone()))
    }

    /// All chunks there: verify. `Ok(Ok(bytes))` and the partial files are
    /// gone; `Ok(Err(got_sha))` on a mismatch, after which the assembler
    /// starts over (every chunk missing).
    pub fn finish(&mut self) -> io::Result<Result<Vec<u8>, String>> {
        match self.finish_verified()? {
            Ok(p) => {
                let bytes = fs::read(&p)?;
                self.discard();
                Ok(Ok(bytes))
            }
            Err(got) => Ok(Err(got)),
        }
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

/// A verified blob on disk (the assembler part file). Install it, then
/// delete the part and the sibling `.have` file.
pub struct FetchedFile {
    pub path: PathBuf,
    pub size: u64,
    pub chunks: u64,
    pub resumed: u64,
    pub retries: u64,
    pub elapsed: Duration,
}

impl FetchedFile {
    /// Remove the part file and its `.have` sidecar.
    pub fn discard(&self) {
        let _ = fs::remove_file(&self.path);
        let _ = fs::remove_file(self.path.with_extension("have"));
    }
}

/// Pull blob `sha` into `partial_dir` and return the verified part file
/// (hashed by streaming, not loaded into RAM).
pub async fn fetch_file<S: ChunkSource>(
    src: &S,
    partial_dir: &Path,
    sha: &str,
    info: &BlobInfo,
    opts: &TransferOpts,
) -> Result<FetchedFile, FetchError> {
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
            match asm.finish_verified()? {
                Ok(path) => {
                    return Ok(FetchedFile {
                        path,
                        size: info.size,
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

/// Pull blob `sha` (`info`) chunk by chunk from `src` into `partial_dir`,
/// asking again only for missing chunks, and return the verified bytes.
pub async fn fetch<S: ChunkSource>(
    src: &S,
    partial_dir: &Path,
    sha: &str,
    info: &BlobInfo,
    opts: &TransferOpts,
) -> Result<Fetched, FetchError> {
    let f = fetch_file(src, partial_dir, sha, info, opts).await?;
    let bytes = fs::read(&f.path)?;
    f.discard();
    Ok(Fetched {
        bytes,
        chunks: f.chunks,
        resumed: f.resumed,
        retries: f.retries,
        elapsed: f.elapsed,
    })
}

/// [`ChunkSource`] over a Zenoh session. Tries the `src` values in order
/// (e.g. `hub`, the sending device, `any`) and sticks to the first that
/// answers. Takes the first non-empty reply without waiting for the final.
pub struct ZenohSource {
    session: Session,
    timeout: Duration,
    sources: Vec<String>,
    pref: AtomicUsize,
    /// `from=` this device on every chunk query.
    from: String,
}

impl ZenohSource {
    /// Ask everyone (diagnostics).
    pub fn new(session: Session, opts: &TransferOpts, from: &str) -> Self {
        Self::with_sources(session, opts, &[SRC_ANY], from)
    }

    /// An agent fetching a file published by `origin`: hub, then `origin`,
    /// then anyone.
    pub fn for_agent(session: Session, opts: &TransferOpts, origin: &str, from: &str) -> Self {
        Self::with_sources(session, opts, &[SRC_HUB, origin, SRC_ANY], from)
    }

    /// The hub pulling a file published by `origin`: `origin`, then anyone.
    pub fn for_hub(session: Session, opts: &TransferOpts, origin: &str, from: &str) -> Self {
        Self::with_sources(session, opts, &[origin, SRC_ANY], from)
    }

    fn with_sources(session: Session, opts: &TransferOpts, srcs: &[&str], from: &str) -> Self {
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
            from: crate::net::clean_device(Some(from)),
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
            let sel = crate::net::with_from(
                &blob_selector_src(sha, i, chunk_size, &self.sources[idx]),
                &self.from,
            );
            if let Some(b) = self.ask(&sel).await {
                self.pref.store(idx, AtomicOrdering::Relaxed);
                return Some(b);
            }
        }
        None
    }
}

/// One chunk sent by [`answer`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Served {
    pub sha: String,
    pub index: u64,
    pub len: u64,
    /// Size of the whole blob.
    pub size: u64,
    pub chunk_size: u64,
    /// Requester (`from` parameter) or `unknown`.
    pub to: String,
}

/// Answer one chunk query with `lookup(sha, index, chunk_size)` -> `(chunk,
/// blob size)` if `role` is asked (`src`).
pub async fn answer(
    query: &zenoh::query::Query,
    role: &Role,
    lookup: impl FnOnce(&str, u64, u64) -> Option<(Vec<u8>, u64)>,
) -> Option<Served> {
    if !role.answers(query.parameters().get("src")) {
        return None;
    }
    let k = query.key_expr().as_str().to_string();
    let (sha, i) = parse_blob_key(&k)?;
    let cs = chunk_size_param(query.parameters().get("cs"));
    let (bytes, size) = lookup(&sha, i, cs)?;
    let len = bytes.len() as u64;
    let to = crate::net::requester(query);
    if let Err(e) = query.reply(k.as_str(), bytes).await {
        tracing::debug!("blob reply {} to {to} failed: {e}", k);
        return None;
    }
    Some(Served {
        sha,
        index: i,
        len,
        size,
        chunk_size: cs,
        to,
    })
}

/// A (requester, blob) transfer is summarised after this long without a
/// chunk request.
pub const SERVE_IDLE: Duration = Duration::from_secs(30);

/// One `served blob ...` log line.
#[derive(Debug, Clone, PartialEq)]
pub struct ServeSummary {
    pub sha: String,
    pub name: Option<String>,
    pub to: String,
    pub chunks: u64,
    pub total: u64,
    pub bytes: u64,
    pub elapsed: Duration,
    pub complete: bool,
}

impl ServeSummary {
    /// `served blob e1936f74b612 (Notes/x.bin) to towerx: 10/10 chunks,
    /// 2621440 B in 3.1 s` (or `partial 4/10 chunks, ... , idle`).
    pub fn line(&self) -> String {
        let name = self
            .name
            .as_ref()
            .map(|n| format!(" ({n})"))
            .unwrap_or_default();
        let secs = self.elapsed.as_secs_f64();
        if self.complete {
            format!(
                "served blob {}{name} to {}: {}/{} chunks, {} B in {secs:.1} s",
                short(&self.sha),
                self.to,
                self.chunks,
                self.total,
                self.bytes
            )
        } else {
            format!(
                "served blob {}{name} to {}: partial {}/{} chunks, {} B in {secs:.1} s, idle",
                short(&self.sha),
                self.to,
                self.chunks,
                self.total,
                self.bytes
            )
        }
    }
}

#[derive(Debug)]
struct Transfer {
    name: Option<String>,
    first: Instant,
    last: Instant,
    have: BTreeSet<u64>,
    bytes: u64,
    total: u64,
}

/// Aggregates served chunks per (requester, blob) into one summary when
/// every chunk was sent or the requester went quiet for [`SERVE_IDLE`].
#[derive(Debug, Default)]
pub struct ServeLog {
    open: HashMap<(String, String), Transfer>,
}

impl ServeLog {
    /// No transfer of `sha` to `to` in progress (the caller may look up a
    /// name before [`Self::record`]).
    pub fn is_new(&self, to: &str, sha: &str) -> bool {
        !self.open.contains_key(&(to.to_string(), sha.to_string()))
    }

    /// Count one served chunk; returns the summary when this completed it.
    pub fn record(
        &mut self,
        s: &Served,
        name: Option<String>,
        now: Instant,
    ) -> Option<ServeSummary> {
        let key = (s.to.clone(), s.sha.clone());
        let t = self.open.entry(key.clone()).or_insert_with(|| Transfer {
            name,
            first: now,
            last: now,
            have: BTreeSet::new(),
            bytes: 0,
            total: chunk_count(s.size, s.chunk_size).max(1),
        });
        t.last = now;
        t.bytes += s.len;
        t.have.insert(s.index);
        if (t.have.len() as u64) < t.total {
            return None;
        }
        let t = self.open.remove(&key)?;
        Some(Self::summary(key, t, true))
    }

    /// Summaries of transfers idle for `idle` or longer (removed).
    pub fn sweep(&mut self, now: Instant, idle: Duration) -> Vec<ServeSummary> {
        let done: Vec<(String, String)> = self
            .open
            .iter()
            .filter(|(_, t)| now.saturating_duration_since(t.last) >= idle)
            .map(|(k, _)| k.clone())
            .collect();
        let mut out: Vec<ServeSummary> = done
            .into_iter()
            .filter_map(|k| {
                let t = self.open.remove(&k)?;
                Some(Self::summary(k, t, false))
            })
            .collect();
        out.sort_by(|a, b| (&a.to, &a.sha).cmp(&(&b.to, &b.sha)));
        out
    }

    pub fn open_transfers(&self) -> usize {
        self.open.len()
    }

    fn summary((to, sha): (String, String), t: Transfer, complete: bool) -> ServeSummary {
        ServeSummary {
            sha,
            name: t.name,
            to,
            chunks: t.have.len() as u64,
            total: t.total,
            bytes: t.bytes,
            elapsed: t.last.saturating_duration_since(t.first),
            complete,
        }
    }
}

/// The running `vault/blob/**` queryable; dropping it stops serving.
pub struct BlobServer {
    task: tokio::task::JoinHandle<()>,
    sweeper: tokio::task::JoinHandle<()>,
}

impl Drop for BlobServer {
    fn drop(&mut self) {
        self.task.abort();
        self.sweeper.abort();
    }
}

/// Declare the `vault/blob/**` queryable and answer it on its own task,
/// one reply task per query. A busy agent loop (catch-up, a fetch, a
/// rescan) never delays a chunk, and pipelined requests are answered side
/// by side. `lookup` gives `(chunk, blob size)`; `name` the vault path of
/// a blob for the one `served blob ... to <requester>` line per transfer.
pub async fn serve<F, N, Fut>(
    session: &Session,
    role: Role,
    lookup: F,
    name: N,
) -> Result<BlobServer, zenoh::Error>
where
    F: Fn(&str, u64, u64) -> Option<(Vec<u8>, u64)> + Send + Sync + 'static,
    N: Fn(String) -> Fut + Send + Sync + 'static,
    Fut: std::future::Future<Output = Option<String>> + Send,
{
    let q = session.declare_queryable(BLOB_WILDCARD).await?;
    let lookup = Arc::new(lookup);
    let name = Arc::new(name);
    let role = Arc::new(role);
    let log = Arc::new(Mutex::new(ServeLog::default()));
    let task = {
        let log = log.clone();
        tokio::spawn(async move {
            while let Ok(query) = q.recv_async().await {
                let (lookup, role, name, log) =
                    (lookup.clone(), role.clone(), name.clone(), log.clone());
                tokio::spawn(async move {
                    let Some(s) = answer(&query, &role, |sh, i, cs| lookup(sh, i, cs)).await else {
                        return;
                    };
                    tracing::debug!(
                        "served blob {} chunk {} to {}",
                        short(&s.sha),
                        s.index,
                        s.to
                    );
                    let new = log.lock().expect("serve log").is_new(&s.to, &s.sha);
                    let label = if new { name(s.sha.clone()).await } else { None };
                    let done = log
                        .lock()
                        .expect("serve log")
                        .record(&s, label, Instant::now());
                    if let Some(sum) = done {
                        tracing::info!("{}", sum.line());
                    }
                });
            }
        })
    };
    let sweeper = tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(5));
        loop {
            tick.tick().await;
            let idle = log
                .lock()
                .expect("serve log")
                .sweep(Instant::now(), SERVE_IDLE);
            for sum in idle {
                tracing::info!("{}", sum.line());
            }
        }
    });
    Ok(BlobServer { task, sweeper })
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

    fn served(to: &str, sha: &str, i: u64, size: u64) -> Served {
        let (_, len) = chunk_range(size, 4096, i).unwrap();
        Served {
            sha: sha.into(),
            index: i,
            len,
            size,
            chunk_size: 4096,
            to: to.into(),
        }
    }

    #[test]
    fn serve_log_one_line_per_transfer() {
        let sha = "e1936f74b612".to_string() + &"0".repeat(52);
        let mut log = ServeLog::default();
        let t0 = Instant::now();
        let size = 10 * 4096 - 100; // 10 chunks
                                    // towerx gets everything, with a duplicate request for chunk 3.
        assert!(log.is_new("towerx", &sha));
        for (n, i) in [0u64, 1, 2, 3, 3, 4, 5, 6, 7, 8].iter().enumerate() {
            let at = t0 + Duration::from_millis(100 * n as u64);
            assert_eq!(
                log.record(
                    &served("towerx", &sha, *i, size),
                    Some("Notes/x.bin".into()),
                    at
                ),
                None
            );
        }
        assert!(!log.is_new("towerx", &sha));
        // s24 asks for the same blob at the same time: a separate transfer.
        log.record(&served("s24", &sha, 0, size), None, t0);
        log.record(
            &served("s24", &sha, 1, size),
            None,
            t0 + Duration::from_secs(1),
        );
        let done = log
            .record(
                &served("towerx", &sha, 9, size),
                None,
                t0 + Duration::from_millis(3100),
            )
            .unwrap();
        assert!(done.complete);
        assert_eq!((done.chunks, done.total), (10, 10));
        assert_eq!(done.bytes, size + 4096, "duplicate chunk counted in bytes");
        assert_eq!(
            done.line(),
            format!(
                "served blob e1936f74b612 (Notes/x.bin) to towerx: 10/10 chunks, {} B in 3.1 s",
                size + 4096
            )
        );
        assert!(log.is_new("towerx", &sha), "closed");
        // s24 goes quiet: one partial line after the idle time, not before.
        assert!(log
            .sweep(t0 + Duration::from_secs(20), SERVE_IDLE)
            .is_empty());
        let idle = log.sweep(t0 + Duration::from_secs(32), SERVE_IDLE);
        assert_eq!(idle.len(), 1);
        assert_eq!(
            idle[0].line(),
            "served blob e1936f74b612 to s24: partial 2/10 chunks, 8192 B in 1.0 s, idle"
        );
        assert_eq!(log.open_transfers(), 0);
        // Unknown requester and a one-chunk blob.
        let one = log
            .record(&served("unknown", &sha, 0, 100), None, t0)
            .unwrap();
        assert_eq!(
            one.line(),
            "served blob e1936f74b612 to unknown: 1/1 chunks, 100 B in 0.0 s"
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

    #[test]
    fn outgoing_cache_serves_from_disk_after_hash_check() {
        let dir = tempfile::tempdir().unwrap();
        let p = dir.path().join("photo.bin");
        let bytes = data(20_000);
        std::fs::write(&p, &bytes).unwrap();
        let sha = sha256_hex(&bytes);
        let c = OutgoingCache::new(1024, Duration::from_secs(60));
        c.insert_file(&sha, &p, "Daily/photo.bin");
        assert!(c.get(&sha).is_none(), "file entries are not held in RAM");
        assert_eq!(c.chunk(&sha, 1, 4096).unwrap(), bytes[4096..8192].to_vec());
        assert_eq!(c.name(&sha).as_deref(), Some("Daily/photo.bin"));
        // A rewrite with different bytes is not served.
        let other = bytes.iter().map(|b| !b).collect::<Vec<_>>();
        std::fs::write(&p, &other).unwrap();
        // Same-size rewrite can keep the previous mtime on a coarse FS;
        // bump it so the cache re-hashes.
        let later = std::time::SystemTime::now() + Duration::from_secs(2);
        std::fs::File::open(&p)
            .unwrap()
            .set_modified(later)
            .unwrap();
        assert!(c.chunk(&sha, 0, 4096).is_none(), "sha no longer matches");
        // Restored file is served again.
        std::fs::write(&p, &bytes).unwrap();
        let later = std::time::SystemTime::now() + Duration::from_secs(4);
        std::fs::File::open(&p)
            .unwrap()
            .set_modified(later)
            .unwrap();
        assert_eq!(c.chunk(&sha, 0, 4096).unwrap(), bytes[..4096].to_vec());
    }
}
