//! The file-watching agent: turns local saves into events and applies
//! incoming events to the vault tree.
//!
//! [`AgentCore`] is synchronous and holds all decisions, so tests can drive
//! it directly. [`run`] wires it to Zenoh and the `notify` watcher.
//!
//! Echo suppression is content-based: after the agent writes a file it
//! records the incoming stamp *with that file's SHA-256*. When the watcher
//! then reports the write, the disk hash equals the recorded one and
//! nothing is published. Publishes also use `Locality::Remote`, so a
//! session never receives its own puts.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::io;
use std::path::{Path, PathBuf};
use std::time::Duration;

use notify::{RecursiveMode, Watcher};
use tokio::sync::mpsc;

use crate::conflict::{self, Decision};
use crate::event::{now_ms, NoteEvent, Stamp};
use crate::key;
use crate::net;
use crate::state::AgentState;
use crate::vault::{is_ignored, Disk, VaultFs};

pub const DEFAULT_MAX_DELETE: usize = 50;
pub const DEFAULT_MIN_FILES: usize = 3;

/// What one incoming event did.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Applied {
    pub decision: Option<Decision>,
    /// Live path written or deleted.
    pub changed: bool,
    /// Conflict copies written (relative paths).
    pub conflicts: Vec<String>,
    /// Events to publish as a result (local winner, conflict copies).
    pub publish: Vec<NoteEvent>,
}

/// Result of turning a batch of local paths into events.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Batch {
    pub publish: Vec<NoteEvent>,
    /// Deletions held back by the mass-delete guard.
    pub refused_deletes: Vec<String>,
}

pub struct AgentCore {
    pub fs: VaultFs,
    pub device: String,
    pub state: AgentState,
    pub state_file: Option<PathBuf>,
    pub max_delete: usize,
    pub min_files: usize,
    dirty: bool,
}

impl AgentCore {
    pub fn new(fs: VaultFs, device: &str, state: AgentState, state_file: Option<PathBuf>) -> Self {
        AgentCore {
            fs,
            device: device.to_string(),
            state,
            state_file,
            max_delete: DEFAULT_MAX_DELETE,
            min_files: DEFAULT_MIN_FILES,
            dirty: false,
        }
    }

    pub fn save_if_dirty(&mut self) -> io::Result<()> {
        if self.dirty {
            if let Some(f) = &self.state_file {
                self.state.save(f)?;
            }
            self.dirty = false;
        }
        Ok(())
    }

    fn record(&mut self, stamp: Stamp) {
        let st = self.fs.stat(&stamp.path);
        self.state.record(stamp, st);
        self.dirty = true;
    }

    /// Apply one event that arrived from the network.
    pub fn on_incoming(&mut self, ev: &NoteEvent) -> io::Result<Applied> {
        let path = ev.stamp.path.clone();
        let mut out = Applied::default();
        if is_ignored(&path) {
            tracing::debug!("ignoring event for ignored path {path}");
            return Ok(out);
        }
        let disk = self.fs.read(&path)?;
        if let Disk::Skipped(why) = &disk {
            tracing::warn!("not applying event for {path}: local file is {why}");
            return Ok(out);
        }
        let local_ts = match &disk {
            Disk::Present { mtime_ms, .. } => *mtime_ms,
            _ => now_ms(),
        };
        let d = conflict::decide(
            self.state.stamp(&path),
            disk.sha(),
            &ev.stamp,
            &self.device,
            local_ts,
        );
        out.decision = Some(d.clone());
        match d {
            Decision::Ignore => {}
            Decision::Adopt => self.record(ev.stamp.clone()),
            Decision::Apply { save_local_as } => {
                if let (Some(name), Disk::Present { bytes, .. }) = (&save_local_as, &disk) {
                    self.write_conflict(name, bytes, &mut out)?;
                }
                match &ev.content {
                    Some(c) if !ev.stamp.deleted => self.fs.write_atomic(&path, c)?,
                    _ => self.fs.remove(&path)?,
                }
                out.changed = true;
                self.record(ev.stamp.clone());
            }
            Decision::KeepLocal {
                local,
                save_incoming_as,
            } => {
                if let (Some(name), Some(c)) = (&save_incoming_as, &ev.content) {
                    self.write_conflict(name, c, &mut out)?;
                }
                let content = match disk {
                    Disk::Present { bytes, .. } => Some(bytes),
                    _ => None,
                };
                let ev = NoteEvent {
                    format: crate::event::FORMAT_VERSION,
                    stamp: local.clone(),
                    content,
                };
                self.record(local);
                out.publish.push(ev);
            }
        }
        Ok(out)
    }

    fn write_conflict(&mut self, name: &str, bytes: &[u8], out: &mut Applied) -> io::Result<()> {
        if let Some(slot) = self.fs.conflict_slot(name, bytes)? {
            self.fs.write_atomic(&slot, bytes)?;
            tracing::warn!("conflict: saved losing copy as {slot}");
            out.conflicts.push(slot.clone());
            if let Some(ev) = self.on_local(&slot)? {
                out.publish.push(ev);
            }
        }
        Ok(())
    }

    /// A local path may have changed (watcher event). Always hashes the
    /// file. Returns the event to publish, if any.
    pub fn on_local(&mut self, rel: &str) -> io::Result<Option<NoteEvent>> {
        self.local_change(rel, false)
    }

    /// `trust_stat`: skip hashing when mtime+size match what was recorded
    /// (periodic rescans only; watcher events always hash, because two
    /// same-size saves inside the filesystem's mtime granularity look
    /// identical by stat alone).
    fn local_change(&mut self, rel: &str, trust_stat: bool) -> io::Result<Option<NoteEvent>> {
        if is_ignored(rel) || key::validate_rel(rel).is_err() {
            return Ok(None);
        }
        let entry = self.state.entries.get(rel).cloned();
        if trust_stat {
            if let (Some(e), Some(st)) = (&entry, self.fs.stat(rel)) {
                if !e.stamp.deleted && e.mtime_ms == Some(st.0) && e.size == Some(st.1) {
                    return Ok(None);
                }
            }
        }
        let prev = entry.as_ref().map(|e| &e.stamp);
        let prev_sha = prev.and_then(|s| (!s.deleted).then(|| s.sha256.clone()));
        let version = prev.map_or(1, |s| s.version + 1);
        match self.fs.read(rel)? {
            Disk::Skipped(why) => {
                tracing::debug!("not syncing {rel}: {why}");
                Ok(None)
            }
            Disk::Present {
                sha256,
                bytes,
                mtime_ms,
                ..
            } => {
                if prev_sha.as_deref() == Some(sha256.as_str()) {
                    // Content unchanged (touch, or our own write): refresh cache.
                    let s = prev.expect("prev").clone();
                    self.record(s);
                    return Ok(None);
                }
                // ts = file mtime (the edit time), so a first join compares
                // like rclone's "newer wins" instead of "whoever published
                // last". Only a tie-break after the version counter.
                let ts = if mtime_ms > 0 { mtime_ms } else { now_ms() };
                let ev = NoteEvent::put(rel, bytes, &self.device, version, prev_sha, ts);
                self.record(ev.stamp.clone());
                Ok(Some(ev))
            }
            Disk::Absent => match prev {
                Some(s) if !s.deleted => {
                    let ev = NoteEvent::delete(rel, &self.device, version, prev_sha, now_ms());
                    self.record(ev.stamp.clone());
                    Ok(Some(ev))
                }
                _ => Ok(None),
            },
        }
    }

    /// Process a set of possibly-changed local paths with the mass-delete
    /// guard: if more than `max_delete` tracked notes vanished at once (an
    /// unmounted vault, a wrong path, a bulk `rm`), no tombstone is
    /// published and the notes stay tracked as live.
    pub fn local_batch(&mut self, paths: &BTreeSet<String>) -> io::Result<Batch> {
        self.batch(paths, false)
    }

    fn batch(&mut self, paths: &BTreeSet<String>, trust_stat: bool) -> io::Result<Batch> {
        let mut out = Batch::default();
        if !self.fs.root().is_dir() {
            tracing::error!(
                "vault root {} is missing; ignoring local changes",
                self.fs.root().display()
            );
            return Ok(out);
        }
        let vanished: Vec<String> = paths
            .iter()
            .filter(|p| {
                self.state.stamp(p).is_some_and(|s| !s.deleted)
                    && matches!(self.fs.read(p), Ok(Disk::Absent))
            })
            .cloned()
            .collect();
        let refuse = vanished.len() > self.max_delete;
        if refuse {
            tracing::error!(
                "mass-delete guard: {} tracked notes vanished at once (max {}); not publishing deletes",
                vanished.len(),
                self.max_delete
            );
            out.refused_deletes = vanished.clone();
        }
        let vanished: HashSet<String> = vanished.into_iter().collect();
        for p in paths {
            if refuse && vanished.contains(p) {
                continue;
            }
            if let Some(ev) = self.local_change(p, trust_stat)? {
                out.publish.push(ev);
            }
        }
        Ok(out)
    }

    /// Full scan: every file on disk plus every tracked path.
    pub fn scan(&mut self) -> io::Result<Batch> {
        let files = self.fs.scan()?;
        let live_tracked = self
            .state
            .entries
            .values()
            .filter(|e| !e.stamp.deleted)
            .count();
        if files.len() < self.min_files && live_tracked >= self.min_files {
            tracing::error!(
                "vault {} has {} files (< MIN_FILES {}) but {} are tracked; refusing scan",
                self.fs.root().display(),
                files.len(),
                self.min_files,
                live_tracked
            );
            return Ok(Batch::default());
        }
        let mut all: BTreeSet<String> = files.into_iter().collect();
        all.extend(self.state.entries.keys().cloned());
        self.batch(&all, true)
    }

    /// Given the hub's stamps: which paths to fetch (hub newer or unknown
    /// here) and which local states to (re)publish (hub older or missing —
    /// e.g. edits made while the hub was unreachable).
    pub fn plan_catchup(&self, hub: &[Stamp]) -> (Vec<String>, Vec<String>) {
        let hub_map: BTreeMap<&str, &Stamp> = hub.iter().map(|s| (s.path.as_str(), s)).collect();
        let mut fetch = Vec::new();
        for s in hub {
            if is_ignored(&s.path) {
                continue;
            }
            match self.state.stamp(&s.path) {
                None => fetch.push(s.path.clone()),
                Some(mine) if s.beats(mine) => fetch.push(s.path.clone()),
                _ => {}
            }
        }
        let mut republish = Vec::new();
        for (p, e) in &self.state.entries {
            match hub_map.get(p.as_str()) {
                None => republish.push(p.clone()),
                Some(h) if e.stamp.beats(h) => republish.push(p.clone()),
                _ => {}
            }
        }
        (fetch, republish)
    }

    /// Rebuild the event for our recorded state of `rel` (so the hub can
    /// store it). If the disk no longer matches, treat it as a local change.
    pub fn republish(&mut self, rel: &str) -> io::Result<Option<NoteEvent>> {
        let Some(st) = self.state.stamp(rel).cloned() else {
            return Ok(None);
        };
        match (self.fs.read(rel)?, st.deleted) {
            (Disk::Absent, true) => Ok(Some(NoteEvent {
                format: crate::event::FORMAT_VERSION,
                stamp: st,
                content: None,
            })),
            (Disk::Present { sha256, bytes, .. }, false) if sha256 == st.sha256 => {
                Ok(Some(NoteEvent {
                    format: crate::event::FORMAT_VERSION,
                    stamp: st,
                    content: Some(bytes),
                }))
            }
            (Disk::Skipped(_), _) => Ok(None),
            _ => self.on_local(rel),
        }
    }
}

/// Options for [`run`].
#[derive(Debug, Clone)]
pub struct AgentOpts {
    pub vault: PathBuf,
    pub device: String,
    pub state_dir: PathBuf,
    pub connect: Vec<String>,
    pub max_bytes: u64,
    pub max_delete: usize,
    pub min_files: usize,
    pub debounce: Duration,
    pub catchup_interval: Duration,
    pub rescan_interval: Duration,
    pub query_timeout: Duration,
}

pub fn state_file(state_dir: &Path) -> PathBuf {
    state_dir.join("agent-state.json")
}

/// Shared handle for tests: lets a caller stop the agent loop.
pub type Shutdown = tokio::sync::watch::Receiver<bool>;

async fn publish_all(session: &zenoh::Session, evs: &[NoteEvent]) {
    for ev in evs {
        match net::publish(session, ev).await {
            Ok(()) => tracing::info!(
                "published {} v{} {}",
                ev.stamp.path,
                ev.stamp.version,
                if ev.stamp.deleted { "(delete)" } else { "" }
            ),
            Err(e) => tracing::warn!("publish {} failed: {e}", ev.stamp.path),
        }
    }
}

/// One catch-up pass against the hub. Returns `false` if no hub answered.
pub async fn catch_up(
    core: &mut AgentCore,
    session: &zenoh::Session,
    timeout: Duration,
) -> Result<bool, net::BoxError> {
    let Some(index) = net::fetch_index(session, timeout).await? else {
        return Ok(false);
    };
    let (fetch, republish) = core.plan_catchup(&index.stamps);
    let (mut applied, mut conflicts) = (0usize, 0usize);
    for p in &fetch {
        match net::fetch_note(session, p, timeout).await? {
            Some(ev) => {
                let a = core.on_incoming(&ev)?;
                applied += usize::from(a.changed);
                conflicts += a.conflicts.len();
                publish_all(session, &a.publish).await;
            }
            None => tracing::warn!("catch-up: hub listed {p} but returned nothing"),
        }
    }
    let mut out = Vec::new();
    for p in &republish {
        if let Some(ev) = core.republish(p)? {
            out.push(ev);
        }
    }
    publish_all(session, &out).await;
    core.save_if_dirty()?;
    tracing::info!(
        "catch-up from hub {}: {} stored, fetched {}, applied {}, conflicts {}, republished {}",
        index.hub,
        index.stamps.len(),
        fetch.len(),
        applied,
        conflicts,
        out.len()
    );
    Ok(true)
}

/// Run the agent until `shutdown` flips to `true` (or forever).
pub async fn run(opts: AgentOpts, mut shutdown: Option<Shutdown>) -> Result<(), net::BoxError> {
    let vault = opts
        .vault
        .canonicalize()
        .map_err(|e| io::Error::other(format!("vault {}: {e}", opts.vault.display())))?;
    let sf = state_file(&opts.state_dir);
    let state = AgentState::load_or_new(&sf, &opts.device, &vault)?;
    let mut core = AgentCore::new(
        VaultFs::new(&vault, opts.max_bytes),
        &opts.device,
        state,
        Some(sf),
    );
    core.max_delete = opts.max_delete;
    core.min_files = opts.min_files;

    let session = loop {
        match zenoh::open(net::client_config(&opts.connect)?).await {
            Ok(s) => break s,
            Err(e) => {
                tracing::warn!("cannot reach hub {:?}: {e}; retrying in 10s", opts.connect);
                tokio::time::sleep(Duration::from_secs(10)).await;
            }
        }
    };
    tracing::info!(
        "agent {} on {} connected to {:?}",
        opts.device,
        vault.display(),
        opts.connect
    );

    let sub = session
        .declare_subscriber(key::NOTES_WILDCARD)
        .allowed_origin(zenoh::sample::Locality::Remote)
        .await?;

    // Watcher -> channel of absolute paths.
    let (tx, mut rx) = mpsc::unbounded_channel::<PathBuf>();
    let mut watcher = notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
        if let Ok(ev) = res {
            for p in ev.paths {
                let _ = tx.send(p);
            }
        }
    })?;
    watcher.watch(&vault, RecursiveMode::Recursive)?;

    // Remote first, then local: incoming changes are applied (with conflict
    // copies) before offline local edits are published.
    match catch_up(&mut core, &session, opts.query_timeout).await {
        Ok(true) => {}
        Ok(false) => tracing::warn!("no hub answered the catch-up query; will retry"),
        Err(e) => tracing::warn!("catch-up failed: {e}"),
    }
    let b = core.scan()?;
    publish_all(&session, &b.publish).await;
    core.save_if_dirty()?;
    tracing::info!("initial scan: published {}", b.publish.len());

    let mut pending: BTreeSet<String> = BTreeSet::new();
    let mut catchup_tick = tokio::time::interval(opts.catchup_interval);
    catchup_tick.tick().await;
    let mut rescan_tick = tokio::time::interval(opts.rescan_interval);
    rescan_tick.tick().await;
    let mut link_tick = tokio::time::interval(Duration::from_secs(5));
    let mut routers = net::router_count(&session).await;

    loop {
        let debounce = tokio::time::sleep(opts.debounce);
        tokio::pin!(debounce);
        tokio::select! {
            s = sub.recv_async() => {
                let Ok(sample) = s else { break };
                let k = sample.key_expr().as_str().to_string();
                let bytes = sample.payload().to_bytes();
                match NoteEvent::decode(&bytes, Some(&k)) {
                    Ok(ev) => match core.on_incoming(&ev) {
                        Ok(a) => {
                            if a.changed {
                                tracing::info!("applied {} v{} from {}", ev.stamp.path, ev.stamp.version, ev.stamp.origin);
                            }
                            publish_all(&session, &a.publish).await;
                        }
                        Err(e) => tracing::error!("apply {} failed: {e}", ev.stamp.path),
                    },
                    Err(e) => tracing::warn!("dropping bad event on {k}: {e}"),
                }
                core.save_if_dirty()?;
            }
            Some(p) = rx.recv() => {
                if let Ok(rel) = key::rel_from_fs(&vault, &p) {
                    if !is_ignored(&rel) {
                        if p.is_dir() {
                            // Directory created / moved in: look at its files.
                            if let Ok(files) = VaultFs::new(&p, opts.max_bytes).scan() {
                                for f in files { pending.insert(format!("{rel}/{f}")); }
                            }
                        }
                        // Directory removed / moved out: tracked children.
                        let prefix = format!("{rel}/");
                        for k in core.state.entries.keys() {
                            if k.starts_with(&prefix) { pending.insert(k.clone()); }
                        }
                        pending.insert(rel);
                    }
                }
            }
            _ = &mut debounce, if !pending.is_empty() => {
                let batch = std::mem::take(&mut pending);
                let b = core.local_batch(&batch)?;
                publish_all(&session, &b.publish).await;
                core.save_if_dirty()?;
            }
            _ = catchup_tick.tick() => {
                if let Err(e) = catch_up(&mut core, &session, opts.query_timeout).await {
                    tracing::warn!("catch-up failed: {e}");
                }
            }
            _ = rescan_tick.tick() => {
                let b = core.scan()?;
                if !b.publish.is_empty() {
                    tracing::info!("rescan found {} unpublished local changes", b.publish.len());
                }
                publish_all(&session, &b.publish).await;
                core.save_if_dirty()?;
            }
            _ = link_tick.tick() => {
                let n = net::router_count(&session).await;
                if n > 0 && routers == 0 {
                    tracing::info!("hub link up; catching up");
                    if let Err(e) = catch_up(&mut core, &session, opts.query_timeout).await {
                        tracing::warn!("catch-up failed: {e}");
                    }
                } else if n == 0 && routers > 0 {
                    tracing::warn!("hub link down; local changes are kept and republished on reconnect");
                }
                routers = n;
            }
            _ = async {
                match shutdown.as_mut() {
                    Some(s) => { let _ = s.changed().await; }
                    None => std::future::pending::<()>().await,
                }
            } => {
                break;
            }
        }
    }
    core.save_if_dirty()?;
    drop(watcher);
    session.close().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn core(dir: &Path, dev: &str) -> AgentCore {
        AgentCore::new(
            VaultFs::new(dir, 1 << 20),
            dev,
            AgentState::new(dev, dir),
            None,
        )
    }

    #[test]
    fn applied_event_is_not_echoed() {
        let a = tempfile::tempdir().unwrap();
        let mut c = core(a.path(), "box");
        let ev = NoteEvent::put("Notes/x.md", b"remote".to_vec(), "s24", 1, None, 1);
        let r = c.on_incoming(&ev).unwrap();
        assert!(r.changed && r.publish.is_empty());
        assert_eq!(
            std::fs::read(a.path().join("Notes/x.md")).unwrap(),
            b"remote"
        );
        // The watcher reports our own write: nothing to publish.
        assert_eq!(c.on_local("Notes/x.md").unwrap(), None);
        // Even with the mtime cache bypassed (touch).
        c.state.entries.get_mut("Notes/x.md").unwrap().mtime_ms = None;
        assert_eq!(c.on_local("Notes/x.md").unwrap(), None);
        // Same event arriving twice is ignored.
        assert_eq!(c.on_incoming(&ev).unwrap().decision, Some(Decision::Ignore));
        // A full scan publishes nothing either.
        assert!(c.scan().unwrap().publish.is_empty());
    }

    #[test]
    fn local_edits_bump_version_and_carry_prev() {
        let a = tempfile::tempdir().unwrap();
        let mut c = core(a.path(), "box");
        c.fs.write_atomic("a.md", b"1").unwrap();
        let e1 = c.on_local("a.md").unwrap().unwrap();
        assert_eq!((e1.stamp.version, e1.stamp.prev_sha256.clone()), (1, None));
        c.fs.write_atomic("a.md", b"2").unwrap();
        let e2 = c.on_local("a.md").unwrap().unwrap();
        assert_eq!(e2.stamp.version, 2);
        assert_eq!(
            e2.stamp.prev_sha256.as_deref(),
            Some(e1.stamp.sha256.as_str())
        );
        c.fs.remove("a.md").unwrap();
        let e3 = c.on_local("a.md").unwrap().unwrap();
        assert!(e3.stamp.deleted && e3.stamp.version == 3);
        assert_eq!(
            c.on_local("a.md").unwrap(),
            None,
            "tombstone published once"
        );
    }

    #[test]
    fn mass_delete_guard_holds_back_tombstones() {
        let a = tempfile::tempdir().unwrap();
        let mut c = core(a.path(), "box");
        c.max_delete = 2;
        c.min_files = 1;
        for i in 0..5 {
            c.fs.write_atomic(&format!("n{i}.md"), b"x").unwrap();
        }
        assert_eq!(c.scan().unwrap().publish.len(), 5);
        for i in 0..4 {
            c.fs.remove(&format!("n{i}.md")).unwrap();
        }
        let b = c.scan().unwrap();
        assert!(b.publish.is_empty());
        assert_eq!(b.refused_deletes.len(), 4);
        // Within the limit, deletes go out.
        c.max_delete = 10;
        assert_eq!(c.scan().unwrap().publish.len(), 4);
    }

    #[test]
    fn catchup_plan_and_republish() {
        let a = tempfile::tempdir().unwrap();
        let mut c = core(a.path(), "box");
        c.fs.write_atomic("mine.md", b"m").unwrap();
        let mine = c.on_local("mine.md").unwrap().unwrap();
        let theirs = NoteEvent::put("theirs.md", b"t".to_vec(), "s24", 1, None, 1);
        let (fetch, republish) = c.plan_catchup(std::slice::from_ref(&theirs.stamp));
        assert_eq!(fetch, vec!["theirs.md".to_string()]);
        assert_eq!(republish, vec!["mine.md".to_string()]);
        assert_eq!(c.republish("mine.md").unwrap().unwrap(), mine);
        // Hub already has our state: nothing to do.
        let (f, r) = c.plan_catchup(std::slice::from_ref(&mine.stamp));
        assert!(f.is_empty() && r.is_empty());
    }

    #[test]
    fn conflict_copy_is_written_and_published() {
        let a = tempfile::tempdir().unwrap();
        let mut c = core(a.path(), "box");
        c.fs.write_atomic("a.md", b"base").unwrap();
        let base = c.on_local("a.md").unwrap().unwrap();
        // We publish v2 "ours"; s24 concurrently publishes v2 "theirs" later.
        c.fs.write_atomic("a.md", b"ours").unwrap();
        let ours = c.on_local("a.md").unwrap().unwrap();
        let theirs = NoteEvent::put(
            "a.md",
            b"theirs".to_vec(),
            "s24",
            2,
            Some(base.stamp.sha256.clone()),
            ours.stamp.ts_ms + 1,
        );
        let r = c.on_incoming(&theirs).unwrap();
        assert!(r.changed);
        assert_eq!(r.conflicts.len(), 1);
        let cp = &r.conflicts[0];
        assert!(
            cp.starts_with("a (conflict box ") && cp.ends_with(").md"),
            "{cp}"
        );
        assert_eq!(std::fs::read(a.path().join(cp)).unwrap(), b"ours");
        assert_eq!(std::fs::read(a.path().join("a.md")).unwrap(), b"theirs");
        assert_eq!(r.publish.len(), 1, "conflict copy goes out as a new note");
        assert_eq!(r.publish[0].stamp.path, *cp);
    }

    #[test]
    fn refuses_paths_outside_vault_and_ignored() {
        let a = tempfile::tempdir().unwrap();
        let mut c = core(a.path(), "box");
        let ev = NoteEvent::put(
            ".obsidian/workspace.json",
            b"{}".to_vec(),
            "s24",
            1,
            None,
            1,
        );
        assert_eq!(c.on_incoming(&ev).unwrap(), Applied::default());
        assert!(!a.path().join(".obsidian").exists());
    }
}
