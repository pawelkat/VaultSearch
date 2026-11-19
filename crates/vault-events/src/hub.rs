//! The hub: an embedded Zenoh router plus the note storage that answers
//! catch-up queries, and (optionally) the one-way Drive backup.

use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, Mutex};
use zenoh::key_expr::KeyExpr;

use crate::blob::{self, TransferOpts, ZenohSource};
use crate::event::{now_ms, NoteEvent};
use crate::history::{History, HistoryEntry, HistoryReply};
use crate::key;
use crate::net::{self, BoxError, IndexReply};
use crate::presence;
use crate::store::HubStore;
use crate::vault::is_synced_path;

#[derive(Debug, Clone)]
pub struct HubOpts {
    pub data_dir: PathBuf,
    pub listen: Vec<String>,
    /// Other routers to link to (e.g. a later Note 4 replica).
    pub connect: Vec<String>,
    pub name: String,
    /// vault-rclone config for the Drive backup; `None` = no backup.
    pub backup_config: Option<PathBuf>,
    pub backup_interval: Duration,
    pub backup_dry_run: bool,
    /// Inline limit / chunk size for the blob store and pulls.
    pub transfer: TransferOpts,
}

/// Store `ev` (if it wins) and log it. Returns `true` if stored.
async fn ingest(store: &Mutex<HubStore>, history: &History, ev: &NoteEvent) -> bool {
    let (stored, replaced, inline_max) = {
        let mut st = store.lock().await;
        let replaced = st.stamp(&ev.stamp.path).cloned();
        (st.offer(ev), replaced, st.inline_max())
    };
    match stored {
        Ok(true) => {
            tracing::info!("{}", stored_line(ev));
            if ev
                .content
                .as_ref()
                .is_some_and(|c| c.len() as u64 > inline_max)
            {
                tracing::info!(
                    "{}: {} B arrived inline (older sender); kept as a blob, served in chunks",
                    ev.stamp.path,
                    ev.size()
                );
            }
            if let Some(c) = &ev.conflict {
                if c.kept_smaller() {
                    tracing::warn!(
                        "conflict on {}: kept the smaller side ({} B {} from {}) at the live path, saved the larger ({} B {} from {}) as {}",
                        c.of, c.winner_size, short(&c.winner_sha256), c.winner_origin,
                        c.loser_size, short(&c.loser_sha256), c.loser_origin, ev.stamp.path
                    );
                }
            }
            let entry = HistoryEntry::new(ev, replaced.as_ref(), now_ms());
            if let Err(e) = history.append(&entry) {
                tracing::warn!("history append for {} failed: {e}", ev.stamp.path);
            }
            true
        }
        Ok(false) => {
            tracing::debug!("kept newer state for {}", ev.stamp.path);
            false
        }
        Err(e) => {
            tracing::error!("store {} failed: {e}", ev.stamp.path);
            false
        }
    }
}

/// Run the hub until `shutdown` flips (or forever).
pub async fn run(
    opts: HubOpts,
    mut shutdown: Option<tokio::sync::watch::Receiver<bool>>,
) -> Result<(), BoxError> {
    let store = Arc::new(Mutex::new(HubStore::open_with(
        &opts.data_dir,
        opts.transfer.inline_max,
        opts.transfer.chunk_size,
    )?));
    {
        let s = store.lock().await;
        tracing::info!(
            "hub {}: {} notes stored ({} live) under {}",
            opts.name,
            s.len(),
            s.live_count(),
            opts.data_dir.join("store").display()
        );
    }
    let history = History::open(&opts.data_dir);
    tracing::info!(
        "hub {}: provenance history in {}",
        opts.name,
        history.file().display()
    );
    let session = zenoh::open(net::router_config(&opts.listen, &opts.connect)?).await?;
    tracing::info!(
        "hub router {} listening on {:?}",
        session.zid(),
        opts.listen
    );

    let sub = session.declare_subscriber(key::NOTES_WILDCARD).await?;
    let notes_q = session
        .declare_queryable(key::NOTES_WILDCARD)
        .complete(true)
        .await?;
    let index_q = session
        .declare_queryable(key::INDEX_KEY)
        .complete(true)
        .await?;
    let history_q = session
        .declare_queryable(key::HISTORY_WILDCARD)
        .complete(true)
        .await?;
    // Chunk queries are answered on their own task (never behind the store
    // lock or a slow note query) and only when asked as `src=hub`/`any`.
    let _blob_server = {
        let blobs = store.lock().await.blobs().clone();
        let names = store.clone();
        blob::serve(
            &session,
            blob::Role::Hub,
            move |sha, i, cs| blobs.read_chunk_sized(sha, i, cs).ok().flatten(),
            move |sha| {
                let names = names.clone();
                async move {
                    let s = names.lock().await;
                    let p = s
                        .stamps()
                        .find(|st| st.sha256 == sha)
                        .map(|st| st.path.clone());
                    p
                }
            },
        )
        .await?
    };
    // Presence: `vault/presence/<device>` liveliness tokens.
    let presence_sub = session
        .liveliness()
        .declare_subscriber(presence::PRESENCE_WILDCARD)
        .history(true)
        .await?;
    let presence_q = session
        .declare_queryable(presence::HUB_PRESENCE_KEY)
        .complete(true)
        .await?;
    let mut registry = presence::Registry::default();
    let (info_tx, mut info_rx) = mpsc::unbounded_channel::<presence::PresenceInfo>();
    let mut online_tick = tokio::time::interval_at(
        tokio::time::Instant::now() + ONLINE_LOG_EVERY,
        ONLINE_LOG_EVERY,
    );
    let hub_build = format!("vault-events {}", net::build_version());
    tracing::info!("hub {} build {hub_build}", opts.name);
    let partial_dir = opts.data_dir.join("partial");
    // Finished pulls: (sha256, the reference to store if the pull worked).
    let (pulled_tx, mut pulled_rx) = mpsc::unbounded_channel::<(String, Option<NoteEvent>)>();
    let mut pulling: HashSet<String> = HashSet::new();
    tracing::info!(
        "hub {}: files > {} B travel as blobs ({} B chunks) under {}",
        opts.name,
        opts.transfer.inline_max,
        opts.transfer.chunk_size,
        opts.data_dir.join("blobs").display()
    );

    let mut backup_due: Option<Instant> = None;
    let mut backup_tick = tokio::time::interval(Duration::from_secs(5));

    loop {
        tokio::select! {
            s = sub.recv_async() => {
                let Ok(sample) = s else { break };
                let k = sample.key_expr().as_str().to_string();
                match NoteEvent::decode(&sample.payload().to_bytes(), Some(&k)) {
                    Ok(ev) if !is_synced_path(&ev.stamp.path) => {
                        tracing::debug!("not storing non-synced path {}", ev.stamp.path);
                    }
                    Ok(ev) if ev.is_blob_ref() => {
                        let (want, have, blobs) = {
                            let st = store.lock().await;
                            (st.would_store(&ev.stamp), st.blobs().has(&ev.stamp.sha256), st.blobs().clone())
                        };
                        let sha = ev.stamp.sha256.clone();
                        if !want {
                            tracing::debug!("kept newer state for {}", ev.stamp.path);
                        } else if have {
                            if ingest(&store, &history, &ev).await
                                && opts.backup_config.is_some()
                                && backup_due.is_none()
                            {
                                backup_due = Some(Instant::now() + opts.backup_interval);
                            }
                        } else if pulling.insert(sha.clone()) {
                            let info = ev.blob.expect("blob ref");
                            tracing::info!(
                                "pulling blob {} for {} v{} from {}: {} B in {} chunks of {} B",
                                short(&sha), ev.stamp.path, ev.stamp.version, ev.stamp.origin,
                                info.size, info.chunks, info.chunk_size
                            );
                            let src = ZenohSource::for_hub(session.clone(), &opts.transfer, &ev.stamp.origin, "hub");
                            let (tx, dir, topts) = (pulled_tx.clone(), partial_dir.clone(), opts.transfer.clone());
                            tokio::spawn(async move {
                                let res = blob::fetch(&src, &dir, &sha, &info, &topts).await;
                                let done = match res {
                                    Ok(f) => match blobs.put(&f.bytes) {
                                        Ok(_) => {
                                            tracing::info!(
                                                "pulled blob {} for {}: {} B, {} chunks in {} ms ({} resumed, {} retried), sha256 ok",
                                                short(&sha), ev.stamp.path, f.bytes.len(), f.chunks,
                                                f.elapsed.as_millis(), f.resumed, f.retries
                                            );
                                            Some(ev)
                                        }
                                        Err(e) => {
                                            tracing::error!("saving blob {} failed: {e}", short(&sha));
                                            None
                                        }
                                    },
                                    Err(e) => {
                                        tracing::warn!(
                                            "pulling blob {} for {} failed: {e}; the sender republishes on catch-up",
                                            short(&sha), ev.stamp.path
                                        );
                                        None
                                    }
                                };
                                let _ = tx.send((sha, done));
                            });
                        } else {
                            tracing::debug!("blob {} already being pulled", short(&sha));
                        }
                    }
                    Ok(ev) => {
                        if ingest(&store, &history, &ev).await
                            && opts.backup_config.is_some()
                            && backup_due.is_none()
                        {
                            backup_due = Some(Instant::now() + opts.backup_interval);
                        }
                    }
                    Err(e) => tracing::warn!("dropping bad event on {k}: {e}"),
                }
            }
            q = notes_q.recv_async() => {
                let Ok(query) = q else { break };
                let qk = query.key_expr().clone();
                tracing::debug!("note query {} from {}", qk.as_str(), net::requester(&query));
                let replies: Vec<(String, Vec<u8>)> = {
                    let s = store.lock().await;
                    let mut v = Vec::new();
                    // Exact key: direct lookup. Wildcard: scan.
                    if let Ok(p) = key::key_to_path(qk.as_str()) {
                        if let Ok(Some(ev)) = s.get(&p) { v.push((qk.as_str().to_string(), ev.encode())); }
                    } else {
                        let paths: Vec<String> = s.stamps().map(|st| st.path.clone()).collect();
                        for p in paths {
                            let Ok(k) = key::path_to_key(&p) else { continue };
                            let Ok(ke) = KeyExpr::try_from(k.clone()) else { continue };
                            if ke.intersects(&qk) {
                                if let Ok(Some(ev)) = s.get(&p) { v.push((k, ev.encode())); }
                            }
                        }
                    }
                    v
                };
                for (k, b) in replies {
                    if let Err(e) = query.reply(k, b).await {
                        tracing::warn!("reply failed: {e}");
                    }
                }
            }
            Some((sha, done)) = pulled_rx.recv() => {
                pulling.remove(&sha);
                if let Some(ev) = done {
                    if ingest(&store, &history, &ev).await
                        && opts.backup_config.is_some()
                        && backup_due.is_none()
                    {
                        backup_due = Some(Instant::now() + opts.backup_interval);
                    }
                }
            }
            q = index_q.recv_async() => {
                let Ok(query) = q else { break };
                let body = {
                    let s = store.lock().await;
                    IndexReply { format: 1, hub: opts.name.clone(), stamps: s.stamps().cloned().collect() }
                };
                tracing::info!("catch-up from {}: {} listed", net::requester(&query), body.stamps.len());
                if let Err(e) = query.reply(key::INDEX_KEY, serde_json::to_vec(&body)?).await {
                    tracing::warn!("index reply failed: {e}");
                }
            }
            q = history_q.recv_async() => {
                let Ok(query) = q else { break };
                let qk = query.key_expr().as_str().to_string();
                tracing::debug!("history query {qk} from {}", net::requester(&query));
                let limit: usize = query
                    .parameters()
                    .get("limit")
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(50);
                match key::history_key_to_path(&qk) {
                    Ok(path) => {
                        let entries = match history.query(&path, limit) {
                            Ok(e) => e,
                            Err(e) => {
                                tracing::warn!("history read failed: {e}");
                                Vec::new()
                            }
                        };
                        let body = HistoryReply { format: 1, hub: opts.name.clone(), path, entries };
                        if let Err(e) = query.reply(qk.as_str(), serde_json::to_vec(&body)?).await {
                            tracing::warn!("history reply failed: {e}");
                        }
                    }
                    Err(e) => tracing::debug!("history query on {qk} ignored: {e}"),
                }
            }
            s = presence_sub.recv_async() => {
                let Ok(sample) = s else { break };
                let Some(dev) = presence::device_of_key(sample.key_expr().as_str()) else { continue };
                let now = now_ms();
                match sample.kind() {
                    zenoh::sample::SampleKind::Put => {
                        if registry.join(&dev, now) {
                            tracing::info!("device online: {dev}");
                            let (s, tx) = (session.clone(), info_tx.clone());
                            tokio::spawn(async move {
                                if let Some(i) = presence::fetch_info(&s, &dev, "hub", Duration::from_secs(5)).await {
                                    let _ = tx.send(i);
                                }
                            });
                        }
                    }
                    zenoh::sample::SampleKind::Delete => {
                        if registry.leave(&dev, now) {
                            tracing::info!("device offline: {dev}");
                        }
                    }
                }
            }
            Some(info) = info_rx.recv() => {
                tracing::info!("device {}: {} {}", info.device, info.role, info.build);
                registry.set_info(info);
            }
            q = presence_q.recv_async() => {
                let Ok(query) = q else { break };
                tracing::debug!("presence query from {}", net::requester(&query));
                let body = presence::PresenceReply {
                    hub: opts.name.clone(),
                    hub_build: hub_build.clone(),
                    now_ms: now_ms(),
                    peers: registry.snapshot(now_ms()),
                };
                if let Err(e) = query.reply(presence::HUB_PRESENCE_KEY, serde_json::to_vec(&body)?).await {
                    tracing::warn!("presence reply failed: {e}");
                }
            }
            _ = online_tick.tick() => {
                tracing::info!("{}", online_line(&registry.online()));
            }
            _ = backup_tick.tick() => {
                if let (Some(due), Some(cfg)) = (backup_due, opts.backup_config.clone()) {
                    if Instant::now() >= due {
                        backup_due = None;
                        let dry = opts.backup_dry_run;
                        let res = tokio::task::spawn_blocking(move || run_backup(&cfg, dry)).await;
                        match res {
                            Ok(Ok(0)) => tracing::info!("drive backup ok"),
                            Ok(Ok(code)) => tracing::warn!("drive backup exit {code}"),
                            Ok(Err(e)) => tracing::warn!("drive backup failed: {e}"),
                            Err(e) => tracing::warn!("drive backup task failed: {e}"),
                        }
                    }
                }
            }
            _ = async {
                match shutdown.as_mut() {
                    Some(s) => { let _ = s.changed().await; }
                    None => std::future::pending::<()>().await,
                }
            } => break,
        }
    }
    session.close().await?;
    Ok(())
}

/// How often the hub logs who is online.
pub const ONLINE_LOG_EVERY: Duration = Duration::from_secs(600);

/// `online: box, s24, towerx` (or `online: none`).
pub fn online_line(devs: &[String]) -> String {
    if devs.is_empty() {
        "online: none".to_string()
    } else {
        format!("online: {}", devs.join(", "))
    }
}

fn short(sha: &str) -> &str {
    &sha[..12.min(sha.len())]
}

/// `stored <path> vN from <origin> by <writer> (<cause>)[ (delete)][ [blob N B, sha256 <12>]]`
pub fn stored_line(ev: &NoteEvent) -> String {
    let s = &ev.stamp;
    let blob = match &ev.blob {
        Some(b) => format!(" [blob {} B, sha256 {}]", b.size, short(&s.sha256)),
        None => String::new(),
    };
    format!(
        "stored {} v{} from {} by {} ({}){}{}",
        s.path,
        s.version,
        s.origin,
        s.writer_str(),
        s.cause_str(),
        if s.deleted { " (delete)" } else { "" },
        blob
    )
}

/// One-way `vault-rclone backup` (rclone copy --update, local -> Drive).
fn run_backup(cfg_file: &std::path::Path, dry_run: bool) -> Result<i32, BoxError> {
    let cfg = vault_rclone::Config::load(Some(cfg_file))?;
    let host = vault_rclone::commands::Host::from_process();
    let mut out = Vec::new();
    let code =
        vault_rclone::sync::backup(&cfg, &host, &vault_rclone::SystemRunner, dry_run, &mut out)?;
    for l in String::from_utf8_lossy(&out).lines() {
        tracing::info!("{l}");
    }
    Ok(code)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::cause;

    #[test]
    fn stored_line_names_writer_and_cause() {
        let e = NoteEvent::put("Daily/x.md", b"a".to_vec(), "box", 3, None, 1)
            .by("box/galanonim", cause::LOCAL_EDIT);
        assert_eq!(
            stored_line(&e),
            "stored Daily/x.md v3 from box by box/galanonim (local-edit)"
        );
        let old = NoteEvent::delete("a.md", "s24", 2, None, 1);
        assert_eq!(
            stored_line(&old),
            "stored a.md v2 from s24 by - (-) (delete)"
        );
    }
}
