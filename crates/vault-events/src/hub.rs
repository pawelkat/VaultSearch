//! The hub: an embedded Zenoh router plus the note storage that answers
//! catch-up queries, and (optionally) the one-way Drive backup.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;
use zenoh::key_expr::KeyExpr;

use crate::event::NoteEvent;
use crate::key;
use crate::net::{self, BoxError, IndexReply};
use crate::store::HubStore;

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
}

/// Run the hub until `shutdown` flips (or forever).
pub async fn run(
    opts: HubOpts,
    mut shutdown: Option<tokio::sync::watch::Receiver<bool>>,
) -> Result<(), BoxError> {
    let store = Arc::new(Mutex::new(HubStore::open(&opts.data_dir)?));
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

    let mut backup_due: Option<Instant> = None;
    let mut backup_tick = tokio::time::interval(Duration::from_secs(5));

    loop {
        tokio::select! {
            s = sub.recv_async() => {
                let Ok(sample) = s else { break };
                let k = sample.key_expr().as_str().to_string();
                match NoteEvent::decode(&sample.payload().to_bytes(), Some(&k)) {
                    Ok(ev) => {
                        let stored = store.lock().await.offer(&ev);
                        match stored {
                            Ok(true) => {
                                tracing::info!("stored {} v{} from {}{}", ev.stamp.path, ev.stamp.version, ev.stamp.origin, if ev.stamp.deleted { " (delete)" } else { "" });
                                if opts.backup_config.is_some() && backup_due.is_none() {
                                    backup_due = Some(Instant::now() + opts.backup_interval);
                                }
                            }
                            Ok(false) => tracing::debug!("kept newer state for {}", ev.stamp.path),
                            Err(e) => tracing::error!("store {} failed: {e}", ev.stamp.path),
                        }
                    }
                    Err(e) => tracing::warn!("dropping bad event on {k}: {e}"),
                }
            }
            q = notes_q.recv_async() => {
                let Ok(query) = q else { break };
                let qk = query.key_expr().clone();
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
            q = index_q.recv_async() => {
                let Ok(query) = q else { break };
                let body = {
                    let s = store.lock().await;
                    IndexReply { format: 1, hub: opts.name.clone(), stamps: s.stamps().cloned().collect() }
                };
                if let Err(e) = query.reply(key::INDEX_KEY, serde_json::to_vec(&body)?).await {
                    tracing::warn!("index reply failed: {e}");
                }
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
