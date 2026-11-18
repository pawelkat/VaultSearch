//! Zenoh plumbing: session configs, publish, and catch-up queries.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use zenoh::qos::CongestionControl;
use zenoh::query::{ConsolidationMode, QueryTarget};
use zenoh::sample::Locality;
use zenoh::{Config, Session};

use crate::event::{NoteEvent, Stamp};
use crate::key;

pub const DEFAULT_PORT: u16 = 7447;

pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

fn json_list(items: &[String]) -> String {
    serde_json::to_string(items).expect("strings serialize")
}

/// Hub: a Zenoh router listening on `listen` (e.g. `tcp/0.0.0.0:7447`).
/// Multicast scouting is off: peers are reached over explicit tailnet
/// endpoints, never by LAN discovery.
pub fn router_config(listen: &[String], connect: &[String]) -> Result<Config, BoxError> {
    let mut c = Config::default();
    c.insert_json5("mode", "\"router\"")?;
    c.insert_json5("listen/endpoints", &json_list(listen))?;
    c.insert_json5("connect/endpoints", &json_list(connect))?;
    c.insert_json5("scouting/multicast/enabled", "false")?;
    Ok(c)
}

/// Agent: a Zenoh client of the hub router(s).
pub fn client_config(connect: &[String]) -> Result<Config, BoxError> {
    let mut c = Config::default();
    c.insert_json5("mode", "\"client\"")?;
    c.insert_json5("connect/endpoints", &json_list(connect))?;
    c.insert_json5("scouting/multicast/enabled", "false")?;
    Ok(c)
}

/// Publish one event, blocking under congestion instead of dropping it, and
/// not delivering it back to subscribers of the same session.
pub async fn publish(session: &Session, ev: &NoteEvent) -> Result<(), BoxError> {
    let k = ev.key()?;
    session
        .put(k, ev.encode())
        .congestion_control(CongestionControl::Block)
        .allowed_destination(Locality::Remote)
        .await?;
    Ok(())
}

/// Reply body of the hub's `vault/hub/index` queryable.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexReply {
    pub format: u32,
    pub hub: String,
    pub stamps: Vec<Stamp>,
}

/// Ask the hub for the stamp of every stored note. `Ok(None)` = no hub
/// answered within `timeout` (offline / unreachable).
pub async fn fetch_index(
    session: &Session,
    timeout: Duration,
) -> Result<Option<IndexReply>, BoxError> {
    let replies = session
        .get(key::INDEX_KEY)
        .target(QueryTarget::All)
        .consolidation(ConsolidationMode::None)
        .allowed_destination(Locality::Remote)
        .timeout(timeout)
        .await?;
    while let Ok(reply) = replies.recv_async().await {
        match reply.result() {
            Ok(sample) => {
                let bytes = sample.payload().to_bytes();
                let r: IndexReply = serde_json::from_slice(&bytes)?;
                return Ok(Some(r));
            }
            Err(e) => {
                tracing::warn!(
                    "index query error reply: {}",
                    e.payload().try_to_string().unwrap_or_default()
                );
            }
        }
    }
    Ok(None)
}

/// Fetch the stored event for one path from the hub.
pub async fn fetch_note(
    session: &Session,
    path: &str,
    timeout: Duration,
) -> Result<Option<NoteEvent>, BoxError> {
    let k = key::path_to_key(path)?;
    let replies = session
        .get(&k)
        .target(QueryTarget::All)
        .consolidation(ConsolidationMode::None)
        .allowed_destination(Locality::Remote)
        .timeout(timeout)
        .await?;
    let mut best: Option<NoteEvent> = None;
    while let Ok(reply) = replies.recv_async().await {
        if let Ok(sample) = reply.result() {
            let bytes = sample.payload().to_bytes();
            match NoteEvent::decode(&bytes, Some(sample.key_expr().as_str())) {
                Ok(ev) => {
                    if best.as_ref().is_none_or(|b| ev.stamp.beats(&b.stamp)) {
                        best = Some(ev);
                    }
                }
                Err(e) => tracing::warn!("bad catch-up reply for {path}: {e}"),
            }
        }
    }
    Ok(best)
}

/// Number of routers this session is connected to right now.
pub async fn router_count(session: &Session) -> usize {
    session.info().routers_zid().await.count()
}
