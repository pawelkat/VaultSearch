//! Zenoh plumbing: session configs, publish, and catch-up queries.

use std::time::Duration;

use serde::{Deserialize, Serialize};
use zenoh::qos::CongestionControl;
use zenoh::query::{ConsolidationMode, QueryTarget};
use zenoh::sample::Locality;
use zenoh::{Config, Session};

use crate::event::{NoteEvent, Stamp};
use crate::history::HistoryReply;
use crate::key;

pub const DEFAULT_PORT: u16 = 7447;

pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// Requester label when a query carries no (valid) `from` parameter.
pub const UNKNOWN_REQUESTER: &str = "unknown";
/// `from` value of the command-line tools.
pub const CLI_REQUESTER: &str = "cli";

/// Add `from=<device>` to a selector (`key` or `key?a=b`), so the answering
/// side can log who asked. Servers that ignore it answer as before.
pub fn with_from(selector: &str, from: &str) -> String {
    let sep = if selector.contains('?') { ';' } else { '?' };
    format!("{selector}{sep}from={}", clean_device(Some(from)))
}

/// A device label safe for logs and keys: ASCII letters, digits, `-`, `_`,
/// `.`, at most 64 chars; anything else (or nothing) is `unknown`.
pub fn clean_device(v: Option<&str>) -> String {
    match v {
        Some(d)
            if !d.is_empty()
                && d.len() <= 64
                && d.bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b)) =>
        {
            d.to_string()
        }
        _ => UNKNOWN_REQUESTER.to_string(),
    }
}

/// Who sent `query` (its `from` parameter), or `unknown`.
pub fn requester(query: &zenoh::query::Query) -> String {
    clean_device(query.parameters().get("from"))
}

/// `<crate version>+<git short sha>` of this build.
pub fn build_version() -> String {
    format!("{}+{}", env!("CARGO_PKG_VERSION"), env!("VAULT_EVENTS_GIT"))
}

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
    from: &str,
    timeout: Duration,
) -> Result<Option<IndexReply>, BoxError> {
    let replies = session
        .get(with_from(key::INDEX_KEY, from))
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
    from: &str,
    timeout: Duration,
) -> Result<Option<NoteEvent>, BoxError> {
    let k = key::path_to_key(path)?;
    let replies = session
        .get(with_from(&k, from))
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

/// Ask the hub for the provenance history of `path` (newest first).
/// `Ok(None)` = no hub answered.
pub async fn fetch_history(
    session: &Session,
    path: &str,
    limit: usize,
    from: &str,
    timeout: Duration,
) -> Result<Option<HistoryReply>, BoxError> {
    let k = key::path_to_history_key(path)?;
    let replies = session
        .get(with_from(&format!("{k}?limit={limit}"), from))
        .target(QueryTarget::All)
        .consolidation(ConsolidationMode::None)
        .allowed_destination(Locality::Remote)
        .timeout(timeout)
        .await?;
    while let Ok(reply) = replies.recv_async().await {
        if let Ok(sample) = reply.result() {
            let r: HistoryReply = serde_json::from_slice(&sample.payload().to_bytes())?;
            return Ok(Some(r));
        }
    }
    Ok(None)
}

/// Number of routers this session is connected to right now.
pub async fn router_count(session: &Session) -> usize {
    session.info().routers_zid().await.count()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn from_parameter_is_added_and_cleaned() {
        assert_eq!(
            with_from("vault/hub/index", "towerx"),
            "vault/hub/index?from=towerx"
        );
        assert_eq!(
            with_from("vault/hub/history/Notes/a.md?limit=5", "s24"),
            "vault/hub/history/Notes/a.md?limit=5;from=s24"
        );
        assert_eq!(
            with_from("vault/blob/x/0?cs=4096;src=hub", "box"),
            "vault/blob/x/0?cs=4096;src=hub;from=box"
        );
        assert_eq!(with_from("k", "bad;name=x"), "k?from=unknown");
        assert_eq!(clean_device(None), "unknown");
        assert_eq!(clean_device(Some("")), "unknown");
        assert_eq!(clean_device(Some("nitro-5_a.b")), "nitro-5_a.b");
        assert_eq!(clean_device(Some("a b")), "unknown");
        assert_eq!(clean_device(Some(&"x".repeat(65))), "unknown");
        // Round trip through Zenoh's own parameter parser.
        let p = zenoh::query::Parameters::from("cs=4096;src=hub;from=towerx");
        assert_eq!(clean_device(p.get("from")), "towerx");
        assert_eq!(
            clean_device(zenoh::query::Parameters::from("cs=1").get("from")),
            "unknown"
        );
        assert!(build_version().starts_with(env!("CARGO_PKG_VERSION")));
    }
}
