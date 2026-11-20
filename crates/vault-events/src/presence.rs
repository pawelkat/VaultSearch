//! Who is online: every agent (and VaultHome) declares a Zenoh liveliness
//! token `vault/presence/<device>` and answers a queryable on the same key
//! with its [`PresenceInfo`] (role, build). The hub subscribes to the tokens,
//! logs `device online: <dev>` / `device offline: <dev>`, keeps last-seen
//! times in a [`Registry`] and answers `vault/hub/presence` for
//! `vault-events peers`.

use std::collections::BTreeMap;
use std::time::Duration;

use serde::{Deserialize, Serialize};
use zenoh::query::{ConsolidationMode, QueryTarget};
use zenoh::Session;

use crate::event::now_ms;
use crate::net::{self, BoxError};

/// Prefix of liveliness tokens and presence queryables.
pub const PRESENCE_PREFIX: &str = "vault/presence";
/// Every device's token / presence queryable.
pub const PRESENCE_WILDCARD: &str = "vault/presence/*";
/// The hub's registry (online + recently seen devices).
pub const HUB_PRESENCE_KEY: &str = "vault/hub/presence";

/// `vault/presence/<device>`
pub fn presence_key(device: &str) -> String {
    format!("{PRESENCE_PREFIX}/{}", net::clean_device(Some(device)))
}

/// Inverse of [`presence_key`].
pub fn device_of_key(k: &str) -> Option<String> {
    let d = k.strip_prefix(PRESENCE_PREFIX)?.strip_prefix('/')?;
    (net::clean_device(Some(d)) == d).then(|| d.to_string())
}

/// What a device says about itself.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PresenceInfo {
    pub device: String,
    /// `agent`, `vaulthome`, ...
    pub role: String,
    /// `vault-events 0.1.0+abc1234`, `vaulthome 0.1.0+def5678`.
    pub build: String,
    pub started_ms: u64,
}

/// A device's token and presence queryable; dropping it goes offline.
pub struct Presence {
    _token: zenoh::liveliness::LivelinessToken,
    task: tokio::task::JoinHandle<()>,
}

impl Drop for Presence {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Declare `vault/presence/<device>` (liveliness token + info queryable).
pub async fn announce(session: &Session, info: PresenceInfo) -> Result<Presence, BoxError> {
    let k = presence_key(&info.device);
    let token = session.liveliness().declare_token(k.clone()).await?;
    let q = session.declare_queryable(k).complete(true).await?;
    let body = serde_json::to_vec(&info)?;
    let task = tokio::spawn(async move {
        while let Ok(query) = q.recv_async().await {
            let _ = query.reply(query.key_expr().clone(), body.clone()).await;
        }
    });
    Ok(Presence {
        _token: token,
        task,
    })
}

/// One row of the hub's registry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Peer {
    pub device: String,
    pub online: bool,
    /// When it (last) came online.
    pub since_ms: u64,
    /// Last time it was seen online (now, if online).
    pub last_seen_ms: u64,
    #[serde(default)]
    pub info: Option<PresenceInfo>,
    /// Live tokens for this device (two VaultHome sessions, say).
    #[serde(default)]
    pub tokens: u32,
}

/// Reply of [`HUB_PRESENCE_KEY`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PresenceReply {
    pub hub: String,
    pub hub_build: String,
    pub now_ms: u64,
    pub peers: Vec<Peer>,
}

/// Online / offline bookkeeping from liveliness puts and deletes.
#[derive(Debug, Default, Clone)]
pub struct Registry {
    peers: BTreeMap<String, Peer>,
}

impl Registry {
    /// A token appeared. `true` if the device just came online.
    pub fn join(&mut self, device: &str, now: u64) -> bool {
        let p = self
            .peers
            .entry(device.to_string())
            .or_insert_with(|| Peer {
                device: device.to_string(),
                online: false,
                since_ms: now,
                last_seen_ms: now,
                info: None,
                tokens: 0,
            });
        p.tokens += 1;
        p.last_seen_ms = now;
        if p.online {
            return false;
        }
        p.online = true;
        p.since_ms = now;
        true
    }

    /// A token went away. `true` if the device is now offline.
    pub fn leave(&mut self, device: &str, now: u64) -> bool {
        let Some(p) = self.peers.get_mut(device) else {
            return false;
        };
        p.tokens = p.tokens.saturating_sub(1);
        p.last_seen_ms = now;
        if p.online && p.tokens == 0 {
            p.online = false;
            return true;
        }
        false
    }

    pub fn set_info(&mut self, info: PresenceInfo) {
        if let Some(p) = self.peers.get_mut(&info.device) {
            p.info = Some(info);
        }
    }

    pub fn online(&self) -> Vec<String> {
        self.peers
            .values()
            .filter(|p| p.online)
            .map(|p| p.device.clone())
            .collect()
    }

    /// All rows, online ones with `last_seen_ms = now`.
    pub fn snapshot(&self, now: u64) -> Vec<Peer> {
        self.peers
            .values()
            .map(|p| {
                let mut p = p.clone();
                if p.online {
                    p.last_seen_ms = now;
                }
                p
            })
            .collect()
    }
}

/// Ask `vault/presence/<device>` for its info.
pub async fn fetch_info(
    session: &Session,
    device: &str,
    from: &str,
    timeout: Duration,
) -> Option<PresenceInfo> {
    let replies = session
        .get(net::with_from(&presence_key(device), from))
        .target(QueryTarget::All)
        .consolidation(ConsolidationMode::None)
        .timeout(timeout)
        .await
        .ok()?;
    while let Ok(r) = replies.recv_async().await {
        if let Ok(s) = r.result() {
            if let Ok(i) = serde_json::from_slice::<PresenceInfo>(&s.payload().to_bytes()) {
                return Some(i);
            }
        }
    }
    None
}

/// The hub's registry; `None` = no hub answered.
pub async fn fetch_hub_presence(
    session: &Session,
    from: &str,
    timeout: Duration,
) -> Option<PresenceReply> {
    let replies = session
        .get(net::with_from(HUB_PRESENCE_KEY, from))
        .target(QueryTarget::All)
        .consolidation(ConsolidationMode::None)
        .timeout(timeout)
        .await
        .ok()?;
    while let Ok(r) = replies.recv_async().await {
        if let Ok(s) = r.result() {
            if let Ok(p) = serde_json::from_slice::<PresenceReply>(&s.payload().to_bytes()) {
                return Some(p);
            }
        }
    }
    None
}

/// Devices with a live token right now (no hub needed).
pub async fn live_tokens(session: &Session, timeout: Duration) -> Vec<String> {
    let mut out = Vec::new();
    if let Ok(replies) = session
        .liveliness()
        .get(PRESENCE_WILDCARD)
        .timeout(timeout)
        .await
    {
        while let Ok(r) = replies.recv_async().await {
            if let Ok(s) = r.result() {
                if let Some(d) = device_of_key(s.key_expr().as_str()) {
                    out.push(d);
                }
            }
        }
    }
    out.sort();
    out.dedup();
    out
}

/// Info for this process (`role` = `agent`, ...).
pub fn this_build(device: &str, role: &str) -> PresenceInfo {
    PresenceInfo {
        device: net::clean_device(Some(device)),
        role: role.to_string(),
        build: format!("vault-events {}", net::build_version()),
        started_ms: now_ms(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_round_trip() {
        assert_eq!(presence_key("towerx"), "vault/presence/towerx");
        assert_eq!(
            device_of_key("vault/presence/towerx").as_deref(),
            Some("towerx")
        );
        assert_eq!(device_of_key("vault/presence/a/b"), None);
        assert_eq!(device_of_key("vault/notes/x"), None);
    }

    #[test]
    fn registry_join_leave_and_last_seen() {
        let mut r = Registry::default();
        assert!(r.join("towerx", 100));
        assert!(!r.join("towerx", 110), "second token, already online");
        assert!(r.join("s24", 120));
        assert_eq!(r.online(), vec!["s24".to_string(), "towerx".to_string()]);
        assert!(!r.leave("towerx", 200), "one token left");
        assert!(r.leave("towerx", 210));
        assert!(!r.leave("towerx", 220), "already offline");
        assert!(!r.leave("nitro5", 230), "never seen");
        assert_eq!(r.online(), vec!["s24".to_string()]);
        let snap = r.snapshot(500);
        let t = snap.iter().find(|p| p.device == "towerx").unwrap();
        assert!(!t.online && t.last_seen_ms == 220 && t.since_ms == 100);
        let s = snap.iter().find(|p| p.device == "s24").unwrap();
        assert!(s.online && s.last_seen_ms == 500);
        assert!(r.join("towerx", 600), "back online");
        r.set_info(this_build("towerx", "agent"));
        let t = r
            .snapshot(700)
            .into_iter()
            .find(|p| p.device == "towerx")
            .unwrap();
        assert_eq!(t.since_ms, 600);
        assert!(t.info.unwrap().build.starts_with("vault-events "));
    }
}
