//! TowerX remote power over Zenoh (see vault note `Grok Bot - TowerX power`).
//!
//! - The ESP32 declares the liveliness token `devices/<id>` (zenoh-pico); it
//!   carries no commands.
//! - `zenoh-mcp-device-bridge` on note4c answers `devices/<id>/call/<tool>`
//!   (`status`, `power_on`, `power_off` with `{"confirm":true}`) and runs
//!   `~/towerx`, which talks to the board's LAN MCP.
//! - "TowerX available for Ask" = the TowerX vault-events agent token
//!   `vault/presence/towerx` is alive **and** the TowerX chat proxy `/health`
//!   is OK (llama-server and vault-searchd both up).

use std::time::Duration;

use serde::Deserialize;
use zenoh::query::{ConsolidationMode, QueryTarget};
use zenoh::Session;

use crate::net::{self, BoxError};
use crate::presence;

/// Zenoh device id of the TowerX power board.
pub const TOWERX_POWER_DEVICE: &str = "towerx-power";
/// vault-events agent device name on TowerX.
pub const TOWERX_AGENT: &str = "towerx";
/// TowerX chat proxy (`qwen-chat-proxy`, :8082) behind Tailscale Serve.
pub const TOWERX_HEALTH_URL: &str = "https://towerx.tailf52603.ts.net/health";

/// What the user asked for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    Status,
    On,
    Off,
}

impl Action {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "status" => Some(Self::Status),
            "on" => Some(Self::On),
            "off" => Some(Self::Off),
            _ => None,
        }
    }

    /// Bridge tool name.
    pub fn tool(self) -> &'static str {
        match self {
            Self::Status => "status",
            Self::On => "power_on",
            Self::Off => "power_off",
        }
    }

    /// Query payload; `off` needs an explicit confirm (6 s force-off).
    pub fn payload(self, confirm: bool) -> Result<Option<&'static str>, String> {
        match self {
            Self::Off if !confirm => {
                Err("off holds the power button for 6 s (force off): pass --yes".into())
            }
            Self::Off => Ok(Some(r#"{"confirm":true}"#)),
            _ => Ok(None),
        }
    }
}

/// `towerx` -> `towerx-power`; a full id ending in `-power` is kept.
pub fn device_id(target: &str) -> String {
    if target.ends_with("-power") {
        target.to_string()
    } else {
        format!("{target}-power")
    }
}

pub fn token_key(device: &str) -> String {
    format!("devices/{device}")
}

pub fn call_key(device: &str, tool: &str) -> String {
    format!("devices/{device}/call/{tool}")
}

#[derive(Debug, Deserialize)]
struct RawReply {
    ok: bool,
    #[serde(default)]
    result: Option<serde_json::Value>,
    #[serde(default)]
    error: Option<String>,
}

/// Bridge reply: `{"ok":true,"result":{"text":".."}}` or `{"ok":false,"error":".."}`.
pub fn parse_reply(bytes: &[u8]) -> Result<String, String> {
    let r: RawReply =
        serde_json::from_slice(bytes).map_err(|e| format!("bad bridge reply: {e}"))?;
    if r.ok {
        Ok(match r.result {
            Some(serde_json::Value::Object(m)) => match m.get("text") {
                Some(serde_json::Value::String(t)) => t.clone(),
                _ => serde_json::Value::Object(m).to_string(),
            },
            Some(v) => v.to_string(),
            None => "ok".into(),
        })
    } else {
        Err(r.error.unwrap_or_else(|| "bridge said ok=false".into()))
    }
}

/// TowerX is usable for Ask only when its agent is online and the proxy is healthy.
pub fn ask_available(agent_alive: bool, health_ok: bool) -> bool {
    agent_alive && health_ok
}

/// Is liveliness token `key` alive right now?
pub async fn token_alive(s: &Session, key: &str, timeout: Duration) -> bool {
    let Ok(replies) = s.liveliness().get(key).timeout(timeout).await else {
        return false;
    };
    while let Ok(r) = replies.recv_async().await {
        if r.result().is_ok() {
            return true;
        }
    }
    false
}

/// `get devices/<id>/call/<tool>`; first reply wins.
pub async fn call(
    s: &Session,
    device: &str,
    tool: &str,
    payload: Option<&str>,
    from: &str,
    timeout: Duration,
) -> Result<String, BoxError> {
    let mut q = s
        .get(net::with_from(&call_key(device, tool), from))
        .target(QueryTarget::All)
        .consolidation(ConsolidationMode::None)
        .timeout(timeout);
    if let Some(p) = payload {
        q = q.payload(p.as_bytes().to_vec());
    }
    let replies = q.await?;
    if let Ok(r) = replies.recv_async().await {
        return match r.result() {
            Ok(sample) => Ok(parse_reply(&sample.payload().to_bytes())?),
            Err(e) => Err(format!(
                "bridge error: {}",
                e.payload().try_to_string().unwrap_or_default()
            )
            .into()),
        };
    }
    Err(format!(
        "no reply on {} within {} s (is zenoh-mcp-device-bridge running on note4c?)",
        call_key(device, tool),
        timeout.as_secs()
    )
    .into())
}

/// `GET url` with curl (no HTTP client crate in this workspace); `Ok` = HTTP 200.
pub fn http_health(url: &str, timeout: Duration) -> Result<String, String> {
    let out = std::process::Command::new("curl")
        .args(["-fsS", "-m", &timeout.as_secs().max(1).to_string(), url])
        .output()
        .map_err(|e| format!("curl: {e}"))?;
    let body = String::from_utf8_lossy(&out.stdout).trim().to_string();
    if out.status.success() {
        Ok(body)
    } else {
        Err(String::from_utf8_lossy(&out.stderr).trim().to_string())
    }
}

/// Is the TowerX vault-events agent online?
pub async fn agent_alive(s: &Session, timeout: Duration) -> bool {
    token_alive(s, &presence::presence_key(TOWERX_AGENT), timeout).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys() {
        assert_eq!(device_id("towerx"), "towerx-power");
        assert_eq!(device_id("towerx-power"), "towerx-power");
        assert_eq!(token_key("towerx-power"), "devices/towerx-power");
        assert_eq!(
            call_key("towerx-power", Action::On.tool()),
            "devices/towerx-power/call/power_on"
        );
        assert_eq!(Action::Off.tool(), "power_off");
        assert_eq!(Action::parse("reset"), None);
    }

    #[test]
    fn off_needs_confirm() {
        assert!(Action::Off.payload(false).is_err());
        assert_eq!(
            Action::Off.payload(true).unwrap(),
            Some(r#"{"confirm":true}"#)
        );
        assert_eq!(Action::On.payload(false).unwrap(), None);
        assert_eq!(Action::Status.payload(true).unwrap(), None);
    }

    #[test]
    fn replies() {
        let ok = br#"{"ok":true,"result":{"text":"power GPIO27 HIGH, reset GPIO26 HIGH, cooldown 0 ms, ip 192.168.1.30"}}"#;
        assert!(parse_reply(ok).unwrap().starts_with("power GPIO27 HIGH"));
        assert_eq!(
            parse_reply(br#"{"ok":false,"error":"cooldown 39000 ms"}"#).unwrap_err(),
            "cooldown 39000 ms"
        );
        assert!(parse_reply(b"nope").is_err());
        assert_eq!(parse_reply(br#"{"ok":true}"#).unwrap(), "ok");
    }

    #[test]
    fn availability() {
        assert!(ask_available(true, true));
        assert!(!ask_available(false, true));
        assert!(!ask_available(true, false));
    }
}
