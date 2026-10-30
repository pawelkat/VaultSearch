use serde::{Deserialize, Serialize};

use crate::Hit;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum IpcRequest {
    Search { query: String, k: Option<u32> },
    Status,
    Reindex,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct IpcHit {
    pub path: String,
    pub heading: String,
    pub title: String,
    pub score: f32,
    pub snippet: String,
    pub text: String,
}

impl From<&Hit> for IpcHit {
    fn from(hit: &Hit) -> Self {
        Self {
            path: hit.path.clone(),
            heading: hit.heading.clone(),
            title: hit.title.clone(),
            score: hit.score,
            snippet: hit.snippet.clone(),
            text: hit.text.clone(),
        }
    }
}

/// Sync health of a `vault-searchd serve --sync-interval N` daemon. Every
/// field is optional on the wire so older daemons and clients still parse.
#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct IpcSyncStatus {
    /// `true` when serve runs the guarded bisync on an interval.
    pub enabled: bool,
    /// Seconds between sync runs (0 = off).
    #[serde(default)]
    pub interval_secs: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub remote: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub local: Option<String>,
    /// Completed sync runs since serve started, and how many failed.
    #[serde(default)]
    pub runs: u64,
    #[serde(default)]
    pub failures: u64,
    /// Unix seconds (UTC) the last run finished.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_run_unix: Option<u64>,
    /// Unix seconds (UTC) of the last run that exited 0.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_ok_unix: Option<u64>,
    /// `true` while a bisync is in flight.
    #[serde(default)]
    pub running: bool,
    /// Exit code of the last run (rclone's own, or 1 for a guard refusal).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_exit: Option<i32>,
    /// Credential-safe one-liner from the last failed run; cleared on success.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_error: Option<String>,
    /// `reindex: ok (...)` / `reindex: FAILED (...)` after the last good sync.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_reindex: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq)]
pub struct IpcResponse {
    pub ok: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub text: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub hits: Option<Vec<IpcHit>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub notes: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub chunks: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub indexing: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub dim: Option<u32>,
    /// Vault root the daemon indexes (status only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub vault: Option<String>,
    /// Whether the loopback embedder accepted a TCP connection (status only).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embed_ok: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub embed_url: Option<String>,
    /// Sync health (status only; absent from daemons older than PR4).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sync: Option<IpcSyncStatus>,
}

/// One request/response with the default 8 second timeouts (search, status).
pub fn roundtrip(socket: &std::path::Path, request: &IpcRequest) -> Result<IpcResponse, String> {
    roundtrip_with_timeout(socket, request, std::time::Duration::from_secs(8))
}

/// Like [`roundtrip`], but waits up to `read_timeout` for the reply. A
/// reindex embeds every changed note before answering, so it needs minutes.
pub fn roundtrip_with_timeout(
    socket: &std::path::Path,
    request: &IpcRequest,
    read_timeout: std::time::Duration,
) -> Result<IpcResponse, String> {
    use std::io::{BufRead, BufReader, Write};
    use std::os::unix::net::UnixStream;
    use std::time::Duration;

    let mut stream = UnixStream::connect(socket).map_err(|e| {
        format!(
            "vault-searchd is not listening on {}: {e}",
            socket.display()
        )
    })?;
    stream
        .set_read_timeout(Some(read_timeout.max(Duration::from_secs(1))))
        .map_err(|e| e.to_string())?;
    stream
        .set_write_timeout(Some(Duration::from_secs(8)))
        .map_err(|e| e.to_string())?;
    let line = serde_json::to_string(request).map_err(|e| e.to_string())?;
    stream
        .write_all(line.as_bytes())
        .and_then(|_| stream.write_all(b"\n"))
        .map_err(|e| e.to_string())?;
    let mut response = String::new();
    BufReader::new(stream)
        .read_line(&mut response)
        .map_err(|e| e.to_string())?;
    if response.is_empty() {
        return Err("empty response from vault-searchd".to_string());
    }
    serde_json::from_str(&response).map_err(|e| format!("bad response: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn old_status_reply_still_parses() {
        let r: IpcResponse =
            serde_json::from_str(r#"{"ok":true,"notes":3,"chunks":9,"indexing":false,"dim":768}"#)
                .unwrap();
        assert_eq!(r.notes, Some(3));
        assert!(r.sync.is_none());
        assert!(r.embed_ok.is_none());
    }

    #[test]
    fn sync_status_roundtrips_and_skips_empty_fields() {
        let r = IpcResponse {
            ok: true,
            sync: Some(IpcSyncStatus {
                enabled: true,
                interval_secs: 120,
                runs: 2,
                last_exit: Some(0),
                ..Default::default()
            }),
            ..Default::default()
        };
        let line = serde_json::to_string(&r).unwrap();
        assert!(!line.contains("last_error"), "{line}");
        assert!(!line.contains("hits"), "{line}");
        let back: IpcResponse = serde_json::from_str(&line).unwrap();
        assert_eq!(back, r);
    }
}
