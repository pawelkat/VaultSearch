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

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
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
