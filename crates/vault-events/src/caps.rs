//! Caps v2 for vault search, served on `vault/caps/<device>`.
//!
//! The document is JSON-LD. Keys are ontology local names from
//! `Ontology/caps-v2.schema.json`. This slice advertises `VaultSkill` only.
//! `CodeSkill`, `codeRoot`, and `maxTokensSuggest` are left out until a later
//! iteration. `ready` and `nCtx` are filled when a query arrives.
//!
//! `nCtx` is `default_generation_settings.n_ctx` from llama-server `GET /props`
//! (TowerX, 2026-11-27: 16384). There is no recorded `maxTokensSuggest` formula,
//! so that key is omitted.

use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::os::unix::net::UnixStream;
use std::path::PathBuf;
use std::time::Duration;

use serde_json::{json, Map, Value};
use zenoh::Session;

use crate::net::BoxError;

pub const CAPS_PREFIX: &str = "vault/caps";
pub const SCHEMA_VERSION: u64 = 2;
pub const CONTEXT_URL: &str = "https://pawelkat.github.io/vault-ontology/context/caps-v2.jsonld";
pub const ID_BASE: &str = "https://pawelkat.github.io/vault-ontology/id/";

/// Static facts for one host's vault-search chat provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VaultCapsIdentity {
    pub device: String,
    pub label: String,
    pub comment: String,
    /// Chat completions URL. Tailnet, not loopback.
    pub endpoint_url: String,
}

/// Values read at query time. `n_ctx` is omitted from the document when `None`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VaultCapsLive {
    pub ready: bool,
    pub n_ctx: Option<u64>,
}

/// Where to read readiness. Llama URL must be loopback HTTP.
#[derive(Debug, Clone)]
pub struct CapsProbe {
    pub llama_url: String,
    pub search_socket: PathBuf,
    pub timeout: Duration,
}

/// `vault/caps/<device>`
pub fn caps_key(device: &str) -> String {
    format!("{CAPS_PREFIX}/{device}")
}

pub fn default_label(device: &str) -> String {
    format!("{device} vault search")
}

pub fn default_comment() -> &'static str {
    "Chat proxy over llama-server with vault search. Read-only."
}

pub fn default_search_socket() -> PathBuf {
    if let Some(dir) = std::env::var_os("XDG_RUNTIME_DIR") {
        return PathBuf::from(dir).join("vault-search.sock");
    }
    let uid = std::fs::metadata("/proc/self")
        .ok()
        .map(|meta| {
            use std::os::unix::fs::MetadataExt;
            meta.uid()
        })
        .unwrap_or(0);
    PathBuf::from(format!("/tmp/vault-search-{uid}.sock"))
}

pub fn identity(
    device: &str,
    endpoint_url: &str,
    label: Option<&str>,
    comment: Option<&str>,
) -> Result<VaultCapsIdentity, String> {
    if !device_id_ok(device) {
        return Err(format!(
            "device {device:?} is not a caps device id (lowercase letters, digits, hyphens)"
        ));
    }
    if !endpoint_ok(endpoint_url) {
        return Err(format!(
            "caps endpoint is not an http(s) URL: {endpoint_url}"
        ));
    }
    let label = match label.map(str::trim).filter(|text| !text.is_empty()) {
        Some(text) => text.to_string(),
        None => default_label(device),
    };
    let comment = match comment.map(str::trim).filter(|text| !text.is_empty()) {
        Some(text) => text.to_string(),
        None => default_comment().to_string(),
    };
    Ok(VaultCapsIdentity {
        device: device.to_string(),
        label,
        comment,
        endpoint_url: endpoint_url.to_string(),
    })
}

/// Caps v2 document. No `CodeSkill`, no `codeRoot`, no `maxTokensSuggest`, no nulls.
pub fn vault_caps_document(identity: &VaultCapsIdentity, live: &VaultCapsLive) -> Value {
    let mut provider = Map::new();
    provider.insert(
        "@id".into(),
        json!(format!("{ID_BASE}{}-qwen", identity.device)),
    );
    provider.insert("@type".into(), json!("LlmChatProvider"));
    provider.insert("label".into(), json!(identity.label));
    provider.insert("comment".into(), json!(identity.comment));
    provider.insert(
        "hostedOn".into(),
        json!({
            "@id": format!("{ID_BASE}{}", identity.device),
            "@type": "Device",
            "deviceId": identity.device,
        }),
    );
    provider.insert("providesSkill".into(), json!(["VaultSkill"]));
    provider.insert("endpointUrl".into(), json!(identity.endpoint_url));
    provider.insert("transport".into(), json!("ChatProxy"));
    provider.insert("scope".into(), json!("Tailnet"));
    provider.insert("accessMode".into(), json!("ReadOnly"));
    provider.insert("requiresService".into(), json!(["LlamaServer"]));
    provider.insert("ready".into(), json!(live.ready));
    if let Some(n_ctx) = live.n_ctx.filter(|n| *n >= 1) {
        provider.insert("nCtx".into(), json!(n_ctx));
    }
    json!({
        "@context": CONTEXT_URL,
        "schemaVersion": SCHEMA_VERSION,
        "@graph": [Value::Object(provider)],
    })
}

/// Read llama-server and the vault-searchd socket. Failure becomes `ready: false`
/// and an omitted `nCtx`, not an error, so a query still gets a document.
pub fn probe_vault_caps(probe: &CapsProbe) -> VaultCapsLive {
    let health = http_get(&join_url(&probe.llama_url, "/health"), probe.timeout).ok();
    let props = http_get(&join_url(&probe.llama_url, "/props"), probe.timeout).ok();
    let llama_ok = health.as_ref().is_some_and(|reply| reply.status == 200);
    let search_ok = UnixStream::connect(&probe.search_socket).is_ok();
    let n_ctx = props
        .as_ref()
        .and_then(|reply| n_ctx_from_props(&reply.body));
    VaultCapsLive {
        ready: llama_ok && search_ok,
        n_ctx,
    }
}

pub fn n_ctx_from_props(body: &[u8]) -> Option<u64> {
    let value: Value = serde_json::from_slice(body).ok()?;
    value
        .pointer("/default_generation_settings/n_ctx")
        .and_then(Value::as_u64)
        .filter(|n| *n >= 1)
}

/// Answer `vault/caps/<device>`. The device id is the one the agent was
/// started with, so the same binary serves TowerX and Nitro. Each query
/// probes again. Dropping the guard stops it.
pub async fn serve(
    session: &Session,
    identity: VaultCapsIdentity,
    probe: CapsProbe,
) -> Result<CapsGuard, BoxError> {
    let key = caps_key(&identity.device);
    let queryable = session.declare_queryable(&key).complete(true).await?;
    let reply_key = key.clone();
    let task = tokio::spawn(async move {
        while let Ok(query) = queryable.recv_async().await {
            let identity = identity.clone();
            let probe = probe.clone();
            let live = tokio::task::spawn_blocking(move || probe_vault_caps(&probe))
                .await
                .unwrap_or(VaultCapsLive {
                    ready: false,
                    n_ctx: None,
                });
            let body =
                serde_json::to_vec(&vault_caps_document(&identity, &live)).unwrap_or_default();
            // A wildcard query still comes back on this device's own key.
            let _ = query.reply(reply_key.clone(), body).await;
        }
    });
    tracing::info!("caps queryable {key} (VaultSkill only)");
    Ok(CapsGuard { task })
}

pub struct CapsGuard {
    task: tokio::task::JoinHandle<()>,
}

impl Drop for CapsGuard {
    fn drop(&mut self) {
        self.task.abort();
    }
}

fn device_id_ok(device: &str) -> bool {
    let mut parts = device.split('-');
    match parts.next() {
        Some(first) if part_ok(first) => parts.all(part_ok),
        _ => false,
    }
}

fn part_ok(part: &str) -> bool {
    !part.is_empty()
        && part
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
}

fn endpoint_ok(url: &str) -> bool {
    let rest = url
        .strip_prefix("https://")
        .or_else(|| url.strip_prefix("http://"));
    matches!(
        rest,
        Some(rest) if !rest.is_empty() && !url.chars().any(char::is_whitespace)
    )
}

fn join_url(base: &str, path: &str) -> String {
    format!("{}{}", base.trim_end_matches('/'), path)
}

struct HttpReply {
    status: u16,
    body: Vec<u8>,
}

fn http_get(url: &str, timeout: Duration) -> Result<HttpReply, String> {
    let target = parse_loopback(url)?;
    let mut stream = TcpStream::connect_timeout(&target.addr, timeout)
        .map_err(|err| format!("connect {}: {err}", target.addr))?;
    stream
        .set_read_timeout(Some(timeout))
        .map_err(|err| err.to_string())?;
    stream
        .set_write_timeout(Some(timeout))
        .map_err(|err| err.to_string())?;
    let request = format!(
        "GET {} HTTP/1.0\r\nHost: {}\r\nConnection: close\r\n\r\n",
        target.path, target.host_header
    );
    stream
        .write_all(request.as_bytes())
        .map_err(|err| err.to_string())?;
    let mut buf = Vec::new();
    let mut chunk = [0u8; 8192];
    loop {
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => {
                buf.extend_from_slice(&chunk[..n]);
                if buf.len() > 1_000_000 {
                    return Err("caps probe response is over 1 MB".into());
                }
            }
            Err(err)
                if err.kind() == std::io::ErrorKind::TimedOut
                    || err.kind() == std::io::ErrorKind::WouldBlock =>
            {
                if buf.is_empty() {
                    return Err(format!("read {url}: {err}"));
                }
                break;
            }
            Err(err) => return Err(format!("read {url}: {err}")),
        }
    }
    let header_end = buf
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .ok_or_else(|| format!("no HTTP headers from {url}"))?;
    let head = String::from_utf8_lossy(&buf[..header_end]);
    let status = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .ok_or_else(|| format!("bad status line from {url}"))?;
    Ok(HttpReply {
        status,
        body: buf[header_end + 4..].to_vec(),
    })
}

struct LoopbackTarget {
    addr: std::net::SocketAddr,
    host_header: String,
    path: String,
}

fn parse_loopback(url: &str) -> Result<LoopbackTarget, String> {
    let rest = url
        .strip_prefix("http://")
        .ok_or_else(|| format!("caps probe refuses non-http URL {url}"))?;
    let (authority, path) = match rest.split_once('/') {
        Some((authority, path)) => (authority, format!("/{path}")),
        None => (rest, "/".to_string()),
    };
    if authority.is_empty() || authority.contains('@') {
        return Err(format!("caps probe refuses URL {url}"));
    }
    let host_port = if let Some(host) = authority.strip_prefix('[') {
        let (host, port) = host
            .split_once("]:")
            .ok_or_else(|| format!("caps probe refuses URL {url}"))?;
        (host.to_string(), port.to_string())
    } else if let Some((host, port)) = authority.rsplit_once(':') {
        (host.to_string(), port.to_string())
    } else {
        (authority.to_string(), "80".to_string())
    };
    if !matches!(host_port.0.as_str(), "127.0.0.1" | "localhost" | "::1") {
        return Err(format!(
            "caps probe refuses non-loopback host {}",
            host_port.0
        ));
    }
    let addr = format!("{}:{}", host_port.0, host_port.1)
        .to_socket_addrs()
        .map_err(|err| err.to_string())?
        .next()
        .ok_or_else(|| format!("no address for {url}"))?;
    Ok(LoopbackTarget {
        addr,
        host_header: authority.to_string(),
        path,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> VaultCapsIdentity {
        identity(
            "towerx",
            "https://towerx.tailf52603.ts.net/v1/chat/completions",
            Some("TowerX chat proxy"),
            Some("Qwen3.5-9B Q5_K_M on llama-server behind the chat proxy. Vault search only."),
        )
        .unwrap()
    }

    #[test]
    fn vault_skill_document_omits_code_and_unknown_live_fields() {
        let doc = vault_caps_document(
            &sample(),
            &VaultCapsLive {
                ready: true,
                n_ctx: Some(16384),
            },
        );
        assert_eq!(doc["schemaVersion"], 2);
        assert_eq!(doc["@context"], CONTEXT_URL);
        let provider = &doc["@graph"][0];
        assert_eq!(
            provider["@id"],
            "https://pawelkat.github.io/vault-ontology/id/towerx-qwen"
        );
        assert_eq!(provider["@type"], "LlmChatProvider");
        assert_eq!(provider["providesSkill"], json!(["VaultSkill"]));
        assert_eq!(provider["requiresService"], json!(["LlamaServer"]));
        assert_eq!(provider["transport"], "ChatProxy");
        assert_eq!(provider["scope"], "Tailnet");
        assert_eq!(provider["accessMode"], "ReadOnly");
        assert_eq!(provider["hostedOn"]["deviceId"], "towerx");
        assert_eq!(provider["hostedOn"]["@type"], "Device");
        assert_eq!(provider["ready"], true);
        assert_eq!(provider["nCtx"], 16384);
        let text = doc.to_string();
        assert!(!text.contains("CodeSkill"));
        assert!(!text.contains("codeRoot"));
        assert!(!text.contains("maxTokensSuggest"));
        assert!(!text.contains("null"));
        assert!(!text.contains("\"tools\""));
        assert_eq!(caps_key("towerx"), "vault/caps/towerx");
    }

    #[test]
    fn omits_n_ctx_when_the_probe_cannot_read_it() {
        let doc = vault_caps_document(
            &sample(),
            &VaultCapsLive {
                ready: false,
                n_ctx: None,
            },
        );
        let provider = &doc["@graph"][0];
        assert_eq!(provider["ready"], false);
        assert!(provider.get("nCtx").is_none());
    }

    #[test]
    fn rejects_a_bad_device_or_endpoint() {
        assert!(identity("TowerX", "https://towerx.example/v1", None, None).is_err());
        assert!(identity("towerx", "not a url", None, None).is_err());
        assert!(parse_loopback("http://192.168.1.10:8080/props").is_err());
        assert!(parse_loopback("https://127.0.0.1:8080/props").is_err());
    }

    #[test]
    fn reads_n_ctx_from_the_towerx_props_shape() {
        let body = br#"{"n_ctx": null, "default_generation_settings": {"n_ctx": 16384}}"#;
        assert_eq!(n_ctx_from_props(body), Some(16384));
        assert_eq!(
            n_ctx_from_props(br#"{"default_generation_settings": {}}"#),
            None
        );
        assert_eq!(
            n_ctx_from_props(br#"{"default_generation_settings": {"n_ctx": 0}}"#),
            None
        );
    }

    /// `vault/caps/**` must be answered on each configured device key.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn wildcard_query_is_answered_on_each_device_key() {
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let listen = format!("tcp/127.0.0.1:{port}");
        let endpoint = std::slice::from_ref(&listen);
        let _router = zenoh::open(crate::net::router_config(endpoint, &[]).unwrap())
            .await
            .unwrap();
        let agent = zenoh::open(crate::net::client_config(endpoint).unwrap())
            .await
            .unwrap();
        let querier = zenoh::open(crate::net::client_config(endpoint).unwrap())
            .await
            .unwrap();
        let probe = CapsProbe {
            llama_url: "http://127.0.0.1:9".into(),
            search_socket: PathBuf::from("/tmp/vault-search-caps-key-test.sock"),
            timeout: Duration::from_millis(50),
        };
        let devices = ["nitro5", "towerx"];
        let mut _guards = Vec::new();
        for device in devices {
            let identity = identity(
                device,
                &format!("https://{device}.example/v1/chat/completions"),
                None,
                None,
            )
            .unwrap();
            _guards.push(serve(&agent, identity, probe.clone()).await.unwrap());
        }

        let mut keys = Vec::new();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
        while keys.len() < devices.len() && tokio::time::Instant::now() < deadline {
            keys.clear();
            let replies = querier
                .get("vault/caps/**")
                .target(zenoh::query::QueryTarget::All)
                .consolidation(zenoh::query::ConsolidationMode::None)
                .timeout(Duration::from_secs(2))
                .await
                .unwrap();
            while let Ok(reply) = replies.recv_async().await {
                let sample = reply.result().unwrap();
                let key = sample.key_expr().as_str().to_string();
                let doc: Value = serde_json::from_slice(&sample.payload().to_bytes()).unwrap();
                let device = doc["@graph"][0]["hostedOn"]["deviceId"]
                    .as_str()
                    .unwrap()
                    .to_string();
                assert_eq!(key, caps_key(&device));
                keys.push(key);
            }
            keys.sort();
            keys.dedup();
            if keys.len() < devices.len() {
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        assert_eq!(
            keys,
            vec![
                "vault/caps/nitro5".to_string(),
                "vault/caps/towerx".to_string()
            ]
        );
    }
}
