//! Post-sync incremental reindex through a running `vault-searchd`.
//!
//! After a successful (non-dry-run) `sync` or `resync`, vault-rclone sends
//! one `{"op":"reindex"}` line to the daemon's Unix socket and waits for the
//! JSON reply. The daemon re-embeds only notes whose mtime changed; nothing
//! here opens the index, wipes it, or links `vault-search` (and its sqlite).
//!
//! A missing daemon, a refused connection, a timeout, or `ok: false` never
//! changes the sync's exit code: the files are already synced, so the run
//! reports the problem and moves on.

use std::path::{Path, PathBuf};
use std::time::Duration;

use serde::Deserialize;

use crate::commands::Host;
use crate::config::Config;

/// The one request this client sends (`IpcRequest::Reindex` in vault-search).
pub const REINDEX_REQUEST: &str = r#"{"op":"reindex"}"#;

/// The fields of vault-search's `IpcResponse` that matter here.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize)]
pub struct ReindexReply {
    pub ok: bool,
    #[serde(default)]
    pub error: Option<String>,
    #[serde(default)]
    pub notes: Option<u64>,
    #[serde(default)]
    pub chunks: Option<u64>,
    /// `true` when another reindex was already running; the daemon then
    /// returns at once with its current counts.
    #[serde(default)]
    pub indexing: Option<bool>,
}

/// Why no usable reply came back.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ReindexError {
    /// No socket file, or nothing accepting on it. Treated as "skipped".
    NotListening(String),
    /// Connected, but the request failed: write error, timeout, empty or
    /// malformed reply.
    Failed(String),
}

/// Sends the reindex request. The real one talks to a Unix socket; tests
/// pass a fake.
pub trait Reindexer {
    fn reindex(&self, socket: &Path, timeout: Duration) -> Result<ReindexReply, ReindexError>;
}

/// Never contacts anything. For callers that do not want a reindex.
pub struct NoReindex;

impl Reindexer for NoReindex {
    fn reindex(&self, socket: &Path, _t: Duration) -> Result<ReindexReply, ReindexError> {
        Err(ReindexError::NotListening(format!(
            "reindex disabled for {}",
            socket.display()
        )))
    }
}

/// Talks to `vault-searchd` over its Unix socket.
pub struct SocketReindexer;

#[cfg(unix)]
impl Reindexer for SocketReindexer {
    fn reindex(&self, socket: &Path, timeout: Duration) -> Result<ReindexReply, ReindexError> {
        use std::io::{BufRead, BufReader, ErrorKind, Write};
        use std::os::unix::net::UnixStream;

        let mut stream = UnixStream::connect(socket).map_err(|e| match e.kind() {
            ErrorKind::NotFound | ErrorKind::ConnectionRefused => {
                ReindexError::NotListening(e.to_string())
            }
            _ => ReindexError::Failed(format!("connect: {e}")),
        })?;
        let fail = |what: &str, e: std::io::Error| ReindexError::Failed(format!("{what}: {e}"));
        stream
            .set_write_timeout(Some(Duration::from_secs(10)))
            .map_err(|e| fail("socket", e))?;
        // `None` would mean "block forever"; zero is rejected by std.
        stream
            .set_read_timeout(Some(timeout.max(Duration::from_secs(1))))
            .map_err(|e| fail("socket", e))?;
        stream
            .write_all(REINDEX_REQUEST.as_bytes())
            .and_then(|_| stream.write_all(b"\n"))
            .map_err(|e| fail("send", e))?;
        let mut line = String::new();
        BufReader::new(stream)
            .read_line(&mut line)
            .map_err(|e| match e.kind() {
                ErrorKind::WouldBlock | ErrorKind::TimedOut => ReindexError::Failed(format!(
                    "no reply within {}s (raise VAULT_REINDEX_TIMEOUT_SECS)",
                    timeout.as_secs()
                )),
                _ => fail("read", e),
            })?;
        parse_reply(&line)
    }
}

#[cfg(not(unix))]
impl Reindexer for SocketReindexer {
    fn reindex(&self, socket: &Path, _t: Duration) -> Result<ReindexReply, ReindexError> {
        Err(ReindexError::NotListening(format!(
            "unix sockets unsupported here ({})",
            socket.display()
        )))
    }
}

/// Parse one reply line from the daemon.
pub fn parse_reply(line: &str) -> Result<ReindexReply, ReindexError> {
    if line.trim().is_empty() {
        return Err(ReindexError::Failed(
            "empty reply from vault-searchd".into(),
        ));
    }
    serde_json::from_str(line.trim())
        .map_err(|e| ReindexError::Failed(format!("bad reply from vault-searchd: {e}")))
}

/// Same rule as `vault-searchd`: `$XDG_RUNTIME_DIR/vault-search.sock`, else
/// `/tmp/vault-search-$UID.sock`.
pub fn default_socket(runtime_dir: Option<&Path>, uid: u32) -> PathBuf {
    match runtime_dir {
        Some(d) => d.join("vault-search.sock"),
        None => PathBuf::from(format!("/tmp/vault-search-{uid}.sock")),
    }
}

/// `VAULT_SEARCH_SOCKET`, else the daemon's default.
pub fn socket_path(cfg: &Config, host: &Host) -> PathBuf {
    cfg.search_socket
        .clone()
        .unwrap_or_else(|| default_socket(host.runtime_dir.as_deref(), host.uid))
}

/// What happened, already worded for stdout and the run log
/// (`reindex: …`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    Disabled,
    Skipped(String),
    Ok(String),
    Failed(String),
}

impl Outcome {
    pub fn message(&self) -> String {
        match self {
            Outcome::Disabled => "reindex: disabled (VAULT_REINDEX=0)".into(),
            Outcome::Skipped(m) => format!("reindex: skipped ({m})"),
            Outcome::Ok(m) => format!("reindex: ok ({m})"),
            Outcome::Failed(m) => format!("reindex: FAILED ({m})"),
        }
    }
}

/// Ask the daemon for an incremental reindex and describe the result.
pub fn request(cfg: &Config, host: &Host, reindexer: &dyn Reindexer) -> Outcome {
    if !cfg.reindex {
        return Outcome::Disabled;
    }
    let socket = socket_path(cfg, host);
    let timeout = Duration::from_secs(cfg.reindex_timeout_secs);
    match reindexer.reindex(&socket, timeout) {
        Err(ReindexError::NotListening(e)) => Outcome::Skipped(format!(
            "vault-searchd not listening on {}: {e}",
            socket.display()
        )),
        Err(ReindexError::Failed(e)) => Outcome::Failed(one_line(&e)),
        Ok(r) if !r.ok => Outcome::Failed(one_line(
            r.error
                .as_deref()
                .unwrap_or("vault-searchd returned ok=false"),
        )),
        Ok(r) => {
            let n = |v: Option<u64>| v.map_or("?".to_string(), |v| v.to_string());
            let mut m = format!("notes={} chunks={}", n(r.notes), n(r.chunks));
            if r.indexing == Some(true) {
                m.push_str("; a reindex was already running, it will pick up the changes");
            }
            Outcome::Ok(m)
        }
    }
}

/// Keep daemon errors to a single, bounded log line.
fn one_line(s: &str) -> String {
    let s = s.split_whitespace().collect::<Vec<_>>().join(" ");
    if s.chars().count() > 300 {
        let cut: String = s.chars().take(300).collect();
        format!("{cut}…")
    } else {
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_shape_matches_vault_search_ipc() {
        let v: serde_json::Value = serde_json::from_str(REINDEX_REQUEST).unwrap();
        assert_eq!(v, serde_json::json!({"op": "reindex"}));
    }

    #[test]
    fn default_socket_matches_daemon() {
        assert_eq!(
            default_socket(Some(Path::new("/run/user/1000")), 1000),
            PathBuf::from("/run/user/1000/vault-search.sock")
        );
        assert_eq!(
            default_socket(None, 1000),
            PathBuf::from("/tmp/vault-search-1000.sock")
        );
    }

    #[test]
    fn parses_daemon_replies() {
        let r = parse_reply(
            "{\"ok\":true,\"notes\":412,\"chunks\":3001,\"indexing\":false,\"dim\":768}\n",
        )
        .unwrap();
        assert!(r.ok);
        assert_eq!((r.notes, r.chunks), (Some(412), Some(3001)));
        let r = parse_reply("{\"ok\":false,\"error\":\"embed: connection refused\"}").unwrap();
        assert_eq!(r.error.as_deref(), Some("embed: connection refused"));
        assert!(matches!(parse_reply(""), Err(ReindexError::Failed(_))));
        assert!(matches!(parse_reply("nope"), Err(ReindexError::Failed(_))));
    }

    #[test]
    fn missing_socket_is_not_listening() {
        let d = tempfile::tempdir().unwrap();
        let e = SocketReindexer
            .reindex(&d.path().join("none.sock"), Duration::from_secs(1))
            .unwrap_err();
        assert!(matches!(e, ReindexError::NotListening(_)), "{e:?}");
    }

    #[cfg(unix)]
    #[test]
    fn real_socket_roundtrip_and_timeout() {
        use std::io::{BufRead, BufReader, Write};
        use std::os::unix::net::UnixListener;

        let d = tempfile::tempdir().unwrap();
        let sock = d.path().join("vs.sock");
        let listener = UnixListener::bind(&sock).unwrap();
        let server = std::thread::spawn(move || {
            // First connection: answer. Second: read and never answer.
            let (s, _) = listener.accept().unwrap();
            let mut line = String::new();
            BufReader::new(&s).read_line(&mut line).unwrap();
            (&s).write_all(b"{\"ok\":true,\"notes\":5,\"chunks\":9}\n")
                .unwrap();
            let (s2, _) = listener.accept().unwrap();
            let mut l2 = String::new();
            BufReader::new(&s2).read_line(&mut l2).unwrap();
            std::thread::sleep(Duration::from_millis(1500));
            line
        });
        let r = SocketReindexer
            .reindex(&sock, Duration::from_secs(5))
            .unwrap();
        assert_eq!((r.ok, r.notes, r.chunks), (true, Some(5), Some(9)));
        let e = SocketReindexer
            .reindex(&sock, Duration::from_secs(1))
            .unwrap_err();
        match e {
            ReindexError::Failed(m) => assert!(m.contains("no reply within 1s"), "{m}"),
            other => panic!("{other:?}"),
        }
        assert_eq!(server.join().unwrap().trim(), REINDEX_REQUEST);
    }

    #[cfg(unix)]
    #[test]
    fn stale_socket_file_is_not_listening() {
        let d = tempfile::tempdir().unwrap();
        let sock = d.path().join("stale.sock");
        drop(std::os::unix::net::UnixListener::bind(&sock).unwrap());
        let e = SocketReindexer
            .reindex(&sock, Duration::from_secs(1))
            .unwrap_err();
        assert!(matches!(e, ReindexError::NotListening(_)), "{e:?}");
    }

    #[test]
    fn long_errors_are_one_bounded_line() {
        let s = one_line(&format!("a\nb {}", "x".repeat(500)));
        assert!(!s.contains('\n'));
        assert!(s.chars().count() <= 301);
    }
}
