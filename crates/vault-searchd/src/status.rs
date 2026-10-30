//! Human summary for `vault-searchd status`.

use std::fmt::Write as _;
use std::path::Path;

use vault_rclone::sync::format_utc;
use vault_search::IpcResponse;

fn opt<T: ToString>(v: Option<T>) -> String {
    v.map_or("?".to_string(), |v| v.to_string())
}

/// One `key: value` line per fact. Fields an older daemon does not send are
/// shown as `?` or left out, never invented.
pub fn render(socket: &Path, r: &IpcResponse) -> String {
    let mut s = String::new();
    let _ = writeln!(s, "socket:   {}", socket.display());
    if let Some(v) = &r.vault {
        let _ = writeln!(s, "vault:    {v}");
    }
    let _ = writeln!(
        s,
        "index:    notes={} chunks={} dim={}{}",
        opt(r.notes),
        opt(r.chunks),
        opt(r.dim),
        if r.indexing == Some(true) {
            " (indexing now)"
        } else {
            ""
        }
    );
    if let Some(e) = &r.error {
        let _ = writeln!(s, "index error: {e}");
    }
    match (&r.embed_url, r.embed_ok) {
        (Some(url), Some(ok)) => {
            let _ = writeln!(
                s,
                "embedder: {url} ({})",
                if ok { "reachable" } else { "NOT reachable" }
            );
        }
        (None, Some(ok)) => {
            let _ = writeln!(
                s,
                "embedder: {}",
                if ok { "reachable" } else { "NOT reachable" }
            );
        }
        _ => {}
    }
    match &r.watch {
        None => {}
        Some(w) if w.ok => {
            let _ = writeln!(
                s,
                "watch:    on (debounce {}ms, {} events, {} reindexes{}), 60s rescan fallback",
                w.debounce_ms,
                w.events,
                w.reindexes,
                w.last_reindex_unix
                    .map(|t| format!(", last {}", format_utc(t)))
                    .unwrap_or_default()
            );
        }
        Some(w) if w.enabled => {
            let _ = writeln!(
                s,
                "watch:    FAILED ({}), 60s rescan only",
                w.error.as_deref().unwrap_or("unknown error")
            );
        }
        Some(_) => {
            let _ = writeln!(s, "watch:    off (--no-watch), 60s rescan only");
        }
    }
    match &r.sync {
        None => {
            let _ = writeln!(s, "sync:     unknown (daemon predates integrated sync)");
        }
        Some(y) if !y.enabled => {
            let _ = writeln!(s, "sync:     off (serve --sync-interval 0)");
        }
        Some(y) => {
            let _ = writeln!(
                s,
                "sync:     every {}s, {} <-> {}{}",
                y.interval_secs,
                y.remote.as_deref().unwrap_or("?"),
                y.local.as_deref().unwrap_or("?"),
                if y.running { " (running now)" } else { "" }
            );
            let _ = writeln!(s, "sync runs: {} ({} failed)", y.runs, y.failures);
            match (y.last_run_unix, y.last_exit) {
                (Some(t), Some(code)) => {
                    let _ = writeln!(
                        s,
                        "last sync: {} {}",
                        format_utc(t),
                        if code == 0 {
                            "ok".to_string()
                        } else {
                            format!("FAILED (exit {code})")
                        }
                    );
                }
                _ => {
                    let _ = writeln!(s, "last sync: none yet");
                }
            }
            if let Some(t) = y.last_ok_unix {
                let _ = writeln!(s, "last ok sync: {}", format_utc(t));
            }
            if let Some(e) = &y.last_error {
                let _ = writeln!(s, "last sync error: {e}");
            }
            if let Some(m) = &y.last_reindex {
                let _ = writeln!(s, "after sync: {m}");
            }
        }
    }
    s
}

#[cfg(test)]
mod tests {
    use super::*;
    use vault_search::IpcSyncStatus;

    #[test]
    fn renders_sync_health() {
        let r = IpcResponse {
            ok: true,
            notes: Some(713),
            chunks: Some(4000),
            dim: Some(768),
            indexing: Some(false),
            vault: Some("/v".into()),
            embed_ok: Some(false),
            embed_url: Some("http://127.0.0.1:8081/v1/embeddings".into()),
            sync: Some(IpcSyncStatus {
                enabled: true,
                interval_secs: 120,
                remote: Some("gdrive:Vault".into()),
                local: Some("/v".into()),
                runs: 3,
                failures: 1,
                last_run_unix: Some(0),
                last_ok_unix: Some(0),
                last_exit: Some(7),
                last_error: Some("FAILED (rclone exit 7): x".into()),
                last_reindex: Some("reindex: ok (notes=713 chunks=4000)".into()),
                running: false,
            }),
            ..Default::default()
        };
        let s = render(Path::new("/run/s.sock"), &r);
        for want in [
            "index:    notes=713 chunks=4000 dim=768",
            "NOT reachable",
            "every 120s, gdrive:Vault <-> /v",
            "sync runs: 3 (1 failed)",
            "last sync: 1970-01-01T00:00:00Z FAILED (exit 7)",
            "last sync error: FAILED (rclone exit 7): x",
            "after sync: reindex: ok",
        ] {
            assert!(s.contains(want), "missing {want:?} in:\n{s}");
        }
    }

    #[test]
    fn renders_watch_health() {
        let on = IpcResponse {
            ok: true,
            watch: Some(vault_search::IpcWatchStatus {
                enabled: true,
                ok: true,
                debounce_ms: 500,
                events: 4,
                reindexes: 2,
                last_reindex_unix: Some(0),
                ..Default::default()
            }),
            ..Default::default()
        };
        let s = render(Path::new("/s"), &on);
        assert!(
            s.contains(
                "watch:    on (debounce 500ms, 4 events, 2 reindexes, last 1970-01-01T00:00:00Z)"
            ),
            "{s}"
        );
        let failed = IpcResponse {
            watch: Some(vault_search::IpcWatchStatus {
                enabled: true,
                error: Some("inotify watch limit reached".into()),
                ..Default::default()
            }),
            ..Default::default()
        };
        let s = render(Path::new("/s"), &failed);
        assert!(
            s.contains("watch:    FAILED (inotify watch limit reached), 60s rescan only"),
            "{s}"
        );
        let off = IpcResponse {
            watch: Some(vault_search::IpcWatchStatus::default()),
            ..Default::default()
        };
        assert!(render(Path::new("/s"), &off).contains("watch:    off (--no-watch)"));
        assert!(!render(Path::new("/s"), &IpcResponse::default()).contains("watch:"));
    }

    #[test]
    fn old_daemon_and_sync_off() {
        let old = IpcResponse {
            ok: true,
            notes: Some(1),
            ..Default::default()
        };
        let s = render(Path::new("/s"), &old);
        assert!(s.contains("sync:     unknown"), "{s}");
        assert!(!s.contains("embedder"), "{s}");
        let off = IpcResponse {
            sync: Some(IpcSyncStatus::default()),
            ..old
        };
        assert!(render(Path::new("/s"), &off).contains("sync:     off"));
    }
}
