//! End-to-end over real Zenoh sessions: one hub router and two agents on
//! loopback TCP, each agent with its own temp vault and state dir.

use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use tokio::sync::watch;
use tokio::task::JoinHandle;
use vault_events::{agent, claims, event, hub, net};

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

struct Running {
    stop: watch::Sender<bool>,
    task: JoinHandle<()>,
}

impl Running {
    async fn stop(self) {
        let _ = self.stop.send(true);
        let _ = tokio::time::timeout(Duration::from_secs(10), self.task).await;
    }
}

fn start_hub(data: &Path, port: u16) -> Running {
    let (tx, rx) = watch::channel(false);
    let opts = hub::HubOpts {
        data_dir: data.to_path_buf(),
        listen: vec![format!("tcp/127.0.0.1:{port}")],
        connect: vec![],
        name: "test-hub".into(),
        backup_config: None,
        backup_interval: Duration::from_secs(60),
        backup_dry_run: true,
        transfer: Default::default(),
    };
    let task = tokio::spawn(async move {
        hub::run(opts, Some(rx)).await.expect("hub");
    });
    Running { stop: tx, task }
}

fn start_agent(vault: &Path, state: &Path, device: &str, port: u16) -> Running {
    start_agent_with(vault, state, device, port, None)
}

fn start_agent_with(
    vault: &Path,
    state: &Path,
    device: &str,
    port: u16,
    bisync_marker_dir: Option<PathBuf>,
) -> Running {
    start_agent_opts(vault, state, device, port, bisync_marker_dir, 1 << 20)
}

fn start_agent_opts(
    vault: &Path,
    state: &Path,
    device: &str,
    port: u16,
    bisync_marker_dir: Option<PathBuf>,
    max_bytes: u64,
) -> Running {
    let (tx, rx) = watch::channel(false);
    let opts = agent::AgentOpts {
        vault: vault.to_path_buf(),
        device: device.into(),
        state_dir: state.to_path_buf(),
        connect: vec![format!("tcp/127.0.0.1:{port}")],
        max_bytes,
        max_delete: 50,
        min_files: 0,
        debounce: Duration::from_millis(150),
        catchup_interval: Duration::from_secs(3),
        rescan_interval: Duration::from_secs(4),
        query_timeout: Duration::from_secs(5),
        program: agent::DEFAULT_PROGRAM.into(),
        bisync_marker_dir,
        transfer: Default::default(),
    };
    let task = tokio::spawn(async move {
        agent::run(opts, Some(rx)).await.expect("agent");
    });
    Running { stop: tx, task }
}

async fn wait_for(what: &str, timeout: Duration, mut f: impl FnMut() -> bool) {
    let t = Instant::now();
    while t.elapsed() < timeout {
        if f() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("timed out waiting for: {what}");
}

fn read(p: PathBuf) -> Option<String> {
    std::fs::read_to_string(p).ok()
}

fn conflict_copies(dir: &Path) -> Vec<String> {
    let mut v: Vec<String> = std::fs::read_dir(dir)
        .map(|r| {
            r.filter_map(|e| e.ok())
                .map(|e| e.file_name().to_string_lossy().to_string())
                .filter(|n| n.contains(" (conflict "))
                .collect()
        })
        .unwrap_or_default();
    v.sort();
    v
}

async fn hub_version(port: u16, path: &str) -> Option<u64> {
    let s = zenoh::open(net::client_config(&[format!("tcp/127.0.0.1:{port}")]).unwrap())
        .await
        .unwrap();
    let r = net::fetch_index(&s, Duration::from_secs(5)).await.unwrap();
    s.close().await.unwrap();
    r.and_then(|r| {
        r.stamps
            .into_iter()
            .find(|st| st.path == path)
            .map(|st| st.version)
    })
}

const T: Duration = Duration::from_secs(20);

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hub_and_two_agents() {
    let root = tempfile::tempdir().unwrap();
    let (hub_dir, va, vb, sa, sb) = (
        root.path().join("hub"),
        root.path().join("vault-a"),
        root.path().join("vault-b"),
        root.path().join("state-a"),
        root.path().join("state-b"),
    );
    for d in [&va, &vb] {
        std::fs::create_dir_all(d.join("Notes")).unwrap();
    }
    let port = free_port();
    let hub = start_hub(&hub_dir, port);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let a = start_agent(&va, &sa, "dev-a", port);
    let mut b = start_agent(&vb, &sb, "dev-b", port);
    tokio::time::sleep(Duration::from_secs(1)).await;

    // 1. Edit on A arrives on B.
    std::fs::write(va.join("Notes/x.md"), "hello from A\n").unwrap();
    wait_for("x.md on B", T, || {
        read(vb.join("Notes/x.md")).as_deref() == Some("hello from A\n")
    })
    .await;
    std::fs::write(va.join("Notes/x.md"), "edit 2 from A\n").unwrap();
    wait_for("x.md edit on B", T, || {
        read(vb.join("Notes/x.md")).as_deref() == Some("edit 2 from A\n")
    })
    .await;

    // And B -> A, including a nested new folder.
    std::fs::create_dir_all(vb.join("Daily/2026")).unwrap();
    std::fs::write(vb.join("Daily/2026/2026-11-18.md"), "daily from B\n").unwrap();
    wait_for("daily on A", T, || {
        read(va.join("Daily/2026/2026-11-18.md")).as_deref() == Some("daily from B\n")
    })
    .await;

    // 2. Delete propagates.
    std::fs::remove_file(va.join("Notes/x.md")).unwrap();
    wait_for("x.md deleted on B", T, || !vb.join("Notes/x.md").exists()).await;

    // No echo loop: the hub's version for the daily note stays put.
    let v1 = hub_version(port, "Daily/2026/2026-11-18.md").await;
    tokio::time::sleep(Duration::from_secs(5)).await;
    let v2 = hub_version(port, "Daily/2026/2026-11-18.md").await;
    assert_eq!(v1, Some(1));
    assert_eq!(v1, v2, "no echo: version must not keep increasing");
    assert_eq!(
        hub_version(port, "Notes/x.md").await,
        Some(3),
        "put, edit, delete"
    );

    // 3. B goes offline; A writes; B catches up from hub storage on restart.
    b.stop().await;
    std::fs::write(va.join("Notes/offline.md"), "written while B was off\n").unwrap();
    std::fs::write(va.join("Notes/shared.md"), "base\n").unwrap();
    // Wait until the hub has stored both.
    let t = Instant::now();
    while hub_version(port, "Notes/shared.md").await.is_none()
        || hub_version(port, "Notes/offline.md").await.is_none()
    {
        assert!(t.elapsed() < T, "hub never stored A's offline writes");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert!(!vb.join("Notes/offline.md").exists());
    b = start_agent(&vb, &sb, "dev-b", port);
    wait_for("offline.md caught up on B", T, || {
        read(vb.join("Notes/offline.md")).as_deref() == Some("written while B was off\n")
    })
    .await;
    wait_for("shared.md caught up on B", T, || {
        read(vb.join("Notes/shared.md")).as_deref() == Some("base\n")
    })
    .await;

    // 4. Conflict: both edit shared.md from the same base while B is offline.
    b.stop().await;
    std::fs::write(vb.join("Notes/shared.md"), "B offline edit\n").unwrap();
    tokio::time::sleep(Duration::from_millis(50)).await;
    std::fs::write(va.join("Notes/shared.md"), "A edit (later clock)\n").unwrap();
    let t = Instant::now();
    while hub_version(port, "Notes/shared.md").await != Some(2) {
        assert!(t.elapsed() < T, "hub never stored A's v2");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    b = start_agent(&vb, &sb, "dev-b", port);
    // Same version (2) on both sides; A's edit has the later clock and wins.
    wait_for("conflict copy on B", T, || {
        !conflict_copies(&vb.join("Notes")).is_empty()
    })
    .await;
    wait_for("winner at live path on B", T, || {
        read(vb.join("Notes/shared.md")).as_deref() == Some("A edit (later clock)\n")
    })
    .await;
    let copies = conflict_copies(&vb.join("Notes"));
    assert_eq!(copies.len(), 1, "{copies:?}");
    assert!(
        copies[0].starts_with("shared (conflict dev-b ") && copies[0].ends_with(").md"),
        "{copies:?}"
    );
    assert_eq!(
        read(vb.join("Notes").join(&copies[0])).as_deref(),
        Some("B offline edit\n")
    );
    // The conflict copy syncs to A as an ordinary note; A's live note is unchanged.
    wait_for("conflict copy on A", T, || {
        conflict_copies(&va.join("Notes")) == copies
    })
    .await;
    assert_eq!(
        read(va.join("Notes/shared.md")).as_deref(),
        Some("A edit (later clock)\n")
    );

    // 5. Hub restart keeps storage (persisted under its data dir).
    b.stop().await;
    a.stop().await;
    hub.stop().await;
    let hub = start_hub(&hub_dir, port);
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(hub_version(port, "Notes/shared.md").await, Some(2));
    assert_eq!(hub_version(port, "Notes/x.md").await, Some(3));
    hub.stop().await;
}

async fn history(port: u16, path: &str) -> Vec<vault_events::history::HistoryEntry> {
    let s = zenoh::open(net::client_config(&[format!("tcp/127.0.0.1:{port}")]).unwrap())
        .await
        .unwrap();
    let r = net::fetch_history(&s, path, 0, Duration::from_secs(5))
        .await
        .unwrap();
    s.close().await.unwrap();
    r.map(|r| r.entries).unwrap_or_default()
}

async fn wait_history(port: u16, path: &str, n: usize) -> Vec<vault_events::history::HistoryEntry> {
    let t = Instant::now();
    loop {
        let h = history(port, path).await;
        if h.len() >= n {
            return h;
        }
        assert!(
            t.elapsed() < T,
            "history of {path} never reached {n}: {h:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn writer_cause_and_history() {
    let root = tempfile::tempdir().unwrap();
    let (hub_dir, va, sa, marker) = (
        root.path().join("hub"),
        root.path().join("vault-a"),
        root.path().join("state-a"),
        root.path().join("vault-rclone"),
    );
    std::fs::create_dir_all(va.join("Notes")).unwrap();
    std::fs::create_dir_all(&marker).unwrap();
    let port = free_port();
    let hub = start_hub(&hub_dir, port);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let a = start_agent_with(&va, &sa, "dev-a", port, Some(marker.clone()));
    tokio::time::sleep(Duration::from_secs(1)).await;

    // 1. An unknown program edits the note: dev-a/external.
    std::fs::write(va.join("Notes/w.md"), "v1\n").unwrap();
    let h = wait_history(port, "Notes/w.md", 1).await;
    assert_eq!(h[0].writer.as_deref(), Some("dev-a/external"));
    assert_eq!(h[0].cause.as_deref(), Some("external-edit"));
    assert_eq!(h[0].size, 3);

    // 2. While vault-rclone's marker says bisync runs: dev-a/bisync.
    let m = vault_rclone::marker::SyncMarker::start(&marker, "sync");
    std::fs::write(va.join("Notes/w.md"), "from drive\n").unwrap();
    let h = wait_history(port, "Notes/w.md", 2).await;
    assert_eq!(h[0].writer.as_deref(), Some("dev-a/bisync"));
    assert_eq!(h[0].cause.as_deref(), Some("bisync-pull"));
    drop(m);
    // Past the 5 s window, edits are external again.
    tokio::time::sleep(Duration::from_millis(agent::BISYNC_WINDOW_MS + 500)).await;

    // 3. A claimed write (what `vault-events write --as galanonim` does).
    let body = b"by the journalist\n";
    claims::put(
        &claims::claims_dir(&sa),
        &claims::Claim {
            path: "Notes/w.md".into(),
            sha256: event::sha256_hex(body),
            writer: "galanonim".into(),
            ts_ms: event::now_ms(),
        },
    )
    .unwrap();
    std::fs::write(va.join("Notes/w.md"), body).unwrap();
    let h = wait_history(port, "Notes/w.md", 3).await;
    assert_eq!(h[0].writer.as_deref(), Some("dev-a/galanonim"));
    assert_eq!(h[0].cause.as_deref(), Some("local-edit"));
    // Newest first, each entry links to the one it replaced.
    assert_eq!(
        h.iter().map(|e| e.version).collect::<Vec<_>>(),
        vec![3, 2, 1]
    );
    assert_eq!(h[0].replaced_sha256.as_deref(), Some(h[1].sha256.as_str()));
    assert_eq!(h[0].prev_sha256.as_deref(), Some(h[1].sha256.as_str()));
    assert_eq!(h[2].replaced_sha256, None);

    // 4. rclone bisync conflict files are never published or stored.
    std::fs::write(va.join("Notes/w.md..conflict-Date1"), "old\n").unwrap();
    std::fs::write(va.join("Notes/after.md"), "x\n").unwrap();
    wait_history(port, "Notes/after.md", 1).await;
    assert!(history(port, "Notes/w.md..conflict-Date1").await.is_empty());
    assert_eq!(hub_version(port, "Notes/w.md..conflict-Date1").await, None);

    // 5. History survives a hub restart (append-only file).
    a.stop().await;
    hub.stop().await;
    let hub = start_hub(&hub_dir, port);
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(history(port, "Notes/w.md").await.len(), 3);
    hub.stop().await;
}

/// Deterministic, incompressible-looking bytes (xorshift).
fn photo_bytes(n: usize, seed: u64) -> Vec<u8> {
    let mut x = seed | 1;
    (0..n)
        .map(|_| {
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            (x >> 24) as u8
        })
        .collect()
}

fn sha_of(p: &Path) -> Option<String> {
    std::fs::read(p).ok().map(|b| event::sha256_hex(&b))
}

fn all_files(dir: &Path) -> Vec<String> {
    let mut out = Vec::new();
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let p = e.path();
            if p.is_dir() {
                out.extend(all_files(&p));
            } else {
                out.push(p.to_string_lossy().to_string());
            }
        }
    }
    out
}

/// A 7 MB photo goes A -> hub -> B as a blob reference plus 256 KiB
/// chunks, the sessions stay up (a small note still syncs afterwards), and
/// a device joining later gets it from the hub alone via catch-up.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn large_file_travels_in_chunks() {
    let root = tempfile::tempdir().unwrap();
    let p = |n: &str| root.path().join(n);
    let (hub_dir, va, vb, vc) = (p("hub"), p("vault-a"), p("vault-b"), p("vault-c"));
    for d in [&va, &vb, &vc] {
        std::fs::create_dir_all(d.join("Daily")).unwrap();
    }
    let port = free_port();
    let hub = start_hub(&hub_dir, port);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let big_max = 16 << 20;
    let a = start_agent_opts(&va, &p("state-a"), "nitro5", port, None, big_max);
    let b = start_agent_opts(&vb, &p("state-b"), "s24", port, None, big_max);
    tokio::time::sleep(Duration::from_secs(1)).await;

    let photo = photo_bytes(7_000_000, 42);
    let sha = event::sha256_hex(&photo);
    let rel = "Daily/2026-11-19-photo.jpg";
    // Saved like a camera/sync app would: temp file, then rename.
    std::fs::write(va.join("Daily/.incoming"), &photo).unwrap();
    std::fs::rename(va.join("Daily/.incoming"), va.join(rel)).unwrap();
    let big_t = Duration::from_secs(60);
    wait_for("7 MB photo on B", big_t, || {
        sha_of(&vb.join(rel)).as_deref() == Some(sha.as_str())
    })
    .await;

    // The hub stores a reference plus the blob, whose bytes hash right.
    let s = zenoh::open(net::client_config(&[format!("tcp/127.0.0.1:{port}")]).unwrap())
        .await
        .unwrap();
    let idx = net::fetch_index(&s, Duration::from_secs(5))
        .await
        .unwrap()
        .unwrap();
    let st = idx.stamps.iter().find(|s| s.path == rel).unwrap();
    assert_eq!(st.sha256, sha);
    let stored = net::fetch_note(&s, rel, Duration::from_secs(5))
        .await
        .unwrap()
        .unwrap();
    assert!(stored.is_blob_ref(), "catch-up reply is small");
    assert_eq!(stored.blob.unwrap().size, 7_000_000);
    assert!(
        stored.encode().len() < 2048,
        "reference is {} B",
        stored.encode().len()
    );
    s.close().await.unwrap();
    assert_eq!(
        sha_of(&hub_dir.join("blobs").join(&sha)).as_deref(),
        Some(sha.as_str())
    );

    // Sessions survived: small notes still sync both ways.
    std::fs::write(vb.join("Daily/after.md"), "still linked\n").unwrap();
    wait_for("note B -> A after the photo", T, || {
        read(va.join("Daily/after.md")).as_deref() == Some("still linked\n")
    })
    .await;
    std::fs::write(va.join("Daily/after2.md"), "and back\n").unwrap();
    wait_for("note A -> B after the photo", T, || {
        read(vb.join("Daily/after2.md")).as_deref() == Some("and back\n")
    })
    .await;

    // No conflict copies, no partial or temp files left in the vaults.
    for v in [&va, &vb] {
        let files = all_files(v);
        assert!(
            files
                .iter()
                .all(|f| !f.contains(" (conflict ") && !f.contains(".vault-events-tmp-")),
            "{files:?}"
        );
    }
    assert!(all_files(&p("state-b").join("partial")).is_empty());

    // The sender goes away; a new device still gets the photo from the hub.
    a.stop().await;
    let c = start_agent_opts(&vc, &p("state-c"), "towerx", port, None, big_max);
    wait_for("photo on late joiner C via catch-up", big_t, || {
        sha_of(&vc.join(rel)).as_deref() == Some(sha.as_str())
    })
    .await;
    assert!(conflict_copies(&vc.join("Daily")).is_empty());

    c.stop().await;
    b.stop().await;
    hub.stop().await;
}
