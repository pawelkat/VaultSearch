use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

use std::io::Read;

use vault_events::{agent, blob, claims, event, hub, key, net, vault};

#[derive(Parser)]
#[command(
    name = "vault-events",
    about = "Event-based vault sync over Zenoh (stage 1)"
)]
struct Cli {
    #[command(subcommand)]
    cmd: Cmd,
}

fn home() -> PathBuf {
    std::env::var_os("HOME")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("."))
}

fn default_share() -> PathBuf {
    // ~/.local/share survives Grok Bot box image updates; ~/.cache and
    // ~/.local/state do not.
    home().join(".local/share/vault-events")
}

fn default_device() -> String {
    std::env::var("VAULT_EVENTS_DEVICE")
        .ok()
        .filter(|s| !s.is_empty())
        .or_else(|| {
            std::fs::read_to_string("/etc/hostname")
                .ok()
                .map(|s| s.trim().to_string())
        })
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "unknown".into())
}

#[derive(Subcommand)]
enum Cmd {
    /// Zenoh router + latest-state storage (+ optional one-way Drive backup).
    Hub {
        /// Storage directory (default ~/.local/share/vault-events/hub).
        #[arg(long)]
        data_dir: Option<PathBuf>,
        /// Zenoh listen endpoint(s).
        #[arg(long, default_values_t = [format!("tcp/0.0.0.0:{}", net::DEFAULT_PORT)])]
        listen: Vec<String>,
        /// Other routers to link to (e.g. tcp/note4.<tailnet>.ts.net:7447).
        #[arg(long)]
        connect: Vec<String>,
        /// Name shown in catch-up replies.
        #[arg(long, default_value = "box")]
        name: String,
        /// vault-rclone config.env; enables `vault-rclone backup` after
        /// stored changes. Off unless given.
        #[arg(long)]
        backup_config: Option<PathBuf>,
        /// Seconds between the first stored change and the backup run.
        #[arg(long, default_value_t = 300)]
        backup_interval: u64,
        /// Run the backup as `--dry-run` only.
        #[arg(long)]
        backup_dry_run: bool,
        /// Files larger than this (bytes) travel as a blob reference plus
        /// chunks pulled over Zenoh queries instead of one inline message.
        #[arg(long, env = "VAULT_EVENTS_INLINE_MAX", default_value_t = blob::DEFAULT_INLINE_MAX)]
        inline_max: u64,
        /// Chunk size (bytes) for blob transfers.
        #[arg(long, env = "VAULT_EVENTS_CHUNK_SIZE", default_value_t = blob::DEFAULT_CHUNK_SIZE)]
        chunk_size: u64,
    },
    /// Watch a vault folder, publish local saves, apply incoming events.
    Agent {
        /// Vault root. Default: VAULT_LOCAL from the vault-rclone config.
        #[arg(long)]
        vault: Option<PathBuf>,
        /// Device id carried in events and conflict names.
        #[arg(long)]
        device: Option<String>,
        /// Agent state (default ~/.local/share/vault-events/agent).
        #[arg(long)]
        state_dir: Option<PathBuf>,
        /// Hub endpoint(s).
        #[arg(long, default_values_t = [format!("tcp/127.0.0.1:{}", net::DEFAULT_PORT)])]
        connect: Vec<String>,
        #[arg(long, default_value_t = vault::DEFAULT_MAX_BYTES)]
        max_bytes: u64,
        #[arg(long, default_value_t = agent::DEFAULT_MAX_DELETE)]
        max_delete: usize,
        #[arg(long, default_value_t = agent::DEFAULT_MIN_FILES)]
        min_files: usize,
        #[arg(long, default_value_t = 750)]
        debounce_ms: u64,
        #[arg(long, default_value_t = 300)]
        catchup_secs: u64,
        #[arg(long, default_value_t = 600)]
        rescan_secs: u64,
        /// Program part of this agent's writer label (`<device>/<program>`).
        #[arg(long, default_value = agent::DEFAULT_PROGRAM)]
        program: String,
        /// vault-rclone marker dir; changes seen while `sync.running` is
        /// there (or <5 s after) are labelled `<device>/bisync`. Default:
        /// next to BISYNC_WORKDIR from the vault-rclone config, if that
        /// config loads.
        #[arg(long)]
        bisync_marker_dir: Option<PathBuf>,
        /// Do not look for a bisync marker (hosts without Drive bisync).
        #[arg(long)]
        no_bisync_marker: bool,
        /// Files larger than this (bytes) travel as a blob reference plus
        /// chunks pulled over Zenoh queries instead of one inline message.
        #[arg(long, env = "VAULT_EVENTS_INLINE_MAX", default_value_t = blob::DEFAULT_INLINE_MAX)]
        inline_max: u64,
        /// Chunk size (bytes) for blob transfers.
        #[arg(long, env = "VAULT_EVENTS_CHUNK_SIZE", default_value_t = blob::DEFAULT_CHUNK_SIZE)]
        chunk_size: u64,
    },
    /// Write (or delete) one note as a named writer: reads the content from
    /// stdin, writes it atomically, and leaves a claim so the running agent
    /// publishes it as `<device>/<name>` (cause local-edit / delete).
    Write {
        /// Writer name (`galanonim`). Default: $VAULT_EVENTS_WRITER.
        #[arg(long = "as", alias = "writer")]
        as_name: Option<String>,
        /// Vault-relative path, e.g. `Daily/2026-11-19.md`.
        path: String,
        /// Delete the note instead of writing stdin.
        #[arg(long)]
        delete: bool,
        /// Vault root. Default: VAULT_LOCAL from the vault-rclone config.
        #[arg(long)]
        vault: Option<PathBuf>,
        /// Agent state dir (claims live under <state-dir>/claims).
        #[arg(long)]
        state_dir: Option<PathBuf>,
        /// Seconds to wait for the agent to pick the change up (0 = don't).
        #[arg(long, default_value_t = 10)]
        wait_secs: u64,
    },
    /// Provenance history of one path from the hub (newest first).
    Who {
        path: String,
        #[arg(long, default_values_t = [format!("tcp/127.0.0.1:{}", net::DEFAULT_PORT)])]
        connect: Vec<String>,
        #[arg(long, default_value_t = 20)]
        limit: usize,
    },
    /// Print what the hub stores (path, version, origin) via the index query.
    Index {
        #[arg(long, default_values_t = [format!("tcp/127.0.0.1:{}", net::DEFAULT_PORT)])]
        connect: Vec<String>,
        /// Only print counts.
        #[arg(long)]
        summary: bool,
    },
}

fn transfer_opts(inline_max: u64, chunk_size: u64) -> blob::TransferOpts {
    blob::TransferOpts {
        inline_max,
        chunk_size,
        ..Default::default()
    }
    .normalized()
}

fn main() -> ExitCode {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_env("VAULT_EVENTS_LOG")
                .unwrap_or_else(|_| EnvFilter::new("info,zenoh=warn")),
        )
        .with_writer(std::io::stderr)
        .init();
    let cli = Cli::parse();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio");
    match rt.block_on(run(cli)) {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("vault-events: {e}");
            ExitCode::from(1)
        }
    }
}

async fn run(cli: Cli) -> Result<(), net::BoxError> {
    match cli.cmd {
        Cmd::Hub {
            data_dir,
            listen,
            connect,
            name,
            backup_config,
            backup_interval,
            backup_dry_run,
            inline_max,
            chunk_size,
        } => {
            let opts = hub::HubOpts {
                data_dir: data_dir.unwrap_or_else(|| default_share().join("hub")),
                listen,
                connect,
                name,
                backup_config,
                backup_interval: Duration::from_secs(backup_interval),
                backup_dry_run,
                transfer: transfer_opts(inline_max, chunk_size),
            };
            hub::run(opts, None).await
        }
        Cmd::Agent {
            vault,
            device,
            state_dir,
            connect,
            max_bytes,
            max_delete,
            min_files,
            debounce_ms,
            catchup_secs,
            rescan_secs,
            program,
            bisync_marker_dir,
            no_bisync_marker,
            inline_max,
            chunk_size,
        } => {
            if !claims::valid_writer_name(&program) {
                return Err(format!("--program {program:?}: use [A-Za-z0-9._-], max 32").into());
            }
            let rclone_cfg = vault_rclone::Config::load(None).ok();
            let vault = match vault {
                Some(v) => v,
                None => vault_rclone::Config::load(None)?
                    .require_local()?
                    .to_path_buf(),
            };
            let bisync_marker_dir = if no_bisync_marker {
                None
            } else {
                bisync_marker_dir
                    .or_else(|| rclone_cfg.as_ref().map(vault_rclone::marker::marker_dir))
            };
            let opts = agent::AgentOpts {
                vault,
                device: device.unwrap_or_else(default_device),
                state_dir: state_dir.unwrap_or_else(|| default_share().join("agent")),
                connect,
                max_bytes,
                max_delete,
                min_files,
                debounce: Duration::from_millis(debounce_ms),
                catchup_interval: Duration::from_secs(catchup_secs.max(5)),
                rescan_interval: Duration::from_secs(rescan_secs.max(5)),
                query_timeout: Duration::from_secs(20),
                program,
                bisync_marker_dir,
                transfer: transfer_opts(inline_max, chunk_size),
            };
            agent::run(opts, None).await
        }
        Cmd::Index { connect, summary } => {
            let s = zenoh::open(net::client_config(&connect)?).await?;
            let r = net::fetch_index(&s, Duration::from_secs(10)).await?;
            s.close().await?;
            let Some(r) = r else {
                return Err("no hub answered vault/hub/index".into());
            };
            let live = r.stamps.iter().filter(|s| !s.deleted).count();
            println!(
                "hub {}: {} stored, {} live, {} tombstones",
                r.hub,
                r.stamps.len(),
                live,
                r.stamps.len() - live
            );
            if !summary {
                println!("# path\tversion\torigin\twriter\tcause\tsha256");
                for st in &r.stamps {
                    println!(
                        "{}\tv{}\t{}\t{}\t{}\t{}",
                        st.path,
                        st.version,
                        st.origin,
                        st.writer_str(),
                        st.cause_str(),
                        if st.deleted {
                            "deleted"
                        } else {
                            &st.sha256[..12.min(st.sha256.len())]
                        }
                    );
                }
            }
            Ok(())
        }
        Cmd::Write {
            as_name,
            path,
            delete,
            vault,
            state_dir,
            wait_secs,
        } => {
            let name = as_name
                .or_else(|| std::env::var("VAULT_EVENTS_WRITER").ok())
                .filter(|s| !s.is_empty())
                .ok_or("give --as <name> or set VAULT_EVENTS_WRITER")?;
            if !claims::valid_writer_name(&name) {
                return Err(format!("writer {name:?}: use [A-Za-z0-9._-], max 32").into());
            }
            key::validate_rel(&path).map_err(|e| format!("{path}: {e}"))?;
            if !vault::is_synced_path(&path) {
                return Err(format!("{path} is not a synced path").into());
            }
            let vault = match vault {
                Some(v) => v,
                None => vault_rclone::Config::load(None)?
                    .require_local()?
                    .to_path_buf(),
            };
            let fs = vault::VaultFs::new(&vault, u64::MAX);
            let state_dir = state_dir.unwrap_or_else(|| default_share().join("agent"));
            let dir = claims::claims_dir(&state_dir);
            let bytes = if delete {
                None
            } else {
                let mut b = Vec::new();
                std::io::stdin().read_to_end(&mut b)?;
                Some(b)
            };
            let sha = bytes.as_deref().map(event::sha256_hex).unwrap_or_default();
            claims::put(
                &dir,
                &claims::Claim {
                    path: path.clone(),
                    sha256: sha.clone(),
                    writer: name.clone(),
                    ts_ms: event::now_ms(),
                },
            )?;
            match &bytes {
                Some(b) => fs.write_atomic(&path, b)?,
                None => fs.remove(&path)?,
            }
            let what = if delete { "deleted" } else { "wrote" };
            println!(
                "{what} {path}{} as {name}",
                bytes
                    .as_ref()
                    .map(|b| format!(" ({} B)", b.len()))
                    .unwrap_or_default()
            );
            if wait_secs > 0 {
                let t = std::time::Instant::now();
                while t.elapsed() < Duration::from_secs(wait_secs) {
                    if claims::peek(&dir, &path).is_none() {
                        println!("agent picked it up (claim consumed)");
                        return Ok(());
                    }
                    tokio::time::sleep(Duration::from_millis(200)).await;
                }
                eprintln!(
                    "warning: the agent has not picked up {path} within {wait_secs}s (is it running with --state-dir {}?)",
                    state_dir.display()
                );
            }
            Ok(())
        }
        Cmd::Who {
            path,
            connect,
            limit,
        } => {
            key::validate_rel(&path).map_err(|e| format!("{path}: {e}"))?;
            let s = zenoh::open(net::client_config(&connect)?).await?;
            let r = net::fetch_history(&s, &path, limit, Duration::from_secs(10)).await?;
            s.close().await?;
            let Some(r) = r else {
                return Err("no hub answered vault/hub/history (older hub build?)".into());
            };
            println!(
                "hub {}: {} history entries for {} (newest first)",
                r.hub,
                r.entries.len(),
                r.path
            );
            for e in &r.entries {
                println!("{}", who_line(e));
            }
            Ok(())
        }
    }
}

fn short(s: &str) -> &str {
    &s[..12.min(s.len())]
}

/// `2026-11-19T20:59:41Z v5 nitro5 nitro5/external (external-edit) 2861 B sha 8f4343466 prev 1a2b..`
fn who_line(e: &vault_events::history::HistoryEntry) -> String {
    let when = vault_rclone::sync::format_utc(e.stored_ms / 1000);
    let what = if e.deleted {
        "deleted".to_string()
    } else {
        format!("{} B sha {}", e.size, short(&e.sha256))
    };
    let mut line = format!(
        "{when} v{} from {} by {} ({}) {what}",
        e.version,
        e.origin,
        e.writer.as_deref().unwrap_or("-"),
        e.cause.as_deref().unwrap_or("-"),
    );
    match &e.prev_sha256 {
        Some(p) => line.push_str(&format!(" based-on {}", short(p))),
        None => line.push_str(" based-on -"),
    }
    match &e.replaced_sha256 {
        Some(r) if r.is_empty() => line.push_str(" replaced tombstone"),
        Some(r) => line.push_str(&format!(" replaced {}", short(r))),
        None => line.push_str(" new-to-hub"),
    }
    if let Some(c) = &e.conflict {
        line.push_str(&format!(
            " | conflict copy of {}: kept {} B {} ({}), saved {} B {} ({}){}",
            c.of,
            c.winner_size,
            short(&c.winner_sha256),
            c.winner_origin,
            c.loser_size,
            short(&c.loser_sha256),
            c.loser_origin,
            if c.kept_smaller() {
                " WARN kept smaller side"
            } else {
                ""
            }
        ));
    }
    line
}
