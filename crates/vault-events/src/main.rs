use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

use vault_events::{agent, hub, net, vault};

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
        } => {
            let opts = hub::HubOpts {
                data_dir: data_dir.unwrap_or_else(|| default_share().join("hub")),
                listen,
                connect,
                name,
                backup_config,
                backup_interval: Duration::from_secs(backup_interval),
                backup_dry_run,
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
        } => {
            let vault = match vault {
                Some(v) => v,
                None => vault_rclone::Config::load(None)?
                    .require_local()?
                    .to_path_buf(),
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
                for st in &r.stamps {
                    println!(
                        "{}\tv{}\t{}\t{}",
                        st.path,
                        st.version,
                        st.origin,
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
    }
}
