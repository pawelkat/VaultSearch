use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};

use vault_rclone::commands::{self, Host, EXIT_FAIL, EXIT_NOT_YET};
use vault_rclone::{Config, SystemRunner};

#[derive(Parser)]
#[command(
    name = "vault-rclone",
    about = "Vault-only wrapper around the rclone binary (gdrive:Vault <-> VAULT_LOCAL)"
)]
struct Cli {
    /// Read settings from this file instead of ~/.config/vault-sync/config.env
    /// and ~/.config/vault-rclone/config.env. Environment still wins.
    #[arg(long, global = true, value_name = "FILE")]
    config: Option<PathBuf>,
    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Local path and file count, remote, rclone version, locks, reachability.
    Status {
        /// Skip `rclone size` on the remote.
        #[arg(long)]
        offline: bool,
    },
    /// rclone.conf present, remote listed, vault reachable. Never prints tokens.
    #[command(alias = "auth")]
    CheckConfig {
        /// Skip `rclone lsd` on the remote.
        #[arg(long)]
        offline: bool,
    },
    /// Incremental bisync with guards. Not available yet.
    Sync,
    /// bisync --resync after empty-side checks. Not available yet.
    Resync,
    /// List the remote's top level. Not available yet.
    Lsf,
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let mut out = std::io::stdout().lock();

    let code = match cli.cmd {
        Cmd::Sync => commands::not_yet("sync", &mut out),
        Cmd::Resync => commands::not_yet("resync", &mut out),
        Cmd::Lsf => commands::not_yet("lsf", &mut out),
        Cmd::Status { offline } | Cmd::CheckConfig { offline } => {
            let cfg = match Config::load(cli.config.as_deref()) {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("vault-rclone: {e}");
                    return ExitCode::from(EXIT_NOT_YET as u8);
                }
            };
            let host = Host::from_process();
            if matches!(cli.cmd, Cmd::Status { .. }) {
                commands::status(&cfg, &host, &SystemRunner, offline, &mut out)
            } else {
                commands::check_config(&cfg, &host, &SystemRunner, offline, &mut out)
            }
        }
    };
    let _ = out.flush();
    match code {
        Ok(c) => ExitCode::from(c as u8),
        Err(e) => {
            eprintln!("vault-rclone: cannot write output: {e}");
            ExitCode::from(EXIT_FAIL as u8)
        }
    }
}
