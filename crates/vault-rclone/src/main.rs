use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};

use vault_rclone::commands::{self, Host, EXIT_CONFIG, EXIT_FAIL};
use vault_rclone::{sync, Config, SystemRunner};

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
    /// Incremental bisync (remote <-> local) after empty-side guards.
    Sync {
        /// Pass --dry-run to rclone: show what would change, change nothing.
        #[arg(long, short = 'n')]
        dry_run: bool,
    },
    /// bisync --resync --resync-mode newer after the same guards. For the
    /// first run, after rclone exit 7, or after editing the filters file.
    Resync {
        /// Pass --dry-run to rclone. Strongly recommended first.
        #[arg(long, short = 'n')]
        dry_run: bool,
    },
    /// List the remote's top level (or VAULT_LOCAL's with --local).
    Lsf {
        /// List VAULT_LOCAL instead of the remote.
        #[arg(long)]
        local: bool,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let mut out = std::io::stdout().lock();

    let cfg = match Config::load(cli.config.as_deref()) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("vault-rclone: {e}");
            return ExitCode::from(EXIT_CONFIG as u8);
        }
    };
    let host = Host::from_process();
    let r = &SystemRunner;
    let code = match cli.cmd {
        Cmd::Status { offline } => commands::status(&cfg, &host, r, offline, &mut out),
        Cmd::CheckConfig { offline } => commands::check_config(&cfg, &host, r, offline, &mut out),
        Cmd::Sync { dry_run } => sync::sync(&cfg, &host, r, dry_run, &mut out),
        Cmd::Resync { dry_run } => sync::resync(&cfg, &host, r, dry_run, &mut out),
        Cmd::Lsf { local } => sync::lsf(&cfg, &host, r, local, &mut out),
    };
    let _ = out.flush();
    match code {
        Ok(c) => ExitCode::from(u8::try_from(c).unwrap_or(EXIT_FAIL as u8)),
        Err(e) => {
            eprintln!("vault-rclone: cannot write output: {e}");
            ExitCode::from(EXIT_FAIL as u8)
        }
    }
}
