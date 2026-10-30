//! Vault-only wrapper around the `rclone` binary.
//!
//! This crate does not talk to Google Drive itself and does not embed rclone.
//! It finds the `rclone` executable, builds argv for it, and checks the
//! answers. Sync policy follows the vault note "DriveSync Replacement":
//! only `gdrive:Vault` against one local folder, never the whole Drive.
//!
//! Commands: `status`, `check-config`, `lsf`, and the guarded `sync` /
//! `resync` (empty-side refuse, `--max-delete`, newer-wins conflicts, run
//! log). The bash `vault-sync` stays in production until this has survived
//! real edits.

pub mod commands;
pub mod config;
pub mod error;
pub mod rclone;
pub mod sync;

pub use config::{
    Config, DEFAULT_CONFLICT_SUFFIX, DEFAULT_MAX_DELETE, DEFAULT_MIN_FILES, DEFAULT_REMOTE,
};
pub use error::Error;
pub use rclone::{BisyncOpts, CommandRunner, RunOutput, SystemRunner};
