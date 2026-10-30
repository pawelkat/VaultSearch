//! Vault-only wrapper around the `rclone` binary.
//!
//! This crate does not talk to Google Drive itself and does not embed rclone.
//! It finds the `rclone` executable, builds argv for it, and checks the
//! answers. Sync policy follows the vault note "DriveSync Replacement":
//! only `gdrive:Vault` against one local folder, never the whole Drive.
//!
//! PR1 ships config loading, `status`, and `check-config`. `sync`, `resync`,
//! and `lsf` are reserved and refuse with exit code 2 until PR2.

pub mod commands;
pub mod config;
pub mod error;
pub mod rclone;

pub use config::{Config, DEFAULT_MAX_DELETE, DEFAULT_MIN_FILES, DEFAULT_REMOTE};
pub use error::Error;
pub use rclone::{CommandRunner, RunOutput, SystemRunner};
