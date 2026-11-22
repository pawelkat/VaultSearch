//! Event-based vault sync over [Zenoh](https://zenoh.io) (stage 1).
//!
//! - [`event`]: the versioned wire format of one note change.
//! - [`ack`]: transfer receipts on `vault/ack/<sha256>/<device>`.
//! - [`blob`]: chunked transfer of files above the inline limit.
//! - [`key`]: vault path <-> `vault/notes/<path>` key expressions.
//! - [`conflict`]: who wins, and the conflict-copy name for the loser.
//! - [`store`]: the hub's persisted latest state per note.
//! - [`history`]: the hub's append-only per-path provenance log.
//! - [`hub`]: embedded Zenoh router + storage queryables (+ Drive backup).
//! - [`agent`]: `notify` watcher that publishes saves and applies events.
//! - [`presence`]: liveliness tokens, `device online/offline`, `peers`.
//! - [`power`]: TowerX power board over Zenoh + Ask availability.
//!
//! See the crate README for the topology and the box setup.

pub mod ack;
pub mod agent;
pub mod blob;
pub mod claims;
pub mod conflict;
pub mod event;
pub mod history;
pub mod hub;
pub mod key;
pub mod net;
pub mod power;
pub mod presence;
pub mod state;
pub mod store;
pub mod vault;

pub use ack::{AckReceipt, AckStatus, TransferAck};
pub use event::{cause, ConflictInfo, NoteEvent, Stamp, FORMAT_VERSION};
