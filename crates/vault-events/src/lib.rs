//! Event-based vault sync over [Zenoh](https://zenoh.io) (stage 1).
//!
//! - [`event`]: the versioned wire format of one note change.
//! - [`key`]: vault path <-> `vault/notes/<path>` key expressions.
//! - [`conflict`]: who wins, and the conflict-copy name for the loser.
//! - [`store`]: the hub's persisted latest state per note.
//! - [`hub`]: embedded Zenoh router + storage queryables (+ Drive backup).
//! - [`agent`]: `notify` watcher that publishes saves and applies events.
//!
//! See the crate README for the topology and the box setup.

pub mod agent;
pub mod conflict;
pub mod event;
pub mod hub;
pub mod key;
pub mod net;
pub mod state;
pub mod store;
pub mod vault;

pub use event::{NoteEvent, Stamp, FORMAT_VERSION};
