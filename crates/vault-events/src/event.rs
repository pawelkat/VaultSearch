//! Wire format of one vault change.
//!
//! Payload is JSON (UTF-8) with an explicit `format` number. Readers accept
//! every `format <= FORMAT_VERSION` they know and reject newer ones instead of
//! guessing. File bytes travel base64-encoded in `content` so binary
//! attachments and non-UTF-8 text survive unchanged.
//!
//! Version clock: a per-note Lamport counter. A device that edits a note it
//! last saw at version `n` publishes `n + 1`. Two devices that edit the same
//! base concurrently both publish `n + 1`; [`Stamp::rank`] breaks the tie
//! deterministically (see [`Stamp::beats`]).

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::key;

/// Current payload format. Bump on incompatible changes.
pub const FORMAT_VERSION: u32 = 1;

/// Version metadata of one note state (live content or tombstone).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Stamp {
    /// Vault-relative path, `/`-separated.
    pub path: String,
    /// Per-note Lamport counter, starting at 1.
    pub version: u64,
    /// Wall clock of the producing device, Unix milliseconds. Tie-break only.
    pub ts_ms: u64,
    /// Device that produced this state (`box`, `latitude`, `s24`, ...).
    pub origin: String,
    /// Tombstone.
    #[serde(default)]
    pub deleted: bool,
    /// Lower-case hex SHA-256 of the content; empty for a tombstone.
    #[serde(default)]
    pub sha256: String,
    /// SHA-256 of the state this edit was based on (`None` for a note the
    /// device had never seen). Lets a receiver tell a fast-forward from a
    /// concurrent edit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prev_sha256: Option<String>,
}

impl Stamp {
    /// Total order used everywhere (hub storage and agents):
    /// 1. higher `version`;
    /// 2. later `ts_ms`;
    /// 3. live content over a tombstone;
    /// 4. lexicographically larger `origin`;
    /// 5. lexicographically larger `sha256`.
    ///
    /// Every device computes the same winner from the same two stamps.
    pub fn rank(&self) -> (u64, u64, bool, &str, &str) {
        (
            self.version,
            self.ts_ms,
            !self.deleted,
            self.origin.as_str(),
            self.sha256.as_str(),
        )
    }

    /// `true` if `self` strictly wins over `other`.
    pub fn beats(&self, other: &Stamp) -> bool {
        self.rank() > other.rank()
    }

    /// Same logical state (same version, origin and content).
    pub fn same_state(&self, other: &Stamp) -> bool {
        self.rank() == other.rank()
    }
}

/// One change, as published on `vault/notes/<path>`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NoteEvent {
    pub format: u32,
    #[serde(flatten)]
    pub stamp: Stamp,
    /// File bytes, base64 on the wire. `None` for a tombstone.
    #[serde(default, with = "b64", skip_serializing_if = "Option::is_none")]
    pub content: Option<Vec<u8>>,
}

#[derive(Debug)]
pub enum DecodeError {
    Json(serde_json::Error),
    UnsupportedFormat(u32),
    BadPath(key::PathError),
    KeyMismatch { key: String, path: String },
    HashMismatch,
    MissingContent,
    TombstoneWithContent,
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DecodeError::Json(e) => write!(f, "bad JSON: {e}"),
            DecodeError::UnsupportedFormat(v) => {
                write!(
                    f,
                    "unsupported event format {v} (this build reads <= {FORMAT_VERSION})"
                )
            }
            DecodeError::BadPath(e) => write!(f, "bad path: {e}"),
            DecodeError::KeyMismatch { key, path } => {
                write!(f, "key {key} does not match payload path {path}")
            }
            DecodeError::HashMismatch => f.write_str("sha256 does not match content"),
            DecodeError::MissingContent => f.write_str("live event without content"),
            DecodeError::TombstoneWithContent => f.write_str("tombstone carries content"),
        }
    }
}

impl std::error::Error for DecodeError {}

/// Lower-case hex SHA-256.
pub fn sha256_hex(bytes: &[u8]) -> String {
    let d = Sha256::digest(bytes);
    let mut s = String::with_capacity(64);
    for b in d {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

pub fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

impl NoteEvent {
    /// Live content event.
    pub fn put(
        path: &str,
        content: Vec<u8>,
        origin: &str,
        version: u64,
        prev_sha256: Option<String>,
        ts_ms: u64,
    ) -> Self {
        NoteEvent {
            format: FORMAT_VERSION,
            stamp: Stamp {
                path: path.to_string(),
                version,
                ts_ms,
                origin: origin.to_string(),
                deleted: false,
                sha256: sha256_hex(&content),
                prev_sha256,
            },
            content: Some(content),
        }
    }

    /// Tombstone.
    pub fn delete(
        path: &str,
        origin: &str,
        version: u64,
        prev_sha256: Option<String>,
        ts_ms: u64,
    ) -> Self {
        NoteEvent {
            format: FORMAT_VERSION,
            stamp: Stamp {
                path: path.to_string(),
                version,
                ts_ms,
                origin: origin.to_string(),
                deleted: true,
                sha256: String::new(),
                prev_sha256,
            },
            content: None,
        }
    }

    pub fn key(&self) -> Result<String, key::PathError> {
        key::path_to_key(&self.stamp.path)
    }

    pub fn encode(&self) -> Vec<u8> {
        serde_json::to_vec(self).expect("NoteEvent serializes")
    }

    /// Decode and validate. `key` is the Zenoh key the sample arrived on;
    /// pass `None` when there is no key to check against.
    pub fn decode(bytes: &[u8], key: Option<&str>) -> Result<Self, DecodeError> {
        // Peek at the format first so a newer, incompatible payload is
        // reported as such and not as a confusing field error.
        #[derive(Deserialize)]
        struct Peek {
            format: u32,
        }
        let peek: Peek = serde_json::from_slice(bytes).map_err(DecodeError::Json)?;
        if peek.format == 0 || peek.format > FORMAT_VERSION {
            return Err(DecodeError::UnsupportedFormat(peek.format));
        }
        let ev: NoteEvent = serde_json::from_slice(bytes).map_err(DecodeError::Json)?;
        key::validate_rel(&ev.stamp.path).map_err(DecodeError::BadPath)?;
        if let Some(k) = key {
            let expect = key::path_to_key(&ev.stamp.path).map_err(DecodeError::BadPath)?;
            if expect != k {
                return Err(DecodeError::KeyMismatch {
                    key: k.to_string(),
                    path: ev.stamp.path.clone(),
                });
            }
        }
        match (&ev.content, ev.stamp.deleted) {
            (Some(_), true) => return Err(DecodeError::TombstoneWithContent),
            (None, false) => return Err(DecodeError::MissingContent),
            (Some(c), false) => {
                if sha256_hex(c) != ev.stamp.sha256 {
                    return Err(DecodeError::HashMismatch);
                }
            }
            (None, true) => {}
        }
        Ok(ev)
    }
}

mod b64 {
    use base64::Engine;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S: Serializer>(v: &Option<Vec<u8>>, s: S) -> Result<S::Ok, S::Error> {
        match v {
            Some(b) => s.serialize_str(&base64::engine::general_purpose::STANDARD.encode(b)),
            None => s.serialize_none(),
        }
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Option<Vec<u8>>, D::Error> {
        let o: Option<String> = Option::deserialize(d)?;
        match o {
            None => Ok(None),
            Some(s) => base64::engine::general_purpose::STANDARD
                .decode(s.as_bytes())
                .map(Some)
                .map_err(serde::de::Error::custom),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_put_and_delete() {
        let e = NoteEvent::put(
            "Notes/a.md",
            b"hello\n".to_vec(),
            "box",
            3,
            Some("x".into()),
            5,
        );
        let k = e.key().unwrap();
        let back = NoteEvent::decode(&e.encode(), Some(&k)).unwrap();
        assert_eq!(back, e);
        let d = NoteEvent::delete("Notes/a.md", "s24", 4, Some(e.stamp.sha256.clone()), 6);
        assert_eq!(NoteEvent::decode(&d.encode(), Some(&k)).unwrap(), d);
        // binary content survives
        let bin = NoteEvent::put(
            "Attachments /x.png",
            vec![0, 255, 10, 13],
            "box",
            1,
            None,
            1,
        );
        assert_eq!(
            NoteEvent::decode(&bin.encode(), None).unwrap().content,
            Some(vec![0, 255, 10, 13])
        );
    }

    #[test]
    fn wire_shape_is_stable() {
        let e = NoteEvent::put("a.md", b"hi".to_vec(), "box", 1, None, 7);
        let v: serde_json::Value = serde_json::from_slice(&e.encode()).unwrap();
        assert_eq!(v["format"], 1);
        assert_eq!(v["path"], "a.md");
        assert_eq!(v["version"], 1);
        assert_eq!(v["origin"], "box");
        assert_eq!(v["deleted"], false);
        assert_eq!(v["content"], "aGk=");
        assert_eq!(
            v["sha256"],
            "8f434346648f6b96df89dda901c5176b10a6d83961dd3c1ac88b59b2dc327aa4"
        );
        assert!(v.get("prev_sha256").is_none());
    }

    #[test]
    fn rejects_bad_payloads() {
        let e = NoteEvent::put("a.md", b"hi".to_vec(), "box", 1, None, 7);
        let mut v: serde_json::Value = serde_json::from_slice(&e.encode()).unwrap();
        v["format"] = 2.into();
        assert!(matches!(
            NoteEvent::decode(v.to_string().as_bytes(), None),
            Err(DecodeError::UnsupportedFormat(2))
        ));
        v["format"] = 1.into();
        v["content"] = "aGo=".into();
        assert!(matches!(
            NoteEvent::decode(v.to_string().as_bytes(), None),
            Err(DecodeError::HashMismatch)
        ));
        assert!(matches!(
            NoteEvent::decode(&e.encode(), Some("vault/notes/b.md")),
            Err(DecodeError::KeyMismatch { .. })
        ));
        let mut v2: serde_json::Value = serde_json::from_slice(&e.encode()).unwrap();
        v2["path"] = "../x".into();
        assert!(matches!(
            NoteEvent::decode(v2.to_string().as_bytes(), None),
            Err(DecodeError::BadPath(_))
        ));
        let mut v3: serde_json::Value = serde_json::from_slice(&e.encode()).unwrap();
        v3["deleted"] = true.into();
        assert!(matches!(
            NoteEvent::decode(v3.to_string().as_bytes(), None),
            Err(DecodeError::TombstoneWithContent)
        ));
    }

    #[test]
    fn ordering_is_total_and_deterministic() {
        let a = NoteEvent::put("a.md", b"a".to_vec(), "box", 2, None, 100).stamp;
        let b = NoteEvent::put("a.md", b"b".to_vec(), "s24", 2, None, 100).stamp;
        let c = NoteEvent::put("a.md", b"c".to_vec(), "box", 3, None, 1).stamp;
        let t = NoteEvent::delete("a.md", "zzz", 2, None, 100).stamp;
        let later = NoteEvent::put("a.md", b"a".to_vec(), "aaa", 2, None, 101).stamp;
        assert!(
            c.beats(&a) && c.beats(&b),
            "higher version wins regardless of clock"
        );
        assert!(later.beats(&b), "same version: later ts wins");
        assert!(b.beats(&a), "same version+ts: origin breaks the tie");
        assert!(a.beats(&t) && b.beats(&t), "live beats tombstone on a tie");
        assert!(a.beats(&b) != b.beats(&a));
        assert!(!a.beats(&a) && a.same_state(&a.clone()));
    }
}
