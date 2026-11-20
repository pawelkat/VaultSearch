//! Wire format of one vault change.
//!
//! Payload is JSON (UTF-8) with an explicit `format` number. Readers accept
//! every `format <= FORMAT_VERSION` they know and reject newer ones instead of
//! guessing. File bytes travel base64-encoded in `content` so binary
//! attachments and non-UTF-8 text survive unchanged.
//!
//! Large files (above the sender's inline limit, default 1 MiB) are not
//! carried inline: the event has no `content` and a `blob` object instead
//! (`size`, `chunk_size`, `chunks`), and receivers pull the bytes in chunks
//! from `vault/blob/<sha256>/<index>` (see [`crate::blob`]). Builds that
//! predate `blob` see a live event without content and drop it as
//! `MissingContent` (no file is written).
//!
//! Version clock: a per-note Lamport counter. A device that edits a note it
//! last saw at version `n` publishes `n + 1`. Two devices that edit the same
//! base concurrently both publish `n + 1`; [`Stamp::rank`] breaks the tie
//! deterministically (see [`Stamp::beats`]).

use std::io::{self, Read};
use std::path::Path;

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
    /// Who wrote this state, `<device>/<program>` (`box/agent`,
    /// `box/bisync`, `box/galanonim`, `s24/vaulthome`, `s24/external`).
    /// Debug provenance only: never part of the ordering. Absent in events
    /// from older builds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub writer: Option<String>,
    /// Why this state was produced; one of the [`cause`] constants.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cause: Option<String>,
}

/// Values of [`Stamp::cause`].
pub mod cause {
    /// A named writer (`vault-events write --as`, the app itself) saved it.
    pub const LOCAL_EDIT: &str = "local-edit";
    /// Changed on disk by a program the agent does not know (editor, bot).
    pub const EXTERNAL_EDIT: &str = "external-edit";
    /// Written by rclone bisync pulling from Drive (marker seen).
    pub const BISYNC_PULL: &str = "bisync-pull";
    /// An applied incoming event published again by a relay.
    pub const EVENT_APPLY_REPUBLISH: &str = "event-apply-republish";
    /// The losing side of a conflict, saved under a conflict name.
    pub const CONFLICT_COPY: &str = "conflict-copy";
    /// Removed on disk.
    pub const DELETE: &str = "delete";
    /// A recorded state the hub lacked, sent again during catch-up.
    pub const CATCHUP_REPUBLISH: &str = "catchup-republish";
    /// Found untracked by the first scan of a new agent state.
    pub const SEED: &str = "seed";

    pub const ALL: [&str; 8] = [
        LOCAL_EDIT,
        EXTERNAL_EDIT,
        BISYNC_PULL,
        EVENT_APPLY_REPUBLISH,
        CONFLICT_COPY,
        DELETE,
        CATCHUP_REPUBLISH,
        SEED,
    ];
}

/// Details carried by a conflict-copy event: which live path lost, and
/// both sides' hashes and sizes. Debug provenance only.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ConflictInfo {
    /// Live path the copy was made for.
    pub of: String,
    /// The state that stays at the live path.
    pub winner_sha256: String,
    pub winner_size: u64,
    #[serde(default)]
    pub winner_origin: String,
    /// The bytes saved in the copy.
    pub loser_sha256: String,
    pub loser_size: u64,
    #[serde(default)]
    pub loser_origin: String,
}

impl ConflictInfo {
    /// The resolution kept the smaller side at the live path.
    pub fn kept_smaller(&self) -> bool {
        self.winner_size < self.loser_size
    }
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

    /// `writer` or `-` (older peers send none).
    pub fn writer_str(&self) -> &str {
        self.writer.as_deref().unwrap_or("-")
    }

    /// `cause` or `-`.
    pub fn cause_str(&self) -> &str {
        self.cause.as_deref().unwrap_or("-")
    }
}

/// Size of a file that travels in chunks instead of inline.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlobInfo {
    /// File size in bytes.
    pub size: u64,
    /// Chunk size the sender suggests (any server answers any size).
    pub chunk_size: u64,
    /// `ceil(size / chunk_size)`.
    pub chunks: u64,
}

impl BlobInfo {
    pub fn new(size: u64, chunk_size: u64) -> Self {
        BlobInfo {
            size,
            chunk_size,
            chunks: crate::blob::chunk_count(size, chunk_size),
        }
    }

    pub fn is_valid(&self) -> bool {
        self.chunk_size >= crate::blob::MIN_CHUNK_SIZE
            && self.chunk_size <= crate::blob::MAX_CHUNK_SIZE
            && self.chunks == crate::blob::chunk_count(self.size, self.chunk_size)
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
    /// Set on conflict-copy events only.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub conflict: Option<ConflictInfo>,
    /// Set instead of `content` when the bytes travel in chunks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub blob: Option<BlobInfo>,
}

#[derive(Debug)]
pub enum DecodeError {
    Json(serde_json::Error),
    UnsupportedFormat(u32),
    BadPath(key::PathError),
    KeyMismatch {
        key: String,
        path: String,
    },
    HashMismatch,
    MissingContent,
    TombstoneWithContent,
    /// `blob` together with `content`, on a tombstone, or inconsistent.
    BadBlob,
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
            DecodeError::BadBlob => f.write_str("bad blob reference"),
        }
    }
}

impl std::error::Error for DecodeError {}

fn sha256_hex_digest(d: sha2::digest::Output<Sha256>) -> String {
    let mut s = String::with_capacity(64);
    for b in d {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

/// Lower-case hex SHA-256.
pub fn sha256_hex(bytes: &[u8]) -> String {
    sha256_hex_digest(Sha256::digest(bytes))
}

/// SHA-256 of a reader, 64 KiB at a time (never holds the whole input).
pub fn sha256_reader(mut r: impl Read) -> io::Result<String> {
    let mut h = Sha256::new();
    let mut buf = [0u8; 65536];
    loop {
        let n = r.read(&mut buf)?;
        if n == 0 {
            break;
        }
        h.update(&buf[..n]);
    }
    Ok(sha256_hex_digest(h.finalize()))
}

/// SHA-256 of a file, streamed.
pub fn sha256_file(path: &Path) -> io::Result<String> {
    sha256_reader(std::fs::File::open(path)?)
}

/// 64 lower-case hex digits.
pub fn is_sha256_hex(s: &str) -> bool {
    s.len() == 64
        && s.bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
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
                writer: None,
                cause: None,
            },
            content: Some(content),
            conflict: None,
            blob: None,
        }
    }

    /// A blob-reference event (no inline content) for a file already hashed
    /// on disk. `size` is the file length; `chunk_size` is announced to
    /// receivers.
    #[allow(clippy::too_many_arguments)]
    pub fn blob_put(
        path: &str,
        sha256: String,
        size: u64,
        chunk_size: u64,
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
                sha256,
                prev_sha256,
                writer: None,
                cause: None,
            },
            content: None,
            conflict: None,
            blob: Some(BlobInfo::new(size, chunk_size)),
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
                writer: None,
                cause: None,
            },
            content: None,
            conflict: None,
            blob: None,
        }
    }

    /// Same event, labelled with `writer` and `cause`.
    pub fn by(mut self, writer: &str, cause: &str) -> Self {
        self.stamp.writer = Some(writer.to_string());
        self.stamp.cause = Some(cause.to_string());
        self
    }

    /// Bytes of the content (0 for a tombstone). Blob references report
    /// the referenced size.
    pub fn size(&self) -> u64 {
        match (&self.content, &self.blob) {
            (Some(c), _) => c.len() as u64,
            (None, Some(b)) => b.size,
            _ => 0,
        }
    }

    /// A reference whose bytes still have to be fetched.
    pub fn is_blob_ref(&self) -> bool {
        self.blob.is_some() && self.content.is_none()
    }

    /// The same state as a blob reference (content dropped), if the content
    /// is larger than `inline_max`. `None` = small enough to stay inline.
    pub fn to_blob_ref(&self, inline_max: u64, chunk_size: u64) -> Option<NoteEvent> {
        let c = self.content.as_ref()?;
        if (c.len() as u64) <= inline_max {
            return None;
        }
        Some(NoteEvent {
            format: self.format,
            stamp: self.stamp.clone(),
            content: None,
            conflict: self.conflict.clone(),
            blob: Some(BlobInfo::new(c.len() as u64, chunk_size)),
        })
    }

    /// Fill a blob reference with fetched bytes. Fails unless size and
    /// SHA-256 match the reference.
    pub fn with_content(&self, bytes: Vec<u8>) -> Result<NoteEvent, DecodeError> {
        let Some(b) = &self.blob else {
            return Err(DecodeError::BadBlob);
        };
        if bytes.len() as u64 != b.size || sha256_hex(&bytes) != self.stamp.sha256 {
            return Err(DecodeError::HashMismatch);
        }
        Ok(NoteEvent {
            format: self.format,
            stamp: self.stamp.clone(),
            content: Some(bytes),
            conflict: self.conflict.clone(),
            blob: None,
        })
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
        if let Some(b) = &ev.blob {
            if ev.content.is_some()
                || ev.stamp.deleted
                || !b.is_valid()
                || !is_sha256_hex(&ev.stamp.sha256)
            {
                return Err(DecodeError::BadBlob);
            }
            return Ok(ev);
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
    fn writer_and_cause_roundtrip_and_stay_out_of_ordering() {
        let e = NoteEvent::put("a.md", b"hi".to_vec(), "box", 1, None, 7)
            .by("box/galanonim", cause::LOCAL_EDIT);
        let v: serde_json::Value = serde_json::from_slice(&e.encode()).unwrap();
        assert_eq!(v["writer"], "box/galanonim");
        assert_eq!(v["cause"], "local-edit");
        assert!(v.get("conflict").is_none());
        let back = NoteEvent::decode(&e.encode(), None).unwrap();
        assert_eq!(back, e);
        let plain = NoteEvent::put("a.md", b"hi".to_vec(), "box", 1, None, 7);
        assert!(plain.stamp.same_state(&e.stamp));
        assert!(!plain.stamp.beats(&e.stamp) && !e.stamp.beats(&plain.stamp));
        // Field order on the wire (the Kotlin port encodes byte-identically).
        let s = String::from_utf8(e.encode()).unwrap();
        assert!(s.ends_with(
            r#""sha256":"8f434346648f6b96df89dda901c5176b10a6d83961dd3c1ac88b59b2dc327aa4","writer":"box/galanonim","cause":"local-edit","content":"aGk="}"#
        ), "{s}");
        let mut c = NoteEvent::put(
            "a (conflict s24 2026-11-18).md",
            b"x".to_vec(),
            "box",
            1,
            None,
            7,
        )
        .by("box/agent", cause::CONFLICT_COPY);
        c.conflict = Some(ConflictInfo {
            of: "a.md".into(),
            winner_sha256: "w".into(),
            winner_size: 1,
            winner_origin: "box".into(),
            loser_sha256: "l".into(),
            loser_size: 9,
            loser_origin: "s24".into(),
        });
        let back = NoteEvent::decode(&c.encode(), None).unwrap();
        assert_eq!(back, c);
        assert!(back.conflict.unwrap().kept_smaller());
    }

    #[test]
    fn decodes_events_from_older_builds() {
        // Exactly what a pre-writer build (d2537fc) put on the wire.
        let old = r#"{"format":1,"path":"Daily/2026-11-19.md","version":5,"ts_ms":1763589540000,"origin":"nitro5","deleted":false,"sha256":"8f434346648f6b96df89dda901c5176b10a6d83961dd3c1ac88b59b2dc327aa4","prev_sha256":"abc","content":"aGk="}"#;
        let e = NoteEvent::decode(old.as_bytes(), Some("vault/notes/Daily/2026-11-19.md")).unwrap();
        assert_eq!(e.stamp.writer, None);
        assert_eq!(e.stamp.cause, None);
        assert_eq!(e.conflict, None);
        assert_eq!((e.stamp.writer_str(), e.stamp.cause_str()), ("-", "-"));
        // And re-encoding an unlabelled event gives the old bytes back.
        assert_eq!(e.encode(), old.as_bytes());
        // Unknown future keys are ignored, like the Kotlin reader does.
        let newer = old.replace(r#""content""#, r#""future":{"x":1},"content""#);
        assert!(NoteEvent::decode(newer.as_bytes(), None).is_ok());
        // An old tombstone and an old index stamp.
        let t = r#"{"format":1,"path":"a.md","version":2,"ts_ms":1,"origin":"s24","deleted":true,"sha256":""}"#;
        assert!(NoteEvent::decode(t.as_bytes(), None).unwrap().stamp.deleted);
        let st: Stamp = serde_json::from_str(
            r#"{"path":"a.md","version":1,"ts_ms":1,"origin":"box","deleted":false,"sha256":"x"}"#,
        )
        .unwrap();
        assert_eq!(st.writer, None);
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

    #[test]
    fn blob_reference_roundtrip_and_validation() {
        let big: Vec<u8> = (0..3000u32).map(|i| i as u8).collect();
        let e = NoteEvent::put("Daily/p.jpg", big.clone(), "nitro5", 2, None, 9)
            .by("nitro5/external", cause::EXTERNAL_EDIT);
        assert!(
            e.to_blob_ref(3000, 4096).is_none(),
            "at the limit stays inline"
        );
        let r = e.to_blob_ref(1000, 4096).unwrap();
        assert!(r.is_blob_ref());
        assert_eq!(r.size(), 3000);
        let v: serde_json::Value = serde_json::from_slice(&r.encode()).unwrap();
        assert!(v.get("content").is_none());
        assert_eq!(v["blob"]["size"], 3000);
        assert_eq!(v["blob"]["chunk_size"], 4096);
        assert_eq!(v["blob"]["chunks"], 1);
        let wire = String::from_utf8(r.encode()).unwrap();
        assert!(
            wire.ends_with(
                r#""cause":"external-edit","blob":{"size":3000,"chunk_size":4096,"chunks":1}}"#
            ),
            "{wire}"
        );
        let back = NoteEvent::decode(&r.encode(), Some(&r.key().unwrap())).unwrap();
        assert_eq!(back, r);
        assert_eq!(back.with_content(big.clone()).unwrap(), e);
        assert!(matches!(
            back.with_content(vec![0; 3000]),
            Err(DecodeError::HashMismatch)
        ));
        assert!(matches!(
            back.with_content(big[..10].to_vec()),
            Err(DecodeError::HashMismatch)
        ));

        // What a build without `blob` sees: a live event without content,
        // rejected as MissingContent (dropped, nothing written).
        let mut old: serde_json::Value = serde_json::from_slice(&r.encode()).unwrap();
        old.as_object_mut().unwrap().remove("blob");
        assert!(matches!(
            NoteEvent::decode(old.to_string().as_bytes(), None),
            Err(DecodeError::MissingContent)
        ));
        // Inconsistent references are rejected.
        for (field, val) in [
            ("content", serde_json::json!("aGk=")),
            ("deleted", serde_json::json!(true)),
            ("sha256", serde_json::json!("abc")),
        ] {
            let mut v: serde_json::Value = serde_json::from_slice(&r.encode()).unwrap();
            v[field] = val;
            assert!(
                matches!(
                    NoteEvent::decode(v.to_string().as_bytes(), None),
                    Err(DecodeError::BadBlob)
                ),
                "{field}"
            );
        }
        let mut v: serde_json::Value = serde_json::from_slice(&r.encode()).unwrap();
        v["blob"]["chunks"] = 5.into();
        assert!(matches!(
            NoteEvent::decode(v.to_string().as_bytes(), None),
            Err(DecodeError::BadBlob)
        ));
        v["blob"]["chunks"] = 1.into();
        v["blob"]["chunk_size"] = 1.into();
        assert!(matches!(
            NoteEvent::decode(v.to_string().as_bytes(), None),
            Err(DecodeError::BadBlob)
        ));
    }
}
