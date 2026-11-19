//! Transfer acknowledgements: after a device has a file on disk and has
//! verified its SHA-256, it publishes on `vault/ack/<sha256>/<device>`.
//! The hub subscribes to `vault/ack/**`, logs the result, and keeps the
//! receipts so `vault-events who` can list who confirmed the current
//! version.

use std::collections::BTreeMap;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use zenoh::qos::CongestionControl;
use zenoh::sample::Locality;
use zenoh::Session;

use crate::key::PathError;
use crate::net::{self, BoxError};

/// Prefix of every transfer-ack key.
pub const ACK_PREFIX: &str = "vault/ack";
/// Wildcard the hub subscribes to.
pub const ACK_WILDCARD: &str = "vault/ack/**";
/// Persisted receipts under the hub data dir.
pub const FILE: &str = "acks.json";

/// `ok` or `fail` on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AckStatus {
    Ok,
    Fail,
}

impl AckStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            AckStatus::Ok => "ok",
            AckStatus::Fail => "fail",
        }
    }
}

/// Wire body of `vault/ack/<sha256>/<device>`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TransferAck {
    pub status: AckStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub path: String,
    pub bytes: u64,
    pub duration_ms: u64,
}

impl TransferAck {
    pub fn ok(path: impl Into<String>, bytes: u64, duration_ms: u64) -> Self {
        TransferAck {
            status: AckStatus::Ok,
            reason: None,
            path: path.into(),
            bytes,
            duration_ms,
        }
    }

    pub fn fail(
        path: impl Into<String>,
        bytes: u64,
        duration_ms: u64,
        reason: impl Into<String>,
    ) -> Self {
        TransferAck {
            status: AckStatus::Fail,
            reason: Some(reason.into()),
            path: path.into(),
            bytes,
            duration_ms,
        }
    }

    pub fn encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }
}

/// One stored receipt (wire body plus who and when the hub saw it).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct AckReceipt {
    pub device: String,
    pub status: AckStatus,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
    pub path: String,
    pub bytes: u64,
    pub duration_ms: u64,
    pub received_ms: u64,
}

impl AckReceipt {
    pub fn from_ack(device: impl Into<String>, ack: &TransferAck, received_ms: u64) -> Self {
        AckReceipt {
            device: device.into(),
            status: ack.status,
            reason: ack.reason.clone(),
            path: ack.path.clone(),
            bytes: ack.bytes,
            duration_ms: ack.duration_ms,
            received_ms,
        }
    }
}

/// Lower-case 64-char hex SHA-256, or `None`.
pub fn normalize_sha256(s: &str) -> Option<String> {
    if s.len() == 64 && s.bytes().all(|b| b.is_ascii_hexdigit()) {
        Some(s.to_ascii_lowercase())
    } else {
        None
    }
}

/// `vault/ack/<sha256>/<device>`
pub fn ack_key(sha256: &str, device: &str) -> Result<String, PathError> {
    let sha = normalize_sha256(sha256).ok_or(PathError::BadEscape)?;
    let dev = net::clean_device(Some(device));
    Ok(format!("{ACK_PREFIX}/{sha}/{dev}"))
}

/// Inverse of [`ack_key`]: `(sha256, device)`.
pub fn parse_ack_key(key: &str) -> Option<(String, String)> {
    let rest = key.strip_prefix(ACK_PREFIX)?.strip_prefix('/')?;
    let (sha, dev) = rest.split_once('/')?;
    let sha = normalize_sha256(sha)?;
    let dev = net::clean_device(Some(dev));
    (dev != net::UNKNOWN_REQUESTER && !dev.contains('/')).then_some((sha, dev))
}

/// `0.4s`, `1.2s`, `0.0s`.
pub fn fmt_secs(duration_ms: u64) -> String {
    format!("{:.1}s", duration_ms as f64 / 1000.0)
}

/// Hub log line: `<device> received <path> (<bytes>) ok in X.Xs` or
/// `<device> received <path> (<bytes>) fail: <reason>`.
pub fn log_line(device: &str, ack: &TransferAck) -> String {
    match ack.status {
        AckStatus::Ok => format!(
            "{device} received {} ({}) ok in {}",
            ack.path,
            ack.bytes,
            fmt_secs(ack.duration_ms)
        ),
        AckStatus::Fail => format!(
            "{device} received {} ({}) fail: {}",
            ack.path,
            ack.bytes,
            ack.reason.as_deref().unwrap_or("-")
        ),
    }
}

/// Publish one ack. Destination is other sessions only (the sender does not
/// echo it back).
pub async fn publish(
    session: &Session,
    device: &str,
    sha256: &str,
    ack: &TransferAck,
) -> Result<(), BoxError> {
    crate::key::validate_rel(&ack.path).map_err(|e| format!("ack path: {e}"))?;
    let k = ack_key(sha256, device)?;
    session
        .put(k, ack.encode()?)
        .congestion_control(CongestionControl::Block)
        .allowed_destination(Locality::Remote)
        .await?;
    Ok(())
}

/// Persisted ok/fail receipts, keyed by content SHA-256 then device.
#[derive(Debug, Default)]
pub struct AckStore {
    file: PathBuf,
    by_sha: BTreeMap<String, BTreeMap<String, AckReceipt>>,
}

impl AckStore {
    pub fn open(data_dir: &Path) -> io::Result<Self> {
        let file = data_dir.join(FILE);
        let by_sha = match fs::read(&file) {
            Ok(b) => match serde_json::from_slice(&b) {
                Ok(m) => m,
                Err(e) => {
                    tracing::warn!(
                        "acks file {} unreadable ({e}); starting empty",
                        file.display()
                    );
                    BTreeMap::new()
                }
            },
            Err(e) if e.kind() == io::ErrorKind::NotFound => BTreeMap::new(),
            Err(e) => return Err(e),
        };
        Ok(AckStore { file, by_sha })
    }

    pub fn file(&self) -> &Path {
        &self.file
    }

    pub fn record(&mut self, sha256: &str, receipt: AckReceipt) -> io::Result<()> {
        let Some(sha) = normalize_sha256(sha256) else {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "bad sha256"));
        };
        let dev = receipt.device.clone();
        self.by_sha.entry(sha).or_default().insert(dev, receipt);
        self.save()
    }

    /// Ok receipts for this content hash, sorted by device.
    pub fn confirmed(&self, sha256: &str) -> Vec<AckReceipt> {
        let Some(sha) = normalize_sha256(sha256) else {
            return Vec::new();
        };
        self.by_sha
            .get(&sha)
            .into_iter()
            .flat_map(|m| m.values())
            .filter(|r| r.status == AckStatus::Ok)
            .cloned()
            .collect()
    }

    /// Every receipt for this hash (ok and fail).
    pub fn all(&self, sha256: &str) -> Vec<AckReceipt> {
        let Some(sha) = normalize_sha256(sha256) else {
            return Vec::new();
        };
        self.by_sha
            .get(&sha)
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default()
    }

    fn save(&self) -> io::Result<()> {
        if let Some(d) = self.file.parent() {
            fs::create_dir_all(d)?;
        }
        let tmp = self.file.with_extension("json.tmp");
        fs::write(&tmp, serde_json::to_vec_pretty(&self.by_sha)?)?;
        fs::rename(&tmp, &self.file)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_round_trip_and_reject_junk() {
        let sha = "ab".repeat(32);
        assert_eq!(sha.len(), 64);
        let k = ack_key(&sha, "towerx").unwrap();
        assert_eq!(k, format!("vault/ack/{sha}/towerx"));
        assert_eq!(parse_ack_key(&k), Some((sha.clone(), "towerx".to_string())));
        // Upper-case hex is canonicalized.
        let k2 = ack_key(&sha.to_ascii_uppercase(), "Box").unwrap();
        assert_eq!(parse_ack_key(&k2), Some((sha, "Box".to_string())));
        assert_eq!(parse_ack_key("vault/notes/x"), None);
        assert_eq!(parse_ack_key("vault/ack/notasha/box"), None);
        assert_eq!(
            parse_ack_key(&format!("vault/ack/{}/a/b", "ab".repeat(32))),
            None
        );
        assert!(ack_key("abcd", "box").is_err());
        assert_eq!(
            ack_key(&"ab".repeat(32), "bad name").unwrap(),
            format!("vault/ack/{}/unknown", "ab".repeat(32))
        );
    }

    #[test]
    fn log_line_ok_and_fail() {
        let ok = TransferAck::ok("Notes/x.md", 123, 1400);
        assert_eq!(
            log_line("box", &ok),
            "box received Notes/x.md (123) ok in 1.4s"
        );
        let fail = TransferAck::fail("Notes/x.md", 123, 500, "sha256 mismatch");
        assert_eq!(
            log_line("s24", &fail),
            "s24 received Notes/x.md (123) fail: sha256 mismatch"
        );
        assert_eq!(fmt_secs(0), "0.0s");
        assert_eq!(fmt_secs(50), "0.1s");
    }

    #[test]
    fn wire_round_trip() {
        let ack = TransferAck::ok("Daily/a.md", 4, 12);
        let v: TransferAck = serde_json::from_slice(&ack.encode().unwrap()).unwrap();
        assert_eq!(v, ack);
        let raw =
            br#"{"status":"fail","reason":"timeout","path":"a.bin","bytes":10,"duration_ms":2000}"#;
        let f: TransferAck = serde_json::from_slice(raw).unwrap();
        assert_eq!(f.status, AckStatus::Fail);
        assert_eq!(f.reason.as_deref(), Some("timeout"));
        assert_eq!(f.bytes, 10);
    }

    #[test]
    fn store_records_confirms_and_survives_reload() {
        let t = tempfile::tempdir().unwrap();
        let sha = "cd".repeat(32);
        let other = "ef".repeat(32);
        let mut s = AckStore::open(t.path()).unwrap();
        assert!(s.confirmed(&sha).is_empty());
        s.record(
            &sha,
            AckReceipt::from_ack("s24", &TransferAck::ok("Notes/a.md", 8, 400), 100),
        )
        .unwrap();
        s.record(
            &sha,
            AckReceipt::from_ack("box", &TransferAck::ok("Notes/a.md", 8, 10), 110),
        )
        .unwrap();
        s.record(
            &sha,
            AckReceipt::from_ack(
                "towerx",
                &TransferAck::fail("Notes/a.md", 8, 900, "timeout"),
                120,
            ),
        )
        .unwrap();
        s.record(
            &other,
            AckReceipt::from_ack("s24", &TransferAck::ok("Notes/b.md", 1, 1), 130),
        )
        .unwrap();
        // A later ok from the same device overwrites the fail.
        s.record(
            &sha,
            AckReceipt::from_ack("towerx", &TransferAck::ok("Notes/a.md", 8, 1500), 200),
        )
        .unwrap();
        let got: Vec<_> = s.confirmed(&sha).into_iter().map(|r| r.device).collect();
        assert_eq!(got, vec!["box", "s24", "towerx"]);
        assert_eq!(s.confirmed(&sha)[2].duration_ms, 1500);
        drop(s);
        let s = AckStore::open(t.path()).unwrap();
        let got: Vec<_> = s.confirmed(&sha).into_iter().map(|r| r.device).collect();
        assert_eq!(got, vec!["box", "s24", "towerx"]);
        assert_eq!(s.confirmed(&other).len(), 1);
    }
}
