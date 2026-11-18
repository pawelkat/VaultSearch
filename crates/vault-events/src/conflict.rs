//! What to do with one incoming event, given what this device knows.
//!
//! Pure logic, no I/O, so every branch is unit-tested.
//!
//! Inputs:
//! - `state`: the last stamp this device accepted (applied or published)
//!   for the path, if any;
//! - `disk`: SHA-256 of the file on disk right now (`None` = absent);
//! - `incoming`: the event's stamp.
//!
//! Rules:
//! 1. Disk already equals the incoming state -> just record it (`Adopt`), or
//!    ignore if what we recorded is newer.
//! 2. Incoming does not beat the accepted state -> `Ignore` (stale, or our
//!    own echo).
//! 3. Disk is *dirty* (differs from the accepted state, i.e. an edit this
//!    device has not published yet): build the local candidate stamp
//!    (`state.version + 1`, origin = this device) and compare with
//!    [`Stamp::beats`]. The winner stays at / goes to the live path; the
//!    loser's bytes are saved as a conflict copy.
//! 4. Disk is clean: if the incoming event was based on what we have
//!    (`prev_sha256 == state.sha256`) it is a fast-forward -> `Apply`.
//!    Otherwise two devices edited the same base concurrently and the
//!    incoming one won the tie-break: our current bytes are saved as a
//!    conflict copy, then the incoming state is applied.
//!
//! Conflict copies are named from the *losing* stamp
//! (`<name> (conflict <origin> <YYYY-MM-DD>).<ext>`), so two devices that
//! resolve the same conflict produce the same file name and the same bytes;
//! the copy then syncs as an ordinary new note and the duplicates collapse.

use crate::event::Stamp;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Decision {
    /// Nothing to do.
    Ignore,
    /// Disk already holds the incoming state; record the stamp only.
    Adopt,
    /// Write (or delete) the live path from the incoming event.
    /// `save_local_as` = keep the current disk bytes under this conflict
    /// name first.
    Apply { save_local_as: Option<String> },
    /// Local unpublished edit wins: keep disk, publish `local` (a stamp
    /// with the disk sha), and save the incoming bytes under
    /// `save_incoming_as` (when the incoming event has content).
    KeepLocal {
        local: Stamp,
        save_incoming_as: Option<String>,
    },
}

/// Decide. `me` is this device's id, `local_ts_ms` the mtime (or now) of the
/// dirty local file.
pub fn decide(
    state: Option<&Stamp>,
    disk: Option<&str>,
    incoming: &Stamp,
    me: &str,
    local_ts_ms: u64,
) -> Decision {
    let incoming_sha = if incoming.deleted {
        None
    } else {
        Some(incoming.sha256.as_str())
    };
    let newer = state.is_none_or(|s| incoming.beats(s));

    // 1. Disk already matches.
    if disk == incoming_sha {
        return if newer {
            Decision::Adopt
        } else {
            Decision::Ignore
        };
    }
    // 2. Stale or echo.
    if !newer {
        return Decision::Ignore;
    }
    let state_sha = state.and_then(|s| (!s.deleted).then_some(s.sha256.as_str()));
    let dirty = disk != state_sha;

    if dirty {
        // 3. Concurrent local edit (or an untracked local file).
        let local = Stamp {
            path: incoming.path.clone(),
            version: state.map_or(1, |s| s.version + 1),
            ts_ms: local_ts_ms,
            origin: me.to_string(),
            deleted: disk.is_none(),
            sha256: disk.unwrap_or("").to_string(),
            prev_sha256: state_sha.map(str::to_string),
        };
        if local.beats(incoming) {
            let save_incoming_as = (!incoming.deleted).then(|| conflict_name(incoming));
            return Decision::KeepLocal {
                local,
                save_incoming_as,
            };
        }
        let save_local_as = disk.is_some().then(|| conflict_name(&local));
        return Decision::Apply { save_local_as };
    }

    // 4. Clean.
    match state {
        None => Decision::Apply {
            save_local_as: None,
        },
        Some(s) if s.deleted => Decision::Apply {
            save_local_as: None,
        },
        Some(s) => {
            let fast_forward = incoming.prev_sha256.as_deref() == Some(s.sha256.as_str());
            if fast_forward || disk.is_none() {
                Decision::Apply {
                    save_local_as: None,
                }
            } else {
                Decision::Apply {
                    save_local_as: Some(conflict_name(s)),
                }
            }
        }
    }
}

/// Keep device ids filename-safe.
fn safe_device(d: &str) -> String {
    let s: String = d
        .chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '-' || c == '_' || c == '.' {
                c
            } else {
                '-'
            }
        })
        .collect();
    if s.is_empty() {
        "unknown".into()
    } else {
        s
    }
}

/// UTC calendar date for Unix milliseconds (proleptic Gregorian).
pub fn utc_date(ts_ms: u64) -> String {
    let days = (ts_ms / 1000 / 86_400) as i64;
    // Howard Hinnant's civil_from_days.
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}")
}

/// `Notes/a.md` + loser {origin: s24, ts 2026-11-18}
/// -> `Notes/a (conflict s24 2026-11-18).md`
pub fn conflict_name(loser: &Stamp) -> String {
    conflict_name_for(&loser.path, &loser.origin, loser.ts_ms)
}

pub fn conflict_name_for(path: &str, origin: &str, ts_ms: u64) -> String {
    let (dir, file) = match path.rfind('/') {
        Some(i) => (&path[..=i], &path[i + 1..]),
        None => ("", path),
    };
    let tag = format!("(conflict {} {})", safe_device(origin), utc_date(ts_ms));
    match file.rfind('.') {
        Some(i) if i > 0 => format!("{dir}{} {tag}{}", &file[..i], &file[i..]),
        _ => format!("{dir}{file} {tag}"),
    }
}

/// `true` for names produced by [`conflict_name_for`]; such files are synced
/// like any note but never get a conflict copy of their own nested again by
/// accident in logs/tests.
pub fn is_conflict_copy(path: &str) -> bool {
    path.contains(" (conflict ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{sha256_hex, NoteEvent};

    const DAY: u64 = 1_763_424_000_000; // 2025-11-18T00:00:00Z

    fn put(c: &str, origin: &str, v: u64, prev: Option<&str>, ts: u64) -> Stamp {
        NoteEvent::put(
            "Notes/a.md",
            c.as_bytes().to_vec(),
            origin,
            v,
            prev.map(sha256_hex_s),
            ts,
        )
        .stamp
    }
    fn sha256_hex_s(s: &str) -> String {
        sha256_hex(s.as_bytes())
    }

    #[test]
    fn date_and_names() {
        assert_eq!(utc_date(0), "1970-01-01");
        assert_eq!(utc_date(DAY), "2025-11-18");
        assert_eq!(utc_date(951_782_400_000), "2000-02-29");
        assert_eq!(
            conflict_name_for("Notes/a.md", "s24", DAY),
            "Notes/a (conflict s24 2025-11-18).md"
        );
        assert_eq!(
            conflict_name_for("README", "a/b", DAY),
            "README (conflict a-b 2025-11-18)"
        );
        assert_eq!(
            conflict_name_for(".hidden", "x", DAY),
            ".hidden (conflict x 2025-11-18)"
        );
        assert!(is_conflict_copy("Notes/a (conflict s24 2025-11-18).md"));
    }

    #[test]
    fn new_note_applies() {
        let inc = put("v1", "s24", 1, None, DAY);
        assert_eq!(
            decide(None, None, &inc, "box", DAY),
            Decision::Apply {
                save_local_as: None
            }
        );
    }

    #[test]
    fn echo_and_stale_are_ignored() {
        let mine = put("v2", "box", 2, Some("v1"), DAY);
        // our own event coming back: disk == content, state == stamp
        let h = sha256_hex_s("v2");
        assert_eq!(
            decide(Some(&mine), Some(&h), &mine, "box", DAY),
            Decision::Ignore
        );
        // an older version arriving late
        let old = put("v1", "s24", 1, None, DAY);
        assert_eq!(
            decide(Some(&mine), Some(&h), &old, "box", DAY),
            Decision::Ignore
        );
    }

    #[test]
    fn identical_content_is_adopted() {
        let st = put("v1", "box", 1, None, DAY);
        let inc = put("v2", "s24", 2, Some("v1"), DAY + 5);
        let h2 = sha256_hex_s("v2");
        assert_eq!(
            decide(Some(&st), Some(&h2), &inc, "box", DAY),
            Decision::Adopt
        );
    }

    #[test]
    fn fast_forward_applies() {
        let st = put("v1", "box", 1, None, DAY);
        let inc = put("v2", "s24", 2, Some("v1"), DAY + 5);
        let h1 = sha256_hex_s("v1");
        assert_eq!(
            decide(Some(&st), Some(&h1), &inc, "box", DAY),
            Decision::Apply {
                save_local_as: None
            }
        );
        let del = NoteEvent::delete("Notes/a.md", "s24", 2, Some(h1.clone()), DAY + 5).stamp;
        assert_eq!(
            decide(Some(&st), Some(&h1), &del, "box", DAY),
            Decision::Apply {
                save_local_as: None
            }
        );
    }

    #[test]
    fn concurrent_published_edit_keeps_ours_as_conflict() {
        // We published v2 "ours" on top of v1; s24 published v2 "theirs" on
        // top of v1 with a later clock, so theirs wins everywhere.
        let ours = put("ours", "box", 2, Some("v1"), DAY);
        let theirs = put("theirs", "s24", 2, Some("v1"), DAY + 1000);
        let h = sha256_hex_s("ours");
        assert_eq!(
            decide(Some(&ours), Some(&h), &theirs, "box", DAY),
            Decision::Apply {
                save_local_as: Some("Notes/a (conflict box 2025-11-18).md".into())
            }
        );
        // And on s24, our losing event is just stale.
        let ht = sha256_hex_s("theirs");
        assert_eq!(
            decide(Some(&theirs), Some(&ht), &ours, "s24", DAY),
            Decision::Ignore
        );
    }

    #[test]
    fn dirty_local_loses_to_newer_version() {
        let st = put("v1", "box", 1, None, DAY);
        let inc = put("remote", "s24", 3, Some("v2"), DAY);
        let dirty = sha256_hex_s("local edit");
        assert_eq!(
            decide(Some(&st), Some(&dirty), &inc, "box", DAY + 9),
            Decision::Apply {
                save_local_as: Some("Notes/a (conflict box 2025-11-18).md".into())
            }
        );
    }

    #[test]
    fn dirty_local_wins_tie_and_saves_incoming() {
        let st = put("v1", "box", 1, None, DAY);
        let inc = put("remote", "s24", 2, Some("v1"), DAY);
        let dirty = sha256_hex_s("local edit");
        match decide(Some(&st), Some(&dirty), &inc, "box", DAY + 60_000) {
            Decision::KeepLocal {
                local,
                save_incoming_as,
            } => {
                assert_eq!(local.version, 2);
                assert_eq!(local.origin, "box");
                assert_eq!(local.sha256, dirty);
                assert_eq!(
                    local.prev_sha256.as_deref(),
                    Some(sha256_hex_s("v1").as_str())
                );
                assert_eq!(
                    save_incoming_as.as_deref(),
                    Some("Notes/a (conflict s24 2025-11-18).md")
                );
            }
            d => panic!("{d:?}"),
        }
    }

    #[test]
    fn untracked_local_file_vs_incoming() {
        // First join: we have a file the hub also has, with other bytes.
        let inc = put("hub copy", "latitude", 1, None, DAY);
        let local = sha256_hex_s("box copy");
        // local mtime earlier -> incoming wins, ours saved
        assert_eq!(
            decide(None, Some(&local), &inc, "box", DAY - 1),
            Decision::Apply {
                save_local_as: Some("Notes/a (conflict box 2025-11-17).md".into())
            }
        );
        // local mtime later -> ours wins, theirs saved
        assert!(matches!(
            decide(None, Some(&local), &inc, "box", DAY + 1),
            Decision::KeepLocal {
                save_incoming_as: Some(_),
                ..
            }
        ));
    }

    #[test]
    fn deletes() {
        let h1 = sha256_hex_s("v1");
        let st = put("v1", "box", 1, None, DAY);
        // already gone locally and remote says deleted -> adopt
        let del = NoteEvent::delete("Notes/a.md", "s24", 2, Some(h1.clone()), DAY).stamp;
        assert_eq!(decide(Some(&st), None, &del, "box", DAY), Decision::Adopt);
        // local delete (unpublished) vs remote edit with higher version -> remote restored
        let inc = put("v3", "s24", 3, Some("v2"), DAY);
        assert_eq!(
            decide(Some(&st), None, &inc, "box", DAY),
            Decision::Apply {
                save_local_as: None
            }
        );
        // remote delete of a note we edited locally after v1 at a later time
        let del2 = NoteEvent::delete("Notes/a.md", "s24", 2, Some(h1), DAY).stamp;
        let dirty = sha256_hex_s("mine");
        assert!(matches!(
            decide(Some(&st), Some(&dirty), &del2, "box", DAY + 1),
            Decision::KeepLocal {
                save_incoming_as: None,
                ..
            }
        ));
        // tombstone state, new content arrives -> apply, nothing to save
        let tomb = NoteEvent::delete("Notes/a.md", "box", 2, None, DAY).stamp;
        let inc2 = put("again", "s24", 3, None, DAY);
        assert_eq!(
            decide(Some(&tomb), None, &inc2, "box", DAY),
            Decision::Apply {
                save_local_as: None
            }
        );
    }
}
