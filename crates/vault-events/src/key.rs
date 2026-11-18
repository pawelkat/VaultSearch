//! Vault path <-> Zenoh key expression mapping.
//!
//! Every note lives under `vault/notes/<path>`, where `<path>` is the
//! vault-relative path with `/` separators. Zenoh key expressions forbid
//! `#` and `?`, and give `*` and `$` wildcard meaning, so those bytes (and
//! `%`, the escape byte itself) are percent-encoded. Everything else,
//! including spaces and non-ASCII, is passed through unchanged so keys stay
//! readable in `z_get` / admin tools.

use std::path::{Component, Path};

/// Prefix of every note key.
pub const NOTES_PREFIX: &str = "vault/notes";
/// Wildcard over every note key.
pub const NOTES_WILDCARD: &str = "vault/notes/**";
/// Hub queryable that answers with the compact list of stored stamps.
pub const INDEX_KEY: &str = "vault/hub/index";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PathError {
    Empty,
    Absolute,
    ParentDir,
    EmptyComponent,
    NotUtf8,
    BadEscape,
    NotANoteKey,
}

impl std::fmt::Display for PathError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            PathError::Empty => "empty path",
            PathError::Absolute => "absolute path",
            PathError::ParentDir => "path contains '..' or '.'",
            PathError::EmptyComponent => "path has an empty component",
            PathError::NotUtf8 => "path is not valid UTF-8",
            PathError::BadEscape => "bad percent escape in key",
            PathError::NotANoteKey => "key is not under vault/notes/",
        };
        f.write_str(s)
    }
}

impl std::error::Error for PathError {}

/// Validate a vault-relative path string (`/`-separated) and return it
/// unchanged. Rejects absolute paths, `.`/`..`, empty components and
/// backslashes so a remote event can never escape the vault root.
pub fn validate_rel(path: &str) -> Result<&str, PathError> {
    if path.is_empty() {
        return Err(PathError::Empty);
    }
    if path.starts_with('/') || path.contains('\\') || path.contains('\0') {
        return Err(PathError::Absolute);
    }
    for c in path.split('/') {
        if c.is_empty() {
            return Err(PathError::EmptyComponent);
        }
        if c == "." || c == ".." {
            return Err(PathError::ParentDir);
        }
    }
    Ok(path)
}

/// Relative path (as stored in events) for `abs` under `root`.
pub fn rel_from_fs(root: &Path, abs: &Path) -> Result<String, PathError> {
    let rel = abs.strip_prefix(root).map_err(|_| PathError::Absolute)?;
    let mut parts = Vec::new();
    for c in rel.components() {
        match c {
            Component::Normal(s) => parts.push(s.to_str().ok_or(PathError::NotUtf8)?.to_string()),
            _ => return Err(PathError::ParentDir),
        }
    }
    if parts.is_empty() {
        return Err(PathError::Empty);
    }
    let joined = parts.join("/");
    validate_rel(&joined)?;
    Ok(joined)
}

fn needs_escape(b: u8) -> bool {
    matches!(b, b'%' | b'#' | b'?' | b'*' | b'$')
}

/// `Notes/A #1.md` -> `vault/notes/Notes/A %231.md`
pub fn path_to_key(path: &str) -> Result<String, PathError> {
    validate_rel(path)?;
    let mut out = String::with_capacity(NOTES_PREFIX.len() + 1 + path.len());
    out.push_str(NOTES_PREFIX);
    out.push('/');
    for ch in path.chars() {
        if ch.is_ascii() && needs_escape(ch as u8) {
            out.push_str(&format!("%{:02X}", ch as u8));
        } else {
            out.push(ch);
        }
    }
    Ok(out)
}

/// Inverse of [`path_to_key`].
pub fn key_to_path(key: &str) -> Result<String, PathError> {
    let rest = key
        .strip_prefix(NOTES_PREFIX)
        .and_then(|r| r.strip_prefix('/'))
        .ok_or(PathError::NotANoteKey)?;
    let bytes = rest.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' {
            let hex = bytes.get(i + 1..i + 3).ok_or(PathError::BadEscape)?;
            let s = std::str::from_utf8(hex).map_err(|_| PathError::BadEscape)?;
            let v = u8::from_str_radix(s, 16).map_err(|_| PathError::BadEscape)?;
            out.push(v);
            i += 3;
        } else {
            out.push(bytes[i]);
            i += 1;
        }
    }
    let s = String::from_utf8(out).map_err(|_| PathError::NotUtf8)?;
    validate_rel(&s)?;
    // Only the canonical encoding is accepted (no `%2F`, no lower-case or
    // needless escapes), so one path has exactly one key.
    if path_to_key(&s)? != key {
        return Err(PathError::BadEscape);
    }
    Ok(s)
}

#[cfg(test)]
mod tests {
    use super::*;
    use zenoh::key_expr::KeyExpr;

    #[test]
    fn roundtrip_and_valid_keyexpr() {
        for p in [
            "Notes/Vault Event Sync Plan.md",
            "Attachments /img 1.png",
            "Daily/2026-11-18.md",
            "Notes/C# and $HOME ?*%.md",
            "Notes/Zürich ✓.md",
            "a",
        ] {
            let k = path_to_key(p).unwrap();
            KeyExpr::try_from(k.clone()).unwrap_or_else(|e| panic!("{k}: {e}"));
            assert!(!k[NOTES_PREFIX.len()..].contains(['#', '?', '*', '$']));
            assert_eq!(key_to_path(&k).unwrap(), p);
        }
    }

    #[test]
    fn rejects_escapes() {
        assert_eq!(path_to_key("../x.md"), Err(PathError::ParentDir));
        assert_eq!(path_to_key("/etc/passwd"), Err(PathError::Absolute));
        assert_eq!(path_to_key("a//b"), Err(PathError::EmptyComponent));
        assert_eq!(path_to_key("a/./b"), Err(PathError::ParentDir));
        assert_eq!(path_to_key("a\\b"), Err(PathError::Absolute));
        assert_eq!(key_to_path("vault/notes/..%2Fx"), Err(PathError::ParentDir));
        assert_eq!(
            key_to_path("vault/notes/a/%2E%2E/b"),
            Err(PathError::ParentDir)
        );
        assert_eq!(key_to_path("vault/other/x"), Err(PathError::NotANoteKey));
        assert_eq!(key_to_path("vault/notes/x%2"), Err(PathError::BadEscape));
        assert_eq!(key_to_path("vault/notes/a%2Fb"), Err(PathError::BadEscape));
        assert_eq!(key_to_path("vault/notes/%61"), Err(PathError::BadEscape));
    }

    #[test]
    fn rel_from_fs_works() {
        let root = Path::new("/v");
        assert_eq!(
            rel_from_fs(root, Path::new("/v/Notes/a.md")).unwrap(),
            "Notes/a.md"
        );
        assert!(rel_from_fs(root, Path::new("/w/a.md")).is_err());
        assert!(rel_from_fs(root, Path::new("/v")).is_err());
    }
}
