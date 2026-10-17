use std::path::{Component, Path};

/// Secondary split used by the VaultHome APK. The standing note's "~512 tokens"
/// is not what the phone shipped.
pub const MAX_CHUNK_CHARS: usize = 2048;
pub const MAX_FILE_BYTES: usize = 1_000_000;
pub const SNIPPET_CHARS: usize = 180;
pub const CHUNKER_VERSION: &str = "1";
pub const PREFIX_VERSION: &str = "1";

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Chunk {
    pub title: String,
    pub heading: String,
    pub text: String,
}

pub fn indexable_path(rel: &str) -> bool {
    if rel.is_empty() || rel.starts_with('/') || rel.contains('\\') {
        return false;
    }
    let path = Path::new(rel);
    let mut saw_file = false;
    for c in path.components() {
        match c {
            Component::Normal(s) => {
                let s = s.to_string_lossy();
                if s.is_empty() || s.starts_with('.') {
                    return false;
                }
                saw_file = true;
            }
            Component::CurDir => {}
            Component::ParentDir | Component::RootDir | Component::Prefix(_) => return false,
        }
    }
    if !saw_file {
        return false;
    }
    matches!(
        path.extension().and_then(|e| e.to_str()),
        Some("md" | "markdown")
    )
}

pub fn indexable_bytes(bytes: &[u8]) -> bool {
    bytes.len() <= MAX_FILE_BYTES && !bytes.contains(&0)
}

pub fn filename_stem(rel: &str) -> String {
    Path::new(rel)
        .file_stem()
        .map(|s| s.to_string_lossy().into_owned())
        .filter(|s| !s.trim().is_empty())
        .unwrap_or_else(|| "none".to_string())
}

pub fn chunk_markdown(rel_path: &str, source: &str) -> Vec<Chunk> {
    let body = strip_frontmatter(source);
    let stem = filename_stem(rel_path);
    let mut chunks = Vec::new();
    for section in sections(body, &stem) {
        for text in split_long(&section.text, MAX_CHUNK_CHARS) {
            chunks.push(Chunk {
                title: section.title.clone(),
                heading: section.heading.clone(),
                text,
            });
        }
    }
    chunks
}

pub fn snippet(text: &str) -> String {
    let trimmed = text.trim();
    let count = trimmed.chars().count();
    if count <= SNIPPET_CHARS {
        return trimmed.to_string();
    }
    trimmed.chars().take(SNIPPET_CHARS).collect()
}

fn strip_frontmatter(source: &str) -> &str {
    let source = source.strip_prefix('\u{feff}').unwrap_or(source);
    if !(source.starts_with("---\n") || source.starts_with("---\r\n")) {
        return source;
    }
    let mut offset = 0;
    let mut lines = source.split_inclusive('\n');
    if let Some(first) = lines.next() {
        offset += first.len();
    }
    for line in lines {
        let marker = line.trim_end_matches(['\n', '\r']);
        offset += line.len();
        if marker == "---" {
            return &source[offset..];
        }
    }
    source
}

struct Section {
    title: String,
    heading: String,
    text: String,
}

fn sections(body: &str, stem: &str) -> Vec<Section> {
    let mut title = stem.to_string();
    let mut heading = String::new();
    let mut buf = String::new();
    let mut out = Vec::new();
    let flush = |title: &str, heading: &str, buf: &mut String, out: &mut Vec<Section>| {
        let text = buf.trim();
        if !text.is_empty() {
            out.push(Section {
                title: title.to_string(),
                heading: heading.to_string(),
                text: text.to_string(),
            });
        }
        buf.clear();
    };
    for line in body.split_inclusive('\n') {
        let raw = line.trim_end_matches(['\n', '\r']);
        if let Some(h) = parse_heading(raw) {
            flush(&title, &heading, &mut buf, &mut out);
            title = h.clone();
            heading = h;
        } else {
            buf.push_str(line);
        }
    }
    flush(&title, &heading, &mut buf, &mut out);
    out
}

fn parse_heading(line: &str) -> Option<String> {
    let bytes = line.as_bytes();
    let mut hashes = 0;
    while hashes < bytes.len() && hashes < 6 && bytes[hashes] == b'#' {
        hashes += 1;
    }
    if hashes == 0 {
        return None;
    }
    if bytes.get(hashes) != Some(&b' ') && bytes.get(hashes) != Some(&b'\t') {
        return None;
    }
    let title = line[hashes..].trim();
    if title.is_empty() {
        None
    } else {
        Some(title.to_string())
    }
}

fn split_long(text: &str, max_chars: usize) -> Vec<String> {
    let mut rest = text.trim();
    if rest.is_empty() {
        return Vec::new();
    }
    if rest.chars().count() <= max_chars {
        return vec![rest.to_string()];
    }
    let mut out = Vec::new();
    while rest.chars().count() > max_chars {
        let mut end = 0;
        let mut split_at = None;
        for (seen, (byte_idx, ch)) in rest.char_indices().enumerate() {
            if seen == max_chars {
                break;
            }
            end = byte_idx + ch.len_utf8();
            if ch.is_whitespace() {
                split_at = Some(end);
            }
        }
        let cut = match split_at {
            Some(at) if at > 0 => at,
            _ => end.max(1),
        };
        let piece = rest[..cut].trim();
        if !piece.is_empty() {
            out.push(piece.to_string());
        }
        rest = rest[cut..].trim_start();
        if cut == 0 {
            break;
        }
    }
    if !rest.is_empty() {
        out.push(rest.to_string());
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn drops_frontmatter_and_splits_headings() {
        let src = "---\ntitle: x\n---\nintro line\n\n# Open loops\n\n- one\n";
        let chunks = chunk_markdown("Daily/2026-10-17.md", src);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].title, "2026-10-17");
        assert_eq!(chunks[0].heading, "");
        assert_eq!(chunks[0].text, "intro line");
        assert_eq!(chunks[1].title, "Open loops");
        assert_eq!(chunks[1].heading, "Open loops");
        assert_eq!(chunks[1].text, "- one");
    }

    #[test]
    fn long_section_splits_on_whitespace() {
        let body = format!("{}\n{}", "a".repeat(100), "b".repeat(3000));
        let chunks = chunk_markdown("Notes/A.md", &body);
        assert!(chunks.len() >= 2);
        assert!(chunks
            .iter()
            .all(|c| c.text.chars().count() <= MAX_CHUNK_CHARS));
        assert_eq!(chunks[0].title, "A");
    }

    #[test]
    fn skips_hidden_and_non_markdown() {
        assert!(indexable_path("Notes/A.md"));
        assert!(indexable_path("Notes/A.markdown"));
        assert!(!indexable_path(".grok/skills/x.md"));
        assert!(!indexable_path("Notes/.secret.md"));
        assert!(!indexable_path("../Notes/A.md"));
        assert!(!indexable_path("/etc/passwd"));
        assert!(!indexable_path("Notes/A.txt"));
        assert!(!indexable_bytes(b"ok\0"));
        assert!(!indexable_bytes(&vec![b'a'; MAX_FILE_BYTES + 1]));
    }
}
