use std::path::Path;

/// Line window. Overlap keeps a function that sits on the boundary in two chunks.
pub const CODE_WINDOW_LINES: usize = 80;
/// Stay inside EmbeddingGemma 2's 8K-token window, prefix included.
/// 6000 characters is about 1.5k–2k tokens of source.
pub const CODE_MAX_CHUNK_CHARS: usize = 6_000;
pub const CODE_OVERLAP_LINES: usize = 15;
const SNIPPET_CHARS: usize = 180;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CodeChunk {
    pub repo: String,
    pub path: String,
    pub start_line: u32,
    pub end_line: u32,
    pub language: String,
    pub symbol: Option<String>,
    pub text: String,
}

impl CodeChunk {
    /// Symbol when one was found, otherwise the file name.
    pub fn title(&self) -> &str {
        self.symbol
            .as_deref()
            .unwrap_or_else(|| file_name(&self.path))
    }
}

/// Official EmbeddingGemma code-retrieval query. The embedder must not add another prompt.
pub fn code_query_prefix(query: &str) -> String {
    format!("task: code retrieval | query: {}", query.trim())
}

/// Official EmbeddingGemma code-retrieval document. Indentation of `text` is kept.
pub fn code_document_prefix(title: &str, text: &str) -> String {
    let title = title.trim();
    let title = if title.is_empty() { "none" } else { title };
    let text = text.trim_end_matches(['\n', '\r']);
    format!("title: {title} | text: {text}")
}

pub fn language_of(path: &str) -> String {
    let name = file_name(path).to_ascii_lowercase();
    match name.as_str() {
        "dockerfile" => return "dockerfile".to_string(),
        "makefile" => return "makefile".to_string(),
        _ => {}
    }
    let ext = Path::new(path)
        .extension()
        .and_then(|ext| ext.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    match ext.as_str() {
        "rs" => "rust",
        "py" => "python",
        "ts" => "typescript",
        "tsx" => "tsx",
        "js" | "mjs" | "cjs" => "javascript",
        "jsx" => "jsx",
        "go" => "go",
        "java" => "java",
        "kt" | "kts" => "kotlin",
        "c" | "h" => "c",
        "cpp" | "cc" | "cxx" | "hpp" | "hh" => "cpp",
        "md" | "markdown" => "markdown",
        "toml" => "toml",
        "json" => "json",
        "yaml" | "yml" => "yaml",
        "sh" | "bash" => "shell",
        "html" | "htm" => "html",
        "css" => "css",
        "sql" => "sql",
        "" => "text",
        other => other,
    }
    .to_string()
}

pub fn chunk_code(repo: &str, path: &str, source: &str) -> Vec<CodeChunk> {
    let language = language_of(path);
    let lines: Vec<&str> = source.lines().collect();
    if lines.is_empty() {
        return Vec::new();
    }
    let mut chunks = Vec::new();
    let mut start = 0usize;
    while start < lines.len() {
        let mut end = (start + CODE_WINDOW_LINES).min(lines.len());
        while end > start + 1 && chars_of(&lines[start..end]) > CODE_MAX_CHUNK_CHARS {
            end -= 1;
        }
        if chars_of(&lines[start..end]) > CODE_MAX_CHUNK_CHARS {
            let line_no = u32::try_from(start + 1).unwrap_or(u32::MAX);
            for piece in split_chars(lines[start], CODE_MAX_CHUNK_CHARS) {
                push_chunk(&mut chunks, repo, path, line_no, line_no, &language, &piece);
            }
            start += 1;
            continue;
        }
        let text = lines[start..end].join("\n");
        let line_start = u32::try_from(start + 1).unwrap_or(u32::MAX);
        let line_end = u32::try_from(end).unwrap_or(u32::MAX);
        push_chunk(
            &mut chunks,
            repo,
            path,
            line_start,
            line_end,
            &language,
            &text,
        );
        if end >= lines.len() {
            break;
        }
        let span = end - start;
        let overlap = CODE_OVERLAP_LINES.min(span.saturating_sub(1));
        let next = end - overlap;
        start = if next <= start { end } else { next };
    }
    chunks
}

fn push_chunk(
    chunks: &mut Vec<CodeChunk>,
    repo: &str,
    path: &str,
    start_line: u32,
    end_line: u32,
    language: &str,
    text: &str,
) {
    if text.chars().all(char::is_whitespace) {
        return;
    }
    chunks.push(CodeChunk {
        repo: repo.to_string(),
        path: path.to_string(),
        start_line,
        end_line,
        language: language.to_string(),
        symbol: symbol_in(text, language),
        text: text.to_string(),
    });
}

fn chars_of(lines: &[&str]) -> usize {
    let breaks = lines.len().saturating_sub(1);
    lines.iter().map(|line| line.chars().count()).sum::<usize>() + breaks
}

fn split_chars(text: &str, max_chars: usize) -> Vec<String> {
    if max_chars == 0 {
        return Vec::new();
    }
    let mut out = Vec::new();
    let mut buf = String::new();
    let mut count = 0usize;
    for ch in text.chars() {
        if count == max_chars {
            out.push(std::mem::take(&mut buf));
            count = 0;
        }
        buf.push(ch);
        count += 1;
    }
    if !buf.is_empty() {
        out.push(buf);
    }
    out
}

pub(crate) fn snippet_text(text: &str) -> String {
    let trimmed = text.trim();
    let count = trimmed.chars().count();
    if count <= SNIPPET_CHARS {
        return trimmed.to_string();
    }
    trimmed.chars().take(SNIPPET_CHARS).collect()
}

fn file_name(path: &str) -> &str {
    Path::new(path)
        .file_name()
        .and_then(|name| name.to_str())
        .filter(|name| !name.is_empty())
        .unwrap_or(path)
}

fn symbol_in(text: &str, language: &str) -> Option<String> {
    for line in text.lines() {
        let found = match language {
            "rust" => rust_symbol(line),
            "python" => python_symbol(line),
            "go" => go_symbol(line),
            "java" | "kotlin" => java_symbol(line),
            "javascript" | "typescript" | "tsx" | "jsx" => js_symbol(line),
            _ => None,
        };
        if found.is_some() {
            return found;
        }
    }
    None
}

fn rust_symbol(line: &str) -> Option<String> {
    let mut rest = line.trim_start();
    loop {
        if let Some(after) = rest.strip_prefix("pub") {
            if let Some(after) = after.strip_prefix('(') {
                let close = after.find(')')?;
                rest = after[close + 1..].trim_start();
                continue;
            }
            if after.starts_with(|ch: char| ch.is_ascii_whitespace()) {
                rest = after.trim_start();
                continue;
            }
        }
        if let Some(after) = strip_word(rest, &["async", "const", "unsafe", "extern"]) {
            rest = after;
            continue;
        }
        break;
    }
    ident_after(rest, &["fn", "struct", "enum", "trait", "type", "mod"])
}

fn python_symbol(line: &str) -> Option<String> {
    let mut rest = line.trim_start();
    if let Some(after) = strip_word(rest, &["async"]) {
        rest = after;
    }
    ident_after(rest, &["def", "class"])
}

fn go_symbol(line: &str) -> Option<String> {
    let line = line.trim_start();
    let rest = line.strip_prefix("func")?;
    if !(rest.starts_with(|ch: char| ch.is_ascii_whitespace()) || rest.starts_with('(')) {
        return None;
    }
    let rest = rest.trim_start();
    if let Some(rest) = rest.strip_prefix('(') {
        let after = rest.split_once(')')?.1.trim_start();
        return take_ident(after);
    }
    take_ident(rest)
}

fn java_symbol(line: &str) -> Option<String> {
    let mut rest = line.trim_start();
    loop {
        if let Some(after) = strip_word(
            rest,
            &[
                "public",
                "private",
                "protected",
                "static",
                "final",
                "abstract",
                "sealed",
                "data",
                "open",
                "override",
                "suspend",
            ],
        ) {
            rest = after;
            continue;
        }
        break;
    }
    ident_after(rest, &["class", "interface", "fun", "object"])
}

fn js_symbol(line: &str) -> Option<String> {
    let mut rest = line.trim_start();
    if let Some(after) = strip_word(rest, &["export"]) {
        rest = after;
        if let Some(after) = strip_word(rest, &["default"]) {
            rest = after;
        }
    }
    if let Some(after) = strip_word(rest, &["async"]) {
        rest = after;
    }
    ident_after(rest, &["function", "class"])
}

fn strip_word<'a>(line: &'a str, words: &[&str]) -> Option<&'a str> {
    for word in words {
        if let Some(after) = line.strip_prefix(word) {
            if after.starts_with(|ch: char| ch.is_ascii_whitespace()) {
                return Some(after.trim_start());
            }
        }
    }
    None
}

fn ident_after(line: &str, keywords: &[&str]) -> Option<String> {
    for keyword in keywords {
        if let Some(after) = line.strip_prefix(keyword) {
            if after.starts_with(|ch: char| ch.is_ascii_whitespace()) {
                return take_ident(after.trim_start());
            }
        }
    }
    None
}

fn take_ident(text: &str) -> Option<String> {
    let mut chars = text.chars();
    let first = chars.next()?;
    if !(first.is_ascii_alphabetic() || first == '_') {
        return None;
    }
    let mut name = String::new();
    name.push(first);
    for ch in chars {
        if ch.is_ascii_alphanumeric() || ch == '_' {
            name.push(ch);
        } else {
            break;
        }
    }
    Some(name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefixes_are_code_retrieval() {
        assert_eq!(
            code_query_prefix("  git ls-files  "),
            "task: code retrieval | query: git ls-files"
        );
        assert_eq!(
            code_document_prefix("indexable_path", "fn indexable_path() {}"),
            "title: indexable_path | text: fn indexable_path() {}"
        );
        assert_eq!(code_document_prefix("  ", "x"), "title: none | text: x");
        assert!(code_document_prefix("a.rs", "    let x = 1;\n").contains("    let x = 1;"));
    }

    #[test]
    fn windows_overlap_and_keep_line_numbers() {
        let mut lines = Vec::new();
        for n in 1..=100 {
            lines.push(format!("line {n}"));
        }
        let source = lines.join("\n");
        let chunks = chunk_code("VaultSearch", "src/main.rs", &source);
        assert!(chunks.len() >= 2);
        assert_eq!(chunks[0].start_line, 1);
        assert_eq!(chunks[0].end_line, CODE_WINDOW_LINES as u32);
        assert_eq!(chunks[0].language, "rust");
        assert!(chunks[1].start_line > 1);
        assert!(chunks[1].start_line <= chunks[0].end_line);
        assert_eq!(chunks.last().unwrap().end_line, 100);
        assert!(chunks
            .iter()
            .all(|chunk| chunk.text.chars().count() <= CODE_MAX_CHUNK_CHARS));
    }

    #[test]
    fn long_line_splits_inside_the_char_budget() {
        let source = "a".repeat(CODE_MAX_CHUNK_CHARS + 50);
        let chunks = chunk_code("demo", "src/big.py", &source);
        assert!(chunks.len() >= 2);
        assert!(chunks
            .iter()
            .all(|chunk| chunk.text.chars().count() <= CODE_MAX_CHUNK_CHARS));
        assert!(chunks.iter().all(|chunk| chunk.start_line == 1));
        assert_eq!(chunks[0].language, "python");
    }

    #[test]
    fn symbol_is_the_title_when_present() {
        let source = "fn helper() {}\npub fn answer() -> i32 { 27 }\n";
        let chunks = chunk_code("VaultSearch", "src/lib.rs", source);
        assert_eq!(chunks.len(), 1);
        assert_eq!(chunks[0].symbol.as_deref(), Some("helper"));
        assert_eq!(chunks[0].title(), "helper");
        assert_eq!(chunks[0].start_line, 1);
        assert_eq!(chunks[0].end_line, 2);
    }

    #[test]
    fn filename_is_the_title_without_a_symbol() {
        let chunks = chunk_code("docs", "README.md", "# Hello\n");
        assert_eq!(chunks[0].symbol, None);
        assert_eq!(chunks[0].title(), "README.md");
        assert_eq!(chunks[0].language, "markdown");
    }
}
