/// Query string sent to EmbeddingGemma 2. The embedder must not add a second prompt.
pub fn query_prefix(query: &str) -> String {
    format!("task: search result | query: {}", query.trim())
}

/// Document string sent to EmbeddingGemma 2. Empty title becomes `title: none`.
pub fn document_prefix(title: &str, text: &str) -> String {
    let title = title.trim();
    let text = text.trim();
    if title.is_empty() {
        format!("title: none | text: {text}")
    } else {
        format!("title: {title} | text: {text}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefixes_match_vaulthome() {
        assert_eq!(
            query_prefix("  open loops  "),
            "task: search result | query: open loops"
        );
        assert_eq!(
            document_prefix("Daily", "hello"),
            "title: Daily | text: hello"
        );
        assert_eq!(document_prefix("  ", "x"), "title: none | text: x");
    }
}
