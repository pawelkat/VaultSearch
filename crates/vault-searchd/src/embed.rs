use serde::Deserialize;

use vault_search::{fit_dim, Error};

/// Loopback HTTP client. Strings are already prefixed. The server must not add another prompt.
#[derive(Clone)]
pub struct HttpEmbedder {
    url: String,
    agent: ureq::Agent,
}

impl HttpEmbedder {
    pub fn new(url: &str) -> Result<Self, String> {
        assert_loopback(url)?;
        let agent = ureq::AgentBuilder::new()
            .timeout(std::time::Duration::from_secs(8))
            .build();
        Ok(Self {
            url: url.to_string(),
            agent,
        })
    }

    pub fn embed(&self, texts: &[String], dim: usize) -> Result<Vec<Vec<f32>>, String> {
        if texts.is_empty() {
            return Ok(Vec::new());
        }
        let response = self
            .agent
            .post(&self.url)
            .send_json(serde_json::json!({
                "input": texts,
                "encoding_format": "float",
            }))
            .map_err(|err| format!("embedder {} failed: {err}", self.url))?;
        let body: EmbedResponse = response
            .into_json()
            .map_err(|err| format!("embedder returned unreadable JSON: {err}"))?;
        let vectors = body.vectors().map_err(|err| err.to_string())?;
        if vectors.len() != texts.len() {
            return Err(format!(
                "embedder returned {} vectors for {} inputs",
                vectors.len(),
                texts.len()
            ));
        }
        vectors
            .into_iter()
            .map(|vector| fit_dim(vector, dim).map_err(|err| err.to_string()))
            .collect()
    }
}

pub fn assert_loopback(url: &str) -> Result<(), String> {
    let rest = url
        .strip_prefix("http://")
        .ok_or_else(|| "embed URL must be http://127.0.0.1 or http://localhost".to_string())?;
    let host = rest.split('/').next().unwrap_or("");
    let host = host.split(':').next().unwrap_or("");
    if host == "127.0.0.1" || host == "localhost" {
        Ok(())
    } else {
        Err(format!("embed URL host {host} is not loopback"))
    }
}

#[derive(Deserialize)]
struct EmbedResponse {
    data: Option<Vec<EmbedItem>>,
    vectors: Option<Vec<Vec<f32>>>,
}

#[derive(Deserialize)]
struct EmbedItem {
    index: Option<usize>,
    embedding: Vec<f32>,
}

impl EmbedResponse {
    fn vectors(self) -> Result<Vec<Vec<f32>>, Error> {
        if let Some(vectors) = self.vectors {
            return Ok(vectors);
        }
        let mut data = self.data.unwrap_or_default();
        data.sort_by_key(|item| item.index.unwrap_or(0));
        Ok(data.into_iter().map(|item| item.embedding).collect())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_a_public_embed_url() {
        assert!(assert_loopback("http://127.0.0.1:8081/v1/embeddings").is_ok());
        assert!(assert_loopback("https://example.com/v1/embeddings").is_err());
        assert!(assert_loopback("http://10.0.0.5:8081/v1/embeddings").is_err());
    }
}
