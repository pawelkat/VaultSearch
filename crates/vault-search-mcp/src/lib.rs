use std::path::Path;

use serde_json::{json, Value};
use vault_search::{limit_model_text, roundtrip, IpcRequest, MODEL_RESULT_CHARS};

const PROTOCOL: &str = "2024-11-05";

/// One NDJSON line from llama-server. Notifications return None.
pub fn dispatch(line: &str, socket: &Path) -> Result<Option<String>, String> {
    let msg: Value = serde_json::from_str(line).map_err(|e| e.to_string())?;
    let method = msg.get("method").and_then(Value::as_str).unwrap_or("");
    let id = msg.get("id").cloned();
    if id.is_none() {
        return Ok(None);
    }
    let response = match method {
        "initialize" => json!({
            "protocolVersion": PROTOCOL,
            "capabilities": {"tools": {}},
            "serverInfo": {"name": "vault-search", "version": "0.1.0"}
        }),
        "tools/list" => json!({
            "tools": [{
                "name": "search",
                "description": "Search the Obsidian vault by meaning and return a few short excerpts. The reply is capped at about 1000 tokens so it cannot fill the chat context. The vault root is fixed by the daemon. Do not pass a filesystem path.",
                "inputSchema": {
                    "type": "object",
                    "properties": {
                        "query": {"type": "string", "description": "Natural-language question"},
                        "k": {"type": "integer", "description": "How many notes to consider. Default 4, maximum 4. The text cap may return fewer."}
                    },
                    "required": ["query"]
                }
            }]
        }),
        "ping" => json!({}),
        "tools/call" => call_tool(&msg, socket)?,
        _ => {
            return Ok(Some(error_line(
                id,
                -32601,
                &format!("Method not found: {method}"),
            )));
        }
    };
    Ok(Some(result_line(id, response)))
}

fn call_tool(msg: &Value, socket: &Path) -> Result<Value, String> {
    let params = msg.get("params").cloned().unwrap_or_else(|| json!({}));
    let name = params.get("name").and_then(Value::as_str).unwrap_or("");
    if name != "search" {
        return Ok(json!({
            "content": [{"type": "text", "text": format!("Unknown tool: {name}")}],
            "isError": true
        }));
    }
    let args = params
        .get("arguments")
        .cloned()
        .unwrap_or_else(|| json!({}));
    let query = args
        .get("query")
        .and_then(Value::as_str)
        .unwrap_or("")
        .trim();
    if query.is_empty() {
        return Ok(json!({
            "content": [{"type": "text", "text": "query is empty"}],
            "isError": true
        }));
    }
    let k = Some(
        args.get("k")
            .and_then(Value::as_u64)
            .map(|n| n as u32)
            .unwrap_or(4)
            .clamp(1, 4),
    );
    match roundtrip(
        socket,
        &IpcRequest::Search {
            query: query.to_string(),
            k,
        },
    ) {
        Ok(response) if response.ok => {
            let text = limit_model_text(&response.text.unwrap_or_default(), MODEL_RESULT_CHARS);
            Ok(json!({
                "content": [{"type": "text", "text": text}],
                "isError": false
            }))
        }
        Ok(response) => Ok(json!({
            "content": [{"type": "text", "text": response.error.unwrap_or_else(|| "search failed".into())}],
            "isError": true
        })),
        Err(err) => Ok(json!({
            "content": [{"type": "text", "text": err}],
            "isError": true
        })),
    }
}

fn result_line(id: Option<Value>, result: Value) -> String {
    serde_json::json!({"jsonrpc": "2.0", "id": id, "result": result}).to_string()
}

fn error_line(id: Option<Value>, code: i32, message: &str) -> String {
    serde_json::json!({
        "jsonrpc": "2.0",
        "id": id,
        "error": {"code": code, "message": message}
    })
    .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader, Write};
    use std::os::unix::net::UnixListener;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn lists_search_and_forwards_a_hit() {
        let init = dispatch(
            r#"{"jsonrpc":"2.0","id":1,"method":"initialize"}"#,
            Path::new("/nope"),
        )
        .unwrap()
        .unwrap();
        assert!(init.contains("2024-11-05"));
        assert!(dispatch(
            r#"{"jsonrpc":"2.0","method":"notifications/initialized"}"#,
            Path::new("/nope")
        )
        .unwrap()
        .is_none());
        let listed = dispatch(
            r#"{"jsonrpc":"2.0","id":2,"method":"tools/list"}"#,
            Path::new("/nope"),
        )
        .unwrap()
        .unwrap();
        assert!(listed.contains("\"search\""));

        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let socket = std::env::temp_dir().join(format!("vault-search-test-{nanos}.sock"));
        let _ = std::fs::remove_file(&socket);
        let listener = UnixListener::bind(&socket).unwrap();
        let path = socket.clone();
        std::thread::spawn(move || {
            let (sock, _) = listener.accept().unwrap();
            let mut line = String::new();
            BufReader::new(&sock).read_line(&mut line).unwrap();
            assert!(line.contains("\"search\""));
            let mut out = sock.try_clone().unwrap();
            writeln!(
                out,
                r#"{{"ok":true,"text":"path: Notes/A.md\nscore: 0.900\nalpha\n"}}"#
            )
            .unwrap();
        });
        let response = dispatch(
            r#"{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"search","arguments":{"query":"alpha"}}}"#,
            &path,
        )
        .unwrap()
        .unwrap();
        assert!(response.contains("Notes/A.md"));
        assert!(!response.contains("\"isError\":true"));
        let _ = std::fs::remove_file(&socket);
    }
}
