use std::io::{BufRead, Write};
use std::path::PathBuf;

use clap::Parser;

use vault_search_mcp::dispatch;

#[derive(Parser)]
#[command(
    name = "vault-search-mcp",
    about = "stdio MCP tool `search`. llama-server exposes it as vault_search when the server name is vault."
)]
struct Cli {
    /// Unix socket of vault-searchd.
    #[arg(long, env = "VAULT_SEARCH_SOCKET")]
    socket: Option<PathBuf>,
}

fn main() {
    let cli = Cli::parse();
    let socket = cli.socket.unwrap_or_else(|| {
        if let Some(dir) = std::env::var_os("XDG_RUNTIME_DIR") {
            return PathBuf::from(dir).join("vault-search.sock");
        }
        eprintln!("vault-search-mcp: pass --socket or set XDG_RUNTIME_DIR");
        std::process::exit(2);
    });
    let stdin = std::io::stdin();
    let mut stdout = std::io::stdout();
    for line in stdin.lock().lines() {
        let line = match line {
            Ok(line) => line,
            Err(err) => {
                eprintln!("vault-search-mcp: stdin: {err}");
                break;
            }
        };
        if line.trim().is_empty() {
            continue;
        }
        match dispatch(&line, &socket) {
            Ok(Some(response)) => {
                if writeln!(stdout, "{response}").is_err() || stdout.flush().is_err() {
                    break;
                }
            }
            Ok(None) => {}
            Err(err) => {
                eprintln!("vault-search-mcp: {err}");
                let _ = writeln!(
                    stdout,
                    r#"{{"jsonrpc":"2.0","id":null,"error":{{"code":-32700,"message":"parse error"}}}}"#
                );
            }
        }
    }
}
