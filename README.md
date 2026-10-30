# VaultSearch

Read-only search over an Obsidian vault. One Rust library chunks notes, stores embedding vectors, and ranks them. Android calls that library through UniFFI. On the desktop, a long-lived daemon holds the index and a short-lived MCP process lets llama.cpp ask it one question.

The library does not load a model. The MCP process does not load a model. EmbeddingGemma 2 runs in a separate loopback HTTP server. llama.cpp on this machine stays the Qwen chat server.

```text
Android (LiteRT-LM)                         llama-server (Qwen)
        |  UniFFI                                  |  stdio, one call, then exit
        v                                          v
vault-search                                 vault-search-mcp
        ^                                          |
        |  in-process                              |  unix socket
        +------------------------------ vault-searchd
                                           HTTP embedder on 127.0.0.1
```

## What is shared

`vault-search` is the reusable crate.

| Function | Rule |
|---|---|
| `query_prefix` | `task: search result \| query: {text}` |
| `document_prefix` | `title: {heading or filename} \| text: {chunk}`, or `title: none` when the title is blank |
| `chunk_markdown` | Drop YAML frontmatter from the indexed text. Split on ATX headings (`#` plus a space). A section longer than 2048 characters splits again on whitespace. |
| `Index` | sqlite. Header stores dimension, chunker version, and prefix version. A changed mtime is re-embedded. Cosine is brute force. |

A `#` line inside a code fence is treated as a heading. Dot-directories, anything except `.md` and `.markdown`, a NUL byte, and a file larger than 1 MB are skipped.

The phone index and the PC index are separate files. VaultHome stores 256-dimensional vectors from LiteRT. This daemon defaults to 768. A longer vector is truncated and L2-normalized (Matryoshka). A shorter vector is rejected. Do not copy one database onto the other machine.

Keep the database outside the vault. The daemon refuses to open an index path inside the vault root, so bisync cannot upload it.

## Desktop

Build:

```bash
cargo test --workspace
cargo install --path crates/vault-searchd --root "$HOME/.local"
cargo install --path crates/vault-search-mcp --root "$HOME/.local"
```

The embedder listens on localhost and must not add its own task prefix. These strings are already prefixed. `contrib/embed_server.py` is an optional CPU FP32 server for EmbeddingGemma 2 text-only. It is not started by the build, and it downloads the model on first run:

```bash
python3 -m pip install -U sentence-transformers transformers torch
python3 contrib/embed_server.py
```

Then, in another terminal:

```bash
vault-searchd serve \
  --vault "$HOME/Obsidian/Vault" \
  --index "$HOME/.local/share/vault-search/index.db" \
  --embed-url http://127.0.0.1:8081/v1/embeddings \
  --dim 768
```

The socket is `$XDG_RUNTIME_DIR/vault-search.sock` (mode 0600). A user systemd unit is `contrib/vault-searchd.service`. It is not installed by the build.

Ask the daemon without a model in the middle:

```bash
vault-searchd query "what is still open today"
vault-searchd reindex          # incremental (changed mtimes only); waits up to 30 min
```

llama-server spawns MCP for one call and then stops it, so the model stays in the daemon. Name the server `vault` and the tool `search`. llama-server shows that pair as `vault_search`.

```bash
llama-server -m "$HOME/models/Qwen3.5-4B-Q4_K_M.gguf" \
  -ngl 99 -c 8192 --reasoning off \
  --host 127.0.0.1 --port 8080 \
  --mcp-servers-config contrib/mcp.json
```

Leave `--tools` off. This MCP server has no write and no shell tool, and the model cannot pass a filesystem path.

## Android

LiteRT-LM stays in Kotlin. It embeds text and passes `Vec<f32>` in. This crate chunks, plans, stores, and ranks.

```bash
cargo ndk -t arm64-v8a -t armeabi-v7a -o ./jniLibs \
  build -p vault-search-ffi --release
cargo run -p vault-search-ffi --bin uniffi-bindgen -- generate \
  --library target/aarch64-linux-android/release/libvault_search_ffi.so \
  --language kotlin --out-dir bindings/kotlin
```

The generated package is `com.pawelkat.vaultsearch`. `cargo-ndk` and the Android targets live on the Grok bot box, not in this repository.

## Layout

| Crate | Role |
|---|---|
| `vault-search` | Library. No model, no MCP, no file watcher. |
| `vault-search-ffi` | UniFFI `cdylib`. |
| `vault-searchd` | Daemon and `query` client. |
| `vault-search-mcp` | Line-delimited JSON-RPC, protocol `2024-11-05`. |
| `vault-rclone` | Vault-only wrapper around the `rclone` binary. |

## Vault sync (rclone)

`crates/vault-rclone` wraps the `rclone` binary for `gdrive:Vault` only. It follows the vault note DriveSync Replacement: one vault folder, no whole-Drive sync, no mount, and no tokens in output. Commands: `status`, `check-config`, `lsf`, and guarded `sync` / `resync` (refuse when either side has fewer than `MIN_FILES` files, always `--max-delete`, newer-wins with `*.conflict-…` losers kept, run log in `~/.local/state/vault-rclone/sync.log`). After a successful, non-dry-run `sync` or `resync` it asks a running `vault-searchd` for an incremental reindex over the Unix socket (`{"op":"reindex"}`); if the daemon is not running or the reindex fails, that is logged and the sync still exits 0. The bash `vault-sync` stays in production until this has survived real edits; the parity checklist against it is in [crates/vault-rclone/README.md](crates/vault-rclone/README.md).

```bash
cargo run -p vault-rclone -- status
cargo run -p vault-rclone -- sync --dry-run
```
