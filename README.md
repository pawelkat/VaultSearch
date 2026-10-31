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
python3 -m pip install -U sentence-transformers transformers torch torchvision Pillow
python3 contrib/embed_server.py
```

`torchvision` and `Pillow` are needed even for text: loading the EmbeddingGemma 2 processor imports its image processor.

Then, in another terminal:

```bash
vault-searchd serve \
  --vault "$HOME/Obsidian/Vault" \
  --index "$HOME/.local/share/vault-search/index.db" \
  --embed-url http://127.0.0.1:8081/v1/embeddings \
  --dim 768
```

The socket is `$XDG_RUNTIME_DIR/vault-search.sock` (mode 0600), or `/tmp/vault-search-$UID.sock` when `XDG_RUNTIME_DIR` is unset; `serve`, `query`, `reindex`, `status`, and the MCP server must agree on it (same `XDG_RUNTIME_DIR`, or pass `--socket`). `serve` watches the vault (inotify via `notify`) and reindexes about half a second after a note changes; the 60 s mtime scan stays as the fallback. A user systemd unit is `contrib/vault-searchd.service`. It is not installed by the build.

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
| `vault-searchd` | Daemon (index, search, optional vault sync) and `query` / `reindex` / `status` clients. |
| `vault-search-mcp` | Line-delimited JSON-RPC, protocol `2024-11-05`. |
| `vault-rclone` | Vault-only wrapper around the `rclone` binary. |

## Vault sync (rclone)

`crates/vault-rclone` wraps the `rclone` binary for `gdrive:Vault` only. It follows the vault note DriveSync Replacement: one vault folder, no whole-Drive sync, no mount, and no tokens in output. Commands: `status`, `check-config`, `lsf`, and guarded `sync` / `resync` (refuse when either side has fewer than `MIN_FILES` files, always `--max-delete`, newer-wins with `*.conflict-…` losers kept, run log in `~/.local/state/vault-rclone/sync.log`). After a successful, non-dry-run `sync` or `resync` it asks a running `vault-searchd` for an incremental reindex over the Unix socket (`{"op":"reindex"}`); if the daemon is not running or the reindex fails, that is logged and the sync still exits 0. On the desktop the daemon calls this library itself (`--sync-interval`); the bash `vault-sync` timer is not a second owner. Cutover notes are in [crates/vault-rclone/README.md](crates/vault-rclone/README.md).

```bash
cargo run -p vault-rclone -- status
cargo run -p vault-rclone -- sync --dry-run
```

## Integrated desktop

One product on the desktop: `vault-searchd serve` can own the vault sync as well as the index and search. The embedder stays a separate loopback process (torch weights never enter the Rust binary).

```text
contrib/embed_server.py (127.0.0.1:8081)
        ^
        |  HTTP, loopback only
vault-searchd serve --sync-interval 120
        |-- every 120 s: vault-rclone guarded bisync  (gdrive:Vault <-> --vault)
        |                then, only if it exited 0: incremental reindex in-process
        |-- vault watch:  note written on disk -> ~0.5 s debounce -> incremental reindex
        |-- every 60 s:  mtime scan + incremental reindex (fallback)
        `-- unix socket: search / reindex / status
```

1. Start the embedder: `python3 contrib/embed_server.py` (or `contrib/vault-embed.service`).
2. Start the daemon with sync on:

   ```bash
   vault-searchd serve \
     --vault "$HOME/Obsidian/Vault" \
     --index "$HOME/.local/share/vault-search/index.db" \
     --embed-url http://127.0.0.1:8081/v1/embeddings \
     --dim 768 \
     --sync-interval 120
   ```

3. Check it: `vault-searchd status` (or `--json`).

How it behaves:

- `--sync-interval 0` (the default) or `--no-sync` leaves sync off; the daemon then behaves exactly as before.
- Sync calls the `vault-rclone` library, not a copy of it: same guards (`MIN_FILES` empty-side refuse, `--max-delete`, newer-wins with conflict suffix, vault-only remote) and the same run log in `~/.local/state/vault-rclone/sync.log`. Config comes from `~/.config/vault-rclone/config.env` and the environment like the CLI, or from `--sync-config FILE`. `VAULT_LOCAL` is always replaced by `--vault`, so there is one vault root; a different `VAULT_LOCAL` is logged as a warning.
- The sync runs on its own thread; the socket keeps answering while bisync or the reindex runs. The first sync starts at once, the next one `N` seconds after the previous one finished.
- A failed sync (guard refusal, rclone non-zero, rclone missing) is logged and shown in `status`; serve keeps running, that cycle skips the reindex, and the next cycle tries again.
- After a successful sync the daemon reindexes in-process (no socket hop to itself). A failed reindex, e.g. the embedder is down, is reported as `after sync: reindex: FAILED (...)` but the sync still counts as ok: the files are synced. `VAULT_REINDEX=0` turns the post-sync reindex off; the 60 s scan still picks changes up.
- Local writes (an editor, GalAnonim, a script, or the sync itself) are picked up by a recursive filesystem watch on `--vault` (`notify`: inotify on Linux, FSEvents on macOS). Changes to `.md` / `.markdown` files and folders are debounced (`--watch-debounce-ms`, default 500; a steady stream still fires at least every 5 s) into one incremental reindex, normally within a second or two. Dot-dirs (`.obsidian`, `.trash`, rclone temp files) and non-markdown files are ignored, like the scan. If a reindex is already running the watch retries until it gets its own pass, and writes during its pass schedule one more. A failed reindex is logged and shown as the index error; the next change or the 60 s scan retries.
- The 60 s mtime scan stays as the fallback for missed events, inotify queue overflow, and filesystems without notify. If the watch cannot start (e.g. `fs.inotify.max_user_watches` too low, permission), serve logs `watch unavailable, 60s rescan only: ...`, shows `watch: FAILED (...)` in `status`, and keeps serving. `--no-watch` turns it off. The post-sync reindex does not depend on the watch.
- The index stays outside the vault; serve refuses an index path inside it, so bisync cannot upload it.
- Run one sync owner per machine. Stop the bash `vault-sync` timer (or any `vault-rclone sync` cron) before turning `--sync-interval` on.
- The `vault-rclone` CLI stays for a one-shot check and for the cutover resync (`vault-rclone status`, `sync --dry-run`, `resync --dry-run`).

Nitro runs it this way (since 2026-10-31). `vault-sync.timer`, `vault-sync.service`, and `vault-watch.service` are disabled. The user unit is `vault-searchd serve` with `--sync-interval 120` and `--sync-config ~/.config/vault-rclone/config.env`. That file points at `gdrive:Vault`, keeps `MAX_DELETE=50` and `MIN_FILES=3`, reuses `~/.config/vault-sync/filters.txt`, and sets `CONFLICT_SUFFIX=.conflict-{Date}` so renamed losers match the old bash script. The bash script's bisync order was local then remote. This crate's order is remote then local, so the first switch has no listing files and exits 7 until one guarded `vault-rclone resync` (dry-run first; refuse if either side is under `MIN_FILES`). After that baseline the 120 s cycle is incremental. Do not start the bash timer again while this interval is on.

`vault-searchd status` prints the socket, vault, index counts (`notes`, `chunks`, `dim`, indexing now), the last index error, whether the embedder accepts connections, and the sync health: enabled and interval, remote and local, runs and failures, last sync time (UTC) and exit code, last ok sync, last sync error, and the reindex result after the last good sync, plus the vault watch (on / FAILED / off, debounce, events, watch-triggered reindexes). The same fields are in the `{"op":"status"}` reply as optional JSON fields (`vault`, `embed_ok`, `embed_url`, `sync{…}`, `watch{…}`), so older clients keep working and older daemons simply omit them.

User systemd units (not installed or enabled by the build): `contrib/vault-embed.service` and `contrib/vault-searchd.service`. The searchd unit ships with `--sync-interval 120` commented out so copying it cannot start a second sync owner. Uncomment that line, and stop the other owner, on a desktop that should sync the way Nitro does.
