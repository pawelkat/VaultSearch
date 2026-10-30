# vault-rclone

A thin, vault-only wrapper around the `rclone` binary. It syncs one thing: `gdrive:Vault` against one local folder. It never touches the whole Drive, never mounts, and never reads or prints OAuth tokens. rclone keeps doing the Drive work and owns `rclone.conf`.

The policy comes from the vault note **DriveSync Replacement**. The bash `vault-sync` skill stays in production until this crate has survived real edits.

## Commands

| Command | What it does |
|---|---|
| `status [--offline]` | Local path and file count, remote, guard values, config files read, bisync lock files, rclone version, and `rclone size` on the remote. Exit 1 if VAULT_LOCAL is missing, rclone is missing, or the remote is unreachable. |
| `check-config [--offline]` (alias `auth`) | `rclone.conf` exists, the remote name is listed by `rclone listremotes`, a `token` key is present (value not shown), and `rclone lsd` on the vault works. |
| `lsf [--local]` | `rclone lsf --max-depth 1 --fast-list <VAULT_REMOTE>` (or `VAULT_LOCAL` with `--local`). Smoke check; prints rclone's output. |
| `sync [-n/--dry-run]` | Guards, then incremental `rclone bisync`, then (if bisync exited 0 and not `--dry-run`) an incremental reindex request to `vault-searchd`. |
| `resync [-n/--dry-run]` | Guards, then `rclone bisync --resync --resync-mode newer`. Only for the first run, after rclone exit 7, or after editing the filters file. Run with `--dry-run` first. Reindexes afterwards like `sync`. |

Exit codes: 0 ok; 1 a check failed or a guard refused; 2 bad config. For `sync`, `resync`, and `lsf`, a non-zero rclone exit code is passed through (bisync: 1 retryable, 2 usage, 7 critical, needs `resync`).

```bash
cargo run -p vault-rclone -- status
cargo run -p vault-rclone -- lsf
cargo run -p vault-rclone -- resync --dry-run   # first run on a machine
cargo run -p vault-rclone -- sync
```

**Production is still the bash `vault-sync`.** Use this binary by hand, side by side, until it has survived real edits. Do not point Termux, Autosync, or a timer at it yet.

## Guards (sync and resync)

Checked in this order; any failure exits 1 before bisync runs:

1. `VAULT_LOCAL` is set and is a directory; rclone is found.
2. `FILTERS_FILE`, if set, exists.
3. **Empty-side refuse:** local holds at least `MIN_FILES` regular files (counted locally, symlinks not followed), then the remote does too (`rclone size --json --fast-list`). If the remote count cannot be read, the run is refused.

Then bisync runs as:

```text
rclone [--config RCLONE_CONFIG] bisync <VAULT_REMOTE> <VAULT_LOCAL> \
  --max-delete <MAX_DELETE> \
  --conflict-resolve newer --conflict-loser num --conflict-suffix <CONFLICT_SUFFIX> \
  --fast-list -v \
  [--filters-file <file>] [--resync --resync-mode newer] [--dry-run]
```

- Path1 is the remote and Path2 the local folder, as on Nitro, so the listing files in `~/.cache/rclone/bisync` have the same names.
- `--max-delete` is always passed. For `bisync` it is a **percentage** of files (rclone default 50%), not a count as it is for `rclone sync`. The default `MAX_DELETE=50` therefore matches rclone's own default; set it lower to be stricter.
- Conflicts: the newer file wins, the loser is renamed, not deleted. With the default `CONFLICT_SUFFIX=.conflict-{DateOnly}-` a losing `note.md` becomes `note.md..conflict-2026-10-30-1` (rclone adds the leading `.` and the number). `{DateOnly}` is an rclone date macro. The existing vault file `2026-10-23.md..conflict-Date1` suggests the bash script passes `.conflict-{Date}`, which rclone does not expand (it writes the literal `Date`); set `CONFLICT_SUFFIX=.conflict-{Date}` for an exact match.
- `--resync-mode newer` keeps the newer copy on resync instead of rclone's default "Path1 (Drive) always wins".
- `--fast-list` is bisync's default already; it is passed explicitly for clarity.

## Reindex after sync

When bisync exits 0 and the run is not `--dry-run`, vault-rclone sends one line, `{"op":"reindex"}` (vault-search's `IpcRequest::Reindex`), to the `vault-searchd` Unix socket and waits for the JSON reply. The daemon re-embeds only notes whose mtime changed with EmbeddingGemma; the index (e.g. `~/.local/share/vault-search/index.db`) is never wiped or opened by this crate, and this crate does not link `vault-search` or sqlite.

- Socket: `VAULT_SEARCH_SOCKET`, else `$XDG_RUNTIME_DIR/vault-search.sock`, else `/tmp/vault-search-$UID.sock` (the daemon's own default).
- Wait: up to `VAULT_REINDEX_TIMEOUT_SECS` (default 1800) for the reply, since embedding many notes over HTTP takes time. Not vault-search's 8 s client timeout.
- Result goes to stdout and the run log, just before `end exit=…`:
  - `reindex: ok (notes=N chunks=M)`
  - `reindex: skipped (vault-searchd not listening on <socket>: …)`: no socket file or connection refused.
  - `reindex: FAILED (…)`: daemon replied `ok: false`, timed out, or sent a bad reply.
  - `reindex: disabled (VAULT_REINDEX=0)`
- None of these change the exit code. The files are already synced; a later `vault-searchd reindex` or the daemon's own watcher catches up.
- No reindex after a guard refusal, a non-zero rclone exit, or `--dry-run`.

By hand: `vault-searchd reindex [--socket PATH] [--timeout-secs 1800]` sends the same request.

## Run log

Each `sync` / `resync` appends to `$XDG_STATE_HOME/vault-rclone/sync.log` (default `~/.local/state/vault-rclone/sync.log`): start (remote, local, dry-run), guard result or refusal, rclone's change totals and verdict, a conflict count, and `end exit=N`. Times are UTC. Per-file lines, note names, rclone.conf, and tokens are never logged; error lines go through the same credential filter as `check-config`. Nothing is written into the vault.

## Settings

Read from the environment, then `~/.config/vault-rclone/config.env`, then `~/.config/vault-sync/config.env` (the bash skill's file, for drop-in reuse), then defaults. `--config FILE` replaces both files. Files use `KEY=value` lines; `export`, quotes, and `#` comments are fine. Unknown keys are ignored and never printed.

| Key | Default |
|---|---|
| `VAULT_LOCAL` | none; required for `status`, `sync`, `resync`, `lsf --local` |
| `VAULT_REMOTE` | `gdrive:Vault` (a bare `gdrive:` is refused) |
| `RCLONE_BIN` | `rclone` on `PATH`, then `~/.local/bin/rclone` |
| `RCLONE_CONFIG` | rclone's own default |
| `MAX_DELETE` | `50` |
| `MIN_FILES` | `3` |
| `FILTERS_FILE` | first existing of `~/.config/vault-rclone/filters.txt`, `~/.config/vault-sync/filters.txt`; none if neither exists |
| `CONFLICT_SUFFIX` | `.conflict-{DateOnly}-` (no `/` allowed) |
| `VAULT_REINDEX` | `1`; `0`/`false`/`no`/`off` skips the post-sync reindex |
| `VAULT_SEARCH_SOCKET` | `$XDG_RUNTIME_DIR/vault-search.sock`, else `/tmp/vault-search-$UID.sock` |
| `VAULT_REINDEX_TIMEOUT_SECS` | `1800` |

## Parity checklist vs bash `vault-sync` (Nitro)

The bash skill scripts (`~/.local/bin/vault-sync`, vault `.grok/skills/vault-sync`) were **not readable** when this was written, so the flags above follow the DriveSync Replacement note and the Nitro as-built note. Before trusting this over the bash script, compare on Nitro:

- [ ] Path order: bash also runs `bisync gdrive:Vault <local>` (Path1 = remote)? If reversed, the bisync listings differ and the first run here needs `resync`.
- [ ] Conflict flags: `--conflict-resolve`, `--conflict-loser`, `--conflict-suffix` match (set `CONFLICT_SUFFIX` to the bash value).
- [ ] `--max-delete` value and meaning (percentage in bisync).
- [ ] Filters file path and contents (`.obsidian` caches, `.trash`, ...); same file means no forced resync.
- [ ] Extra flags in bash not here: `--resilient`, `--recover`, `--max-lock`, `--check-access`, `--create-empty-src-dirs`, `--drive-skip-gdocs`, `--compare`, `--track-renames`, `--log-file`. Any of these change behaviour; add them here before cut-over.
- [ ] Empty-side threshold (`MIN_FILES`) and how bash counts files.
- [ ] Lock handling and what bash does on exit 7.
- [ ] Whether bash writes a summary into `Vault/Reviews/` (this crate does not).
- [ ] `vault-rclone sync --dry-run` on a quiet vault reports no changes right after a bash run.

## Tests

Every rclone call goes through the `CommandRunner` trait and every reindex request through the `Reindexer` trait. Unit tests use fakes and never reach Drive or a real daemon; the socket client itself is tested against a throwaway Unix socket in a temp dir.
