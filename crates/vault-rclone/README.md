# vault-rclone

A thin, vault-only wrapper around the `rclone` binary. It syncs one thing: `gdrive:Vault` against one local folder. It never touches the whole Drive, never mounts, and never reads or prints OAuth tokens. rclone keeps doing the Drive work and owns `rclone.conf`.

The policy comes from the vault note **DriveSync Replacement**. The bash `vault-sync` skill stays in production until this crate has survived real edits.

## Status (PR1)

| Command | State |
|---|---|
| `status [--offline]` | Local path and file count, remote, guard values, config files read, bisync lock files, rclone version, and `rclone size` on the remote. Exit 1 if VAULT_LOCAL is missing, rclone is missing, or the remote is unreachable. |
| `check-config [--offline]` (alias `auth`) | `rclone.conf` exists, the remote name is listed by `rclone listremotes`, a `token` key is present (value not shown), and `rclone lsd` on the vault works. |
| `sync`, `resync`, `lsf` | Reserved. Print "coming in a later release" and exit 2. |

Exit codes: 0 ok, 1 a check failed, 2 not available yet or bad config.

```bash
cargo run -p vault-rclone -- status
cargo run -p vault-rclone -- check-config --offline
```

## Settings

Read from the environment, then `~/.config/vault-rclone/config.env`, then `~/.config/vault-sync/config.env` (the bash skill's file, for drop-in reuse), then defaults. `--config FILE` replaces both files. Files use `KEY=value` lines; `export`, quotes, and `#` comments are fine. Unknown keys are ignored and never printed.

| Key | Default |
|---|---|
| `VAULT_LOCAL` | none; required for `status` |
| `VAULT_REMOTE` | `gdrive:Vault` (a bare `gdrive:` is refused) |
| `RCLONE_BIN` | `rclone` on `PATH`, then `~/.local/bin/rclone` |
| `RCLONE_CONFIG` | rclone's own default |
| `MAX_DELETE` | `50` |
| `MIN_FILES` | `3` |

## Guards planned for `sync` (PR2)

Refuse when either side has fewer than `MIN_FILES` files, always pass `--max-delete`, keep newer-wins with `*.conflict-…` copies, and log run summaries outside the vault. Conflict and filter flags will be copied from the bash `vault-sync` script, not invented.

## Tests

Every rclone call goes through the `CommandRunner` trait. Unit tests use a fake runner and never reach Drive.
