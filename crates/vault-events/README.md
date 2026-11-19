# vault-events (stage 1)

Event-based vault sync over [Zenoh](https://zenoh.io), following the vault
note *Vault Event Sync Plan*. Stage 1 runs on the Grok Bot box only: one hub
and one agent for the box vault. Drive becomes a one-way backup; the existing
`vault-rclone` bisync stays installed as the fallback.

Dependency: `zenoh` **1.10.2** (crates.io, released 2026-10-28, MSRV 1.75),
licence **EPL-2.0 OR Apache-2.0**. Built with `default-features = false,
features = ["transport_tcp"]` (no QUIC/TLS/WS/shared memory). `notify` 8
(CC0-1.0), `sha2`/`base64`/`serde` (MIT OR Apache-2.0). This crate:
Apache-2.0 like the rest of the workspace.

## Topology

```text
 S24 VaultHome (M4)   Grok Build hosts (M2)        Grok Bot box
 zenoh-kotlin client  vault-events agent            +-------------------------------+
        \                   |                       | vault-events hub              |
         \--- tcp/7447 over the tailnet ----------> |  zenoh router (tcp/0.0.0.0:7447)
                                                    |  storage ~/.local/share/       |
                                                    |    vault-events/hub/store/     |
                                                    |  queryables: vault/notes/**,   |
                                                    |              vault/hub/index   |
                                                    |  (opt) vault-rclone backup ->  |
                                                    |        gdrive:Vault (copy)     |
                                                    | vault-events agent --vault ... |
                                                    |  notify watch <-> ~/Obsidian/  |
                                                    |  Vault  <- vault-searchd watch |
                                                    +-------------------------------+
```

## Event schema (format 1)

Key: `vault/notes/<path>`, `<path>` vault-relative with `/`. The bytes `%`,
`#`, `?`, `*`, `$` are percent-encoded (`%23` ...) because Zenoh forbids or
reserves them; only that canonical encoding is accepted, so one path has one
key. `..`, `.`, empty components, absolute paths and backslashes are rejected
on both encode and decode, so a remote event cannot escape the vault root.

Payload: JSON.

| Field | Meaning |
|---|---|
| `format` | `1`. Readers reject `0` and anything newer than they know. |
| `path` | Vault-relative path; must match the key. |
| `version` | Per-note Lamport counter. A device that last saw `n` publishes `n + 1`. |
| `ts_ms` | Producer clock (Unix ms); file mtime for edits. Tie-break only. |
| `origin` | Device id (`box`, `latitude`, `s24`, ...). |
| `deleted` | Tombstone. No `content`, empty `sha256`. |
| `sha256` | Hex SHA-256 of the content; verified on decode. |
| `prev_sha256` | SHA-256 the edit was based on (absent for a note the device had never seen). Distinguishes a fast-forward from a concurrent edit. |
| `writer` | Optional. Who wrote this state, `<device>/<program>`: `box/agent` (the agent's own conflict copies), `box/bisync` (rclone bisync pulled it from Drive), `box/galanonim` (a bot via `vault-events write --as`), `s24/vaulthome`, `<device>/external` (an editor or tool the agent does not know). Debug only, never part of the ordering. |
| `cause` | Optional. `local-edit`, `external-edit`, `bisync-pull`, `event-apply-republish`, `conflict-copy`, `delete`, `catchup-republish`, `seed`. |
| `content` | File bytes, base64. |
| `conflict` | Optional, conflict copies only: `{of, winner_sha256, winner_size, winner_origin, loser_sha256, loser_size, loser_origin}`. |
| `blob` | Optional, instead of `content` for files above the inline limit: `{size, chunk_size, chunks}`. See "Large files". |

`writer`, `cause` and `conflict` are new optional fields in format 1: older
builds ignore them and events without them still decode (`-` in logs).

### Large files (blob references + chunks)

One Zenoh message per file broke on photos: a 4.9 MB or 7 MB event filled
the session's non-droppable queue and dropped the link (`Unable to push non
droppable network message`). Files above `--inline-max`
(`VAULT_EVENTS_INLINE_MAX`, default 1 MiB) now travel in two parts:

1. The event on `vault/notes/<path>` carries the stamp (with the whole
   file's `sha256`) and `blob: {size, chunk_size, chunks}`, no `content`.
   It is a few hundred bytes.
2. The bytes are pulled with Zenoh queries on
   `vault/blob/<sha256>/<index>?cs=<chunk_size>` (`--chunk-size` /
   `VAULT_EVENTS_CHUNK_SIZE`, default 256 KiB). Queries ask for
   `Priority::DataLow` and `CongestionControl::Drop`, and replies inherit
   both, so bulk bytes never block note events; a dropped reply is a
   missing chunk that is asked for again.

Who serves chunks: the hub, from `<data-dir>/blobs/<sha256>` (required), and
the sending agent, from an in-memory cache of what it published in the last
30 minutes (max 256 MiB). Receivers ask the best-matching queryable first,
then all of them.

Hub ingest is pull-based: the hub gets the reference, pulls the chunks from
the sender's queryable, checks the SHA-256, and only then stores the entry
(and logs `pulled blob ... sha256 ok`, then `stored ... [blob N B, sha256
...]`). The sender never puts the whole file. If the pull fails (sender gone),
nothing is stored, so the hub index lacks the state and the sender republishes
it on its next catch-up. Inline large events from older agents are accepted
and moved into the blob store, and older hub stores are converted on start,
so catch-up replies are always small.

Receivers (agents, and the hub when pulling) assemble chunks in
`<state-dir>/partial/<sha256>.part` (+ `.have`, the finished chunk list),
outside the vault, so the watcher, Obsidian and rclone bisync never see a
partial file. A retry or a restart asks only for missing chunks. A SHA-256
mismatch discards the partial file and starts over. Only verified bytes reach
the normal apply path, which writes with a same-directory temp file + rename.
A reference whose bytes were not fetched is never applied and never becomes a
conflict copy. Catch-up uses the same path for big files.

Older builds (no `blob` field) see a live event without `content` and drop
it as `MissingContent` (`dropping bad event ... live event without content`
live, `bad catch-up reply ... ` + `hub listed <path> but returned nothing` on
catch-up): no file is written, nothing is deleted, and the file reaches
those devices only after they update (or via Drive). Their own large files
still go out inline (the old behaviour); new hubs and agents accept that.

### Who wrote it

- The agent labels a change it sees on disk. A claim left by
  `vault-events write --as <name> <path> < content` (or `--delete`) makes it
  `<device>/<name>`. While vault-rclone's bisync runs, and for 5 s after,
  it is `<device>/bisync` (`bisync-pull`). vault-rclone writes
  `~/.local/share/vault-rclone/sync.running` (`{pid, start_ms, run}`) for
  the length of each non-dry-run bisync and `sync.last` when it ends. A
  `sync.running` whose pid is dead is ignored. Anything else is
  `<device>/external`.
- Hub log: `stored <path> vN from <origin> by <writer> (<cause>)`. The agent
  logs `published ... by ...`, `applied ... from ... by ...` and every
  conflict with both sides' sizes and hashes. A conflict that keeps the
  smaller side at the live path logs a WARN on the agent and on the hub.
- `vault-events index` prints `path version origin writer cause sha256`.
- The hub appends every stored event to `<data-dir>/history.jsonl`, rotated
  to `history.jsonl.1` at 10 MiB, so about 20 MiB at most. Each line holds
  the time, path, version, origin, writer, cause, sha256, prev_sha256, the
  sha it replaced, the size, the delete flag and the conflict details.
  `vault-events who <path> [--limit N] [--connect ...]` prints them newest
  first (Zenoh query `vault/hub/history/<path>?limit=N`). Provenance never
  goes into note content.

### Winner and conflict copies

Total order, same on every device and on the hub (`Stamp::beats`):
higher `version`, then later `ts_ms`, then live over tombstone, then larger
`origin`, then larger `sha256`.

On an incoming event the agent compares it with the last stamp it accepted
for that path and with the bytes on disk (`conflict::decide`):

- disk already equals it -> record only; not newer -> ignore (stale / echo);
- disk has an unpublished local edit -> local candidate (`version + 1`,
  origin = this device) vs incoming; winner at the live path, loser saved;
- clean disk, `prev_sha256` matches -> fast-forward;
- clean disk, different base -> concurrent edit that lost the tie-break:
  current bytes saved, incoming applied.

Loser copy: `<name> (conflict <origin> <YYYY-MM-DD>).<ext>` next to the note,
named from the *losing* stamp (UTC date), so devices that resolve the same
conflict produce the same file; it then syncs as an ordinary new note. If the
name is taken by different bytes, ` 2`, ` 3` ... is appended. This is a
different scheme from rclone's `*.conflict-{Date}`; do not let both write the
same tree at once (see "Switching" below).

Renames are a tombstone at the old path plus a new note at the new path.
Tombstones are kept in hub storage indefinitely in stage 1.

## What syncs

Regular files up to `--max-bytes` (default 8 MiB). Larger files, symlinks,
anything under a dot-directory or dotfile (`.obsidian/`, `.trash/`, `.git/`),
except the `.grok/skills/` allow-list (shared agent skills: every regular file
under it syncs, but other `.grok/` content and nested dot segments such as
`.grok/skills/x/.DS_Store` do not; the hub refuses non-synced paths too),
editor/download temp files (`*.tmp`, `*~`, `*.swp`, `*.part`, ...), rclone
bisync's own conflict losers (`*.conflict-*`, `*..conflict-*`, `*.conflictN`;
each bisync device makes its own, and syncing them spread them to every
device), and the agent's own `.vault-events-tmp-*` files do not. The agent's
own `(conflict <origin> <date>)` copies do sync. A conflict copy is never
made when both sides hold the same bytes. Files above the inline limit
(1 MiB) up to `--max-bytes` sync as blob references plus chunks; larger
files stay on the Drive fallback path.

## Hub

```bash
vault-events hub [--listen tcp/0.0.0.0:7447] [--data-dir ~/.local/share/vault-events/hub] \
                 [--connect tcp/<other-router>:7447] [--backup-config ~/.config/vault-rclone/config.env \
                  --backup-interval 300 [--backup-dry-run]]
```

- Embedded Zenoh router (`mode: router`, multicast scouting off; peers come
  in over explicit endpoints only).
- Subscribes to `vault/notes/**` and stores an event only if it beats the
  stored one. One JSON file per path (`store/<sha256(path)>.json`), written
  via temp file + rename. Only stamps are kept in memory.
- Queryable `vault/notes/**`: the stored event(s) for a key or wildcard.
- Queryable `vault/hub/index`: compact list of all stored stamps, used by
  agents to work out what to fetch and what to republish.
- `--backup-config`: after a stored change, wait `--backup-interval` seconds
  and run `vault-rclone backup` (below). Off unless given.
- Storage lives under `~/.local/share`, which survives Grok Bot box image
  updates (`~/.cache` and `~/.local/state` do not).

`vault-events index [--summary]` prints what the hub stores.

## Agent

```bash
vault-events agent --vault ~/Obsidian/Vault --device box \
                   [--connect tcp/127.0.0.1:7447] [--state-dir ~/.local/share/vault-events/agent]
```

Start-up: open a Zenoh client session (retries every 10 s while the hub is
unreachable), subscribe, start the `notify` watcher, **catch up** (fetch the
hub index; fetch and apply every note the hub has newer; republish every local
state the hub lacks or has older), then **scan** the vault and publish local
changes made while the agent was down. Then:

- watcher events are debounced (`--debounce-ms`, 750) and published;
- incoming samples are applied with the rules above;
- catch-up repeats every `--catchup-secs` (300) and whenever the router link
  comes back; a full rescan runs every `--rescan-secs` (600).

Echo suppression: after writing a file the agent records the incoming stamp
together with that file's SHA-256, so its own write seen by the watcher hashes
equal and is not published. Puts use `Locality::Remote` and the subscriber
`allowed_origin(Remote)`, so a session never receives its own events.

Guards:

- **mass delete**: if more than `--max-delete` (50) tracked notes vanish in one
  batch or scan, no tombstones are published (an unmounted or wrong vault path
  must not wipe the hub);
- **min files**: a scan of a vault with fewer than `--min-files` (3) files
  while more are tracked is refused;
- incoming events for ignored paths, for paths that are a directory/symlink
  locally, or that fail path validation or the SHA-256 check are dropped.

State: `<state-dir>/agent-state.json` (last accepted stamp + mtime/size per
path). It records the device and vault; the agent refuses a state file made
for another device or vault.

## vault-searchd

No code change needed. The agent writes through temp file + rename inside
the vault, and `vault-searchd serve` already watches the vault with `notify`
(its watch treats the rename target / create / remove of a `.md` file as
relevant) and reindexes incrementally after ~0.5 s; the 60 s rescan stays as
the fallback. Applied deletes are removes, which the same watch picks up.

## Drive backup: `vault-rclone backup`

`rclone copy VAULT_LOCAL VAULT_REMOTE --update --fast-list -v --disable-http2
[--filters-file ...] [--dry-run]`. `copy` never deletes on Drive, and
`--update` never overwrites a Drive file that is newer than the local one, so a
stale box tree cannot clobber edits that reached Drive via the bisync
fallback. Guards: `VAULT_LOCAL` must be a directory with at least `MIN_FILES`
files. Logged to the same `sync.log` as `sync` / `resync`.

## Box supervision (`contrib/vault-search-watchdog`)

Off by default. Enable with `~/.config/vault-events/config.env`
(template: `contrib/vault-events.config.env.example`):

```sh
VAULT_EVENTS_ENABLE=1
VAULT_EVENTS_DEVICE=box
# VAULT_EVENTS_LISTEN=tcp/0.0.0.0:7447
# VAULT_EVENTS_HUB_CONNECT=tcp/note4.<tailnet>.ts.net:7447   # dial out to a peer router
# VAULT_EVENTS_BACKUP=0            # 1: hub runs vault-rclone backup
# VAULT_EVENTS_BACKUP_DRY_RUN=1
# VAULT_EVENTS_DISABLE_BISYNC=0    # 1: vault-searchd without --sync-interval
```

The watchdog loop (every 15 s) then also keeps `vault-events hub` and
`vault-events agent` running, logging to `~/.local/share/vault-search/logs/
hub.log` and `agent.log` with the same copy-truncate rotation.
`vault-search-watchdog status` shows an `events:` line.

## Switching from bisync to the event path (not done in stage 1)

Running two-way bisync and the event agent on the same tree risks write loops
and two conflict schemes. Stage 1 leaves the box bisync setting as it was.
Switch only after the hub and agent have run cleanly for a while:

1. Bring the box vault up to date with Drive first (guarded
   `vault-rclone resync --dry-run`, then `resync`), so the hub is not seeded
   from a stale tree.
2. Optionally reset the hub seed: stop hub + agent, move
   `~/.local/share/vault-events/{hub,agent}` aside, start again (the agent
   republishes the current tree).
3. Set `VAULT_EVENTS_DISABLE_BISYNC=1` (vault-searchd then runs without
   `--sync-interval`) and `VAULT_EVENTS_BACKUP=1` with
   `VAULT_EVENTS_BACKUP_DRY_RUN=1`; check `sync.log` for the dry-run copy
   list, then set `VAULT_EVENTS_BACKUP_DRY_RUN=0`.
4. Restart: kill the `vault-searchd serve` and `vault-events hub` processes;
   the watchdog restarts them with the new flags within 15 s.
5. Fallback when the hub is down for long: stop the agent, run the guarded
   bisync again.

## Tailscale

See the PR for the stage-1 finding on whether tailnet devices can reach the
box hub inbound. Zenoh needs nothing tailnet-specific: a device connects to
`tcp/<hub-host>.<tailnet>.ts.net:7447`, or the hub connects *out* to a router
on a tailnet host with `--connect` (Zenoh links are bidirectional once open).

## Shared endpoint list: `app_settings/VaultEvents/endpoints.json`

VaultHome (Kotlin port, pawelkat/VaultHome PR #7) keeps a shared list of Zenoh endpoints in the vault,
next to `app_settings/GymMonth/`. It is an ordinary vault file, so it travels as `vault/notes/...`
events and over Drive. This crate does **not** read it yet; the box hub's dial-out list is still
`VAULT_EVENTS_HUB_CONNECT` (watchdog) / `hub --connect`. Format (schemaVersion 1):

```json
{
  "schemaVersion": 1,
  "source": { "note": "..." },
  "routers": [
    { "id": "note4", "endpoint": "tcp/note4.tailf52603.ts.net:7447", "dialable": true, "note": "planned" },
    { "id": "box", "tailnetName": "grokbot-user-155710816.tailf52603.ts.net", "dialable": false,
      "note": "box hub: outbound-only, dials routers; not a connect endpoint" }
  ],
  "devices": {
    "s24": { "app": "VaultHome", "mode": "router", "listen": ["tcp/0.0.0.0:7447"],
             "endpoint": "tcp/pawels-s24-ultra.tailf52603.ts.net:7447",
             "tailnetName": "pawels-s24-ultra.tailf52603.ts.net", "lastSeen": "2026-11-18T17:00:00Z" }
  }
}
```

- `routers`: hosts that accept connections; `dialable: false` = dial-out only (the box). Written when
  the file is seeded.
- `devices`: each device writes only `devices.<device>` and keeps every other key as read.
  `endpoint` is present only for router/peer devices.
- A reader's connect list = its own configured endpoints, then dialable `routers[].endpoint`, then
  other router/peer `devices[].endpoint`; never its own endpoint.
- A future `hub --connect-from <vault>/app_settings/VaultEvents/endpoints.json` (or the watchdog)
  would add those endpoints to the hub's dial-out list; until then set `VAULT_EVENTS_HUB_CONNECT`
  by hand (e.g. `tcp/pawels-s24-ultra.tailf52603.ts.net:7447` when VaultHome runs in Router mode).

## Tests

- Unit: key encoding (incl. `KeyExpr` validity), wire format and rejection of
  bad payloads, ordering, every `decide` branch, conflict names/slots, hub
  store persistence, agent echo suppression, version/prev chaining,
  mass-delete guard, catch-up planning.
- `tests/sync.rs`: real Zenoh router + two agents on loopback TCP with temp
  vaults: edit A -> B, B -> A in a new folder, delete propagates, no echo
  (hub version stable), B offline then catches up from hub storage, concurrent
  offline edit -> winner at live path + `(conflict dev-b <date>)` copy on both
  sides, hub restart keeps storage; a 7 MB photo A -> hub -> B in 256 KiB
  chunks (hub stores a small reference + a blob with the right hash, notes
  still sync both ways afterwards, no conflict or partial files), and a late
  joiner gets it from the hub alone after the sender stopped.
- Unit (blob): chunk math and keys, out-of-order assembly, resume after a
  restart (only missing chunks asked again), hash mismatch -> discard and
  refetch, a source that always lies never yields bytes, outgoing cache
  limits, the hub's blob store and conversion of older inline entries, and
  that a reference is never applied without verified bytes.
