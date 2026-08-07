# control — in-process control plane + live reconfigure

The daemon (`walshadow-stream`) embeds a control plane: a request/response Unix
socket for management, config as a merged TOML with a `conf.d` drop-in, and
**live reload** so source/dest/table/pause changes apply without restarting;
only the connection whose address changed is redialled. No separate binary, no
child process. The intended external consumer is the `walshadow-peerdb` HTTP
shim (branch `origin/peerdbapi`, `walshadow-peerdb/`), which translates
PeerDB's flow API onto this socket.

## Control socket + `ctl`

`--control-socket <path>` binds a `UnixListener` (`src/ops/control.rs::serve`,
modeled on `metrics::serve` + the `shadow_stream.rs` bind pattern). Absent →
disabled. The client is the same binary: `walshadow-stream ctl <verb>` with the
command body as a TOML fragment on stdin (`ctl apply <<TOML … TOML`); detected in
`main` before daemon-arg parsing so it needs no daemon flags (`--socket`, env
`WALSHADOW_CONTROL_SOCKET`, default `/run/walshadow/control.sock`).

The command is a bare **verb**. The TOML body already has section headers, so
target rides in body, not command: `apply` takes an arbitrary fragment (any mix
of `[source]` / `[ch]` / `[table.*]` / `[stream]`) and merges it in one atomic
reload. CLI ergonomics (friendly aliases) are a separate layer above these verbs,
not the daemon's concern.

Wire protocol (one request per connection, `handle_conn`): a `<verb>` header
line selects the handler; everything after the first newline is the config, a
TOML document, read to EOF (the client half-closes its write side). TOML keeps
full value typing (int/float/±inf/nan/array) and quotes arbitrary strings, so a
value may contain spaces, `=`, newlines. Response: `OK\n` + an optional payload
that is *also* TOML (`show`/`status` are tables, `tables`/`columns` are
`[[tables]]`/`[[columns]]` arrays, `schemas` an array of strings), or
`ERR <message>\n`. `Request::parse` + `encode_request` are the codec.

### Verbs
- `apply` — deep-merge the request body (any sections) into the fragment,
  validate the merged effective config the way boot does
  (`EmitterConfig::from_table`), reload. A fragment that won't parse is rejected
  and rolled back so it can't wedge the next reload / restart. Connection config,
  table opt-in (`[table.<ns>.<rel>] replicate = true`, `initial_load`), and pause
  (`[stream] paused = true`) are all just sections in the body.
- `unset` — mask keys out of the fragment, then validate + reload. The body
  mirrors the config shape (same TOML dialect as `apply`, inverted): a non-table
  value — the sentinel `""` — removes that key including any subtree, a section
  recurses. So `[source] password = ""` drops one key, `[table] demo = ""` one
  namespace, top-level `table = ""` the whole section. The sole delete
  primitive; only the API's own fragment is touched.
- `reload` — live reload (re-read merged config + republish; SIGHUP over the
  socket).
- `show` — merged effective config (passwords masked).
- `status` — a TOML table: `paused = true|false` + `rows_synced`,
  `backfills_pending`, `lag_bytes`, `lag_seconds`, `uptime_secs`, and the
  switchover surface (all from the metrics snapshot):
  `source_swap_pending` with the `source_swap_blocked_on` proof that refused
  the last attempt; the cluster `source_system_id`; the branches
  `source_timeline`, `floor_timeline`,
  `shadow_served_timeline`, `shadow_replay_timeline`; a parked crossing as
  `crossing_blocked_on` plus `crossing_detail`; the promotion gate
  `promotion_ready`, `promotion_blocked_on`, `target_in_recovery`,
  `pause_refrozen`; and the LSNs `pause_consumed_lsn`, `pause_received_lsn`,
  `target_replay_lsn`, `target_receive_lsn`, `floor`, `source_received`,
  `drain`, `emitter_ack`, `shadow_replay` in `pg_lsn` text. The two `pause_*`
  values are frozen when the pump observes the pause and read `0/0`
  otherwise, so a switchover decision compares against a frontier that cannot
  move under it; the `target_*` pair and the gate are read from the source
  endpoint while paused, which is the promotion target once step 4's repoint
  lands ([failover.md](failover.md) §Operator protocol).
- `tables` (`namespace`) — enumerate source `pg_class` as an `[[tables]]` array
  (`namespace`, `name`, `selected`, `replica_identity`), marking `selected` from
  the merged config; `schemas` (array of strings), `columns` (`namespace`,
  `relname` → `[[columns]]` with `name`, `type`, `notnull`) — source-PG
  introspection.

`SharedCtx { ch_config, source_base, metrics, reloader, frag_lock }` is handed to
the handlers; `frag_lock` serializes fragment read-modify-write so concurrent
`apply`/`unset` can't lose an update or race a rollback. `Reloader` holds only
the running session's `Arc<ConfigResolver>` (`set_resolver`, `reload`) — there is
no start/stop/restart state machine.

## Config: base + conf.d merge

Config is the daemon's own TOML, `--ch-config` **plus** every `*.toml` in the
sibling `<ch-config>.d/` directory (e.g. `ch-config.toml` → `ch-config.d/`),
deep-merged in lexical filename order — Postgres `include_dir` style
(`ch_emitter::load_merged` / `merge_tables`). `load_effective(path, base)` layers
the CLI-arg `[source]` defaults *under* the file so source connection resolves
file-over-CLI (matches `EmitterConfig` boot).

The control API writes **only its own fragment**, `ch-config.d/50-api.toml`
(`frag_path`) — sparse, only the keys `apply`/`unset` set. The operator's base
`ch-config.toml` is never rewritten (can be read-only mounted); other channels
can own other fragments. `show`/introspection/`status` read the merged
effective config (`get_config` = `load_effective`).

Sections: `[source]` (source conn), `[ch]` (dest conn + emitter knobs),
`[table.<ns>.<rel>]`, `[namespace.<ns>]`, `[runtime_config] schema`,
`[stream] paused`. `EmitterConfig::from_table` parses a merged table (the thin
`from_toml_str` wrapper parses a string then calls it).

## Live reconfigure (no restart)

SIGHUP (`spawn_sighup_reload`, unconditional — independent of the control
socket) and `ctl reload` both call `Reloader::reload` → `ConfigResolver::reload`
→ re-read via `load_effective`, rebuild `inner.base`, `republish` on the watch.
Consumers pick it up live:

- **mappings / namespaces / budgets / compression / drop-strategy / retry** —
  batcher + inserter + mapping-refresher + DDL applicator already read the watch
  (`src/config.rs`).
- **ClickHouse connection** — `ResolvedConfig` carries the CH conn fields; the
  inserter pool (`pipeline/inserter.rs`) and `DdlApplicator` (`ch_ddl.rs`)
  compare the conn tuple off the watch and `reconnect()` at a batch/apply
  boundary (same mechanism as compression).
- **table selection** — `ResolvedConfig.table_opt_ins` carries the columns-less
  `[table.*]` opt-in intents; the reorder coordinator
  (`pipeline/reorder.rs::maybe_apply_reload`, called per commit in the drain
  driver) diffs desired-vs-applied and runs `apply_table_opt_in` (add,
  auto-create) / `exclude_table` + `note_opt_out` (remove, CH table retained).
  Applies at the next commit (deferred while idle).
- **pause** — `[stream] paused` in `ResolvedConfig`; the pump reads it live from
  a `config_rx` and gates the `feed.next_chunk` `select!` arm off when paused.
- **source connection** — `ResolvedConfig.source` (`[source]`
  host/port/user/password/dbname/sslmode plus `slot`, a `SourceConn`); the pump
  diffs it off the same watch it reads `paused` from and swaps its `SourceFeed`
  between chunks, resuming at `stream.next_lsn()`.

### Source endpoint move

A moved endpoint is a config change, not a restart. `apply [source] host = …`
(plus any of port / user / password / dbname / sslmode / slot) reloads, the
pump sees a different `SourceConn`, and `resume_source_feed` dials it:

- `IDENTIFY_SYSTEM` must report the manifest's `system_id` and timeline. A
  foreign cluster would replay another cluster's WAL into these artifacts; a
  newer timeline is a promotion, owned by the crossing
  ([failover.md](failover.md)), not by an endpoint swap. So a switchover
  repoints while the target is still a standby, and an endpoint applied after
  promotion sits unadopted (`source_swap_pending`).
- the old feed stays up until the new one is streaming, so a wrong address
  costs a warning and a retry every 2s, not the stream.
- resume is LSN-exact, so `WalStream`, filter, and `CatalogTracker` state
  stand: no WAL re-read, no catalog reseed, no rebootstrap.
- a swap that lands while `paused` just replaces the idle connection; the
  resume drains the frozen backlog over the new address.
- `walshadow_source_endpoint_swaps_total` /
  `walshadow_source_endpoint_swap_failures_total` /
  `walshadow_source_endpoint_swap_pending` (also `ctl status`
  `source_swap_pending`) report adoption, since `show` only reports intent.
- backfills opened after the move COPY from the new address
  (`CopyBackfiller::source_pg`); a pass already running keeps its session.

`slot` moves with the endpoint because a promotion target need not name its
slot the way the old primary did, and a slot binds at `START_REPLICATION`, so
a rename takes the same reconnect an address change takes. Two things the
rename does not do:

- **create** the new slot. One created at that moment reserves WAL at the
  server's head, which proves nothing about coverage of the resume floor, so
  pre-creating it is the operator's step. A name no slot answers to fails
  `START_REPLICATION`, which is the same fail-closed retry a wrong address
  gets: warning every 2s, current feed still streaming.
- **drop** the old slot. Its retention is what a rollback to that server
  reads. Left behind, it pins WAL there until the operator drops it.

`--slot` pins the name for the process: the CLI layer outranks the file, so a
daemon started with it needs a restart to move slots.

An ordinary connection drop takes the same identity-proving path
(`SourceRecovery::recover` reads the live endpoint per call), so a drop that
races a move reconnects to the new address rather than the old one.

## Table selection is config-driven

`[table.<ns>.<rel>]` with `columns` → pinned mapping (`EmitterConfig.tables`, as
before). Without `columns` → an opt-in intent (`runtime_config::TableRow` into
`EmitterConfig.table_opt_ins`), materialized via `apply_table_opt_in`
(descriptor-derived auto-create, optional `initial_load` backfill) — the exact
path the source-PG `config_table` overlay uses. So opting a table in is
`ctl apply <<'[table.<ns>.<rel>]' replicate = true` (out → `replicate = false`,
or `unset` the block), applied on reload — **no source-PG writes from control**.
Because `apply` is a deep-merge, opting one table in leaves every other opt-in
and every operator-pinned base mapping alone. The `config_table` + WAL overlay
([config.md], [future/runtime_config_from_pg.md]) still exists independently for
direct-PG operators.

## Pause

`apply` of `[stream] paused = true` writes the flag to the fragment + reloads;
the pump stops consuming source WAL (idles at `next_chunk`), freezing its LSN and
the slot's confirmed position — nothing downstream drops. `paused = false`
resumes; the pump continues from the same LSN, so every table picks up where it
left off.
Retention across a pause requires a replication slot (`[source] slot`); without
one, `wal_keep_size` bounds pause duration before the source recycles WAL. A
pause longer than `wal_sender_timeout` may drop the replication connection;
resume reconnects from the slot (still no data loss with a slot).
Pause is also the only control input a switchover needs: it freezes the
consumed and received frontiers the promotion decision reads
([failover.md](failover.md)).

## Lifecycle

The daemon runs **one** streaming session (`run_session`), forever, until Ctrl-C
/ CopyDone / fatal. `run` binds metrics + control socket + SIGHUP, then calls
`run_session` once; Ctrl-C is a pump-loop `select!` arm that breaks and drains
the pipeline gracefully. There is no supervisor loop, no restart, no
running/stopped/exited machine — reconfigure is always a live reload; pause is a
config flag.

## Deploy

`docker/entrypoint.sh` passes `--control-socket` and `mkdir -p
"${CH_CONFIG%.toml}.d"` so the API can drop fragments; the image creates
`/etc/walshadow/ch-config.d` (postgres-owned). Base `ch-config.toml` stays a
read-only mount.

## Files
- `src/ops/control.rs` — socket, protocol, handlers, `Reloader`, `SharedCtx`.
- `src/bin/stream.rs` — `--control-socket`, `ctl` subcommand, `run`/`run_session`,
  `cli_source_base`, `spawn_sighup_reload`, pump `paused` gate + endpoint swap
  (`resume_source_feed`, `SOURCE_SWAP_RETRY`).
- `src/emit/ch_emitter.rs` — `load_merged`/`load_effective`/`merge_tables`,
  `from_table` (columns-optional), `EmitterConfig.{table_opt_ins,paused}`.
- `src/config.rs` — `ResolvedConfig` (CH conn + `SourceConn` + `table_opt_ins`
  + `paused`), `reload` via `load_effective`,
  `ConfigResolver.cli_source_base`.
- `src/emit/pipeline/inserter.rs`, `src/emit/ch_ddl.rs` — live CH reconnect.
- `src/emit/pipeline/reorder.rs` — `maybe_apply_reload` opt-in diff at commit.

## Status / open edges
- e2e-verified live: pause/resume, table add (auto-create), and source endpoint
  move via `ctl apply` (`[stream] paused` / `[table.*] replicate` /
  `[source] host`) + `ctl reload` and via SIGHUP, no restart, base file
  untouched (`tests/control_plane_e2e.rs`, whose source cluster listens on two
  socket dirs so the move has a second live address of the same cluster).
- Live CH-connection swap needs a second ClickHouse to test fully. Backfills
  take it at spawn (`CopyBackfiller::dest_emitter`), so a pass starting after
  the move connects to the new destination instead of dialling boot's address
  and reconnecting at its first batch.
- `ClickHouseChunkStore` (`[toast] mode = clickhouse`) holds the boot CH conn
  and names its `pg_toast_*` tables off boot's `[ch] database`, so a
  destination move leaves chunk writes on the old server. Making it live means
  threading the watch through the store and clearing its created-table set on
  swap; off by default, so it is a known gap rather than a broken path.
- Control still opens source-PG read connections for introspection
  (`// TODO` on `pg_connect`) — route through the daemon's catalog later.
- The `walshadow-peerdb` shim's PAUSED/RUNNING map to `apply [stream] paused`;
  create-mirror maps to one `apply` carrying `[source]` + `[ch]` + `[table.*]`
  (source, dest, and tables in a single atomic reload). Repointing a live
  mirror's peer is that same `apply`, no restart.
