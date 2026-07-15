# control — in-process control plane + live reconfigure

The daemon (`walshadow-stream`) embeds a control plane: a request/response Unix
socket for management, config as a merged TOML with a `conf.d` drop-in, and
**live reload** so source/dest/table/pause changes apply without dropping
connections or restarting. No separate binary, no child process. The intended
external consumer is the `walshadow-peerdb` HTTP shim (branch `origin/peerdbapi`,
`walshadow-peerdb/`), which translates PeerDB's flow API onto this socket.

## Control socket + `ctl`

`--control-socket <path>` binds a `UnixListener` (`src/ops/control.rs::serve`,
modeled on `metrics::serve` + the `shadow_stream.rs` bind pattern). Absent →
disabled. The client is the same binary: `walshadow-stream ctl <words…>`
(detected in `main` before daemon-arg parsing so it needs no daemon flags;
`--socket`, env `WALSHADOW_CONTROL_SOCKET`, default `/run/walshadow/control.sock`).

Wire protocol (one request per connection, `handle_conn`): a single
`\n`-terminated line `<noun> <verb> [key=value …] [positional …]`,
whitespace-split into borrowed `&str` slices (`Request<'a>`); values cannot
contain whitespace (v1 limitation). Response: `OK\n` + optional payload (kv or
tab-separated lines), or `ERR <message>\n`. `dispatch` matches `(noun, verb)`.

### Verbs
- `source set|get|test`, `dest set|get|test` — connection config. `set` writes
  the fragment; `test` connects (source: `tokio-postgres` NoTls; dest: TCP
  probe), honoring ephemeral `key=value` overrides.
- `tables list [<ns>]`, `tables select <ns.rel>…`, `tables clear` — table
  opt-in. `list` enumerates source `pg_class` + marks `selected` from the merged
  config; `select` writes `[table.<ns>.<rel>] replicate` to the fragment.
- `schemas list`, `columns list <ns> <rel>` — source-PG introspection.
- `stream stop|start` — pause/resume (writes `[stream] paused` + reloads).
- `stream reload` / `config reload` — live reload.
- `stream status` — `state=running|paused` + `rows_synced`, `backfills_pending`,
  `lag_bytes`, `lag_seconds`, `uptime_secs` (all from the metrics snapshot).
- `config show` — merged effective config (passwords masked).

`SharedCtx { ch_config, source_base, metrics, reloader }` is handed to the
handlers. `Reloader` holds only the running session's `Arc<ConfigResolver>`
(`set_resolver`, `reload`) — there is no start/stop/restart state machine.

## Config: base + conf.d merge

Config is the daemon's own TOML, `--ch-config` **plus** every `*.toml` in the
sibling `<ch-config>.d/` directory (e.g. `ch-config.toml` → `ch-config.d/`),
deep-merged in lexical filename order — Postgres `include_dir` style
(`ch_emitter::load_merged` / `merge_tables`). `load_effective(path, base)` layers
the CLI-arg `[source]` defaults *under* the file so source connection resolves
file-over-CLI (matches `EmitterConfig` boot).

The control API writes **only its own fragment**, `ch-config.d/50-api.toml`
(`frag_path`) — sparse, only the keys it sets. The operator's base
`ch-config.toml` is never rewritten (can be read-only mounted); other channels
can own other fragments. `get`/`test`/introspection/`status` read the merged
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

**Source connection is not live**: `reload()` doesn't touch the pump's
`SourceFeed`. A DNS change is picked up by the pump's existing
`reconnect_or_fatal`; a real host change needs a process restart.

## Table selection is config-driven

`[table.<ns>.<rel>]` with `columns` → pinned mapping (`EmitterConfig.tables`, as
before). Without `columns` → an opt-in intent (`runtime_config::TableRow` into
`EmitterConfig.table_opt_ins`), materialized via `apply_table_opt_in`
(descriptor-derived auto-create, optional `initial_load` backfill) — the exact
path the source-PG `config_table` overlay uses. So `ctl tables select` writes
`[table.<ns>.<rel>] replicate = true` (deselect → `replicate = false`) and the
daemon applies it on reload — **no source-PG writes from control**. The
`config_table` + WAL overlay ([config.md], [future/runtime_config_from_pg.md])
still exists independently for direct-PG operators.

## Pause

`stream stop` writes `[stream] paused = true` to the fragment + reloads; the pump
stops consuming source WAL (idles at `next_chunk`), freezing its LSN and the
slot's confirmed position — nothing downstream drops. `stream start` clears it;
the pump resumes from the same LSN, so every table continues where it left off.
Retention across a pause requires a replication slot (`[source] slot`); without
one, `wal_keep_size` bounds pause duration before the source recycles WAL. A
pause longer than `wal_sender_timeout` may drop the replication connection;
resume reconnects from the slot (still no data loss with a slot).

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
  `cli_source_base`, `spawn_sighup_reload`, pump `paused` gate.
- `src/emit/ch_emitter.rs` — `load_merged`/`load_effective`/`merge_tables`,
  `from_table` (columns-optional), `EmitterConfig.{table_opt_ins,paused}`.
- `src/config.rs` — `ResolvedConfig` (CH conn + `table_opt_ins` + `paused`),
  `reload` via `load_effective`, `ConfigResolver.cli_source_base`.
- `src/emit/pipeline/inserter.rs`, `src/emit/ch_ddl.rs` — live CH reconnect.
- `src/emit/pipeline/reorder.rs` — `maybe_apply_reload` opt-in diff at commit.

## Status / open edges
- e2e-verified live: table add (auto-create) + opt-out (retain) via `ctl tables
  select` + `ctl reload`, no restart, base file untouched.
- The lifecycle-strip + pause-as-config is the newest slice; build/e2e it after
  the wiring lands (`RUSTFLAGS="-D warnings" cargo build --all-targets`).
- Live CH-connection swap needs a second ClickHouse to test fully.
- `ToastResolver` live CH reconnect (for `[toast] mode = disk`) is a TODO.
- Control still opens source-PG read connections for introspection/test
  (`// TODO` on `pg_connect`) — route through the daemon's catalog later.
- The `walshadow-peerdb` shim's PAUSED/RUNNING map to `stream stop/start`;
  create-mirror maps to `tables select` + `reload` (no separate `start`).
