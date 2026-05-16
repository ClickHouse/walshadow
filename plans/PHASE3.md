# PHASE3 — shadow PG lifecycle

Closes [Phase 3 of `PLAN.md`](PLAN.md#phase-3--shadow-pg-lifecycle). Lands `walshadow::shadow`: a small struct
that wraps PG's external binaries (`initdb`, `pg_ctl`, `psql`) plus
the on-disk plumbing (`postgresql.conf`, `standby.signal`,
`restore_command`) into one ergonomic Rust API. Higher layers (Phase
4 catalog cache, Phase 5 E2E drill, Phase 7 daemon) compose against
this surface instead of shelling out themselves.

## What landed

| item | files | tests |
|---|---|---|
| `ShadowConfig` + `Shadow` + `HealthReport` | `src/shadow.rs` | unit + integration |
| `Shadow::initdb` (bootstrap empty cluster) | `src/shadow.rs` | `normal_mode_lifecycle` |
| `Shadow::write_base_conf` (port, socket, autovacuum off, …) | `src/shadow.rs` | `restore_command_filename_is_segment_relative` |
| `Shadow::enable_standby_recovery` (drop `standby.signal`, append `restore_command`) | `src/shadow.rs` | `restore_command_filename_is_segment_relative`, `standby_mode_lifecycle` |
| `Shadow::start` / `stop` / `is_running` (pg_ctl wrappers) | `src/shadow.rs` | both lifecycle tests |
| `Shadow::apply_schema_dump` (`psql -f -` from a SQL string) | `src/shadow.rs` | `standby_mode_lifecycle` |
| `Shadow::psql_one` / `is_in_recovery` / `last_replay_lsn` (probes) | `src/shadow.rs` | both lifecycle tests |
| `Shadow::wait_for_replay(target, timeout)` (poll loop) | `src/shadow.rs` | `standby_mode_lifecycle` |
| `Shadow::health` (in-recovery + replay LSN + `pg_class` count + `pg_proc` lookup) | `src/shadow.rs` | both lifecycle tests |
| `parse_pg_lsn` (PG `X/Y` hex text → `u64`) | `src/shadow.rs` | `parse_pg_lsn_basic`, `parse_pg_lsn_rejects` |
| Integration scenarios: normal-mode + standby-mode end-to-end against PG on PATH | `tests/shadow_lifecycle.rs` | new |

All probes shell out to `psql -tAXq -c`. No new crate dependencies; a
proper libpq client lands with Phase 4's `ShadowCatalog`.

## Design decisions

### `standby.signal`, not `recovery.signal`

[PLAN.md Phase 3](PLAN.md#phase-3--shadow-pg-lifecycle) says "Write `recovery.signal`". The architecture
diagram and §Architecture prose describe shadow as a *standby*. The
two primitives behave differently:

* `recovery.signal` — archive-recovery mode. PG exits recovery when
  `restore_command` returns non-zero (interpreted as "no more archive
  WAL"). Wrong primitive: walshadow's filter feeds WAL continuously,
  so shadow must wait for new segments rather than declaring recovery
  done.
* `standby.signal` — standby mode. PG stays in recovery forever,
  retrying `restore_command` (and/or streaming, but walshadow leaves
  `primary_conninfo` empty). Matches walshadow's continuous-feed
  topology.

Shadow ships `standby.signal`. [PLAN.md](PLAN.md#phase-3--shadow-pg-lifecycle)
text wasn't amended; treat it as a typo with this PHASE3 doc as the
corrective record.

### Probes via `psql`, not libpq

A real libpq client (tokio-postgres / postgres) belongs to Phase 4
`ShadowCatalog`, which needs a long-lived connection, named
statements, generation-counter cache invalidation. Phase 3's probes
are five well-known queries each executed at human cadence
(start-up gating, periodic health checks). Spawning `psql` is ~10ms
per probe, dominated by the lifecycle operations themselves
(`pg_ctl start` is ~500ms). Adding a tokio-postgres dependency for
five short queries is a net negative until Phase 4 actually needs it.

`Shadow::psql_one` is the single internal helper; every probe routes
through it. Phase 4 can swap the implementation under that method
without touching callers.

### `apply_schema_dump` takes a `&str`, not a source connection

[PLAN.md Phase 3](PLAN.md#phase-3--shadow-pg-lifecycle) says "restore schema-only dump from source". Two
shapes for the API:

1. `Shadow::bootstrap_from_source(source_conn_str)` — opens an
   outbound libpq connection to source PG, pipes
   `pg_dump --schema-only` into `psql`, owns the full orchestration.
2. **Chosen**: `Shadow::apply_schema_dump(sql: &str)` — accepts a
   SQL payload, leaves the dump-from-source step to the caller.

Reasoning: source-PG connection management is Phase 7 (daemon)
territory — credentials, retry, slot pre-creation, primary-failover
handling. Bundling it into the shadow module would create one of
two bad outcomes:

* a half-implemented `bootstrap_from_source` that Phase 7 has to
  replace, leaving dead code; or
* a fully-implemented one duplicating Phase 7's connection layer.

Phase 7 will own outbound connections and call `apply_schema_dump`
with the bytes it pulled. The library stays composable.

### `wait_for_replay` with `target = 0`

`pg_last_wal_replay_lsn()` returns NULL briefly after standby
start-up while the startup process catches up. Once any LSN is
visible, the function returns it monotonically. Callers gating on
"shadow has begun replaying" pass `target = 0` and wait for the
first non-NULL observation; callers gating on "shadow caught up to
source commit LSN X" pass `target = X`. Same API, both call-sites
covered.

### Append-only `postgresql.conf`

`write_base_conf` and `enable_standby_recovery` both append rather
than rewrite. Each block is preceded by a `# walshadow …` comment
so an operator can diff their effect against the initdb default.
Re-running either function double-appends; callers that rebuild a
data directory should `initdb` fresh first. Acceptable trade-off:
the alternative (parse + edit) requires either a TOML-like rewrite
of a free-form `key = value` file or a separate include-file (which
itself needs a `include_dir =` line back in `postgresql.conf`).
Append-and-let-later-win matches how PG itself treats duplicate
settings.

### `fsync = off`, `wal_compression = off`

Shadow's data dir is disposable: a corrupted cluster is fixed by
`rm -rf data && Shadow::initdb` followed by re-replay of catalog
WAL. Trading durability for replay speed is the right call for a
catalog mirror. `wal_compression = off` doesn't change source
acceptance (shadow replays whatever the source wrote) but ensures
shadow's own emitted records (the few catalog updates from
`autovacuum = off`-driven hint-bit writes etc.) stay simple to
inspect.

### `autovacuum = off`

[PLAN.md pitfall #3](PLAN.md#3-catalog-index-bloat) flags catalog-index bloat on a busy DDL workload.
Phase 3's default is "accept bloat"; `autovacuum = off` avoids
autovacuum interfering with recovery (vacuum on a standby is a no-op
anyway, but the launcher running adds noise). When pitfall #3 needs
addressing, the operator promotes shadow briefly, lets autovacuum
run, then re-attaches — same posture as
[PLAN.md](PLAN.md#3-catalog-index-bloat) proposes.

## Deviations from [PLAN.md Phase 3](PLAN.md#phase-3--shadow-pg-lifecycle)

* `standby.signal` instead of `recovery.signal` — see above.
* No `--initial_target` boot gate in `Shadow::start`. The PLAN
  paragraph reads "`pg_ctl start`, wait for `pg_is_in_recovery()
  AND pg_last_wal_replay_lsn() >= initial_target`". Phase 3 splits
  those two concerns: `start` returns after `pg_ctl -w` reports the
  postmaster is up, and the LSN gate is `wait_for_replay`. Callers
  needing both run them back-to-back; the failure modes (postmaster
  failed to start vs. replay never reached target) are then
  distinguishable.
* `apply_schema_dump` decoupled from source connection (see above).
* No outbound `pg_dump` invocation. Phase 7.

## What didn't get done

* No CLI binary. `walshadow-classify` and `walshadow-filter` set a
  precedent, but Phase 3's lifecycle operations are stateful (start →
  query → stop) and don't compose well as one-shot CLI invocations.
  When Phase 7's `walshadow` daemon lands, it absorbs this surface.
* No `pg_basebackup` integration. [PLAN.md](PLAN.md#phase-3--shadow-pg-lifecycle) says "schema-only dump",
  not "full base backup"; we honour that. If shadow's `pg_control`
  starting LSN ever needs to be moved to match a source backup
  position (currently it's just initdb's own LSN, which is sufficient
  for catalog-only replay), Phase 5 or Phase 7 can layer that on.
* No `pg_filenode.map` import from source. Phase 1 docs flag this as
  a Phase 2/3 follow-up; Phase 2 deferred it, and Phase 3 didn't pick
  it up either. The relmap tracker still populates only when source
  emits `XLOG_RELMAP_UPDATE`. Bootstrap rule
  `relfilenode < FirstNormalObjectId` continues to carry the cluster
  through most workloads. Either Phase 4 or Phase 7 will resolve.
* No port-collision detection. Default port `55434` can clash if
  another shadow is already running. Operator picks a fresh port via
  `ShadowConfig::port` until/unless this surfaces.

## Test counts

* `cargo test --lib`: 41 passed (was 38; +3 = `parse_pg_lsn_basic`,
  `parse_pg_lsn_rejects`, `config_socket_dir_default_sits_next_to_data_dir`).
* `cargo test --tests`: 8 passed (2 classify fixture + 3 filter
  round-trip + 3 new shadow lifecycle).
* `cargo clippy --all-targets -- -D warnings`: clean.

Total: 49 passing (was 43 at end of Phase 2).

## Live-cluster observations

`standby_mode_lifecycle` against local PG 18.4 (Arch Linux):

* `initdb` produces a ~50 MiB data directory with `pg_class`
  populated to ~400 rows.
* `apply_schema_dump` on a 2-table CREATE finishes in <50 ms.
* `pg_ctl start` (normal mode) reaches accepting-connections in
  ~400 ms; standby start in ~600 ms (extra time is the startup
  process walking its own initdb WAL to consistency).
* `pg_last_wal_replay_lsn()` becomes non-NULL within ~200 ms of
  the standby reaching consistency; `wait_for_replay(0, 30s)`
  observes a real LSN on its first or second poll.
* Schema dumped in normal mode survives the standby flip — the
  filter primitive lands durable changes via the normal recovery
  path (the startup-process WAL pre-standby is replayed when the
  cluster re-opens with `standby.signal`).

## Files touched

```
walshadow/src/lib.rs                       declare new `shadow` module
walshadow/src/shadow.rs                    new — Phase 3 module
walshadow/tests/shadow_lifecycle.rs        new — integration tests
walshadow/PHASE3.md                        new (this doc)
```

LOC: 456 src/shadow.rs, 168 tests/shadow_lifecycle.rs. No new crate
dependencies.
