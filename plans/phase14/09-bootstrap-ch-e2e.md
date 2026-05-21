# 09 — Phase 12 direct + CH-backed bootstrap e2e

Closes the two open items from
[PHASE12 §"Open items carrying forward"](../PHASE12.md#open-items-carrying-forward):

- "phase12_direct_e2e.rs — exercise DirectSource end-to-end" (only
  ObjectStoreSource is covered today)
- "CH-server-backed bootstrap e2e — prove rows land in CH end-to-end"
  (today's e2e uses `RecordingObserver`, not a live CH)

Merged into one phase-14 item because both share the bootstrap
orchestrator fixture; the variants differ only in (a) `BackupSource`
impl and (b) tuple sink. Two test files; shared helpers

## Why

Phase 12's `phase12_object_store_e2e.rs` covers the orchestrator +
drain + page-walk path against a `RecordingObserver`. The
transitional emitter ([`backfill_bootstrap::drain_backfill`](../../src/backfill_bootstrap.rs)
+ `CatalogMapResolver`) is unit-tested but never exercised against a
real CH server. Item 09 closes that hole

The DirectSource variant exercises the replication-protocol
`BackupEvent` channel under load — different code path from
ObjectStoreSource's tar-over-AsyncRead, same orchestrator API

## Surface

Shared helper in `tests/common/bootstrap_ch_fixture.rs` (or
inline): bootstrap source PG, load 64 int4+text rows, spawn CH
server, write a CH-config TOML mapping the test table

### Test A — `tests/phase14_bootstrap_direct_ch.rs`

1. Source PG via `Shadow` fixture, table `s14.t (id int4, name text)`
   loaded with 64 rows
2. CH server via `ChServer::spawn`, dest table `default.t` with
   matching shape + synthetic `_lsn` / `_xid` / `_op` /
   `_commit_ts`
3. Walshadow daemon spawned with `--bootstrap-mode=direct
   --bootstrap-autospawn-shadow --ch-config <toml>`
4. Wait for bootstrap end (cursor reports `bootstrap_end_lsn` advance)
5. Assert CH `SELECT count(*), sum(id), md5(string_agg(name, ','
   ORDER BY id)) FROM default.t` == source's

### Test B — `tests/phase14_bootstrap_object_store_ch.rs`

Same shape but the bootstrap path is `--bootstrap-mode=object_store`
against an `FsStorage` root populated by
`wal_rs::pg::backup::push::handle` (the fixture from
[`phase12_object_store_e2e`](../../tests/phase12_object_store_e2e.rs))

Differences from test A:

- Setup step inserts a `wal_rs::pg::backup::push::handle` call
  between PG load and walshadow daemon spawn — produces the wal-g
  layout on disk
- Daemon flags: `--bootstrap-mode=object_store
  --bootstrap-object-store-prefix=file://<tmpdir>/wal-g
  --bootstrap-backup-name=base_000...`

Both tests share the assertion shape — single helper
`assert_ch_matches_source(ch_conn, pg_conn, table)` covers both

## Concurrency contract

Both tests must drive the same emitter wiring as production: the
`drain_backfill` future must call `on_xact_end` on the per-table
emitter before the daemon transitions to WAL-pump mode. Verify via
CH `system.query_log` showing the INSERT completed before the next
WAL-source query fires

## Size

~200 LOC per test + ~100 LOC shared helper = ~500 LOC total

## Risks

- **CH server startup cost in CI.** Each test spawns its own CH
  server; serial test execution is ~5 s per `ChServer::spawn`.
  Acceptable given the small test count. If the matrix grows, hoist
  CH server start into a shared `OnceCell` fixture
- **DirectSource's `pg_basebackup`-equivalent timing.** Phase 12's
  DirectSource wraps `wal_rs::pg::replication::base_backup::run_base_backup`,
  which streams against an active source. Under CI load the
  basebackup can take several seconds; the test's overall timeout
  needs to be ≥ 60 s
- **FsStorage's `file://` URI parsing on macOS-CI.** PHASE12's
  fixture works on Linux; the `file://<absolute_path>` shape may
  trip up macOS path conventions if CI runs there. Pin the test to
  Linux runners via `#[cfg(target_os = "linux")]` if needed —
  matches the existing `bin_stream_e2e.rs` posture
