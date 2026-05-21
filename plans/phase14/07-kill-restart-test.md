# 07 — kill -9 + restart integration test (v1.0 acceptance §5)

Closes the [PHASE13 retro carry-over](../PHASE13.md#acceptance-items-audited):
"Kill-walshadow-mid-stream-restart integration test is *not* in the
test tree." Phase 11's cursor unit tests cover the file format;
Phase 13 wired dual-cursor durability. What's missing is the
end-to-end drill that kills the daemon mid-stream, restarts it
with the same `--walsender-bind`, and confirms CH end-state matches
a non-interrupted run

## Why

v1.0 acceptance §5 explicitly names this as a gate. Phase 11's
retro flagged the gap and PHASE13 hand-rolled the dual-cursor
machinery that makes the resume safe; only the test surface is
missing

The test is harder than it looks because deterministic kill at a
chosen LSN requires either a `SIGSTOP`-style pause hook in the
daemon binary (which PHASE13 punted on the basis that "shimming a
graceful kill in production code is wrong-shaped") or probabilistic
kills across N runs with checksum agreement. Item 07 takes the
probabilistic shape

## Surface

`tests/phase14_kill_restart.rs`. Reuses the
[`phase8_e2e`](../../tests/phase8_e2e.rs) bootstrap:

- source PG via the `Shadow` fixture
- shadow PG bootstrapped via `pg_basebackup`
- CH server via [`ChServer::spawn`](../../tests/phase8_e2e.rs)
- walshadow daemon spawned as
  [`bin/stream`](../../src/bin/stream.rs) subprocess via
  `tokio::process::Command`

Drill:

1. Spawn daemon, wait for `start_replication` ack on the source
2. Start a write workload: continuous `INSERT INTO t (i, v) SELECT
   gs, repeat('x', 100) FROM generate_series(1, 10) gs` in a loop
   from a dedicated client, ≥ 5 inserts / s
3. After ≥ 100 rows visible on source, sleep for a randomised
   `kill_delay` (default 250 ms — 750 ms; seeded via
   `WALSHADOW_KILL_SEED` env var so CI is reproducible, operator
   can rotate the seed locally to widen coverage)
4. `kill -9` the daemon process
5. Stop the write loop, capture source's final row set
6. Restart the daemon with the same flags (same `--walsender-bind`,
   same `--spill-dir`, no `--ignore-cursor`)
7. Wait for the daemon's `apply_lsn` to reach source's
   `pg_current_wal_lsn()` (the source's idle WAL position)
8. Assert CH row count + `md5(string_agg(v, ',' ORDER BY i))` ==
   source's

Three pinned cutoff strategies (the `kill_delay` randomisation
runs within each strategy):

1. **mid-segment** — kill before the in-flight segment reaches its
   16 MiB seal. Walshadow's cursor resumes from a sub-segment LSN;
   the streaming-fed shadow re-streams the un-sealed bytes via the
   wire, the archive path catches up via partial-segment
   re-fetch from source
2. **mid-xact** — kill while at least one large xact is open
   (sized to spill to disk via the `XactBuffer` largest-first
   eviction). Workload extension: one `BEGIN; INSERT × 10000;
   COMMIT` running alongside the small-write loop; kill window
   targets the gap between first INSERT and COMMIT
3. **post-commit / pre-CH-ack** — kill between
   `XactBuffer::commit` returning success and the CH emitter
   ack'ing the block. Workload extension: configure
   `--ch-batch-byte-budget=1` so every tuple flushes immediately;
   inject a CH-side artificial 200 ms delay via a `clickhouse local
   --query "... SETTINGS http_send_timeout=..."` sleep proxy. Kill
   targets the delay window

Each strategy runs 5 times per CI invocation with a different
`WALSHADOW_KILL_SEED` derivative. All 15 checks must pass

## Cleanup hygiene

The `--walsender-bind` socket needs `SO_REUSEADDR` (already in
Phase 13). Spill dir + cursor file persist between kill and restart;
the restart path consumes them. CH server stays alive across the
kill — it's the destination, not part of the daemon

## Size

~250 LOC test

## Risks

- **Test flakiness.** Probabilistic kills are inherently noisy;
  CI signal vs noise depends on the seed being reproducible. The
  `WALSHADOW_KILL_SEED` env var is the lever. CI uses a fixed seed
  per PR; nightly runs rotate through five seeds to surface
  rare-window bugs
- **`kill -9` vs orphan TCP connections.** Walshadow's source-side
  connection dies with the process; source's slot keeps the WAL
  retained for `wal_keep_size` (Phase 11's resume contract). The
  restart's `START_REPLICATION` from cursor LSN must fit inside the
  retention window; test fixture pins `wal_keep_size=128MB` to
  ensure 250 ms-of-WAL gap stays well inside
- **CH artificial-delay shim for strategy 3.** Inserting a sleep
  proxy into the CH path adds complexity to the test fixture. If
  the shim proves fragile, swap strategy 3 to "kill immediately
  after the first commit drain returns" — same intent, simpler
  shape, but covers a narrower window of the pre-ack path
- **Determinism of source LSN advance.** Source's WAL position
  during the workload is not byte-for-byte deterministic across
  runs (autovacuum, checkpointer, background writer all emit WAL).
  The assertion compares CH end-state to source's row set, not to
  a specific LSN — so background WAL has no impact on the
  checksum check
