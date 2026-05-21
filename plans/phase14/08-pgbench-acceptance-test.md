# 08 — pgbench acceptance integration test (v1.0 acceptance §1)

Closes the [v1.0 acceptance §1 gap](../PLAN.md#acceptance-criteria):
"A 30-second `pgbench -T 30 -c 8` workload intermixed with one
`ALTER TABLE ... ADD COLUMN ... DEFAULT k` (fast-path) and one
`CREATE INDEX CONCURRENTLY` produces matching row counts & checksums
on source and CH after walshadow drains"

Today's test surface exercises individual decoder paths
([`phase8_e2e`](../../tests/phase8_e2e.rs)) but not the combined
workload that v1.0 ships against. The PLAN acceptance criterion
explicitly names this as the gate

## Why

§1 has been the v1.0 ship gate from the start. Phase 12 made
pgbench's `-i` initial-load visible to walshadow via the backfill
bridge; phase 14 item 01 closes the read-time-defaults gap that
`ADD COLUMN ... DEFAULT k` requires. With both landed, §1 is
end-to-end testable from `walshadow-stream` alone

The integration test pins the combined behaviour and surfaces any
regression during phase 15+ work

## Surface

`tests/phase14_pgbench_acceptance.rs`. Bootstrap path:

1. source PG `initdb`, `wal_level=logical`, restart
2. `pgbench -i -s 1` (≈100k rows across `pgbench_accounts`,
   `pgbench_branches`, `pgbench_tellers`, `pgbench_history`)
3. `ALTER TABLE pgbench_accounts REPLICA IDENTITY FULL` (and the
   other three; preflight check would refuse otherwise)
4. Spawn CH server via [`ChServer::spawn`](../../tests/phase8_e2e.rs)
5. Write a CH-config TOML mapping the four pgbench tables; create
   the destination tables in CH with matching column shapes (int4 /
   numeric / text) + the synthetic `_lsn UInt64`, `_xid UInt32`,
   `_op Enum8`, `_commit_ts DateTime64(6)` Phase 7 emits
6. Spawn walshadow daemon with `--bootstrap-mode=direct`,
   `--bootstrap-autospawn-shadow`, `--ch-config <toml>`
7. Wait for `bootstrap_end_lsn` ack; assert the four pgbench tables'
   row counts on CH match source's post-pgbench-i counts

Workload:

8. `pgbench -T 30 -c 4 -j 2` (4 clients, 2 worker threads — keeps CI
   load reasonable while still exercising concurrency)
9. At +10 s: `ALTER TABLE pgbench_accounts ADD COLUMN c int DEFAULT
   7` (exercises §1's `ADD COLUMN ... DEFAULT k` clause — requires
   item [01](01-read-time-defaults.md))
10. At +20 s: `CREATE INDEX CONCURRENTLY` on `pgbench_history (bid)`
    (exercises Phase 4/4b's catalog cache against a long-running
    DDL that does not block writers)
11. Wait for pgbench's `-T 30` to elapse; wait an additional 5 s for
    walshadow's drain to settle (cursor reaches source's
    `pg_current_wal_lsn`)

Assertions per table:

- `SELECT count(*)` source vs CH `SELECT count(*) FROM <dest> FINAL`
- `SELECT sum(<numeric_col>), md5(string_agg(...))` source vs CH
  equivalent
- For `pgbench_accounts`: `SELECT id, c FROM pgbench_accounts ORDER
  BY id` on source vs CH must agree on `c` for every row (pre-ALTER
  rows show `7` via the read-time default, post-ALTER UPDATE rows
  show the modified value)

## CI matrix

Run against PG 16, PG 17, PG 18. Same fixture, different
`postgres` binary. The existing CI matrix (PHASE9 added
postgresql-server-dev-<major>) is the right home

## Size

~300 LOC test (bulk is bootstrap orchestration; assertions are
small)

## Risks

- **Drain latency under PG 18.** Phase 13's streaming-fed shadow
  shows ≤ 50 ms gate clearance against PG 18 in `phase8_e2e`.
  pgbench's `-c 4` concurrency stresses the xact buffer more than
  `phase8_e2e`'s sequential workload — if the 5 s drain budget
  proves too tight, raise to 15 s rather than re-architecting the
  assertion
- **`pgbench -T 30` flakiness in CI.** Wallclock-bounded tests are
  noisy in shared CI runners. If 30 s is too unstable, drop to `-t
  10000` (transaction-count bound) — same workload shape, no
  wall-clock dependency
- **CH `FINAL` cost on `pgbench_accounts`.** 100k+ rows × every
  UPDATE during the 30 s workload may push the `FINAL` checksum
  query past CI timeout. If measured, switch the assertion to
  `OPTIMIZE TABLE <dest> FINAL; SELECT ... FROM <dest>` to force
  the merge first; cheaper than `FINAL` at query time
- **`CREATE INDEX CONCURRENTLY`'s WAL pattern under load.** CIC
  emits a long sequence of catalog updates without blocking
  writers; the shadow-catalog cache invalidation surface gets
  exercised hard. Phase 13's streaming-fed shadow makes the gate
  fast, but this is the first test that combines CIC with high
  concurrent write throughput. If a flake surfaces, file as a
  follow-up against [`catalog_tracker`](../../src/catalog_tracker.rs)
  rather than masking with a sleep
- **`REPLICA IDENTITY FULL` cost on pgbench.** Marking every
  pgbench table FULL means UPDATEs carry the full old-tuple in WAL.
  Doubles WAL volume; acceptable in CI (pgbench's data is small)
  but document in PLAN.md that the v1.0 acceptance criterion
  assumes FULL on tracked tables (already implied by PLAN.md
  §"Pitfall #4")
