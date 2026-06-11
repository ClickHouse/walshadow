# walshadow overview

walshadow turns source Postgres's physical-WAL stream into ClickHouse
Native blocks without a logical-decoding plugin. Two consumers share
one wire: per-record WAL filter feeds co-located shadow Postgres
running schema-only catalog replay, and in-tree heap-tuple decoder
emits user rows to ClickHouse, using shadow as live catalog oracle for
every relation lookup

## Supported PostgreSQL versions

Source: PG 16+, enforced at daemon boot by `src/preflight.rs`. Shadow
runs same major as source; minor mismatch fine. PG 15 captures parse
(wal-rs's FPI dispatch keys on `magic >= 0xD110`) but stay
operationally unsupported. PG ≤ 14 rejected at segment walker

## Why a shadow Postgres

Static-catalog snapshot would force three concessions:

1. Operator coordinates every DDL
2. Relfilenode rewrites (`VACUUM FULL`, `CLUSTER`, `REINDEX`,
   `SET TABLESPACE`) not observable without external signal
3. No in-tree oracle when decoder disagrees with PG on Tier 3 values

Second Postgres next to wal-rs, schema only with WAL-driven catalog,
fixes all three. DDL on source writes catalog heap records; replay
those into shadow, `pg_catalog` stays current with zero operator
coordination. Relfilenode rewrites ride same WAL. typsend / typoutput
on shadow provide differential oracle over libpq

Cost: one extra `postgres` process, schema-sized data dir (MiB-scale),
plus CPU to filter and CRC-rewrite catalog WAL. Catalog WAL is small
fraction of total, so steady state is DDL-rate-bound, not
data-rate-bound

## Filter contract

| rmgr | kept records | reason |
|---|---|---|
| `RM_HEAP_ID`, `RM_HEAP2_ID` | record's `RelFileLocator` in catalog set | DDL writes catalog rows |
| `RM_BTREE_ID` | relation is catalog index | catalog SELECT plans |
| `RM_RELMAP_ID` | all | shared-catalog relfilenode rewrites |
| `RM_XACT_ID` | all | commit / abort visibility |
| `RM_CLOG_ID`, `RM_MULTIXACT_ID` | all | xact status for catalog tuples |
| `RM_STANDBY_ID` | all | recovery housekeeping |
| `RM_XLOG_ID` | checkpoint, nextoid, parameter-change | recovery plumbing |
| `RM_SMGR_ID`, `RM_DBASE_ID`, `RM_TBLSPC_ID` | all | file / database / tablespace lifecycle |
| `RM_COMMIT_TS_ID`, `RM_REPL_ORIGIN_ID` | all | xact metadata replay |

Everything else drops. Catalog set bootstrapped from `pg_class WHERE
oid < FirstNormalObjectId` (16384) on freshly-initdb'd shadow, then
tracked live by `CatalogTracker` (`RM_RELMAP_ID` plus `pg_class` heap
writes) so rewrites stay in whitelist. Shared catalogs (`global/`,
`dbNode = 0`) kept unconditionally

### Rewrite over fork

Per record, parse header, walk block refs, decide keep / drop /
placeholder. At least one catalog block → emit synthesized record with
kept blocks only and recomputed CRC32C; otherwise emit `XLOG_NOOP` of
identical `xl_tot_len` so subsequent `xl_prev` chain stays valid.
Shadow PG runs as standby pointed at filter output via walsender wire
plus `restore_command` archive fallback; unmodified upstream PG binary

Alternative — patch recovery dispatcher with relfilenode whitelist —
rejected: maintaining PG fork is permanent spend, CRC rewrite is
one-time, CRC32C on SSE4.2 is ~1 ns/byte. Reconsider only if
measurement says otherwise

## Component map

Component docs live alongside this overview:

- [filter.md](filter.md) — per-record keep/drop, CRC32C rewrite,
  `CatalogTracker` whitelist via `RM_RELMAP_ID` + `pg_class` heap
  writes, `main_data` reclassifier
- [source.md](source.md) — WAL ingestion: wal-rs replication client,
  `SourceFeed`, walsender server feeding shadow at record cadence,
  `WalStream` page walker, `streaming_walker`, `QueueingRecordSink`
  decoupling pump from decoder, `decoder_sink`
- [shadow.md](shadow.md) — shadow PG lifecycle (`initdb`,
  `recovery.signal`, supervision), `ShadowCatalog` libpq cache with
  generation counter + `relation_at` replay-LSN gate, per-relation
  `SchemaEvent` channel feeding CH DDL applicator
- [decoder.md](decoder.md) — `heap_decoder` Tier 1/2 type matrix,
  `MULTI_INSERT` fan-out, FPI decompression, `main_data` parsing,
  `pg_class_decoder` driving `CatalogTracker`
- [xact.md](xact.md) — `XactBuffer` per-xid hold-and-flush, append-only
  per-xid spill at `{spill_dir}/xid-<xid>-<first_lsn>.bin`,
  `SubxactTracker` + commit-record subxact list authority, TOAST chunk
  reassembly inside buffer
- [emitter.md](emitter.md) — parallel decode+insert pipeline
  (`src/pipeline/`): reorder coordinator → decode pool ×M →
  `InsertBatcher` (seal complete INSERTs on deadline / row / byte
  budget) → inserter pool ×N → contiguous-done ack watermark;
  `ch_ddl` applicator inside the DDL barrier, `type_bridge` PG-OID
  → CH `TypeAst`
- [bootstrap.md](bootstrap.md) — greenfield path:
  `backup_source_direct` + `backup_source_object_store`,
  `backup_page_walk`, `MultiplexSink` fanning to shadow's data dir and
  CH simultaneously, `backfill_bootstrap` orchestrator, cursor handoff
  to streaming pump at `end_lsn`
- [ops.md](ops.md) — `preflight` boot-time validators, Prom metrics
  scrape, `tracing_subscriber`, segment `retention`, `cursor` file
  (v2, six LSNs), per-xact `commit_lsn` carrier, slot advance on
  `min(shadow_replay, emitter_ack)`
- [oracle.md](oracle.md) — differential decode oracle: re-encode +
  `SELECT $1::bytea::<typ>::text` round-trip against shadow,
  `--validate <N>` sampling, walshadow PG extension (`pgext/`) exposing
  `walshadow_decode_disk(oid, bytea) -> text` for Tier 3 types
- [clickhouse-c-rs Safety model](../clickhouse-c-rs/README.md#safety-model)
  — clickhouse-c-rs unsafe surface (audited 2026-05-17 at `b5af579`):
  `Client` ownership of `PosixIo`/`Codec`, `&[u8]` over
  `from_utf8_unchecked`, `Codec::raw_mut` unsafe, C-side trust boundary,
  `checked_mul`, `BorrowedFd`, packet-payload union

## Pitfalls and ordering invariants

1. **Shared catalogs in `global/`.** `pg_database`, `pg_authid`,
   `pg_tablespace`, `pg_shdepend` carry `dbNode = 0`. Filter keeps
   unconditionally; shadow won't start without them
2. **CLOG / multixact wholesale.** Catalog replay needs xact-status
   records. Tiny volume, no per-record filtering
3. **Catalog bloat vacuumed by replay.** Shadow's own autovacuum stays
   off (recovery blocks it anyway, local writes would diverge
   offset-exact pages). Filter keeps every catalog
   prune/vacuum/freeze/index-cleanup record, so source autovacuum on
   system catalogs replays & reclaims same bytes on shadow. Shadow
   catalog bloat tracks source within replay lag; cannot out-bloat
   source
4. **wal_level.** Catalog needs `replica`; user-table decoder needs
   `logical` for old-tuple. Net: `wal_level=logical` plus a usable
   replica-identity key (PRIMARY KEY, `USING INDEX`, or `FULL`) on every
   replicated table, both preflighted. DELETE only needs the key to mark
   the row; `FULL` is accepted, not required
5. **Source DDL that rewrites a user table.** Ordering invariant:
   shadow replay LSN ≥ decoder read LSN. `ShadowCatalog::relation_at`
   blocks until `pg_last_wal_replay_lsn() >= commit_lsn` so decoder
   reads post-DDL catalog for heap records. Fast-path `ADD COLUMN`
   skips rewrite; read-time defaults via `attmissingval` cover
   bootstrap-then-ALTER skew
6. **Shadow PG version skew.** Same major as source. Daemon refuses to
   start on mismatch or PG < 16
7. **Catalog cache invalidation granularity.** Single generation bumps
   on any `pg_class` write — over-invalidates. Decoder fidelity
   unaffected; cache freshness coarse. Defer finer scheme until
   measured
8. **Bootstrap-then-ADD-COLUMN column nullability.** Bootstrap walks
   heap pages where post-ALTER attnums don't yet exist; emitter writes
   NULL for missing-attnum mapping columns. CH-side schema must use
   `Nullable(T)` for any column likely added post-attach
9. **Source primary failover.** Slot doesn't follow. Operator
   pre-creates slot on standby (PG 17+ failover-aware slots) or accepts
   re-bootstrap from new LSN. Catalog preserved on shadow across
   re-attach via `rebind` disposition; diverged clusters need `rebuild`

## Acceptance criteria

Source pinned at `wal_level=logical` + a usable replica-identity key
(PRIMARY KEY / `USING INDEX` / `FULL`) on every replicated table

### v1.0

1. `pgbench -T 30 -c 8` mixed with one fast-path
   `ALTER TABLE ADD COLUMN ... DEFAULT k` and one
   `CREATE INDEX CONCURRENTLY` produces matching row counts and
   checksums on source and CH after drain. **Code-complete.**
   `tests/pgbench_acceptance.rs` covers it end-to-end with runtime
   skip-gate (no `initdb` / `pgbench` / `clickhouse` on PATH →
   `eprintln!("skip"); return`). Asserts adjusted to
   `c Nullable(Int32)` because bootstrap walks pre-ALTER pages
2. `VACUUM FULL` on a tracked table mid-workload, no operator
   intervention, CH matches source within one merge cycle. **Live**
   via `ShadowCatalog` generation bump on `pg_class` writes
3. Shadow's `pg_last_wal_replay_lsn` lags source's
   `pg_current_wal_lsn` by < 1 s of WAL at steady state. **Live**;
   surfaced as `walshadow_shadow_apply_lag_bytes` +
   `walshadow_shadow_apply_lag_seconds` on metrics endpoint

### v1.1

4. `--validate` catches a planted decoder regression on the first
   sampled row of the bad type. **Live** via differential oracle +
   `pgext/`; absent extension surfaces as `oracle fallback=N` and
   raw-bytes pass-through for `PgPending`
5. `kill -9` of walshadow mid-workload, restart, CH end-state matches
   non-interrupted run modulo merge transients. **Code-complete.**
   `tests/kill_restart.rs` exercises three kill strategies × five
   seeded LCG windows = 15 cycles, runtime skip-gated on PG / CH
   availability. `WALSHADOW_KILL_SEED` (default `0xC11AC11A`) seeds
   LCG for reproducibility
6. `pg_ctl restart` of shadow mid-workload, walshadow continues without
   operator intervention. **Live** via `ShadowCatalog` auto-reconnect
   + generation bump on reconnect

Acceptance tests (`tests/kill_restart.rs`, `pgbench_acceptance.rs`,
`bootstrap_direct_ch.rs`, `bootstrap_object_store_ch.rs`, `copy_into.rs`,
`truncate.rs`, `subxact.rs`, `add_column_default.rs`) are **not**
`#[ignore]`-gated; they runtime-skip when prerequisites (`initdb`,
`pg_basebackup`, `clickhouse`, `pgbench`) aren't on PATH. CI fixture
support for driving them end-to-end on PG 16/17/18 stays open work,
see [future/parked.md](future/parked.md)

## Deferred items

Tracked in [`plans/future/`](future/INDEX.md):

- **Sequence state.** Filter drops `RM_SEQ_ID`. Tables with `serial`
  PKs replicate values correctly via heap; downstream can't reconstruct
  `last_value`. CH-side synthetic `_sequence_value` if asked
- **Cross-table WAL ordering inside an xact.** Per-(table, xact)
  batching collapses interleaved writes across T1 / T2 into "all T1
  then all T2". End-state consistent via `_lsn` dedup; mid-drain
  readers see partial state
- **Two-phase commit.** `XLOG_XACT_PREPARE` ignored; `PREPARE` ↔
  `COMMIT PREPARED` across daemon restarts can lose prepared writes
- **CH-server-bounce recovery.** Bounded retry; expired budget kills
  daemon, cursor file resumes on restart

Speculative, not committed:
[future/shadow_schema_export.md](future/shadow_schema_export.md) (ship
shadow's catalog as DDL or hollow data dir) and
[future/sync_commit_witness.md](future/sync_commit_witness.md)
(walshadow as RPO=0 quorum acker under
`ANY 1 (walshadow, fullpg)`)
