# walshadow — schema-only Postgres as live catalog mirror for CDC

Physical-WAL → ClickHouse read replica with no logical-decoding plugin.
Source PG's physical WAL stream feeds two consumers in the daemon: a
co-located **shadow Postgres** that holds schema only and replays only
catalog WAL, and an in-tree decoder that turns user-heap records into
ClickHouse Native blocks. Shadow PG doubles as a live catalog oracle
for the decoder.

## Supported PostgreSQL versions

walshadow targets **PG 16+** as source-PG. Rationale:

* PG 15 reshuffled `bimg_info` FPI bits (commit `a14354c`). PG 16 is
  the first major where the new layout is the only layout we have to
  carry in code; nothing in walshadow needs the PG-14 dual-branch
  predicate.
* PG 16 stabilised RelFileLocator as the on-disk-identifier name
  (replacing RelFileNode). On-disk binary is unchanged but the source
  references this naming.
* PG 13 EOL Nov 2025; PG 14 EOL Nov 2026; PG 15 EOL Nov 2027. PG 16+
  matches the supported-source window for any greenfield deployment.

Shadow PG runs the same major as source — same constraint as before.
Source/shadow minor mismatches are fine.

The segment walker's technical floor is PG 15's page magic (0xD110),
i.e. the FPI bit-shuffle boundary. PG 15 captures are tolerated because
wal-rs's FPI dispatch keys off `magic >= 0xD110` and accepting PG 15
costs no extra code paths; "PG 16+" is the *operationally supported*
floor, not the technical one. PG ≤ 14 captures are rejected.

## Status

Plan-file index lives at [INDEX.md](INDEX.md).

Roadmap: Phases 5–12 as listed below (heap decoder → TOAST/xact →
CH emitter → DDL drill → Tier 3 + oracle → operational scaffolding →
durability + resume → backfill bridge). Phase 10 was originally
slated as the closing phase; the Phase 9 retro surfaced that v1.0
acceptance §5 (`kill -9` + restart → matching CH end-state) cannot
pass without a cursor file plus slot-advance gating on emitter-ack
LSN — split out as Phase 11. The absence of an initial-snapshot
path forces Phase 12. Each phase closes with `PHASE<N>.md` under
`plans/`; [INDEX.md](INDEX.md) is the mutable index.

Reuses without modification:

- `~/s/wal-rs` Phase D `START_REPLICATION PHYSICAL` client, slot
  keepalive, TLS, SCRAM. walshadow consumes WAL bytes from this layer.
- `clickhouse-c-rs` (workspace member under `clickhouse-c-rs/`). Rust
  bindings to clickhouse-c's Native wire client: raw block
  encode/decode plus the full TCP packet loop (Hello / Query / Data /
  EOS / Exception / Progress) with LZ4 / ZSTD. walshadow's emitter
  builds blocks via `BlockBuilder` & ships them over `Client`.
- `~/s/walhouse/rust-hack` informally as a reference for off-disk
  parsing ergonomics. Not a runtime dependency.

## Why a shadow Postgres

A static-catalog snapshot (the simpler alternative — bootstrap pg_class
/ pg_attribute / pg_type / pg_index once at start, never refresh)
forces three concessions:

1. Operator coordinates every DDL: pause the daemon, run DDL on both
   sides, re-bootstrap, resume.
2. Relfilenode rewrites (`VACUUM FULL`, `CLUSTER`, `REINDEX`,
   `ALTER TABLE ... SET TABLESPACE`) aren't observable without an
   external signal.
3. No in-tree oracle when the decoder disagrees with PG on Tier 3
   values (numeric edge cases, jsonb canonicalisation, array layout).

A second Postgres process sitting next to wal-rs, with schema only and
WAL-driven catalog, fixes all three at a bounded cost:

1. **Live catalog.** DDL on source writes heap records into
   `pg_class` / `pg_attribute` / `pg_type` / `pg_index` / `pg_depend`.
   Replay those records into shadow PG and `pg_catalog` stays current
   with zero operator coordination. Decoder queries catalog via libpq
   SQL connection to shadow PG, re-issued on cache invalidation.
2. **Relfilenode rewrites.** `pg_class.relfilenode` changes ride the
   same heap WAL into shadow PG. Decoder's `RelFileLocator → relation`
   index follows automatically.
3. **Decode oracle.** Shadow PG runs the same binary that wrote source
   WAL. typsend / typoutput functions are right there over libpq — pipe
   decoded values back through them for differential coverage.

Price: one extra `postgres` process, schema-sized data directory
(usually MiB), plus the CPU to filter and CRC-rewrite WAL records.
Catalog WAL is a small fraction of total WAL (no user heap, no user
indexes, no FPI churn from heavy writes), so the steady-state load is
DDL-rate-bound, not data-rate-bound.

## What "replay only catalog" filters in

Source WAL stream is a single byte feed touching every relfilenode in
the cluster: user heap, user indexes, system catalogs, toast tables,
sequences, multixact, clog, xlog control records. Shadow PG only needs:

| rmgr | kept records | reason |
|---|---|---|
| `RM_HEAP_ID`, `RM_HEAP2_ID` | record's `RelFileLocator` in catalog set | DDL writes catalog rows |
| `RM_BTREE_ID` | record's relation is a catalog index | catalog SELECT plans |
| `RM_RELMAP_ID` | all | shared-catalog relfilenode rewrites |
| `RM_XACT_ID` | all | commit / abort visibility |
| `RM_CLOG_ID`, `RM_MULTIXACT_ID` | all | xact status for catalog tuples |
| `RM_STANDBY_ID` | all | recovery housekeeping |
| `RM_XLOG_ID` | checkpoint, nextoid, parameter-change | recovery plumbing |

Everything else (user heap, user indexes, FPIs for non-catalog blocks,
sequences for user counters, gin, spgist, brin) is dropped at the
filter.

Catalog set is derived once at bootstrap from `pg_class WHERE oid <
FirstNormalObjectId` (16384) on a freshly-initdb'd shadow, captured as
the relfilenode whitelist. Shared catalogs (`global/`) carry `dbNode =
0`; included unconditionally.

## Filter implementation: rewrite over fork

Two paths considered.

**Path A — rewrite WAL before recovery.** Per record: parse header,
walk `XLogRecordBlockHeader` entries, decide keep / drop / placeholder.
If at least one block ref is catalog-relevant, emit a synthesized
record with kept blocks only and a recomputed CRC32C. Otherwise emit
an `XLOG_NOOP` of identical `xl_tot_len` so subsequent records'
`xl_prev` chain stays valid. Shadow PG runs as a standby pointed at
walshadow's filtered `pg_wal/` directory via `restore_command`; no
`primary_conninfo`. Unmodified upstream PG binary.

**Path B — patch the recovery dispatcher.** Inject a relfilenode
whitelist into `~/s/postgresql/src/backend/access/transam/xlog.c` so
records targeting un-whitelisted nodes skip their `rm_redo` callback.
Maintained PG fork.

**Default: Path A.** Maintaining a PG fork is permanent; CRC rewrite
is one-time spend, and CRC32C on x86 SSE4.2 is ~1 ns/byte — irrelevant
next to network and recovery cost. Reconsider only if measurement
proves otherwise.

In practice no heap WAL record touches more than one relation, so the
"keep some blocks, drop others" case doesn't arise for `RM_HEAP*` —
the record is either fully kept or fully dropped. Multi-block records
exist in B-tree split / heap multi-insert / xact_commit subxact lists,
and for those the keep/drop is uniform (catalog or not).

## Architecture

```
                  +--------------------+
                  |  Source Postgres   |
                  |  wal_level=logical |
                  |  + write workload  |
                  +---------+----------+
                            | START_REPLICATION PHYSICAL
                            v
       +----------------------------------------+
       |  walshadow daemon                      |
       |                                        |
       |  wal-rs replication client             |
       |          |                             |
       |          v                             |
       |    +-----------+                       |
       |    | record    |                       |
       |    | classifier|---catalog-keep---+    |
       |    +-----------+                  |    |
       |          |                        v    |
       |          | user-rel        +-----------+
       |          v                 | CRC       |
       |    +-----------+           | rewrite   |
       |    | heap      |           +-----+-----+
       |    | decoder   |                 |      |
       |    +-----+-----+                 v      |
       |          |                +------------+|
       |          |        catalog | shadow     ||
       |          |        SQL <---| postgres   ||
       |          |                | (recovery) ||
       |          v                +------------+|
       |    +------------+                       |
       |    | chc-rs     |                       |
       |    | emitter    |                       |
       |    +-----+------+                       |
       +----------|-----------------------------+
                  v
            +-----------+
            | ClickHouse|
            +-----------+
```

Two writers, one reader. Filter writes shadow's `pg_wal/`; decoder
reads catalog from shadow via libpq. The decoder's catalog-read must
lag the filter's WAL write so shadow has already replayed the relevant
DDL. Cheap to enforce: every xact commit on the source is observed by
both paths, decoder gates on `pg_last_wal_replay_lsn() >= commit_lsn`
on shadow before reading catalog for that xact's user records.

## Decoder catalog interface

Async libpq client to shadow PG, generation counter, replay-LSN gate.
Landed under `src/shadow_catalog.rs` (Phase 4 / 4b).

```rust
pub struct ShadowCatalog {
    /* libpq SQL conn to shadow PG, generation counter, cached entries */
}

impl ShadowCatalog {
    pub async fn relation_at(
        &self,
        rfn: RelFileLocator,
        at_lsn: Lsn,
    ) -> Result<&RelDescriptor, Error>;
    /* blocks until pg_last_wal_replay_lsn(shadow) >= at_lsn, then
       fetches pg_class/pg_attribute/pg_type/pg_index for rfn if not
       cached, caches keyed by (rfn, generation). On commit-LSN observed
       to write into pg_catalog relfilenodes, bumps generation. */
}
```

LRU bounded by configured `shadow_catalog_cache_max`. Cache miss path
is a small SQL fan-out (one query, four joins); not on the per-record
hot path because each `RelFileLocator` is looked up at most once per
generation.

## "In/out" decode oracle

Tier 1/2 codecs are mechanical byte-shuffles; offline fixtures are
enough. Tier 3 — `numeric`, `jsonb`, arrays, `inet`, `interval`,
`tsvector` — is where decoders disagree with PG on edge cases.

Shadow PG closes the loop without a source-PG round-trip:

1. Fixture row inserted into source PG (or replayed from a captured
   WAL fixture).
2. walshadow decoder produces a Rust value V.
3. Re-encode V into PG wire form (inverse codec).
4. Probe shadow PG: `SELECT $1::bytea::<typ>::text` for the typoutput
   string; compare against PG's text form for the same input.
5. For symmetric coverage: `SELECT $text::<typ>::bytea`, decode bytes
   with our codec, compare to V.

(4) catches encode bugs, (5) catches decode bugs. Both lean on shadow
PG's typsend / typrecv / typinput / typoutput, which are version-pinned
to the same PG major that wrote the source WAL.

A `--validate` runtime mode samples 1-in-N live tuples and pipes them
through (4) before CH emit. Off by default; cost is one extra SQL
round-trip per sampled row.

## ClickHouse side

Emitter is built on `clickhouse-c-rs` (workspace member). Two paths
exposed by that crate cover both the prod & test/CI ingestion modes:

- **TCP `Client`**: `INSERT INTO t FORMAT Native` against a remote
  server; LZ4 compression on by default. Used in production.
- **Block frame over `PosixIo`**: pipe `BlockBuilder` output into
  `clickhouse local --input-format Native`. Used in fixtures &
  smoke tests where spinning a full server is overkill.

Type matrix (PG OID → CH `TypeAst`) lives in-tree under `src/type_map.rs`
(Phase 7). `_lsn` synthetic column carries **source** LSN (not shadow
LSN), so CH `ReplacingMergeTree` dedup survives walshadow restarts &
cursor rewinds.

## Pitfalls

### 1. Shared catalogs live in `global/`

`pg_database`, `pg_authid`, `pg_tablespace`, `pg_shdepend` carry
`dbNode = 0`. Filter keeps these unconditionally — shadow won't start
cleanly without them.

### 2. CLOG / multixact

Catalog replay needs xact-status records to mark catalog tuples
visible. Keep `RM_CLOG_ID` and `RM_MULTIXACT_ID` wholesale. Cheap, tiny
volume.

### 3. Catalog index bloat

Shadow PG is in recovery, autovacuum suspended. A busy DDL workload
churns `pg_class` / `pg_attribute` rows, bloating their indexes.
Mitigations (in order of operator preference): periodically restart
shadow (drops cache, resumes recovery from cursor); accept it
(catalog stays MiB-scale); promote shadow briefly to allow autovacuum
to run, then re-attach as standby. Default: accept it; revisit when
measured to hurt.

### 4. wal_level on source

Catalog-only replay only needs `wal_level=replica`. Decoder for user
tables still needs `logical` for old-tuple on UPDATE/DELETE. Net
requirement on source: `wal_level=logical` plus
`REPLICA IDENTITY FULL` on every replicated table.

### 5. Source DDL that rewrites a user table

`ALTER TABLE ... ADD COLUMN ... DEFAULT 'x'` (PG < 11) or any
`ALTER TYPE` that triggers a rewrite emits catalog records *and*
millions of user heap records in one xact. Shadow PG sees the catalog
half; decoder sees the full xact. Ordering invariant (shadow replay
LSN ≥ decoder read LSN) ensures decoder reads the post-DDL catalog
shape for the heap records. Fast-path `ADD COLUMN` (always present
since PG 11, baseline for PG 16+) skips the rewrite entirely; no
user heap records, only catalog — trivial case.

### 6. Two-phase commit

`PREPARE TRANSACTION` then `COMMIT PREPARED` minutes later. Shadow PG
holds the catalog xact in `pg_prepared_xacts`; decoder buffers user
records keyed by gxid. `COMMIT PREPARED` drops the shadow prepare &
flushes the buffer to the CH emitter.

### 7. Shadow PG version skew

Shadow PG must be the same major as source, and source must be PG 16
or newer (see "Supported PostgreSQL versions" above). Different
minor is fine. Daemon refuses to start on major mismatch or on
source-PG < 16 with a precise error.

### 8. Path A CRC at very high WAL rates

CRC32C at 1 ns/byte = 1 s/s of CPU at 1 GB/s WAL — single-core.
Records are independent, so record-level parallelism via a small thread
pool is trivial. Defer the thread pool until a measurement asks for it.

### 9. Source primary failover

Replication slot doesn't follow a failover. Operator either pre-creates
a slot on the standby, or accepts a re-bootstrap from a new LSN with a
snapshot bridge. Catalog state is preserved on shadow PG across the
re-attach, so only user-heap backfill is lost — the decoder can keep
serving from cache while the new physical stream catches up to the old
slot's LSN.

### 10. Shadow PG `pg_wal/` retention

Filtered WAL accumulates in shadow's `pg_wal/`. Trim on
`pg_last_wal_replay_lsn` advance, retain a configurable window for
debug replay.

## Phasing

Each phase produces an independent slice. Phase 1, 3, 4, then the
decoder chain (5→6→7) are sequential; Phase 2 ran in parallel with
Phase 3; Phase 4b ran in parallel with Phase 5; Phase 9 & 10 are
parallel once Phase 8 closes. Phase 11 sequences after Phase 10
(the cursor schema rides on the metrics + standby-status surfaces
Phase 10 lands). Phase 12 is parallel with Phase 11 — different code
paths, no shared state. Phase docs follow the `PHASE<N>.md`
convention from `~/s/wal-rs`.

### Phase 0 — record-classification fixture

Capture WAL via `pg_receivewal` against a Postgres running a mixed
DDL + DML workload (`pgbench -i` plus a few `ALTER TABLE` cycles).
Classify each record's `RelFileLocator` set into catalog / user /
special. Confirm catalog fraction is bounded (expect well under 1%
for realistic workloads).

Deliverable: numbered fixture under `fixtures/wal/classify/` plus a
small CLI printing kept/dropped split per rmgr.

Size: ~200 LOC.

### Phase 1 — WAL filter + CRC rewrite

Per-record parse → keep/drop decision → synthesize new bytes →
recompute CRC32C → write to a `pg_wal/`-style segmented directory with
a (filtered_lsn, source_lsn) manifest sidecar. Round-trip test: feed
the filtered stream back through wal-rs's `WalParser`, assert it
parses cleanly and the kept records' decoded tuples match the
unfiltered baseline.

Scope additions surfaced by Phase 0:

- `CatalogTracker`: consume `RM_RELMAP_ID` plus heap writes targeting
  `pg_class` to keep the catalog relfilenode whitelist live after
  `VACUUM FULL` / `REINDEX` / mapped-relation rewrites. Phase 0's
  whitelist is bootstrap-only and goes stale on the first such event.
- `main_data` reclassification for the `Empty` class: records like
  `XLOG_HEAP_VISIBLE`, `XLOG_HEAP_FREEZE_PAGE`, btree vacuum carry
  their target relation in `main_data`, not in block refs. Phase 0
  buckets them as `Empty`; Phase 1 must crack the rmgr-specific
  payload to keep/drop correctly.
- Workload upgrade: Phase 0's DDL-heavy fixture pins catalog fraction
  at 85–95%. Phase 1 ships a steady-state OLTP fixture (`pgbench -T
  30 -c 8` per acceptance §1) and re-tightens the integration-test
  bound from `< 0.99` toward "well under 1%".

Prereq landed: wal-rs `WalParser` now reads `bimg_info` version-aware
off `XLogPageHeader.magic` (PG ≤ 14 vs PG ≥ 15 bit shuffle from
`a14354cac`). Captures from PG 15+ parse cleanly; no longer a Phase 1
blocker.

Size: ≈600 LOC.

### Phase 2 — PG-16-minimum cleanup

Codify the "Supported PostgreSQL versions" banner in code. Reject
PG ≤ 14 captures at the segment walker; rename the `XLP_PAGE_MAGIC_PG15`
constant to `XLP_PAGE_MAGIC_MIN` to surface its new role (FPI-layout
floor, doubles as the minimum-accepted magic).

PG 15 captures are tolerated, not rejected: wal-rs's FPI dispatch
already keys off `magic >= 0xD110`, and there's no extra code to write
to accept PG 15. "PG 16+" is the operationally supported floor; PG 15
is the technical floor.

Concrete changes:

- `walshadow/src/wire.rs`: rename `XLP_PAGE_MAGIC_PG15` →
  `XLP_PAGE_MAGIC_MIN` (value unchanged at 0xD110). No PG16 constant
  introduced; the supported-version banner is policy, not a wire-level
  predicate.
- `walshadow/src/segment.rs`: add `WalkError::UnsupportedSourceVersion`;
  reject pages whose magic is below `XLP_PAGE_MAGIC_MIN`.
- `walshadow/fixtures/wal/{classify,filter}/capture.sh`: default
  `WALSHADOW_PG_IMAGE` to `postgres:16`. Reject `WALSHADOW_USE_LOCAL=1`
  when local `postgres -V` major < 15.
- Upstream wal-rs (out-of-tree, tracked separately): drop
  `BKP_IMAGE_IS_COMPRESSED_PG14`, collapse `is_compressed(page_magic)`
  to a single `info & BKP_IMAGE_COMPRESS_MASK_PG15 != 0` predicate,
  remove `WalParser::page_magic`'s PG-14 default. walshadow can vendor
  the relevant constants in `wire.rs` until wal-rs lands the cleanup;
  the version-aware predicate path becomes dead-code-eliminable.

Risk: zero against shadow PG behavior — reader-side only. Existing
round-trip + classifier fixtures re-capture cleanly against
`postgres:16`.

Size: ~30 LOC walshadow + ~20 LOC docs. Can run parallel with
Phase 3.

### Phase 3 — shadow PG lifecycle

`initdb` once at bootstrap, restore schema-only dump from source.
Write `recovery.signal` and a `restore_command` pointing at
walshadow's filter output directory. `pg_ctl start`, wait for
`pg_is_in_recovery() AND pg_last_wal_replay_lsn() >= initial_target`.
Health probe: periodic `SELECT count(*) FROM pg_class` and a one-row
`SELECT relname FROM pg_class WHERE oid = 'pg_proc'::regclass`.

Size: ≈400 LOC.

### Phase 4 — catalog cache integration

Landed `walshadow::shadow_catalog`: async tokio-postgres client to
shadow PG, generation counter, `relation_at(rfn, at_lsn)` gating on
shadow's `pg_last_wal_replay_lsn`. Decoder relfilenode→relation lookup
goes through this cache. See `src/shadow_catalog.rs` &
[PHASE4.md](PHASE4.md).

Size: ≈300 LOC.

### Phase 4b — restart resilience

`ShadowCatalog` from Phase 4 holds a single `tokio_postgres::Client`;
once the underlying connection drops, every subsequent call returns
`connection closed` forever. Shadow PG bounces (operator-initiated
`pg_ctl restart`, OOM kill recovered by systemd, kernel signal)
become hard failures of the walshadow daemon, which is excessive
given shadow PG's own crash recovery handles the WAL side cleanly.

Scope:

- Auto-reconnect inside `ShadowCatalog`. On a closed-connection
  error from `client.query*`, transparently rebuild the client
  (`tokio_postgres::connect` + driver `spawn`) and retry once.
- Top-level retry policy for transient unavailability ("the database
  system is starting up", "could not connect"): exponential backoff
  capped at `replay_timeout`. Sits outside `ShadowCatalog` so the
  cache machinery stays unaware of retries.
- Generation bump on every successful reconnect. Catalog mutations
  landing during the down window may not produce an `invalidate`
  call from the upstream catalog tracker, so all cache entries are
  treated as stale on reconnect.
- `last_replay_lsn` reset on reconnect — previous high-water mark is
  meaningless against a freshly-restarted PG instance.

Out of scope:

- Shadow PG process supervision. Production runs shadow PG under
  systemd; walshadow does not own the postgres process lifetime past
  Phase 3's bootstrap path.
- Reconnect for the sync `Shadow` probe path. `psql_one` shells out
  fresh per call; existing error propagation on transient failures
  is correct.

### Phase 5 — heap tuple decoder + Tier 1/2 type matrix

Prerequisites, each landed as its own commit ahead of decoder work:

* [FPI_COMPRESSION.md](FPI_COMPRESSION.md). User-heap records carry
  full-page images on first-modification-after-checkpoint; tuple
  bytes live inside the FPI when `XLOG_HEAP_INIT_PAGE` rides along.
  Compressed-FPI sources (`wal_compression = pglz|lz4|zstd`) are
  common in production. Silent skip is not acceptable, so the
  primitive lands first.
* Async `RecordSink` + `SegmentSink`. Both trait methods flip to
  `async fn` shape (`Pin<Box<dyn Future + Send + 'a>>` desugaring for
  dyn-compat); `WalStream::push` / `WalStream::close` /
  `flush_current` flip with them. Reason for `RecordSink`: decoder
  calls `ShadowCatalog::relation_at` (tokio-postgres) on the hot
  path, and the alternatives — synchronous libpq side-channel, or
  shunting records onto an mpsc with a separate consumer task —
  either duplicate the catalog state or decouple decoder cadence
  from segment cadence. Reason for `SegmentSink`: 16 MiB segment
  writes through `DirSegmentSink` are sync filesystem I/O that
  blocks the calling tokio worker until the write returns;
  `walshadow-stream` runs `worker_threads = 2`, so the block parks
  half the pool. Flipped to `tokio::fs::write` (internally
  `spawn_blocking`) so ctrl_c, status timer, invalidation drain
  stay responsive while the write is in flight. Mechanical lift: `Metrics`, `Collecting`,
  `Counting`, `Composite` record sinks, `Collecting` + `Dir`
  segment sinks, plus the per-record + per-segment dispatch in
  `WalStream`. Affects `bin/stream.rs`'s `WalStream::push` call
  site and the eight existing integration tests that drive sinks
  directly.
* `ReplIdent::Default` carries resolved primary-key attnums.
  Today's `ReplIdent::Default` is a unit variant; the decoder
  needs `pg_index.indkey` where `indisprimary = true` to interpret
  `XLH_UPDATE_CONTAINS_OLD_KEY` under `relreplident='d'`. Extend
  the variant to `Default { pk_attnums: Option<Vec<i16>> }`
  (`None` when the table has no PK), resolved at descriptor build
  alongside the existing `UsingIndex` lookup. Single-query lift
  inside `fetch_replident`.

Walk `RM_HEAP_ID` / `RM_HEAP2_ID` records the filter classifies as
`User`, project `HeapTupleHeader` + payload through a per-relation
descriptor from `ShadowCatalog`, emit a structured `Tuple { rfn, xid,
op, new, old }` per record. HOT updates collapse to the new image
only when no logged columns moved (`XLH_UPDATE_HOT` set, no
`XLH_UPDATE_CONTAINS_OLD_*`). UPDATE/DELETE old-tuple decode honours
every `relreplident` variant the catalog exposes:

| `relreplident` | old payload | Phase 5 behaviour |
|---|---|---|
| `Full` (`'f'`) | every non-dropped column | decode full old tuple, every attnum populated |
| `UsingIndex { key_attnums }` (`'i'`) | indexed columns only | decode subset; non-indexed attnums emit as `None` in `old` |
| `Default { pk_attnums }` (`'d'`) | PK columns when a key column moved or DELETE | decode subset when `XLH_UPDATE_CONTAINS_OLD_KEY` is set; `old = None` when the bit is clear; `pk_attnums = None` (table without PK) → `old = None` always |
| `Nothing` (`'n'`) | empty | `old = None`; emitter routes UPDATE/DELETE to the skip-on-update set with a stat counter so silent loss is visible |

Type matrix covers Tier 1 (fixed-width: int2/4/8, float4/8, bool, date,
time, timestamp, timestamptz, uuid, oid, char) & Tier 2
(length-prefixed mechanical: bytea, text, varchar, name). Output Rust
value type is a fixed-width-friendly enum that maps 1:1 onto
`clickhouse-c-rs`'s `chc_col_kind` slots. Tier 3 (numeric, jsonb,
arrays, inet, interval, tsvector) lands in Phase 9 alongside the
oracle.

**Rollback status, explicit.** Phase 5 has no xact buffer. The
decoder emits `Tuple` eagerly the moment the heap record arrives,
without waiting for the matching `XLOG_XACT_COMMIT` /
`XLOG_XACT_ABORT`. Consequences:

* Aborted xacts produce ghost rows downstream. PG writes user-heap
  WAL ahead-of-write even when the xact subsequently aborts;
  walshadow sees those records in WAL order, the decoder emits
  tuples, the abort arrives later as `RM_XACT_ID` and Phase 5 has
  no path to retract. Downstream CH ReplacingMergeTree dedups on
  `_lsn` so a daemon restart won't double the ghosts, but they
  persist as permanent rows in the target table until Phase 6
  lands.
* No commit ordering. Multi-statement xacts emit per-statement in
  WAL order, not as one atomic batch at commit. A reader querying
  CH mid-xact (impossible against PG without dirty reads) sees
  partial state.
* `_commit_ts` cannot be populated. The synthetic column
  ([PLAN.md Phase 7](#phase-7--ch-native-emitter-via-clickhouse-c-rs))
  is `NULL` for Phase 5 emissions; Phase 6 fills it from the
  commit record at flush time.

The `Tuple { xid }` field carries the bridge so Phase 6's xact
buffer keys on `xid` and flushes / drops without an interface
change. Phase 5 e2e tests pin themselves to auto-commit single-
statement workloads to sidestep ghost emission; any test that
exercises rollback must wait for Phase 6.

Size: ≈700 LOC decoder + ~150 LOC async-sink refactor + Default-PK
fetch ~30 LOC. FPI_COMPRESSION's ~400 LOC ships under its own
commit, separately accounted.

### Phase 6 — TOAST reassembly + xact buffer

Landed: per-xid buffer holds every [`DecodedHeap`] plus
`(toast_relid, value_id, chunk_seq)` TOAST chunks from first heap
touch until matching `XLOG_XACT_COMMIT` / `XLOG_XACT_ABORT`. Commit
drains in WAL order with `ExternalToast` columns reassembled into
`Bytea` / `Text` (pglz + lz4 decompression paths). Abort discards
the buffer plus any spill file. Largest-xact-first eviction mirrors
PG `ReorderBufferLargestTXN`. Local-disk spill primitive lands in
the same commit; `{data_dir}/spill/xid-<xid>-<first_lsn>.bin` per
xact, atomic-rename on writer finish, unlink on abort. Spill dir
wiped at startup per "drained or replayable from cursor" contract.

`XactRecordSink` observes `RM_XACT_ID` records: COMMIT /
COMMIT_PREPARED drain, ABORT / ABORT_PREPARED drop, PREPARE keeps
the buffer alive for COMMIT_PREPARED. `BufferingDecoderSink`
replaces Phase 5's direct-emit `DecoderSink` in the production
dispatch chain: user-heap records park in the buffer, TOAST inserts
(`rel.kind == 't'`) reinterpret as chunks keyed on the toast
relation's pg_class OID.

CH-as-scratch was considered and rejected on commit-drain latency +
bandwidth doubling + MergeTree part hygiene; v1 has no
`spill_backend` knob — the diskless walshadow path is future work
with a fresh config-surface decision at that point. Design +
comparison: [PHASE6disk.md](PHASE6disk.md). Retro:
[PHASE6.md](PHASE6.md).

Size delivered: ~2380 LOC (`src/spill.rs` 825, `src/xact_buffer.rs`
1102, `tests/xact_buffer.rs` 451, daemon wiring ~40). Source-only
sizing lands at ≈900 LOC matching PHASE6disk's estimate. Tests: +11
unit (5 spill + 6 xact_buffer catalog-free) plus +7 live-shadow-PG
integration tests in `tests/xact_buffer.rs`.

Detoast catalog access is direct via
[`ShadowCatalog`](../src/shadow_catalog.rs) at drain — no per-xact
descriptor cache, the catalog's own LRU covers repeat lookups.

Deferred to Phase 7 / followups: cursor file
(`(filter_lsn, decoder_lsn, emitter_lsn)` atomic write — needs the
CH emitter's ack), subxact lineage, `XLOG_HEAP2_MULTI_INSERT`
fan-out, full record→decoder→buffer chain integration test for
`BufferingDecoderSink`. See [PHASE6.md "Followups"](PHASE6.md#followups).

### Phase 7 — CH Native emitter via clickhouse-c-rs

Translate per-relation `Tuple` streams into `BlockBuilder` calls. One
`Client` per CH replica, INSERT statement issued lazily on first row
for a destination table; subsequent rows accumulate into a block until
either the row-count budget or the byte budget trips, then
`send_data(Some(&bb))` flushes & a fresh builder is started. End of
xact closes the INSERT with `send_data(None)` so each xact lands as
a single CH block group, matching the dedup model.

Schema mapping: source `RelDescriptor` (from `ShadowCatalog`) →
destination table name + per-column `TypeAst`. Mapping config lives in
the TOML config (`[table."public.foo"] target = "default.foo"` etc.).
`_lsn UInt64`, `_xid UInt32`, `_op Enum8('insert'=1,'update'=2,
'delete'=3)`, `_commit_ts DateTime64(6)` are appended synthetically.

LZ4 by default; ZSTD opt-in via feature flag passed through to
`clickhouse-c-rs`. Exception packets from the server propagate as
`Error::Exception` to walshadow's top-level retry loop.

Size: ≈500 LOC.

### Phase 8 — end-to-end DDL drill

Source script: `CREATE TABLE t (...)`, `INSERT INTO t ...`,
`ALTER TABLE t ADD COLUMN c int DEFAULT 7` (fast-path), `UPDATE t SET
c = c + 1`, `DROP TABLE t`. walshadow + decoder + CH emitter run the
whole script unmodified. CH end-state matches source end-state.

Size: ≈200 LOC of test glue.

### Phase 9 — differential decode oracle + Tier 3 hot types

Landed as a hybrid. `numeric` / `inet` / `cidr` / `interval` decoded
locally in [`src/codecs.rs`](../src/codecs.rs); `jsonb`, arrays,
`tsvector`, ranges, custom domains, every other long-tail Tier 3
type surface as `ColumnValue::PgPending` and resolve at emit time
via a `walshadow_oracle` shadow-PG extension exposing
`walshadow_decode_disk(oid, bytea) -> text` (reconstructs a Datum
from raw on-disk bytes and runs `typoutput`). Extension is optional
— absent shadow extension makes the emitter fall back to writing
raw on-disk bytes; no failure, no operator action required. The
[`Oracle`](../src/oracle.rs) module hosts the libpq bridge, a
lock-free 1-in-N sampler, and an `OracleObserver` wrapper that
rewrites `PgPending` → `Text` before the inner observer sees the
tuple. `walshadow-stream --validate <N>` switches on the sampler.
Retro: [PHASE9.md](PHASE9.md).

Size delivered: ~2050 LOC (Rust 1380, C 125, regress 249, CI 32,
docs the rest). Followups: local codecs for jsonb / arrays if
measurement shows the libpq round-trip is hot; sampler
auto-tuning; mismatch ring buffer for debugging.

### Phase 10 — operational scaffolding

Renamed from "operational" (PLAN's original closing phase) to
"operational scaffolding" once the Phase 9 retro made clear that the
load-bearing slot-advance + cursor work belongs in its own phase
([Phase 11](#phase-11--durability--resume)). Phase 10's scope is the
surrounding plumbing the daemon needs to be observable + reloadable +
safely connected; it lands the *shape* of standby-status reporting
but doesn't change *what value* the daemon advances the slot to —
that flip is Phase 11.

Scope:

- **Pre-flight validators at connect.** Daemon refuses to start on
  source `server_version_num < 16` (PLAN §"Supported PostgreSQL
  versions" + §"Pitfall #7"), shadow / source major mismatch,
  source `wal_level != 'logical'`, any mapped relation without
  `REPLICA IDENTITY FULL`, `--slot` set against a non-existent
  slot. Precise error per failure mode, no silent-skip fall-throughs.
- **HTTP / Prometheus metrics surface.** Source received LSN,
  filter LSN, shadow replay LSN, decoder commit LSN, CH ack LSN
  (the last two are Phase 11 outputs but the surface lands here so
  Phase 11 only adds the values, not the endpoint). Per-rmgr
  kept/dropped counters, xact-buffer occupancy, spill activity,
  oracle sampler stats — everything the stderr status line already
  carries.
- **`tracing` subscriber pipeline.** `source_feed.rs`'s
  `tracing_debug` stub today drops every `tracing::debug!` call site
  wal-rs offers. Wire `tracing_subscriber::EnvFilter` so
  `RUST_LOG=walshadow=debug` surfaces frame-level diagnostics.
- **Filtered `pg_wal/` trim on shadow replay advance.** Segments
  shipped to shadow's restore-command dir accumulate forever today.
  Trim drops segments older than `pg_last_wal_replay_lsn() -
  retention_window`. Retention defaults to 1 hour; configurable.
- **Standby-status update shape.** `send_status` in
  [`source_feed.rs`](../src/source_feed.rs) currently collapses
  `(write_lsn, flush_lsn, apply_lsn)` to one value (filter
  position). Phase 10 splits the three fields and parameterises
  them; Phase 11 fills in the resume-safe values.
- **SIGHUP reload of `--ch-config`.** Table mapping is boot-only
  today. SIGHUP re-parses the TOML and swaps the live mapping
  (atomic via `ArcSwap` or equivalent).
- **Shadow PG major-version restart.** A `postgresql.conf` change
  that requires a shadow restart (rare; mostly memory knobs) needs
  the daemon to drive `pg_ctl restart` rather than die on connection
  drop. Phase 4b's reconnect already handles the libpq side; this
  is the supervisor side.
- **CH emitter retry/reconnect.** Today one `Exception` packet from
  the CH server kills the daemon. Bounded retry with exponential
  backoff against a single replica; multi-replica fan-out remains
  Phase 7-followup, not Phase 10.

Out of scope:
- Slot-advance value (Phase 11 territory; Phase 10 ships the
  reporting shape but not the policy).
- Cursor file (Phase 11).
- Initial snapshot (Phase 12).

Size: ≈600 LOC (revised up from PLAN's original 400 once the HTTP
surface + pre-flight validators + tracing pipeline + CH-emitter
retry are accounted).

### Phase 11 — durability + resume

Cursor file plus slot-advance gating on emitter-ack LSN. Today's
daemon advances the source slot to `stream.dispatched_lsn()` (filter
position) which is unsafe: a committed xact whose `XLOG_XACT_COMMIT`
record has been filtered but not yet drained from the xact buffer
into CH is lost on `kill -9` + restart, because source has been
given permission to recycle that WAL and the spill dir is wiped on
every startup ([`xact_buffer`](../src/xact_buffer.rs) +
[`spill`](../src/spill.rs)'s "drained-or-replayable-from-cursor"
invariant, where the cursor has never existed). Acceptance §5
cannot pass under Phase 10's state.

Scope:

- **Cursor file** under `{spill_dir}/cursor.bin` (sibling to spill
  files so a `mv` of the working dir keeps state coherent). Schema:
  `version: u32`, `source_received_lsn: u64`,
  `filter_durable_lsn: u64`, `shadow_replay_lsn: u64`,
  `drain_lsn: u64`, `emitter_ack_lsn: u64`. Atomic-rename writer,
  fsync, written on every emitter-acked xact drain.
- **Slot advance** keyed on `emitter_ack_lsn`, not
  `dispatched_lsn`. Phase 10's standby-status split is the
  carrier: `write_lsn = source_received_lsn`,
  `flush_lsn = filter_durable_lsn`,
  `apply_lsn = min(shadow_replay_lsn, emitter_ack_lsn)`. wal-rs's
  `build_status_update` already takes three LSNs; the daemon side
  threads the right values through.
- **Filter durability.** `tokio::fs::write` on segment seal is
  rename-after-write but doesn't fsync the directory or the segment
  file. Phase 11 adds explicit fsync so `filter_durable_lsn` is
  honest — without it walshadow's "I flushed" ack to source is a
  lie under a host power loss.
- **Startup resume path.** Read cursor at boot; if present, resume
  WAL stream from `cursor.emitter_ack_lsn` rounded down to a segment
  boundary. Replay any spill files keyed on xids whose first-seen
  LSN > `cursor.emitter_ack_lsn` (drained-but-not-acked xacts get
  re-emitted; CH dedup on `_lsn` collapses them). If cursor missing
  (greenfield boot), fall back to today's `--start-lsn` / source
  `pg_current_wal_lsn` behaviour.
- **Audit follow-up from [PHASE8 §"Bug 2"](PHASE8.md).** The
  `BlockBuilder` `Allocator` is unpinned today — fine because the
  builder doesn't outlive its construction frame. Phase 11's emitter
  refactor for budget-triggered mid-xact flush (the followup
  [PHASE7 §"Budget-triggered mid-xact flush"](PHASE7.md) defers)
  may stretch the builder lifetime across awaits; pin it
  `Pin<Box<Allocator>>` like `Client` if so.

Out of scope:
- Multi-replica fan-out cursor (single-CH-replica deployment; per-
  replica acks are independent surface).
- Per-source-shard cursor (single-source-daemon assumption holds).
- Two-phase-commit cursor entries (`PREPARE` xacts can sit
  arbitrarily long; cursor handling defers to the same followup
  that builds proper 2PC support).

Size: ≈500 LOC.

### Phase 12 — backfill bridge

Initial snapshot path for pre-existing source data. Today
`START_REPLICATION PHYSICAL` only sees post-attach WAL — any row
inserted on source before the daemon's first connect is invisible to
CH forever. Greenfield CH deployments against a non-empty source
need a backfill bridge.

Two shapes, both leaning on the existing per-table emitter:

- **Source-direct COPY** (default). `COPY (SELECT * FROM <rel>) TO
  STDOUT BINARY` against source PG, per mapped relation, under a
  `pg_export_snapshot()` shared with the replication slot's
  start-LSN export. walshadow's emitter consumes the COPY stream
  through the same `ColumnValue` → CH encoding the WAL hot path
  uses, ships as one block group per relation under a synthetic
  `_lsn = backfill_lsn` (sub-segment minimum across the export
  snapshot's xmin). CH `ReplacingMergeTree(_lsn)` collapses
  backfill rows with future WAL-driven updates.
- **Shadow-as-source** (operator opt-in). Same shape but COPY
  against shadow. Requires shadow to carry user-heap data files
  ([BASEBACKUP](BASEBACKUP.md) Use Case 1B / 2A), not the MiB-
  scale catalog-only shape Phase 3 ships. Trade-off: zero source
  CPU + IO during backfill, at the cost of shadow data-dir size.

Sequencing primitive: backfill completes at LSN `B` (the snapshot's
`pg_export_snapshot()` LSN); daemon's `--start-lsn` for the WAL pump
is `B` so the backfill's snapshot and the WAL tail meet seamlessly.
Mirrors `pg_dump`'s parallel-dump co-ordination.

Out of scope:
- DDL during backfill — operator must quiesce DDL for the backfill
  window. A mid-backfill DDL means re-snapshotting the affected
  relation.
- Backfill restart mid-flight. v1 is single-shot; on failure,
  truncate CH dest + retry.
- Per-relation back-pressure against active WAL. Backfill takes
  whatever bandwidth the CH emitter is willing to accept;
  the WAL pump's xact buffer holds back-pressure in parallel.

Size: ≈700 LOC (one-shot COPY orchestrator + per-relation encoder
reuse + snapshot co-ordination + retry semantics).

## Known correctness gaps

Surfaced during the Phase 9 retro / Phase 10 re-plan. Each is silent
loss or quiet skew today, not visible to a metrics watcher. Listed
separately from "Risks" because the decision to ship is "yes,
deferred", not "open question".

1. **`XLOG_HEAP2_MULTI_INSERT` silently dropped.**
   [`heap_decoder.rs:370-373`](../src/heap_decoder.rs) returns
   `Ok(None)` for every `RmId::Heap2` op (Phase 5 punt). `COPY` and
   `INSERT INTO ... SELECT` paths emit `MULTI_INSERT`; their rows
   never reach CH. Surfaces as row-count mismatch on any workload
   that uses COPY-into. Defer to a Phase 7-followup once per-tuple
   offset fan-out lands; until then, document as "COPY-into a
   tracked table is unsupported, use multi-row INSERT".
2. **Subxact lineage collapsed.** Phase 6 ships top-level-xact-only;
   `XLOG_XACT_ASSIGNMENT` is ignored. `ROLLBACK TO SAVEPOINT`
   mid-xact still lands every pre-savepoint write in CH at commit.
   ORMs that wrap each statement in a savepoint (Django,
   Hibernate, Rails) emit ghost rows under exception paths.
   Phase-6-followup; non-blocking for §1 acceptance because pgbench
   doesn't use savepoints.
3. **`TRUNCATE` not replicated to CH.**
   [`main_data.rs:15-19`](../src/main_data.rs) recognises
   `XLOG_HEAP_TRUNCATE` for filter-keep purposes but the decoder
   does not route it through to the emitter. CH dest accumulates
   stale rows after a source `TRUNCATE`.
4. **PG read-time defaults (`atthasmissing` + `attmissingval`).**
   PHASE8 schema-evolution drill pins this: a pre-ALTER row, read on
   source after `ALTER TABLE ADD COLUMN c int DEFAULT 7`, shows
   `c = 7` because PG injects the missing default at read time.
   walshadow's decoder reads the physical tuple bytes and emits
   NULL. Acceptance §1 (`ALTER TABLE ... ADD COLUMN ... DEFAULT k`)
   **fails today** when checksum compares the post-ALTER reader's
   view against CH. Decoder needs to consult
   `pg_attribute.atthasmissing` + `attmissingval` at decode time.
5. **Sequence state not replicated.** Filter drops `RM_SEQ_ID`
   (not in the catalog-keep table — PLAN's "What 'replay only
   catalog' filters in"). CH never observes `nextval()` advances.
   Tables with `serial` PKs replicate row values correctly, but a
   downstream consumer cannot reconstruct source's sequence
   `last_value`. Worth a CH-side synthetic `_sequence_value` per
   tracked sequence if measurement demands.
6. **Cross-table WAL ordering inside an xact** ([PHASE7 §"Cross-
   table ordering inside an xact"](PHASE7.md)). Per-(destination
   table, xact) batching collapses interleaved writes across T1 / T2
   into "all T1, then all T2". Foreign-key invariants between T1
   and T2 in CH readers can see partial state mid-drain. `_lsn`
   dedup keys correctly so end-state is consistent.
7. **Two-phase commit handling.** `XLOG_XACT_PREPARE` is ignored;
   the xact stays buffered until `COMMIT_PREPARED` lands.
   `PREPARE` followed by an arbitrarily long pause leaves the spill
   file alive across multiple daemon restarts (where it gets wiped
   per Phase 11's startup contract) — losing the prepared xact's
   writes. PLAN §"Pitfall #6" names the design; no code today.
8. **CH-server-bounce recovery.** Phase 10 lands bounded retry but
   not a "re-emit the last drained xact's block from spill replay";
   if the retry budget expires the daemon dies and Phase 11's
   cursor resumes from `emitter_ack_lsn`. Operationally fine; named
   for completeness.

## Risks & open questions

* **Recovery loop performance on shadow.** Recovery is single-threaded
  on PG ≤ 17 (parallel-recovery patch landed in PG 18 for hot-standby
  only, not crash recovery). DDL-bound replay is comfortable; if a
  workload ever DDLs faster than shadow can replay, the source has
  bigger problems. Document as a limit, not a fix.
* **Catalog cache invalidation granularity.** Bumping a single
  generation counter on any catalog write over-invalidates. A finer
  scheme (per-relation invalidation keyed on which catalog row was
  touched) is possible but parses every catalog write — defer until a
  workload makes it matter.
* **Filter ↔ decoder ordering near boundaries.** Decoder reading at
  LSN X gates on shadow replay ≥ X. If shadow stalls (recovery loop,
  I/O hiccup), decoder stalls. Blast radius is bounded by the WAL
  retention window plus the cursor file's last commit LSN — surface
  the gap (filter LSN − shadow replay LSN) in metrics.
* **Shadow PG restart mid-flight.** `pg_ctl restart` or a
  systemd-driven restart drops `ShadowCatalog`'s libpq connection.
  Phase 4b handles via transparent reconnect plus generation bump;
  without that layer the daemon needs an external restart.
* **Differential oracle false positives.** PG's typoutput for some
  types is locale-dependent (`numeric` thousands separators aren't,
  but `to_char` formatting paths are). Pin shadow's `lc_numeric` and
  `lc_time` at bootstrap to known values; document.
* **Path A CRC at >1 GB/s WAL.** Measure before parallelising.
* **PG fork temptation.** Path B keeps surfacing because Path A's
  rewrite "feels heavy". Resist until measured.

## Acceptance criteria

walshadow passes when, with `wal_level=logical` and `REPLICA IDENTITY
FULL` on source:

1. A 30-second `pgbench -T 30 -c 8` workload intermixed with one
   `ALTER TABLE ... ADD COLUMN ... DEFAULT k` (fast-path) and one
   `CREATE INDEX CONCURRENTLY` produces matching row counts &
   checksums on source and CH after walshadow drains. *Gated on
   [Phase 12](#phase-12--backfill-bridge) (pgbench's pre-workload
   `pgbench -i` data lands via the backfill bridge, not WAL) plus
   [Known correctness gaps §4](#known-correctness-gaps) (read-time
   defaults must replicate before `ADD COLUMN ... DEFAULT k` passes
   checksum).*
2. A `VACUUM FULL` on a tracked table during the workload doesn't
   require operator intervention; CH state matches source within one
   merge cycle. *Live today through the Phase 4/4b catalog cache +
   relfilenode-rewrite handling.*
3. Shadow PG's `pg_last_wal_replay_lsn` lags source's
   `pg_current_wal_lsn` by less than 1 s of WAL bytes at steady state
   on the workload above. *Live today; depends on filter throughput
   not on any unfinished phase.*
4. `--validate` mode catches a planted decoder regression (e.g. a
   patched `numeric` codec that off-by-ones the dscale) on the first
   sampled row of the bad type. *Live through Phase 9's oracle.*
5. `kill -9` of walshadow during the workload, restart, end-state on
   CH matches a non-interrupted run (modulo merge transients).
   *Gated on [Phase 11](#phase-11--durability--resume) — fails today
   because the source slot is advanced to filter-position, not
   emitter-ack, so committed-but-not-acked xacts vanish after
   restart.*
6. `pg_ctl restart` of shadow PG during the workload, walshadow
   continues without operator intervention, CH end-state matches a
   non-interrupted run. *Live through Phase 4b's auto-reconnect.*

(1)–(3) gate v1.0; (4)–(6) gate v1.1. v1.0 cannot ship until
Phase 11 (for §5's `kill -9` resume) + Phase 12 (for §1's
pgbench initial data) land. v1.0 also needs [Known correctness
gaps §4](#known-correctness-gaps) (read-time defaults) closed
before §1's `ADD COLUMN ... DEFAULT k` checksum compares cleanly.
