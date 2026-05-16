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

- **Phase 0** — record-classification fixture. [PHASE0.md](PHASE0.md).
- **Phase 1** — WAL filter + CRC rewrite. [PHASE1.md](PHASE1.md).
- **Phase 2** — PG-16-minimum cleanup. [PHASE2.md](PHASE2.md).
- **Phase 3** — shadow PG lifecycle. [PHASE3.md](PHASE3.md).
- **Phase 4** — catalog cache integration. [PHASE4.md](PHASE4.md).
- **Phase 4b** — restart resilience. [PHASE4b.md](PHASE4b.md).
- **PRE5** — pre-Phase-5 cleanup: streaming filter pipeline
  (`WalStream`, `RecordSink`, `DirSegmentSink`), `SourceFeed`
  (`START_REPLICATION PHYSICAL` pump), `walshadow-stream` binary,
  `pg_class` heap-write decoder, `CatalogTracker::seed_from_source`
  bootstrap, `XLOG_SWITCH` pass-through test. [PRE5.md](PRE5.md).
- **clickhouse-c-rs** — vendored as workspace member. Provides the
  Native-wire emitter for Phase 7. Not gated by a `PHASE*.md`: the
  crate is upstream code, walshadow just consumes it.

Roadmap: Phases 5–10 as listed below (heap decoder → TOAST/xact →
CH emitter → DDL drill → Tier 3 + oracle → operational). Each phase
closes with `PHASE<N>.md` under `plans/`; the [status list](#status)
is the mutable index.

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
parallel once Phase 8 closes. Phase docs follow the `PHASE<N>.md`
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

Walk `RM_HEAP_ID` / `RM_HEAP2_ID` records the filter classifies as
`User`, project `HeapTupleHeader` + payload through a per-relation
descriptor from `ShadowCatalog`, emit a structured `Tuple { rfn, xid,
op, new, old }` per record. UPDATE/DELETE carry the old tuple under
`REPLICA IDENTITY FULL`; HOT updates collapse to the new image only
when no logged columns moved.

Type matrix covers Tier 1 (fixed-width: int2/4/8, float4/8, bool, date,
time, timestamp, timestamptz, uuid, oid, char) & Tier 2
(length-prefixed mechanical: bytea, text, varchar, name). Output Rust
value type is a fixed-width-friendly enum that maps 1:1 onto
`clickhouse-c-rs`'s `chc_col_kind` slots. Tier 3 (numeric, jsonb,
arrays, inet, interval, tsvector) lands in Phase 9 alongside the
oracle.

Size: ≈700 LOC.

### Phase 6 — TOAST reassembly + xact buffer

`HeapTuple` columns flagged `VARATT_IS_EXTERNAL_ONDISK` reference a
TOAST chunk relation; reassembly reads chunks from the same WAL stream
the decoder sees (no source-PG round-trip), keyed by
`(toast_relid, va_valueid)`. Chunks may arrive before or after the
referring tuple; buffer until both halves are present, flush on xact
commit.

Xact buffer holds per-xid records until `XLOG_XACT_COMMIT` / abort is
observed. Abort drops the buffer; commit flushes records in WAL order
to the emitter, tagged with `(source_lsn, xid, commit_ts)`. Cursor
file (`{data_dir}/cursor`) persists `(filter_lsn, decoder_lsn,
emitter_lsn)` atomically (write tmp + rename) on each commit drain.

Size: ≈600 LOC.

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

### Phase 9 — differential decode oracle + Tier 3 type matrix

Implement Tier 3 codecs (numeric, jsonb, arrays, inet, interval,
tsvector) & lock them down with the oracle. For each Tier 3 fixture
row, additionally probe shadow PG's typsend/typoutput & compare
against decoder output. Add `--validate` runtime mode (1-in-N
sampling, configurable). Captures a regression suite for codec edge
cases (numeric `NaN`, jsonb key ordering, array NULL bitmap layouts).

Size: ≈900 LOC.

### Phase 10 — operational

Slot keepalive (walshadow's physical slot on source must advance with
shadow's replay LSN, not the decoder's commit LSN — slot retention is
bounded by the slower of the two). Filtered `pg_wal/` trim. Metrics:
source LSN, filter LSN, shadow replay LSN, decoder commit LSN, CH ack
LSN. SIGHUP reload of table mapping. Shadow PG restart on PG-major
config change.

Size: ≈400 LOC.

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
   checksums on source and CH after walshadow drains.
2. A `VACUUM FULL` on a tracked table during the workload doesn't
   require operator intervention; CH state matches source within one
   merge cycle.
3. Shadow PG's `pg_last_wal_replay_lsn` lags source's
   `pg_current_wal_lsn` by less than 1 s of WAL bytes at steady state
   on the workload above.
4. `--validate` mode catches a planted decoder regression (e.g. a
   patched `numeric` codec that off-by-ones the dscale) on the first
   sampled row of the bad type.
5. `kill -9` of walshadow during the workload, restart, end-state on
   CH matches a non-interrupted run (modulo merge transients).
6. `pg_ctl restart` of shadow PG during the workload, walshadow
   continues without operator intervention, CH end-state matches a
   non-interrupted run.

(1)–(3) gate v1.0; (4)–(6) gate v1.1.
