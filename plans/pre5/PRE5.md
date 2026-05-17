# PRE5 — cleanup pass before the decoder chain

[Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix)
(heap tuple decoder + Tier 1/2 type matrix) needs four things to land
first. None are scoped large enough to be a phase of their own;
together they close out loose threads carried since
[Phase 1](PLAN.md#phase-1--wal-filter--crc-rewrite) and reshape the
filter into the streaming surface the decoder will sit on top of.

## Scope

| item | rationale | size |
|---|---|---|
| 1. Streaming filter event design | unifies `walshadow-filter` CLI (fixture) and the future live wal-rs feed under one record-event pipeline; gives [Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix) a place to consume parsed records without re-walking the segment | ~600 LOC |
| 2. `pg_class` heap-write decoding | closes a [Phase 1](PLAN.md#phase-1--wal-filter--crc-rewrite) gap: VACUUM FULL / REINDEX of non-mapped catalogs silently breaks the live filter set today | ~250 LOC |
| 3. `pg_filenode.map` attach-time bootstrap | seeds the catalog whitelist with the source's *current* mapping so a long-running source that already rotated a mapped catalog doesn't trip the bootstrap rule | ~120 LOC |
| 4. `XLOG_SWITCH` segment-boundary test | [Phase 1](PLAN.md#phase-1--wal-filter--crc-rewrite) deferral; cheap | ~60 LOC |

Sequencing: (3) first (cleanest, no shared structures), (4) anytime,
(2) before (1) so the catalog tracker is correct under the new event
pipeline, (1) last because it touches the most surface area.

## 1. Streaming filter event design

### Current shape

`filter_segment(bytes, name) -> (filtered_bytes, Manifest)` parses each
record internally, computes `Decision::{Keep,Drop}`, rewrites dropped
records to NOOP, returns the rewritten byte buffer. Parsed `XLogRecord`
values are thrown away after the rewrite decision. Caller is the
`walshadow-filter` CLI; one segment in, one segment out.

Two gaps:
* [Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix)'s decoder needs the parsed records the filter already
  produces. Re-parsing from the segment bytes is the obvious workaround
  but duplicates the page-stitching logic in `SegmentWalker` and means
  two divergent walks of the same bytes.
* Live operation needs the filter to consume wal-rs `START_REPLICATION
  PHYSICAL` chunks rather than a pre-materialised segment file. Chunks
  arrive at arbitrary offsets (not segment-aligned, can split a record
  header). The current `SegmentWalker::new(&[u8])` shape can't handle
  that incrementally.

### Target shape

One walker, three traits.

```rust
// Input: pushes WAL bytes, yields parsed records when complete.
pub struct WalStream { /* buffered tail + walker state */ }

impl WalStream {
    pub fn push(&mut self, chunk: &[u8]) -> impl Iterator<Item = Result<Record, WalkError>>;
    pub fn finish(&mut self) -> Result<(), WalkError>;  // assert no half-record left
}

// Per-record output of the streaming walker.
pub struct Record {
    pub parsed: XLogRecord,            // wal-rs parsed form
    pub logical_bytes: Vec<u8>,        // contiguous record bytes
    pub byte_ranges: Vec<(u64, usize)>,// (absolute_lsn, len) chunks where the record sits in source
    pub source_lsn: u64,               // start LSN
    pub page_magic: u16,
}

// Sink consumed by the orchestrator. One sink for shadow PG's
// pg_wal/ writer, one sink for the decoder, in parallel.
pub trait RecordSink {
    fn on_record(&mut self, rec: &Record, decision: Decision) -> Result<()>;
}
```

`WalStream::push` is the only place page-stitching and record
reassembly happens. `SegmentWalker`'s current logic (handles 3+-page
records, header-straddling boundaries) ports over with a buffered tail
to absorb partial input across chunk boundaries.

### Sink fan-out

Two production sinks:

* `ShadowWalSink` — writes record bytes (rewritten to NOOP if
  `Decision::Drop`) into shadow's `pg_wal/` segment files, rolling at
  segment boundaries. Owns the manifest sidecar too.
* `DecoderSink` ([Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix)) — receives `(rec, Decision::Drop)` for
  `RM_HEAP_ID` / `RM_HEAP2_ID` user records, hands them to the heap
  tuple decoder.

Filter test sinks: `CollectingSink` (records into a `Vec` for
assertion), `CountingSink` (`Decision` histogram only). Existing
`filter_segment` round-trip tests rewrite against `ShadowWalSink`
collecting into a `Vec<u8>` so the byte-for-byte comparison still works.

Orchestrator drives the walker, computes the decision once, broadcasts
to each sink. Records arrive in WAL order, so each sink sees a strictly
monotonic LSN sequence.

### LSN bookkeeping

LSN derivation moves from segment-offset-implicit to explicit on each
`Record`. `WalStream::new(start_lsn)` anchors the stream; each record's
`source_lsn` is computed as `start_lsn + start_offset - sum(page_headers
crossed)`. Fixture path passes the segment's filename-derived LSN;
live path passes the LSN wal-rs reports on the first byte of each
`CopyData` payload.

### Migration

* `src/filter_segment.rs` becomes a thin shim: build a `WalStream`,
  push all bytes at once, drive a `ShadowWalSink` collecting into a
  `Vec<u8>`. Existing CLI and tests keep working without change.
* `src/segment.rs` walker logic moves into `WalStream` internals. The
  current iterator-of-borrowed-slices API goes away; nothing outside
  `filter_segment` calls it directly.
* New module `src/wal_stream.rs` carries `WalStream`, `Record`,
  `RecordSink`, `Decision` (re-exported from `filter.rs`).
* `walshadow-filter` CLI unchanged externally.

### Tests

* Walker chunk-boundary stress: feed an existing fixture segment one
  byte at a time, assert the record sequence matches the bulk-push run.
* `ShadowWalSink` round-trip: feed [Phase 1](PLAN.md#phase-1--wal-filter--crc-rewrite) fixtures through the new
  pipeline, assert filtered bytes byte-for-byte match the old
  `filter_segment` output.
* `CollectingSink` against the OLTP fixture: assert decoder sink sees
  the right `RM_HEAP*` user records and zero catalog records.

### Out of scope for (1)

* wal-rs replication client wiring. [Phase 7](PLAN.md#phase-7--ch-native-emitter-via-clickhouse-c-rs) territory; PRE5 ships the
  `WalStream::push(&[u8])` surface ready to be driven by `CopyData`
  payloads, not the daemon that actually drives it.
* Segment-file output rolling beyond the fixture's single-segment case.
  `ShadowWalSink` writes to one in-memory `Vec<u8>` for now;
  multi-segment rolling lands when [Phase 7](PLAN.md#phase-7--ch-native-emitter-via-clickhouse-c-rs) needs it.

## 2. `pg_class` heap-write decoding

### Why

VACUUM FULL / REINDEX / CLUSTER on a **non-mapped** catalog
(`pg_depend`, `pg_namespace`, `pg_index`, `pg_constraint`, …) rewrites
the catalog to a fresh relfilenode `>= 16384`. PG records the new
filenode by UPDATEing the `pg_class` row for that catalog. No
`XLOG_RELMAP_UPDATE` fires — relmap is only for mapped catalogs.
Today's `CatalogTracker` counts these as `pg_class_writes_undecoded`
and otherwise does nothing; the post-rewrite WAL stream for that
catalog then looks like user-relation traffic and the filter drops it.

[PHASE1.md](PHASE1.md#pg_class-heap-write-tracking-is-best-effort)
marked this "Phase 3 closes the gap".
[Phase 3](PLAN.md#phase-3--shadow-pg-lifecycle) and
[Phase 4](PLAN.md#phase-4--catalog-cache-integration) both deferred.
With `ShadowCatalog` available, the missing piece is a narrow
heap-tuple decoder for `pg_class` rows.

### Implementation

`pg_class` itself is a mapped catalog, so its layout is stable and its
filenode is tracked via the relmap path. Heap-tuple decoder for one
fixed schema:

* New module `src/pg_class_decoder.rs`.
* Hardcode pg_class's column layout at the granularity needed: column
  ordinals for `oid`, `relname`, `relnamespace`, `relfilenode`. These
  are byte-stable across PG 16+; if they shift in a future major the
  unit test catches it via a fixture snapshot.
* Parse `XLogRecord` bodies for `RM_HEAP_ID / XLOG_HEAP_INSERT|UPDATE`
  and `RM_HEAP2_ID / XLOG_HEAP2_MULTI_INSERT` targeting the pg_class
  filenode (already known from the relmap). Walk `HeapTupleHeader`,
  honour the null bitmap, advance by column with PG type alignment
  rules, extract the four fields above.
* Feed `(oid, new_filenode, db_node)` to `CatalogTracker` as a fresh
  catalog entry. Old filenode entry is *not* removed — VACUUM FULL
  leaves the old heap intact until the transaction commits, and
  catalog tracker hits are upper-bounded by the cache eviction in
  [Phase 4](PLAN.md#phase-4--catalog-cache-integration) anyway.

### Tests

* Fixture: bootstrap a table, populate `pg_depend` indirectly via DDL,
  `VACUUM FULL pg_depend`, then DDL again to write to the new
  filenode. Capture WAL.
* Assert `CatalogTracker::is_catalog(db, new_filenode_of_pg_depend) ==
  true` after replaying the VACUUM FULL record.
* Negative test: a regular `INSERT INTO public.t` doesn't bump the
  catalog set (i.e., the decoder doesn't fire on non-pg_class
  filenodes).

### Out of scope

* General heap tuple decoder. That's
  [Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix)'s
  whole point. The pg_class-specific path here lives in isolation
  and can be deleted (or unified) when Phase 5's decoder generalises.
  PRE5 doesn't try to pre-emptively share code with Phase 5; the
  surface area is small enough that a parallel narrow implementation
  is cheaper than speculative abstraction.
* `pg_class` heap DELETEs (DROP TABLE flow). The catalog entry stays
  in the whitelist after the relation is gone; harmless since the
  filenode is no longer in use and filter just keeps already-zero-byte
  records targeting it. Address if it ever shows up in metrics.

## 3. `pg_filenode.map` attach-time bootstrap

### Why

Bootstrap rule today: `rel_node < FirstNormalObjectId` (16384) is
catalog. Holds at initdb because mapped catalogs initially live below
16384. Breaks if the source PG has already had `VACUUM FULL pg_class`
(or any mapped catalog) before walshadow attaches: the post-rewrite
filenode is `>= 16384`, the `XLOG_RELMAP_UPDATE` is buried in pre-attach
WAL we never see, and the filter drops records targeting the new
filenode until a fresh relmap update happens to fire (which could be
indefinitely).

### Implementation

Two paths considered.

**Path A — parse `pg_filenode.map` binary files.** PG layout: 512-byte
struct, magic `0x592717`, `num_mappings: int32`, up to 64 `(oid:
int32, filenode: int32)` pairs, trailing CRC32C. Read via libpq
`pg_read_binary_file('global/pg_filenode.map', 0, 512, true)` and
`pg_read_binary_file('base/<dbid>/pg_filenode.map', 0, 512, true)`.
Requires `pg_read_server_files` role on source.

**Path B — SQL query against source's pg_class.** Mapped catalogs have
`pg_class.relfilenode = 0`; `pg_relation_filenode(oid)` returns the
real filenode from the relmap. One query:

```sql
SELECT oid::int8, pg_relation_filenode(oid)::int8
FROM pg_class
WHERE relfilenode = 0;
```

Returns every mapped catalog (per-database + shared) with current
filenode. Run once against source via the libpq connection walshadow
already has (replication slot connection or a sidecar).

**Default: Path B.** No extra permissions, no binary format parsing,
result is the same set of `(oid, filenode)` pairs. The binary format
is stable but parsing it is more code than the SQL path with no upside.

Implementation:

* New `CatalogTracker::seed_from_source(client)` async method (libpq
  client passed in by caller; PRE5 does not own connection lifetime).
* Query the SQL above against source PG, populate `nodes` with
  `(db_node, filenode)` pairs. Shared catalogs (those whose
  `pg_class.relnamespace = 'pg_catalog'::regnamespace` AND
  `relisshared`) seed with `db_node = 0`.
* Called once at attach, before the `START_REPLICATION` cursor is
  set. Bootstrap rule (`< 16384`) still applies as a fallback for the
  non-mapped initial-relfilenode case.

### Tests

* Live integration: spin two source PG clusters, one with
  `VACUUM FULL pg_class` run before attach, one without. Seed the
  tracker from each, assert the rotated cluster's tracker contains a
  `>= 16384` filenode for pg_class and the fresh cluster's doesn't.
* Unit: stub libpq client returning a known result set, assert seeded
  `nodes` content matches.

## 4. `XLOG_SWITCH` segment-boundary test

[Phase 1](PLAN.md#phase-1--wal-filter--crc-rewrite) deferral. `XLOG_SWITCH` (rmgr `RM_XLOG_ID`, info `0x40`) zeroes
the rest of the current segment; the next record starts in the next
segment. Filter classifies it as `Keep` (special rmgr); the rewriter
passes through. No test covers the case today.

Test plan:

* New fixture under `fixtures/wal/xlog_switch/` (or extend the
  [Phase 1](PLAN.md#phase-1--wal-filter--crc-rewrite) filter fixture):
  workload triggers `SELECT pg_switch_wal()` mid-run,
  capture covers the segment containing `XLOG_SWITCH` plus the next
  segment.
* Round-trip through `WalStream` (the new pipeline from (1)): assert
  the `XLOG_SWITCH` record appears in the event sequence with
  `Decision::Keep`, its bytes are passed through unchanged, and the
  next record arrives at the expected LSN (first byte of the next
  segment's data area).

If the new pipeline lands first, this test sits in `tests/wal_stream.rs`
naturally; if (4) precedes (1), it lands against `filter_segment` and
gets ported.

## Out of scope

* Daemon binary. [Phase 7](PLAN.md#phase-7--ch-native-emitter-via-clickhouse-c-rs).
* Multi-segment `ShadowWalSink` file rolling. [Phase 7](PLAN.md#phase-7--ch-native-emitter-via-clickhouse-c-rs).
* General heap tuple decoder. [Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix).
* Per-relation catalog cache invalidation. Deferred per
  [PLAN.md risks](PLAN.md#risks--open-questions).
* `pg_index` join in `RelDescriptor`.
  [PHASE4.md](PHASE4.md#what-didnt-get-done) deferral; not on the
  decoder's hot path.

## Exit criteria

PRE5 closes when:

1. `cargo test --lib && cargo test --tests` clean against the new
   pipeline (existing fixtures still round-trip, new tests pass).
2. `cargo clippy --all-targets -- -D warnings` clean.
3. `walshadow-filter` CLI behaviour unchanged from a user's
   perspective (input segment → filtered segment + manifest sidecar).
4. `WalStream` is the only walker in the tree; `SegmentWalker` is
   either deleted or reduced to an internal detail of `WalStream`.
5. `CatalogTracker::pg_class_writes_undecoded` counter is gone (or
   pinned at zero for the VACUUM-FULL-on-non-mapped-catalog fixture).
6. `CatalogTracker::seed_from_source` covered by an integration test
   against a live source PG that rotated a mapped catalog before
   attach.

After PRE5, [Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix)
attaches a `DecoderSink` to `WalStream`, consumes
`(rec, Decision::Drop)` events for user heap records, and produces
`Tuple { rfn, xid, op, new, old }` per
[PLAN.md](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix).

## Estimate

Total: ~1,000 LOC across `src/` plus ~400 LOC of tests. Roughly two
phases worth of cleanup compressed into one prep pass; the streaming
walker (item 1) carries most of the weight.

## Files expected to change

```
src/wal_stream.rs                  new — streaming walker + sink trait
src/segment.rs                     gutted into wal_stream internals (or deleted)
src/filter_segment.rs              thin shim over wal_stream + ShadowWalSink
src/pg_class_decoder.rs            new — narrow heap-tuple decoder for pg_class
src/catalog_tracker.rs             wire in pg_class_decoder; add seed_from_source
src/bin/filter.rs                  follow shim-shape changes (likely zero churn)
tests/filter_round_trip.rs         retarget against wal_stream + ShadowWalSink
tests/wal_stream.rs                new — chunk-stress, XLOG_SWITCH, fan-out
tests/catalog_seed.rs              new — seed_from_source integration
fixtures/wal/xlog_switch/          new — pg_switch_wal() fixture
fixtures/wal/vacuum_full_catalog/  new — VACUUM FULL pg_depend fixture
PLAN.md                            status list: add PRE5 entry, note Phase 4b
                                   landed; minor "ran in parallel with Phase 5"
                                   wording fix
PRE5.md                            this doc
```
