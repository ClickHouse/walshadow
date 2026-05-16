# PRE5b — close PRE5 silent-correctness gaps before Phase 5

[PRE5](PRE5.md) landed with `cargo test --lib && cargo test --tests`
clean (66 + 18 tests, 0 ignored) and `cargo clippy --all-targets -- -D
warnings` clean. The surface is fine. Four items beneath it did not
actually wire into the production path, plus a handful of foundation
gaps that
[Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix)
will hit on its first day.

The B-items are correctness regressions that compound silently: the
daemon emits a manifest, the filter stats look reasonable, the round-
trip tests still pass, but the catalog whitelist underneath has been
wiped or the cached shape is stale. The moment Phase 5's `DecoderSink`
consumes the resulting `Record` stream, the gaps surface as data
corruption against source PG, found late (Phase 8 DDL drill or Phase 9
oracle).

## Scope

| item | category | rationale |
|---|---|---|
| B1. Lift `Filter` out of per-segment scope | correctness | `CatalogTracker` state thrown away every 16 MiB |
| B2. Wire `seed_from_source` in `walshadow-stream` | correctness | PRE5 item 3 exists in tests only |
| B3. `pg_class_decoder` UPDATE prefix/suffix compression | correctness | mis-parses every `VACUUM FULL pg_<non-mapped>` |
| B4. `CatalogTracker` → `ShadowCatalog::invalidate` | correctness | descriptor cache silently stales on first DDL |
| S1. Widen `RecordEvent` to a real `Record` | foundation | event lacks `XLogRecord`, `main_data`, `rfn`, `xact_id` |
| S2. Sink fan-out | foundation | one `RecordSink` per `WalStream::push` |
| S3. `ShadowCatalog` concurrency story | foundation | spec'd `&self`, ships `&mut self` |
| S4. `RelDescriptor` `relreplident` + `pg_index` | foundation | mandatory for UPDATE/DELETE old-tuple decode |
| L1. `walshadow-stream` shutdown + memory hygiene | operational | unbounded `CollectingRecordSink`, no `close()` on signal |
| L2. Smaller debts | hygiene | `pub mod segment`, untested classes, missing fixtures |

Sequencing: B-items first (each independently shippable). S1 before S2
(sink trait shape changes with `Record`). S3/S4 anytime. L-items last.

## B1. `Filter` lifetime

### Why

`filter_segment.rs:38` constructs `let mut filter = Filter::new()`
inside the function called once per segment. `WalStream::flush_current`
(`wal_stream.rs:308`) and `WalStream::close` invoke it. Every RELMAP
update, every successful pg_class heap-write decode, every bootstrap
state dies at segment N's end. Segment N+1's first record runs against
an empty `CatalogTracker`.

Fully masked in tests: every fixture under `fixtures/wal/` is a single
segment, and every round-trip test calls `filter_segment` against
bytes that include both the catalog-learning record and the records
that depend on it. No test crosses a segment boundary.

### Implementation

* `Filter` ownership moves to `WalStream`. `WalStream::new` constructs
  a `Filter`; `flush_current` borrows `&mut filter` from `self`.
* `filter_segment` grows a `filter: &mut Filter` parameter. The
  standalone constructor inside it goes away.
* CLI `walshadow-filter` (`src/bin/filter.rs`) owns its own `Filter`,
  passes it in. Single-segment input → same observable output as
  today.
* `ManifestStats` stays per-segment. `FilterStats` on the long-lived
  `Filter` rolls cumulative across the stream; `walshadow-stream`
  surfaces cumulative numbers in its per-segment log line.

### Tests

* New: two consecutive synthetic segments. Segment 1 carries an
  `XLOG_RELMAP_UPDATE` introducing a `>= 16384` pg_class filenode;
  segment 2 carries a heap record targeting that filenode. Assert the
  segment-2 record is `Decision::Keep`. Today it would be
  `Decision::Drop`.

## B2. `seed_from_source` wiring

### Why

`CatalogTracker::seed_from_source` (`src/catalog_tracker.rs:205`) is
called only from `tests/catalog_seed.rs:108,184,220`. `src/bin/stream.rs:146`
issues `START_REPLICATION` without seeding. PRE5 item 3's whole
purpose, closing the pre-attach mapped-catalog-rotation hole, does
nothing in production.

### Implementation

* `SourceFeed` exposes a sidecar libpq `tokio_postgres::Client` for
  the same `PgConfig` minus `replication=true`. Replication-mode
  connections can't run `pg_class` queries cleanly; a second
  connection is the cheapest correct path. Opened lazily on first
  `seed_from_source` call.
* `walshadow-stream` calls `tracker.seed_from_source(&sql_client)`
  after `IDENTIFY_SYSTEM`, before `START_REPLICATION`.
* Snapshot consistency: `IDENTIFY_SYSTEM` does not expose a snapshot.
  The seed query runs against the source's current catalog. If a
  rotation finalized before the seed, the seed already covered it. If
  a `XLOG_RELMAP_UPDATE` fires between seed and replication-start, the
  WAL stream re-adds it. No special coordination needed beyond
  ordering seed-then-START_REPLICATION.
* `--start-lsn` users still seed (idempotent on `HashSet`).

### Tests

* Strengthen `tests/catalog_seed.rs:144-190`: loop
  `VACUUM FULL pg_class` until `pg_relation_filenode(1259) >= 16384`.
  Today's test silently passes when the post-rewrite filenode stays
  low.
* New integration: `walshadow-stream --max-segments=1` against a
  source whose pg_class was rotated above 16384 pre-attach. Assert no
  records targeting the rotated filenode appear as `Decision::Drop`
  in the manifest.

## B3. `pg_class_decoder` prefix/suffix compression

### Why

PG's `heap_update` (`~/s/postgresql/src/backend/access/heap/heapam.c:8984-9036`)
writes UPDATE block-0 data as:

```
+- uint16 prefixlen   (if XLH_UPDATE_PREFIX_FROM_OLD) -+
+- uint16 suffixlen   (if XLH_UPDATE_SUFFIX_FROM_OLD) -+
+- xl_heap_header (5 bytes)                           -+
+- bitmap [+ padding] [+ oid] (t_hoff - 23 bytes)     -+
+- column data starting at reconstructed tuple        -+
+- offset t_hoff + prefixlen                          -+
```

Suffix is symmetric: trailing column bytes that match the old tuple
are omitted. Recovery at `heap_xlog_update` reconstructs by copying
prefix/suffix from the old page.

`pg_class_decoder.rs:90-110` does

```rust
let t_hoff = block_data[XL_HEAP_HEADER_SIZE - 1] as usize;
```

i.e. reads `block_data[4]` as `t_hoff`. Wrong whenever any prefix or
suffix flag is set on `xl_heap_update.flags`, which lives in
`record.main_data`, not in block data.

`VACUUM FULL` on a non-mapped catalog (`pg_depend`, `pg_namespace`,
`pg_constraint`, `pg_index`) updates `pg_class.relfilenode` for that
catalog's row. pg_class cols 1–7 occupy 88 bytes, all unchanged; PG's
prefix-compute (`heapam.c:8904-8915`) yields `prefixlen ≈ 88` (modulo
MAXALIGN). The OID column lands inside the prefix and never appears
in the WAL record.

Consequence: every `VACUUM FULL pg_<non-mapped>` produces a pg_class
WAL record whose `(oid, relfilenode)` the decoder cannot extract.
`pg_class_writes_undecoded` ticks silently;
`CatalogTracker::is_catalog(db, new_filenode)` stays `false`;
subsequent WAL targeting the rotated catalog gets classified as User
and dropped. PRE5 exit criterion 5 (counter pinned at zero) currently
holds only because no fixture exercises the case. Unit test
`pg_class_heap_update_adds_post_vacuum_full_filenode`
(`catalog_tracker.rs:391`) passes with synthetic block data that omits
prefix compression.

### Implementation

Two parts.

**Part 1 — read `xl_heap_update.flags` from `main_data`.** Lift the
decoder to take `record: &XLogRecord, block_idx: usize` instead of
`block_data: &[u8]`. For `HEAP_UPDATE` / `HEAP_HOT_UPDATE` (info masks
0x20 / 0x40), the `main_data` `xl_heap_update` layout
(`SizeOfHeapUpdate = 13`) is `old_xmax(4) + old_offnum(2) +
old_infobits_set(1) + flags(1) + new_xmax(4) + new_offnum(2)`. Read
byte 7 as `flags`.

* `XLH_UPDATE_PREFIX_FROM_OLD = 0x20`,
  `XLH_UPDATE_SUFFIX_FROM_OLD = 0x40`.
* `skip = (prefix ? 2 : 0) + (suffix ? 2 : 0)`.
* Strip leading `skip` bytes from `block_data` before reading
  `xl_heap_header`.
* Capture `prefixlen` from the stripped uint16 for column-offset
  arithmetic.

HEAP_INSERT (info 0x00) carries no prefix/suffix; existing arithmetic
stays.

**Part 2 — column offset with prefix.** Column data starts at
*reconstructed* tuple offset `t_hoff + prefixlen`, not `t_hoff`.
pg_class col 1 (oid) sits at reconstructed offset `t_hoff`. If
`prefixlen >= 4`, OID is entirely in the prefix and unrecoverable from
the WAL record alone. Three cases:

* `prefixlen == 0`: current arithmetic correct.
* `0 < prefixlen < 4`: OID overlaps the boundary; reject as
  unrecoverable.
* `prefixlen >= 4`: OID in prefix. Path A would maintain a
  `(pg_class_block, offset) → oid` index seeded from
  `seed_from_source` and resolve via `xl_heap_update.new_offnum`. Path
  B increments a new counter `pg_class_writes_oid_in_prefix` and
  leaves the catalog set unchanged for that record. **Default to Path
  B**: failure is bounded (next DDL touching the affected catalog
  re-emits without prefix-on-OID), Path A duplicates state the source
  PG already has, and the rotation can be observed via the
  forthcoming `XLOG_RELMAP_UPDATE` for any subsequent index/relation
  touch on shared catalogs.

### Tests

* Fixture: capture WAL from `VACUUM FULL pg_depend` on a live source.
  Replay through `walshadow-filter`; assert
  `tracker.pg_class_writes_oid_in_prefix` ticks and
  `tracker.pg_class_writes_undecoded` does not.
* Unit: synthesise `XLogRecord`s with `XLH_UPDATE_PREFIX_FROM_OLD` set
  in `main_data` and prefixlen uint16 in `block_data`. Cover
  `prefixlen ∈ {0, 2, 4, 88}` plus `suffixlen ∈ {0, 4}`. Assert decode
  succeeds for small prefix, returns the "oid in prefix" sentinel for
  `prefixlen >= 4`.
* Positive: `prefixlen == 0 && suffixlen > 0`. Confirm decode succeeds
  using stripped suffix bytes (suffix never overlaps the OID column).

### Out of scope

* General heap decoder with prefix/suffix handling.
  [Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix)
  inherits the same constraint for user heap; the pg_class-specific
  path stays narrow.

## B4. `CatalogTracker` → `ShadowCatalog::invalidate`

### Why

`ShadowCatalog::invalidate` (`src/shadow_catalog.rs:213`) is called
only from `tests/shadow_catalog.rs:218`. The module doc at
`shadow_catalog.rs:18-21` claims an upstream caller; none exists.
[Phase 4b](PHASE4b.md)'s "generation bump on commit-LSN observed to
write into pg_catalog relfilenodes" never wired.

Cached `RelDescriptor`s go stale at the first DDL on shadow and stay
stale until shadow PG bounces.
[Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix)
calling `relation_at(rfn, commit_lsn)` will hand back pre-DDL column
shape for post-DDL records.

### Implementation

* `CatalogTracker` carries an optional
  `tokio::sync::mpsc::UnboundedSender<()>`. Sync side; async side runs
  a small drain task that calls `cat.invalidate()` per signal,
  coalescing adjacent signals if backpressure permits.
* On `handle_relmap_update` and on `harvest_pg_class_blocks` paths
  where `pg_class_writes_decoded` ticks, send one signal. Both paths
  know they touched the catalog set; granularity stays coarse (bump
  generation), matching [PLAN.md](PLAN.md#risks--open-questions)'s
  intentional over-invalidation.
* No signal sent when the tracker has no consumer attached
  (offline CLI use of `walshadow-filter`).

### Tests

* New `tests/shadow_catalog.rs` case: spin live PG, populate one user
  table, fetch `relation_at`. Issue `ALTER TABLE ... ADD COLUMN` via
  SQL helper. Send the invalidation signal (or drive it end-to-end via
  the channel). Re-fetch and assert the new column appears in
  `RelDescriptor.attributes`.

## S1. `Record` shape

### Why

`RecordEvent` (`wal_stream.rs:80-92`) carries `source_lsn, len, rmid,
info, decision`.
[Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix)'s
`Tuple { rfn, xid, op, new, old }` needs the parsed `XLogRecord` (for
`main_data` and per-block payload), the `RelFileLocator` (for
`relation_at` lookup), and `xact_id`. `filter_segment` already parses
all of this (`filter_segment.rs:46`) to feed `Filter::decide`, then
discards it; the `Manifest` is written as flat scalars. A `RecordSink`
consumer must re-parse from segment bytes to get back what was just
thrown away.

[PRE5.md:58-64](PRE5.md) spec'd the right shape (`Record { parsed:
XLogRecord, logical_bytes, byte_ranges, source_lsn, page_magic }`) and
the contract was watered down during PRE5 implementation. PRE5b
closes the gap, with one simplification (drop `logical_bytes` and
`byte_ranges`,
[Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix)'s
decoder consumes the parsed form, not raw bytes).

### Implementation

```rust
pub struct Record {
    pub parsed: XLogRecord,
    pub source_lsn: u64,
    pub page_magic: u16,
    pub decision: Decision,
}
```

`parsed.header.{resource_manager_id, info, xact_id}`,
`parsed.blocks[i].header.location.rel`, `parsed.main_data` cover every
field a downstream consumer needs.

* `filter_segment` returns `(Vec<u8>, Manifest, Vec<XLogRecord>)`
  (parses-once contract). `Manifest` continues to be the
  on-disk-serializable view; `Vec<XLogRecord>` is the in-memory
  hand-off to the `RecordSink`.
* `WalStream::flush_current` zips parsed records with manifest entries
  by index, dispatches `Record` to `RecordSink::on_record(&Record)`.

### Tests

* Update `tests/wal_stream_e2e.rs` to assert
  `Record.parsed.header.resource_manager_id`,
  `Record.parsed.blocks[0].header.location.rel`,
  `Record.parsed.header.xact_id` directly.
* New: a fixture xact's records (all bracketed by one BEGIN…COMMIT)
  all carry the same `xact_id`.

## S2. Sink fan-out

### Why

`WalStream::push` takes `&mut dyn RecordSink` singular
(`wal_stream.rs:249`).
[Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix)'s
`DecoderSink` runs alongside `SegmentSink` (the `ShadowWalSink`);
metrics may want a third tap.

### Implementation

```rust
pub struct CompositeRecordSink {
    pub inner: Vec<Box<dyn RecordSink + Send>>,
}

impl RecordSink for CompositeRecordSink {
    fn on_record(&mut self, r: &Record) -> Result<(), SinkError> {
        for s in &mut self.inner {
            s.on_record(r)?;
        }
        Ok(())
    }
}
```

Short-circuits on the first `Err`. Tuple `impl<A, B> RecordSink for
(A, B)` could land alongside as a zero-alloc convenience for tests;
not required.

### Tests

* `CompositeRecordSink` of `CollectingRecordSink + CountingRecordSink`,
  push one segment, assert both observers see the full event sequence
  in order.
* Error propagation: one inner sink returns `Err` on a specific
  record. Assert `push` returns `Err` and document the post-error
  state explicitly (see L2 on `next_lsn` rollback).

## S3. `ShadowCatalog` concurrency

### Why

`relation_at` (`shadow_catalog.rs:361`), `relation_by_oid`,
`wait_for_replay`, `invalidate` all take `&mut self`. PLAN.md:217
specified `&self`. Single-tasked use today is fine; future emitter
and oracle want concurrent lookups.

### Decision

Defer the interior-mutable refactor. PRE5b wraps the catalog in
`Arc<tokio::sync::Mutex<ShadowCatalog>>` at the daemon level so
[Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix)'s
call shape works without surgery, and tracks the refactor as a
follow-up. Rationale: optimising lock contention before the lookup-
rate hot path exists is speculative; the cache-hit path is cheap
enough that single-task serialisation dwarfs nothing measurable yet.

### Implementation

* Daemon binary holds `Arc<Mutex<ShadowCatalog>>`. Pass clones to
  every component that touches the cache.
* Internal `ShadowCatalog` API unchanged.
* Module doc at `shadow_catalog.rs:18-21` updated to reflect the
  `&mut self` reality and the planned refactor.

### Tests

* Add to `tests/shadow_catalog.rs`: hold the mutex across
  `relation_at` from one task, await from another, confirm clean
  serialisation (no `would deadlock` panic, no hang). Sanity-check
  the wrap, not the lock-free path that isn't built yet.

## S4. `RelDescriptor` `relreplident` + `pg_index`

### Why

[Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix)
needs `pg_class.relreplident` to decide whether
`XLH_UPDATE_CONTAINS_OLD_TUPLE` / `XLH_UPDATE_CONTAINS_OLD_KEY` are
expected on UPDATE/DELETE and how to interpret the old-tuple payload.
[PRE5.md:299-301](PRE5.md) deferred `pg_index` join with "not on the
decoder's hot path", which is true for *value* decoding, false for
*identity* decoding under `REPLICA IDENTITY USING INDEX`. First
non-FULL table trips this.

### Implementation

```rust
pub enum ReplIdent {
    Default,
    Nothing,
    Full,
    UsingIndex { index_oid: u32, key_attnums: Vec<i16> },
}

pub struct RelDescriptor {
    /* existing fields */
    pub replident: ReplIdent,
}
```

* Extend `fetch_by_filenode` SQL (`shadow_catalog.rs:445`) to select
  `c.relreplident`.
* When `relreplident = 'i'`, second query against `pg_index` filtered
  by `indrelid = $relation_oid AND indisreplident = true`; pull
  `indexrelid` and `indkey`. Cache alongside `RelDescriptor`.
* Other values map to `Default` / `Nothing` / `Full`. `n` (nothing) is
  legal on user tables; decoder must surface it to Phase 5 so the
  emitter can drop UPDATE/DELETE rows that would have no key.

### Tests

* Live: create three tables with `REPLICA IDENTITY DEFAULT`, `FULL`,
  `USING INDEX <name>`. Fetch each, assert the enum variant matches.
* For `UsingIndex`, assert `key_attnums` matches the index's column
  list.

## L1. Daemon hygiene

### Why

* `bin/stream.rs:151` uses `CollectingRecordSink::default()` in the
  production binary. `Vec<Record>` grows forever; long-running daemon
  OOMs on its own success.
* `bin/stream.rs:189` `break` exits the loop, function returns without
  calling `WalStream::close()`. Partial segment vanishes despite
  `bin/stream.rs:22-24` claiming "writes the current partial segment
  (if any)". No SIGINT/SIGTERM handler.
* `src/source_feed.rs:184` drops `server_wal_end`; `:209-215`
  `tracing_debug` is a no-op stub.

### Implementation

* Replace `CollectingRecordSink` in `bin/stream.rs` with a
  `MetricsRecordSink` that maintains counters per `(rmid, decision)`
  and discards events. Periodic print on segment emit.
* `tokio::select!` between `feed.next_chunk(...)` and
  `tokio::signal::ctrl_c()`. On signal: drop out of the loop, call
  `stream.close(Some(&mut segment_sink), &mut record_sink)`,
  flushing the partial as `.partial` per `DirSegmentSink` convention.
* Surface `chunk.server_wal_end - dispatched_lsn` as a "source ahead
  by N bytes" log line. Operator visibility, no behaviour change.

### Tests

* Add a variant of `tests/wal_stream_e2e.rs` that signals the daemon
  mid-stream (or asserts `close()` path via direct API). Confirm a
  `.partial` lands and a subsequent `--start-lsn` resumes cleanly.

## L2. Smaller debts

Not blocking
[Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix);
should not accumulate.

* `pub mod segment` in `src/lib.rs:22` exposes `SegmentWalker`. PRE5
  exit criterion 4 said `WalStream` would be the only walker. Inline
  `SegmentWalker` into `wal_stream`'s internals or document the
  waiver. As shipped, `SegmentWalker` remains the workhorse:
  `WalStream::push` buffers 16 MiB then calls `filter_segment` which
  calls `SegmentWalker::new(...)`.
* `WalStream::push` is not record-streaming; accumulates a full 16 MiB
  before any `RecordSink` fires (`wal_stream.rs:11-16` admits the
  compromise).
  [Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix)
  tolerates per-segment latency;
  [Phase 7](PLAN.md#phase-7--ch-native-emitter-via-clickhouse-c-rs)'s
  emitter may not. Comment the latency contract; defer the refactor.
* `main_data::relation_for_empty` (`main_data.rs:33-51`) only handles
  `XLOG_HEAP2_NEW_CID`. PRE5 implied Phase 0's Empty-bucket bug fixed;
  only NEW_CID was. Audit `XLOG_HEAP_VISIBLE`, `XLOG_HEAP2_VACUUM`,
  btree vacuum records — confirm they carry block refs (don't fall
  through to Empty) or expand the reclassifier.
* `WalStream::push` does not roll back `next_lsn` on sink error
  (`wal_stream.rs:270`). Either document the stream as poisoned
  post-error or roll back. Phase 5 sinks need a known contract.
* Chunk-boundary stress test promised at [PRE5.md:121-122](PRE5.md)
  never landed. Production `walshadow-stream` consumes wal-rs
  `CopyData` payloads at arbitrary boundaries; a one-byte-at-a-time
  feed vs bulk-push equivalence test is small and catches obvious
  drift if `WalStream::push` is ever rewritten as chunk-streaming.
* `fixtures/wal/xlog_switch/` and `fixtures/wal/vacuum_full_catalog/`
  referenced in [PRE5.md:343-344](PRE5.md) were never created.
  Captured offline equivalents exist nowhere; PRE5's "Files expected
  to change" list is stale.
* `ShadowCatalog` FIFO eviction at `shadow_catalog.rs:424-437` uses
  `min_by_key` over the full `HashMap`; O(n) per insert when full.
  With `max_entries=4096` and a working set that overflows, every
  insert linear-scans 4096 entries. Replace with a `BTreeMap` index
  by insert order when a workload makes this visible.

## Exit criteria

PRE5b closes when:

1. `cargo test --lib && cargo test --tests` clean, including new
   tests for B1, B2 (strengthened), B3, B4, S1, S2, S4.
2. `cargo clippy --all-targets -- -D warnings` clean.
3. New regression test for B1 demonstrates segment-boundary survival
   of `CatalogTracker` state.
4. `walshadow-stream` runs against a source that had
   `VACUUM FULL pg_class` pre-attach and produces filtered output
   indistinguishable (per manifest stats) from a daemon attached to
   a fresh cluster doing the same workload.
5. `tracker.pg_class_writes_undecoded` pinned at zero (or replaced by
   `pg_class_writes_oid_in_prefix`) on the
   `VACUUM FULL pg_depend` fixture.
6. `RelDescriptor` carries `relreplident` and, for `UsingIndex`, the
   replica-identity index attribute set.
7. `bin/stream.rs` no longer leaks `Record`s and writes a `.partial`
   segment on SIGINT.
8. `CatalogTracker` mutations on the catalog set produce
   `ShadowCatalog::invalidate` calls in production paths.

After PRE5b,
[Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix)
attaches its `DecoderSink` to `WalStream`, consumes `(Record,
Decision::Drop)` events for RM_HEAP* / RM_HEAP2* user records, queries
`ShadowCatalog::relation_at(rfn, commit_lsn)` for the per-relation
descriptor (including `relreplident` and dropped columns), and emits
`Tuple { rfn, xid, op, new, old }` per
[PLAN.md](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix).

## Out of scope

* General heap tuple decoder.
  [Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix).
* Per-record streaming sub-segment latency. Deferred to whichever
  later phase forces it.
* `ShadowCatalog` interior-mutable refactor (lock-free hit path).
  Tracked as follow-up to PRE5b's `Arc<Mutex<_>>` wrap.
* Tier 1/2 type-matrix fixture.
  [Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix).
* Observability infra (tracing subscriber).
  [Phase 10](PLAN.md#phase-10--operational).
* `bin/stream.rs` --slot keepalive policy beyond what wal-rs already
  provides. [Phase 10](PLAN.md#phase-10--operational).

## Files expected to change

```
src/filter_segment.rs              accept &mut Filter parameter; return Vec<XLogRecord>
src/wal_stream.rs                  own Filter; widen RecordEvent → Record;
                                   add CompositeRecordSink
src/catalog_tracker.rs             mpsc sender to ShadowCatalog::invalidate;
                                   pg_class_writes_oid_in_prefix counter
src/pg_class_decoder.rs            accept &XLogRecord; honour xl_heap_update.flags
src/shadow_catalog.rs              add relreplident + pg_index; module doc
src/source_feed.rs                 expose sql_client() for seed_from_source;
                                   surface server_wal_end
src/bin/stream.rs                  seed_from_source; ctrl_c; close() on shutdown;
                                   MetricsRecordSink replaces CollectingRecordSink
tests/wal_stream_e2e.rs            assertions on Record fields; multi-segment;
                                   shutdown drill
tests/filter_round_trip.rs         updated for Record shape
tests/catalog_seed.rs              force pg_class filenode >= 16384 before assert
tests/shadow_catalog.rs            relreplident + USING INDEX; tracker-driven
                                   invalidation; Arc<Mutex<_>> sanity
fixtures/wal/vacuum_full_pg_depend/  new — VACUUM FULL on a non-mapped catalog
plans/PLAN.md                      status list: add PRE5b entry
plans/PRE5b.md                     this doc
```
