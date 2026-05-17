# PRE5b10 — smaller debts

[PRE5b](PRE5b.md) item L2. Grab-bag of cleanup items that should not
accumulate into
[Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix);
none blocks it. Bundled into one commit because each fix is small
and the unit-of-review fits comfortably together.

## Items

### 1. `pub mod segment` exposure

`pub mod segment` in `src/lib.rs:22` exposes `SegmentWalker`. PRE5
exit criterion 4 said `WalStream` would be the only walker. Inline
`SegmentWalker` into `wal_stream`'s internals or document the
waiver. As shipped, `SegmentWalker` remains the workhorse:
`WalStream::push` buffers 16 MiB then calls `filter_segment` which
calls `SegmentWalker::new(...)`.

Action: drop `pub` (make `mod segment` private under `wal_stream`),
update any cross-module use sites, leave the implementation intact.

### 2. `WalStream::push` latency contract

`WalStream::push` is not record-streaming; accumulates a full 16 MiB
before any `RecordSink` fires (`wal_stream.rs:11-16` admits the
compromise).
[Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix)
tolerates per-segment latency;
[Phase 7](PLAN.md#phase-7--ch-native-emitter-via-clickhouse-c-rs)'s
emitter may not.

Action: comment the latency contract at `WalStream::push` so the
deferred refactor's trigger is visible. No code change beyond docs.

### 3. Empty-bucket reclassifier audit

`main_data::relation_for_empty` (`main_data.rs:33-51`) only handles
`XLOG_HEAP2_NEW_CID`. PRE5 implied Phase 0's Empty-bucket bug fixed;
only NEW_CID was. Audit `XLOG_HEAP_VISIBLE`, `XLOG_HEAP2_VACUUM`,
btree vacuum records — confirm they carry block refs (don't fall
through to Empty) or expand the reclassifier.

Action: cross-check each rmgr's record types against block-ref
presence. Either confirm Empty-bucket is unreachable for them or
extend `relation_for_empty` with a positive test fixture.

### 4. `WalStream::push` sink-error rollback

`WalStream::push` does not roll back `next_lsn` on sink error
(`wal_stream.rs:270`). Either document the stream as poisoned
post-error or roll back. Phase 5 sinks need a known contract.

Action: pick poisoned-on-error (simpler, no rollback needed in the
sink protocol); document on `RecordSink` trait and at `push`. Add a
state field that rejects further `push` calls after a sink error.

### 5. Chunk-boundary stress test

Chunk-boundary stress test promised at [PRE5.md:121-122](PRE5.md)
never landed. Production `walshadow-stream` consumes wal-rs
`CopyData` payloads at arbitrary boundaries; a one-byte-at-a-time
feed vs bulk-push equivalence test is small and catches obvious
drift if `WalStream::push` is ever rewritten as chunk-streaming.

Action: new `tests/wal_stream_chunk_boundary.rs` (or new case in
`wal_stream_e2e`) feeding an existing fixture one byte at a time,
asserting the `Record` sequence matches the bulk-push run.

### 6. Missing fixtures

`fixtures/wal/xlog_switch/` and `fixtures/wal/vacuum_full_catalog/`
referenced in [PRE5.md:343-344](PRE5.md) were never created.
Captured offline equivalents exist nowhere; PRE5's "Files expected
to change" list is stale.

Action: capture both fixtures from a live source, land them with
brief README pointing at the capture script. `vacuum_full_catalog`
overlaps [PRE5b3](PRE5b3.md)'s `vacuum_full_pg_depend`; rename or
dedupe.

### 7. `ShadowCatalog` FIFO eviction

`ShadowCatalog` FIFO eviction at `shadow_catalog.rs:424-437` uses
`min_by_key` over the full `HashMap`; O(n) per insert when full.
With `max_entries=4096` and a working set that overflows, every
insert linear-scans 4096 entries.

Action: replace with a `BTreeMap` index by insert order, or a
`VecDeque<oid>` companion to the cache map. O(log n) or O(1)
respectively. Pick whichever leaves the existing surface area
unchanged.

## Tests

* Per item above: confirm each lands with the test or assertion
  named. Particularly item 5 (chunk-boundary) and item 6 (fixtures)
  introduce new tests; items 1, 4, 7 strengthen existing coverage.

## Exit criteria

1. `cargo test --lib && cargo test --tests` clean, including the
   chunk-boundary test and any item-specific cases added.
2. `cargo fmt --all -- --check` and
   `cargo clippy --all-targets -- -D warnings` clean. Run both at
   the end of the implementing phase before commit.
3. Each item above either resolved with a code change or, where
   resolution is explicitly deferred, captured as a `// FIXME(PRE5b)`
   with a pointer to the relevant follow-up.

## Files expected to change

```
src/lib.rs                         drop pub on mod segment
src/wal_stream.rs                  push-latency comment; poisoned-on-error
                                   state field; FIFO eviction strategy
src/main_data.rs                   relation_for_empty audit & expansion
src/shadow_catalog.rs              eviction index
tests/wal_stream_chunk_boundary.rs new chunk-boundary stress
fixtures/wal/xlog_switch/          captured fixture
plans/PRE5b10.md                   this doc
```
