# PRE5b1 — lift `Filter` out of per-segment scope

[PRE5b](PRE5b.md) item B1. First of the four B-item correctness
fixes; no dependency on the others, independently shippable.

## Why

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

## Implementation

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

## Tests

* New: two consecutive synthetic segments. Segment 1 carries an
  `XLOG_RELMAP_UPDATE` introducing a `>= 16384` pg_class filenode;
  segment 2 carries a heap record targeting that filenode. Assert the
  segment-2 record is `Decision::Keep`. Today it would be
  `Decision::Drop`.
* Existing single-segment round-trip tests stay green (caller now
  owns the `Filter`; observable output unchanged).

## Exit criteria

1. `cargo test --lib && cargo test --tests` clean, including the new
   segment-boundary regression.
2. `cargo clippy --all-targets -- -D warnings` clean.
3. Regression test demonstrates `CatalogTracker` state survives the
   segment-N flush.

## Files expected to change

```
src/filter_segment.rs              accept &mut Filter parameter
src/wal_stream.rs                  own Filter; cumulative FilterStats
src/bin/filter.rs                  own and pass Filter
src/bin/stream.rs                  surface cumulative FilterStats
tests/wal_stream_e2e.rs            new multi-segment regression
plans/PRE5b1.md                    this doc
```
