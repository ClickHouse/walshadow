# PRE5b1 — lift `Filter` out of per-segment scope (retrospective)

[PRE5b](PRE5b.md) item B1. First of the four B-item correctness fixes;
no dependency on the others, independently shipped.

## Why (preserved)

`filter_segment.rs:38` constructed `let mut filter = Filter::new()`
inside the function called once per segment. `WalStream::flush_current`
(`wal_stream.rs:308`) and `WalStream::close` invoked it. Every RELMAP
update, every successful pg_class heap-write decode, every bootstrap
state died at segment N's end. Segment N+1's first record ran against
an empty `CatalogTracker`.

Fully masked in tests: every fixture under `fixtures/wal/` is a single
segment, and every round-trip test called `filter_segment` against
bytes that include both the catalog-learning record and the records
that depend on it. No test crossed a segment boundary.

## What landed

* `Filter` is now a field on `WalStream`, constructed in
  `WalStream::new`. Both `flush_current` and `close` pass
  `&mut self.filter` into `filter_segment`. `CatalogTracker.nodes`,
  `pg_class_filenode`, `relmap_updates`, `pg_class_writes_*` all
  survive segment boundaries.
* `filter_segment` grew a `filter: &mut Filter` parameter. The
  standalone `Filter::new()` inside the function is gone.
* `ManifestStats` stays per-segment. Implementation: snapshot
  `filter.stats` + `tracker.relmap_updates` + `tracker.pg_class_writes_undecoded`
  at function entry; the manifest stats reflect the post-call values
  minus the snapshot. `FilterStats` grew `delta_from(&self, prev)`
  and `Copy` so the snapshot is one-line.
* `walshadow-filter` (`src/bin/filter.rs`) owns its own `Filter::new()`
  and passes it in. Single-segment input → observable output identical
  to before.
* `walshadow-stream` (`src/bin/stream.rs`) calls `stream.filter()` and
  surfaces `kept`/`dropped`/`relmap_updates`/`pg_class_undecoded` in
  its per-segment log line, now cumulative across the stream.
* `WalStream::filter()` accessor returns `&Filter` so callers reach
  both `stats` and `tracker` without exposing the field directly.

## Tests

* New: `tests/multi_segment_filter.rs::catalog_tracker_state_survives_segment_boundary`.
  Two synthetic 8 KiB single-page segments pushed through `WalStream`.
  Seg 1 carries an `XLOG_RELMAP_UPDATE` mapping `pg_class` to filenode
  50000 in dbid 5; seg 2 carries a heap insert with one block ref at
  `(spc=1663, db=5, rel=50000)`. Manifest for seg 2 must list the
  heap record as `Kind::Kept`. The cumulative `WalStream::filter()`
  shows `kept=2, dropped=0, relmap_updates=1` post-push.
* Existing single-segment round-trip tests stay green; the two callers
  in `tests/filter_round_trip.rs` and the four in
  `src/filter_segment.rs::tests` were updated to thread their own
  local `Filter::new()` in.
* `cargo test --lib`: 66 pass.
* `cargo test --tests`: every integration suite green including the
  new regression and the live-PG `wal_stream_e2e`.
* `cargo clippy --all-targets -- -D warnings`: clean.

## Deviations from plan

* **Test file location.** Plan named `tests/wal_stream_e2e.rs` for the
  new regression. That file is the live-PG end-to-end test (initdb,
  replication protocol, real source feed); a synthetic byte-built
  test wedged in there would share none of its scaffolding. Put it
  in `tests/multi_segment_filter.rs` instead. Same test surface, no
  PG dependency, runs in milliseconds.
* **`FilterStats::delta_from` added.** Plan implied per-segment stats
  would "just work" once the filter was lifted, but the long-lived
  `Filter` accumulates `stats` across calls — the manifest emit
  needed an explicit subtraction. `delta_from` plus a snapshot at
  `filter_segment` entry handles it; `FilterStats` gained `Clone`,
  `Copy` for the snapshot to be cheap.

## Implementation notes for follow-on work

The relmap test record (`tests/multi_segment_filter.rs`) uses
`XLR_BLOCK_ID_DATA_LONG` because the payload is 536 bytes (12-byte
header + 524-byte `RelMapFile` for 64 mappings). Helpers in the test
file (`build_record_with_main_data`, `build_record_with_block_ref`,
`build_one_page_segment`) are simpler than the equivalents in
`src/filter_segment.rs::tests` — if a future regression needs synthetic
multi-segment WAL, lift them rather than duplicating.

`WalStream` accepts `seg_size = 8192` (one page) because the math
in `segment_for_lsn` only needs `seg_size` to divide `2^32`. Useful
for any future synthetic test that wants to span N segments cheaply.

## Files actually changed

```
src/filter.rs                      FilterStats: Clone, Copy, delta_from
src/filter_segment.rs              accept &mut Filter; compute per-segment stats via delta
src/wal_stream.rs                  own Filter; filter() accessor; pass &mut self.filter
src/bin/filter.rs                  own + pass Filter
src/bin/stream.rs                  surface cumulative FilterStats + tracker counters
tests/filter_round_trip.rs         update callers
tests/multi_segment_filter.rs      new: multi-segment regression
plans/PRE5b1.md                    this retrospective
```
