# PRE5b5 — widen `RecordEvent` to a real `Record` (retrospective)

[PRE5b](PRE5b.md) item S1. First of the foundation changes. Lands the
parsed-record handoff that
[Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix)'s
`DecoderSink` will consume. Blocks PRE5b6 (sink fan-out) and PRE5b7
(daemon `Arc<Mutex<ShadowCatalog>>`); independent of the four B-item
correctness fixes.

## Why (preserved)

`RecordEvent` (`wal_stream.rs:80-92`) carried `source_lsn, len, rmid,
info, decision`.
[Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix)'s
`Tuple { rfn, xid, op, new, old }` needs the parsed `XLogRecord` (for
`main_data` and per-block payload), the `RelFileLocator` (for
`relation_at` lookup), and `xact_id`. `filter_segment` already parsed
all of this (`filter_segment.rs:46`) to feed `Filter::decide`, then
discarded it; the `Manifest` was written as flat scalars. A
`RecordSink` consumer would have had to re-parse from segment bytes to
get back what was just thrown away.

[PRE5.md:58-64](PRE5.md) spec'd the right shape (`Record { parsed:
XLogRecord, logical_bytes, byte_ranges, source_lsn, page_magic }`) and
the contract was watered down during PRE5 implementation. PRE5b5
closes the gap, with one simplification (`logical_bytes` and
`byte_ranges` dropped —
[Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix)'s
decoder consumes the parsed form, not raw bytes).

## What landed

* **`Record` replaces `RecordEvent`.** Same name as PRE5.md's spec.
  Fields: `parsed: XLogRecord`, `source_lsn: u64`, `page_magic: u16`,
  `decision: Decision`. The `len`, `rmid`, `info` scalars on the old
  `RecordEvent` are now read through `parsed.header.total_record_length`,
  `parsed.header.resource_manager_id`, `parsed.header.info`. The
  `Manifest` entry shape is unchanged (still flat scalars on disk).
* **`RecordSink::on_record(&Record)`.** Trait signature widens; every
  implementor (`CollectingRecordSink`, future `DecoderSink`) sees the
  full parsed shape.
* **`Record::from_parsed(seg_start_lsn, ParsedRecord, &Entry)`.**
  Constructor zips one parsed record with its manifest entry plus the
  segment's start LSN — replaces the old
  `RecordEvent::from_manifest_entry`. `source_lsn` is computed as
  `seg_start_lsn + entry.offset`; `decision` derived from
  `entry.kind`.
* **`filter_segment` returns `(Vec<u8>, Manifest, Vec<ParsedRecord>)`.**
  The third tuple element is the parses-once hand-off: entries match
  `manifest.records` by index. See deviations for why
  `Vec<ParsedRecord>` rather than the plan's `Vec<XLogRecord>`.
* **`ParsedRecord { record: XLogRecord, page_magic: u16 }`.** New
  public struct in `filter_segment.rs`. Pairs each parsed record with
  the page magic of the page its header sat on, so downstream
  consumers can interpret PG-15-vs-PG-14 FPI bits via
  `XLogRecordBlockImageHeader::is_compressed(page_magic)` without a
  separate side-channel.
* **`WalStream::flush_current` + `close` iterate
  `manifest.records.iter().zip(parsed)`.** Each
  `(entry, parsed)` pair feeds `Record::from_parsed`, the result is
  dispatched to `RecordSink::on_record`. Index alignment between
  `manifest.records` and the parsed vec is the `filter_segment`
  contract, asserted at construction time by the matching `push`s.
* **`CollectingRecordSink.events` → `.records`.** Field rename to
  match the new type name. The two existing call sites
  (`bin/stream.rs::run`, `tests/wal_stream_e2e.rs`,
  `tests/multi_segment_filter.rs`) follow.
* **`bin/filter.rs` destructures the 3-tuple.** Single-segment CLI
  ignores the parsed vec (it has no downstream sink); the filtered
  bytes + manifest sidecar contract is unchanged.

## Tests

* `cargo test --lib`: 83 pass (unchanged). The
  `record_event_lsn_offset_is_seg_start_plus_entry_offset` unit test
  in `wal_stream.rs` is rewritten as
  `record_lsn_offset_is_seg_start_plus_entry_offset` against the new
  `Record::from_parsed` shape; asserts `source_lsn`, parsed header
  fields (`resource_manager_id`, `xact_id`), `page_magic`, and
  `decision` survive the zip.
* `cargo test --tests`: 24 pass (was 23; +1). New
  `tests/multi_segment_filter.rs::records_in_one_xact_share_xact_id_through_stream`
  builds three records with `xid = 42` plus one stray with `xid = 99`,
  all on pg_class so each lands as `Class::Catalog` → `Kept`, pushes
  through `WalStream`, and asserts the three cohort records arrive at
  the sink with `parsed.header.xact_id == 42` and
  `parsed.blocks[0].header.location.rel.rel_node == PG_CLASS_OID`. The
  stray xid=99 record arrives distinguishable on the same surface.
* `tests/wal_stream_e2e.rs::full_pipeline_source_to_filtered_segments_on_disk`
  walks every collected `Record` and asserts
  `parsed.header.resource_manager_id <= 21` (PG 17 RmId range),
  `source_lsn >= aligned`, `parsed.header.total_record_length >= 24`.
  Then finds a record carrying a populated block locator and a record
  carrying a non-zero `xact_id`, asserts both fields read through
  cleanly. See deviations for why these are existence-quantified
  rather than the plan's specific block/xid checks.
* `tests/filter_round_trip.rs` updated for the three-tuple return.
  The `parsed.len() == manifest.records.len()` invariant added on the
  primary fixture path; the OLTP + vacuum-full tests drop `_parsed`
  since they assert only on manifest stats.
* `cargo fmt --all -- --check` clean.
* `cargo clippy --all-targets -- -D warnings` clean.

## Deviations from plan

* **`Vec<ParsedRecord>` rather than `Vec<XLogRecord>`.** Plan said
  `filter_segment` returns `(Vec<u8>, Manifest, Vec<XLogRecord>)`,
  but `Record` carries `page_magic` and `page_magic` lives per-page
  on the walker (`WalkedRecord.page_magic`), not on the parsed
  `XLogRecord`. A `Vec<XLogRecord>` alone would force WalStream to
  either re-derive page_magic out-of-band or drop it from `Record` —
  neither faithful to the original PRE5.md spec.
  `ParsedRecord { record, page_magic }` keeps the cohesion explicit;
  `WalStream::flush_current`'s zip is single-pass and untangled.
* **Synthetic xact_id cohesion test, not on-disk fixture.** Plan
  called for "a fixture xact's records (all bracketed by one
  BEGIN…COMMIT) all carry the same `xact_id`". Two readings: an
  on-disk WAL fixture, or a live-PG e2e wrapped in `BEGIN; ...;
  COMMIT;`. Neither chosen — the test lives in
  `tests/multi_segment_filter.rs` as a synthetic byte-built check
  alongside the existing PRE5b1 regression. Reasoning: PG 17 PG
  fixture would need a `capture.sh` to write deterministic xids
  (PG hands them out from a counter; replayable but coupled to
  fixture re-capture), and the live-PG path requires `initdb` on
  `$PATH`. Synthetic gives bit-exact xact_id control with no PG
  dependency, runs in microseconds, fits the file's existing test
  style. The live-PG e2e (`wal_stream_e2e.rs`) still covers
  xact_id-survives-the-chain end-to-end via the "find any record with
  a non-zero `xact_id`" assertion.
* **Helpers in `multi_segment_filter.rs` gained an xid parameter.**
  `write_header` now takes `xid: u32`; new
  `build_record_with_main_data_xid` and
  `build_record_with_block_ref_xid` wrap the existing helpers. The
  zero-xid wrappers preserve PRE5b1's call sites verbatim. Plan was
  silent on test-helper shape; this was the smallest diff that kept
  the synthetic cohort test deterministic.
* **`wal_stream_e2e.rs` assertions are existence-quantified.** Plan
  called for asserting `Record.parsed.header.resource_manager_id`,
  `Record.parsed.blocks[0].header.location.rel`,
  `Record.parsed.header.xact_id` directly — read as "pick a record
  and check these three fields". In practice the captured segment
  content depends on which transactions the driver thread races
  against the pump's segment boundary, and PG's checkpoint records
  may or may not land. The test now iterates every collected record
  asserting field-level invariants (`rmid <= 21`,
  `total_record_length >= 24`), then existence-finds one record per
  field-of-interest (`blocks[0].header.location.rel.rel_node > 0`,
  `parsed.header.xact_id != 0`) and asserts on those. Same coverage,
  no flakiness from segment-content non-determinism.
* **`bin/filter.rs` ignores the parsed vec.** Plan listed it as a
  file expected to change "follow Record shape" — the CLI has no
  downstream sink, so the change is one binding (`let (filtered,
  manifest, _parsed) = filter_segment(...)`) and no behavioural
  drift. Output bytes + manifest sidecar are byte-identical to the
  pre-PRE5b5 build.

## Implementation notes for follow-on work

`ParsedRecord` is named on the boundary between
`filter_segment.rs` (where parsed records leave a sealed function)
and `wal_stream.rs` (where they meet manifest entries and become
`Record`s). PRE5b6's `CompositeRecordSink` should consume `&Record`,
not `&ParsedRecord` — the page_magic + decision + source_lsn shape
is the right surface for downstream sinks. If `CompositeRecordSink`
needs to clone the record for fan-out, `Record` is `Clone` (every
field is `Clone`); cost is dominated by `parsed.main_data` /
`parsed.blocks` vec copies, which is acceptable for a 2-3-way fan-out
but not for a high-cardinality observability tap. Profile before
adding more sinks than the heap-decoder + the segment writer.

`Record.page_magic` is the field
[Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix)
will read when interpreting FPI compression bits on
`parsed.blocks[i].image_header`. The heap-tuple decoder itself
doesn't need it (tuple data flows in `parsed.blocks[i].data`, not in
images), but [FPI_COMPRESSION](FPI_COMPRESSION.md)'s
`restore_block_image` does — keep `page_magic` on `Record` even if
Phase 5's first cut never reads it.

The zip pattern in `WalStream::flush_current`
(`manifest.records.iter().zip(parsed)`) relies on `filter_segment`'s
"entries match parsed by index" contract. Future changes to
`filter_segment` that filter records mid-stream (drop a malformed
record from the walked vec while keeping the manifest entry, or
vice-versa) must update both vecs in lockstep or surface the
divergence — silent index drift would route the wrong `parsed` to
each sink. The shared loop is the right place to encode that
invariant; if a future refactor splits parsing from manifest emit,
preserve the zip-by-index property at the API boundary.

Phase 5's `DecoderSink::on_record(&Record)` will gate on `decision`
plus `parsed.header.resource_manager_id` — `Decision::Drop` records
that classify as user heap operations are exactly the input the
decoder wants (the record bytes are NOOP-overwritten in the filtered
segment, but the `Record` carries the pre-rewrite parsed form). The
`Drop` records still flow through `RecordSink`; the rewrite happens
on the byte side, not the parsed side.

## Files actually changed

```
src/filter_segment.rs              ParsedRecord struct; return
                                   (Vec<u8>, Manifest, Vec<ParsedRecord>)
src/wal_stream.rs                  RecordEvent → Record;
                                   Record::from_parsed; zip parsed
                                   with manifest entries; rename
                                   CollectingRecordSink.events → records
src/bin/filter.rs                  destructure 3-tuple
src/bin/stream.rs                  follow CollectingRecordSink.records rename
tests/wal_stream_e2e.rs            existence-quantified Record.parsed assertions
tests/filter_round_trip.rs         destructure 3-tuple;
                                   parsed.len() == manifest.records.len()
tests/multi_segment_filter.rs      xid-parameterised helpers;
                                   records_in_one_xact_share_xact_id_through_stream;
                                   records.events → records.records
plans/PRE5b5.md                    this retrospective
```
