# PRE5b3 — `pg_class_decoder` prefix/suffix compression (retrospective)

[PRE5b](PRE5b.md) item B3. Third of the four B-item correctness fixes;
independent of [PRE5b1](PRE5b1.md), [PRE5b2](PRE5b2.md),
[PRE5b4](PRE5b4.md). Closes the `VACUUM FULL pg_<non-mapped>`
silent-decode hole flagged by PRE5 exit criterion 5.

## Why (preserved)

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

`pg_class_decoder.rs` previously did

```rust
let t_hoff = block_data[XL_HEAP_HEADER_SIZE - 1] as usize;
```

i.e. read `block_data[4]` as `t_hoff`. Wrong whenever any prefix or
suffix flag is set on `xl_heap_update.flags`, which lives in
`record.main_data`, not in block data.

`VACUUM FULL` on a non-mapped catalog (`pg_depend`, `pg_namespace`,
`pg_constraint`, `pg_index`) updates `pg_class.relfilenode` for that
catalog's row. pg_class cols 1–7 occupy 88 bytes, all unchanged; PG's
prefix-compute yields `prefixlen ≈ 88`. The OID column lands inside the
prefix and never appears in the WAL record.

Consequence: every `VACUUM FULL pg_<non-mapped>` produced a pg_class
WAL record whose `(oid, relfilenode)` the decoder could not extract,
ticking `pg_class_writes_undecoded` and silently leaving the catalog
set unchanged for the rotated filenode. PRE5 exit criterion 5
(counter pinned at zero) held only because no fixture exercised the
case.

## What landed

* **`pg_class_decoder` signature change.**
  `decode_pg_class_tuple(record: &XLogRecord, block_idx: usize)
  -> DecodeOutcome` replaced the old `(block_data: &[u8]) -> Option<…>`.
  The decoder now reads `xl_heap_update.flags` from `record.main_data`
  at byte offset 7 (`SizeOfHeapUpdate = 14` on the wire — PG's
  `XLogRegisterData` strips the C-struct trailing pad that makes
  in-memory `sizeof = 16`). HEAP_INSERT (info 0x00) skips the flags
  read entirely.
* **Three-way outcome enum.** `DecodeOutcome::{Decoded(PgClassRow),
  OidInPrefix, Undecoded}` lets the tracker distinguish "PG omitted
  the OID by design" from "WAL was malformed". Any `prefixlen > 0`
  (overlap or fully-in-prefix) returns `OidInPrefix`; truncated /
  invalid `t_hoff` returns `Undecoded`.
* **New tracker counter.** `CatalogTracker.pg_class_writes_oid_in_prefix`
  ticks on the prefix path; `pg_class_writes_undecoded` is now reserved
  for genuinely malformed input. Both counters are surfaced through
  `ManifestStats` and the `walshadow-stream` per-segment log line.
* **`harvest_pg_class_blocks` narrowed to block 0.** PG's
  `heap_insert` / `heap_update` / `heap_hot_update` always register
  the new tuple via `XLogRegisterBufData(0, …)`. The previous loop
  iterated every block reference and fed the empty "old page" block-1
  back through the decoder, ticking spurious `pg_class_writes_undecoded`
  counts. Constraining to `record.blocks.first()` matched PG's
  contract and dropped the false-positive `undecoded` ticks to zero on
  the new fixture (was 9 of 19 prefix-compressed records before the
  narrowing).
* **Live fixture.**
  `fixtures/wal/vacuum_full_pg_depend/{capture.sh,workload.sql,segments/000000010000000000000002.gz}`.
  Workload runs `VACUUM FULL` on `pg_depend`, `pg_namespace`,
  `pg_constraint`. `capture.sh` follows the
  `fixtures/wal/filter/capture.sh` shape — phase A primes catalog
  state and `pg_switch_wal()`s into segment 2; phase B runs the target
  operation. Captured under PG 18.4.
* **Round-trip assertion.**
  `tests/filter_round_trip.rs::vacuum_full_pg_depend_ticks_oid_in_prefix_not_undecoded`
  drives the fixture through `filter_segment` and asserts
  `tracker.pg_class_writes_oid_in_prefix > 0` and
  `tracker.pg_class_writes_undecoded == 0`. Skips silently when the
  fixture is absent (matches the existing `fixtures/wal/filter` test).
* **New unit tests.** `pg_class_decoder` gained nine tests covering
  `prefixlen ∈ {0, 2, 4, 88}`, `suffixlen ∈ {0, 4}`, both flags set,
  HOT_UPDATE parity with UPDATE, and short-`main_data` rejection.
  `catalog_tracker` gained
  `pg_class_heap_update_with_prefix_compression_increments_oid_in_prefix`.

## Tests

* `cargo test --lib`: 76 pass (was 66; +10 unit). Old
  `pg_class_decoder` tests rewritten to use `record + block_idx`
  inputs; old `catalog_tracker` UPDATE tests gained an explicit
  `xl_heap_update` main_data (else the decoder correctly trips on the
  empty main_data).
* `cargo test --tests`: 21 integration tests pass (was 20; +1
  fixture-backed test in `filter_round_trip.rs`). The new test only
  fires when the fixture exists; on a clean clone with no docker /
  local PG it skips.
* `cargo fmt --all -- --check` clean.
* `cargo clippy --all-targets -- -D warnings` clean.

VACUUM FULL fixture stats: 874 records, 19 `oid_in_prefix`, 0
`undecoded`, 16 `decoded` (mix of REINDEX-side INSERTs for new pg_class
rows, and HOT_UPDATEs that fit in the prefix-free path).

## Deviations from plan

* **`0 < prefixlen < 4` collapsed into `OidInPrefix`.** Plan called for
  "reject as unrecoverable" — separate from `prefixlen >= 4`. In
  practice both cases leave the OID irrecoverable from the record
  alone; splitting them would mean two near-identical counters with no
  consumer that cares. Single counter, single outcome. Documented in
  the `DecodeOutcome::OidInPrefix` doc comment.
* **Decoder no longer iterates blocks.** Plan kept the per-block
  iteration shape from the original code. The live fixture showed
  this was over-eager: PG's `heap_update` register a block-1 "old
  page" reference with no data, and the loop fed it back into the
  decoder for `Undecoded` ticks. The fix is to constrain to block 0,
  not to teach the decoder to recognise "I am being called on the
  wrong block." Encapsulates PG's convention at the harvester
  boundary.
* **Path A (synthesised oid index) explicitly skipped.** Plan offered
  Path A / Path B; Path B (counter only) shipped. No state added
  beyond the counter. PRE5b2's `seed_from_source` plus subsequent
  `XLOG_RELMAP_UPDATE`s cover the "but how does the catalog set
  actually learn the rotated filenode" question that Path A would
  have addressed.
* **`ManifestStats` widened.** Plan didn't call out manifest fields.
  Added `pg_class_writes_oid_in_prefix` alongside the existing
  `pg_class_writes_undecoded` so the per-segment view tracks both;
  `from_filter` grew a parameter.
* **`SizeOfHeapUpdate` is 14, not 13.** Plan said 13. Verified against
  PG source plus a one-shot C program: in-memory `sizeof` is 16
  (trailing pad), wire size via `XLogRegisterData(&xlrec,
  SizeOfHeapUpdate)` is 14, `flags` at byte 7. Constants in the
  module reflect the wire layout.

## Implementation notes for follow-on work

`decode_pg_class_tuple`'s signature is now self-contained — it reads
both `record.header.info` and `record.main_data`, so callers no
longer need to pre-mask info high-nibble before calling. The
`info_carries_new_tuple_heap` gate in `CatalogTracker::observe` stays
because it short-circuits the harvester entirely (and keeps the
"non-INSERT info ignored" unit test honest).

The block-0-only contract in `harvest_pg_class_blocks` is the right
shape for [Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix)'s
general heap decoder too: PG's tuple data always rides block 0 for
heap WAL ops that carry tuples. When `DecoderSink` lands, lift the
same `record.blocks.first()` pattern.

[PRE5b8](PRE5b8.md) will need to read `xl_heap_update.flags` for
UPDATE / DELETE old-tuple decode. Reuse the
`SIZE_OF_HEAP_UPDATE` / `XL_HEAP_UPDATE_FLAGS_OFFSET` constants —
they're module-private today but the right place for them when Phase
5 starts is a shared `heapam_xlog.rs` next to `main_data.rs`.

`pg_class_writes_oid_in_prefix > 0` does *not* mean the catalog
filenode rotation has been missed end-to-end. PRE5b2's
`seed_from_source` populates `pg_class_filenode[db]` so subsequent
`is_pg_class_relfilenode` checks land correctly; the next
`XLOG_RELMAP_UPDATE` from any mapped-catalog touch refreshes the
non-mapped catalog set too (shared `pg_database`, `pg_shdepend`
churn is common in active workloads). If a long-running source goes
extended periods with only `VACUUM FULL pg_<non-mapped>` activity
and no relmap events, the oid_in_prefix records leave a gap until
the next attach-time seed — out of scope for B3.

## Files actually changed

```
src/pg_class_decoder.rs                              accept &XLogRecord; honour xl_heap_update.flags;
                                                     three-way DecodeOutcome
src/catalog_tracker.rs                               pg_class_writes_oid_in_prefix counter;
                                                     narrow harvest to block 0;
                                                     route decoder through new signature
src/filter_segment.rs                                thread new counter into ManifestStats
src/manifest.rs                                      pg_class_writes_oid_in_prefix field
src/bin/stream.rs                                    surface new counter in per-segment log
tests/filter_round_trip.rs                           vacuum_full_pg_depend fixture assertion
fixtures/wal/vacuum_full_pg_depend/                  new — capture.sh, workload.sql, segments/
plans/PRE5b3.md                                      this retrospective
```
