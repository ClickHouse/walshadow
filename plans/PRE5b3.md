# PRE5b3 — `pg_class_decoder` prefix/suffix compression

[PRE5b](PRE5b.md) item B3. Fixes the `VACUUM FULL pg_<non-mapped>`
silent-decode hole flagged by PRE5 exit criterion 5.

## Why

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

## Implementation

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

## Tests

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

## Out of scope

* General heap decoder with prefix/suffix handling.
  [Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix)
  inherits the same constraint for user heap; the pg_class-specific
  path stays narrow.

## Exit criteria

1. `cargo test --lib && cargo test --tests` clean, including unit
   cases for `prefixlen ∈ {0, 2, 4, 88}` and the live-fixture test.
2. `cargo clippy --all-targets -- -D warnings` clean.
3. `tracker.pg_class_writes_undecoded` pinned at zero (or replaced by
   `pg_class_writes_oid_in_prefix`) on the
   `VACUUM FULL pg_depend` fixture.

## Files expected to change

```
src/pg_class_decoder.rs            accept &XLogRecord; honour xl_heap_update.flags
src/catalog_tracker.rs             pg_class_writes_oid_in_prefix counter; route
                                   decoder through new signature
tests/filter_round_trip.rs         add VACUUM FULL pg_depend assertion
fixtures/wal/vacuum_full_pg_depend/  new — VACUUM FULL on a non-mapped catalog
plans/PRE5b3.md                    this doc
```
