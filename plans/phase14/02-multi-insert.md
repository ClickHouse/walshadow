# 02 — `XLOG_HEAP2_MULTI_INSERT` per-tuple fan-out

Closes [PLAN.md §"Known correctness gaps" #1](../PLAN.md#known-correctness-gaps).
[`heap_decoder.rs:376-380`](../../src/heap_decoder.rs) returns
`Ok(None)` for every `RmId::Heap2` op. `COPY foo FROM stdin` and
`INSERT INTO foo SELECT ...` (>= 2 rows) emit MULTI_INSERT records;
those rows never reach CH. Phase 12's bootstrap covers pgbench's
initial data load, but any post-attach COPY silently drops

## Why

`xl_heap_multi_insert` packs N tuples into one record to amortise
WAL overhead. PG header (`access/heapam_xlog.h`):

```c
typedef struct xl_heap_multi_insert {
    uint8       flags;          // XLH_INSERT_*
    uint16      ntuples;
    // offsetnumber array follows: OffsetNumber offsets[ntuples]
    // when not XLH_INSERT_NO_LOGICAL — otherwise the offsetnumber
    // array is the only thing after ntuples
    // then per-tuple blocks of xl_multi_insert_tuple + data
} xl_heap_multi_insert;

typedef struct xl_multi_insert_tuple {
    uint16 datalen;
    uint16 t_infomask2;
    uint16 t_infomask;
    uint8  t_hoff;
    // data[datalen] follows
} xl_multi_insert_tuple;
```

Block 0's `data` field carries the offsets array + the per-tuple
blocks; the record's `main_data` carries the flags + ntuples header
(varies by PG version — verify against PG 16/17/18 source). Decoder
walks the per-tuple structure, decodes each tuple through the
existing `decode_new_tuple_block` path

## Surface

New `decode_multi_insert` in
[`heap_decoder.rs`](../../src/heap_decoder.rs):

```rust
fn decode_multi_insert(
    record: &XLogRecord,
    source_lsn: u64,
    rfn: RelFileNode,
    xid: u32,
    rel: &RelDescriptor,
) -> Result<SmallVec<[DecodedHeap; 1]>, DecodeError>;
```

`SmallVec<[_; 1]>` keeps the single-tuple INSERT/UPDATE/DELETE path
zero-allocation (one stack slot); MULTI_INSERT spills to heap only
beyond ntuples == 1. Mirrors the
[POST13zerocopy §9 "smallvec for blocks"](../POST13zerocopy.md#other-wal-rs-allocation-review)
shape — smallvec earns its keep on the dispatch boundary, not just
on the parser internals

`decode_heap_record` switches its return type:

```rust
pub fn decode_heap_record(
    record: &XLogRecord,
    source_lsn: u64,
    rel: &RelDescriptor,
) -> Result<SmallVec<[DecodedHeap; 1]>, DecodeError>;
```

Single-row INSERT/UPDATE/DELETE returns a one-element smallvec;
MULTI_INSERT returns ntuples elements; skipped ops (LOCK / INPLACE
/ CONFIRM) return an empty smallvec. Caller iterates regardless

`BufferingDecoderSink::on_record`
([`xact_buffer.rs:817-820`](../../src/xact_buffer.rs)) iterates the
smallvec, pushing each `DecodedHeap` into the xact buffer. The xid /
xact_buffer keying is unchanged — all tuples in one MULTI_INSERT
record share the same xid

`XLH_INSERT_IS_SPECULATIVE` (the speculative-insert flag for
ON CONFLICT) propagates as a per-tuple flag through `xl_multi_insert_tuple`'s
`t_infomask` bits; the filter Phase 5 uses for single-INSERTs
applies unchanged

## Tests

Unit ([`heap_decoder.rs`](../../src/heap_decoder.rs) test module):
- Drive a hand-built MULTI_INSERT record with 3 rows through
  `decode_multi_insert`, assert ntuples + ColumnValue contents
- Empty MULTI_INSERT (ntuples == 0) is a malformed record; assert
  `DecodeError`
- Speculative-insert flag is honoured on a per-tuple basis

Integration (`tests/phase14_copy_into.rs`):
- `COPY t (id, name) FROM stdin` with 1000 rows post-attach
- Source `SELECT count(*), md5(string_agg(name, ',' ORDER BY id))`
  vs CH equivalent must agree

## Size

~150 LOC product + ~120 LOC test

## Risks

- **`XLH_INSERT_NO_LOGICAL`.** PG uses this for catalog bulk-INSERT
  during certain DDL paths. Verify the decoder skips the record when
  the flag is set (same posture as Phase 5's single-INSERT path).
  Without the skip we'd emit synthesized rows for system-catalog
  inserts that shouldn't reach user-facing CH tables
- **Per-tuple offsets vs block 0's `data` layout.** PG's exact
  layout shifted slightly between majors (per-tuple `data` lives in
  block 0's data area, ordered by offset). Snapshot a fixture from
  each of PG 16 / 17 / 18 and pin the decoder against all three —
  the existing `fixtures/wal/` infra ([`classify_fixture.rs`](../../tests/classify_fixture.rs))
  is the right home
- **`SmallVec` dep.** Already a transitive workspace dep via
  POST13zerocopy's wal-rs work; verify before adding to walshadow's
  `Cargo.toml`
