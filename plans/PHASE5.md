# PHASE5 — heap-tuple decoder + Tier 1/2 type matrix

Closes [Phase 5 of PLAN.md](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix).
Walks `RM_HEAP_ID` / `RM_HEAP2_ID` records the filter classifies as
`User`, projects payload bytes through a per-relation
`RelDescriptor` fetched from
[`ShadowCatalog`](../src/shadow_catalog.rs), and emits a structured
`DecodedHeap { rfn, xid, source_lsn, op, new, old }`. Tier 1 (fixed-
width) + Tier 2 (length-prefixed mechanical) types decode inline;
Tier 3 (`numeric`, `jsonb`, arrays, `inet`, `interval`, `tsvector`)
surfaces as `ColumnValue::Unsupported { type_oid, raw }` and is
deferred to [Phase 9](PLAN.md#phase-9--differential-decode-oracle--tier-3-type-matrix)
alongside the differential decode oracle.

## What landed

| item | files | tests |
|---|---|---|
| `heap_decoder` module — `decode_heap_record` + Tier 1/2 type matrix + replica-identity dispatcher | `src/heap_decoder.rs` | 14 unit tests inline |
| `ColumnValue` + `DecodedTuple` + `DecodedHeap` + `HeapOp` public surface | `src/heap_decoder.rs` | every decoder unit test |
| `ToastPointer` (`varatt_external` on-disk shape) for Phase 6 hand-off | `src/heap_decoder.rs` | `decode_external_toast_pointer` |
| `is_replica_identity_attr` predicate over [`ReplIdent`](../src/shadow_catalog.rs) | `src/heap_decoder.rs` | `is_replica_identity_attr_matrix` |
| `decoder_sink` module — `DecoderSink<O: TupleObserver>` adapter into `WalStream` | `src/decoder_sink.rs` | 4 unit tests |
| `MetricsTupleObserver` + `CollectingTupleObserver` | `src/decoder_sink.rs` | `metrics_observer_buckets_by_op`, `collecting_observer_keeps_full_clone` |
| `DecoderStats` summary line for daemon status output | `src/decoder_sink.rs` | `stats_summary_skips_zero_buckets` |
| `DaemonSinks` inline composite in `walshadow-stream` (metrics + decoder) | `src/bin/stream.rs` | live PG, not unit-tested |
| Status line extended with decoder stats | `src/bin/stream.rs` | live PG |

No new runtime dependencies. Builds clean on `cargo clippy
--all-targets -- -D warnings`. Test counts:

- `cargo test --lib`: 121 passed (was 103 at end of PRE5b; +18 = 14
  `heap_decoder::tests::*` + 4 `decoder_sink::tests::*`).
- Existing integration tests untouched, all green.

## What didn't get done

Three items deferred explicitly:

- **Live-PG roundtrip test.** Phase 5 ships unit-only coverage. A live
  test would `INSERT`/`UPDATE`/`DELETE` against source PG, capture
  the segment, run it through the filter + decoder, and assert
  decoded values match. Live tests already exist for the filter
  half (`tests/wal_stream_e2e.rs`); extending them with a decode
  pass needs a `ShadowCatalog` stub (the decoder hot path calls
  `relation_at` and a real `ShadowCatalog` requires a running shadow).
  Mechanical follow-up — punt to Phase 5 follow-up or join with
  Phase 8's e2e DDL drill.
- **TOAST detoasting.** `ColumnValue::ExternalToast(ToastPointer)`
  surfaces the on-disk pointer (`va_rawsize`, `va_extinfo`,
  `va_valueid`, `va_toastrelid`) but does not fetch chunks. Phase 6's
  TOAST reassembly will key on `(va_toastrelid, va_valueid)` and
  pull chunks from the same WAL stream. The decoder hands off
  enough metadata.
- **Xact buffering.** Decoder emits eagerly. Aborted xacts produce
  ghost rows downstream. PLAN.md §Phase 5 "Rollback status, explicit"
  documents this. The output's `xid` field is the bridge for Phase 6's
  buffer.

## Design decisions

### Decoder reads `block.data`, not the FPI

Source PG with `wal_level=logical` (walshadow's hard floor, per
[PLAN.md "wal_level on source"](PLAN.md#4-wal_level-on-source)) sets
`bufflags |= REGBUF_KEEP_DATA` in
`heap_insert`/`heap_update`/`heap_delete`
([heapam.c L2202](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/access/heap/heapam.c#L2202),
 [heapam.c L8973](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/access/heap/heapam.c#L8973),
 [heapam.c L3022-3030](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/access/heap/heapam.c#L3022)).
`KEEP_DATA` guarantees the tuple bytes ride in `block.data` even when
PG also attaches an FPI for the page. So Phase 5 reads exclusively
from `block.data`; the FPI-restore path
([`fpi.rs`](../src/fpi.rs)) is reserved for Phase 6's TOAST chunk
re-reads and the [BASEBACKUP](BASEBACKUP.md) initial-load path.

The alternative (always restore FPI, walk the page, find the tuple
slot) would have to re-implement PG's `heap_xlog_insert` page logic
and would gain nothing on the `wal_level=logical` path. Keeping the
two surfaces independent matches PG's own split between
`XLogRecGetBlockData` and `XLogRecGetBlockImage`.

### Block-data column-data offset = `5 + (t_hoff - 23)`

PG writes the per-tuple WAL payload as `xl_heap_header (5 bytes) +
[t_data + SizeofHeapTupleHeader .. t_data + t_len]`
([heapam.c L2222-2226 for INSERT](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/access/heap/heapam.c#L2222),
 [heapam.c L9012-9036 for UPDATE](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/access/heap/heapam.c#L9012)).
`SizeofHeapTupleHeader` is the 23-byte `HeapTupleHeaderData` prefix
that gets stripped before WAL because every byte in it is either
recoverable from the WAL record's own xact_id (xmin/xmax/cid) or
recoverable from the buffer ID (ctid). The bitmap + alignment pad
between offset 23 and `t_hoff` is always written verbatim, so the
column data starts at `block.data[5 + (t_hoff - 23)]`.

`t_hoff` itself is MAXALIGN'd (8 on every supported platform —
[htup_details.h L172](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/include/access/htup_details.h#L172)),
so the logical column-data offset always begins on an 8-byte boundary.
Per-column alignment via `att_align_nominal`
([tupmacs.h L150](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/include/access/tupmacs.h#L150))
treats column 1 as if it starts at logical offset 0 — and that offset
is itself 8-aligned because of `t_hoff`.

### `att_align_nominal` mirroring + the short-varlena peek

PG's `att_align_pointer` ([tupmacs.h L118](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/include/access/tupmacs.h#L118))
skips alignment for varlena columns (`typlen == -1`) when the next
byte is a *non-zero* 1-byte length word — because PG forbids zero
padding bytes (every padding byte is `0`, every short-varlena header
starts non-zero per the bit layout in
[varatt.h L142-165](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/include/varatt.h#L142)).
The decoder mirrors this: `att_align_nominal` in `heap_decoder.rs`
peeks at the WAL byte before aligning a varlena. The peek is the
reason walking has to be byte-accurate — getting alignment off by 1
on a varlena column desyncs every subsequent column.

When we can't peek (the column lives inside a prefix-compressed
region of an UPDATE), we fall back to plain MAXALIGN — PG itself
aligns the old tuple this way before computing the prefix, so the
fallback is correct on the byte-budget accounting.

### Prefix/suffix-compressed UPDATE → partial decode + Phase 6 hand-off

`heap_update` writes prefix and suffix as `uint16` length headers
before the `xl_heap_header` when
`XLH_UPDATE_PREFIX_FROM_OLD` / `XLH_UPDATE_SUFFIX_FROM_OLD` are set
([heapam.c L8985-8999](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/access/heap/heapam.c#L8985)),
and the column-data slice it logs is
`[t_hoff + prefixlen .. t_len - suffixlen]`. The leading `prefixlen`
bytes of the logical column-data region — which may straddle one or
more columns — are *not in WAL*; they're recovered at PG replay time
by copying from the old page.

Phase 5's decoder walks all columns in attnum order tracking the
logical offset `cur`. When `cur < prefixlen`, the column landed
inside the prefix and the decoder emits `None` (advance `cur` by
`attlen`). When `cur >= prefixlen + (wal bytes available)`, the
column landed inside the suffix and the decoder emits `None`. The
`DecodedTuple` is flagged `partial = true`.

For Tier 1/2 fixed-width columns this is byte-accurate — PG's prefix
compute is byte-level and doesn't split columns mid-byte unless the
bytes happen to match. For varlena columns *inside* a prefix the
decoder bails out: it has no way to advance the cursor without
reading the column length, and the length is in the un-logged
prefix. Remaining columns also surface as `None` in that case. Phase
6's xact buffer can reconstruct the missing columns from the
previously-buffered tuple image.

PHASE5 ships `DecoderStats.partial` so operators see the cadence of
compressed updates; in practice prefix/suffix-compression triggers
for UPDATEs that change one column late in the row, which is common
in OLTP.

### Replica-identity matrix → `is_replica_identity_attr`

PG's `ExtractReplicaIdentity`
([heapam.c L9150](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/access/heap/heapam.c#L9150))
writes the old-key tuple for UPDATE/DELETE under
`relreplident = 'i'` (UsingIndex) or `'d'` (Default with PK) by
calling `heap_form_tuple(desc, values, nulls)` with `nulls` set to
true for every column *not* in the replica-identity bitmap. So the
WAL bytes for an old-key tuple include the bitmap, with non-key
columns marked NULL.

Phase 5's decoder walks all columns just like for a new tuple. NULL
bits in the bitmap surface as `Some(ColumnValue::Null)`. Downstream
(Phase 7 emitter, Phase 9 oracle) consults `is_replica_identity_attr`
(in [`heap_decoder.rs`](../src/heap_decoder.rs)) to ignore the
non-key NULLs vs treat them as decoded values.

`Default` with no PK never gets a replica-identity write because
PG's `bms_is_empty(idattrs)` short-circuits at
[heapam.c L9196](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/access/heap/heapam.c#L9196).
The decoder returns `old = None` in that case (no `XLH_UPDATE_CONTAINS_OLD_*`
bit is set).

`Nothing` is the same — `ExtractReplicaIdentity` returns NULL at
[heapam.c L9166](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/access/heap/heapam.c#L9166).

### HOT updates split as a separate `HeapOp` variant

PG's `HEAP_HOT_UPDATED` ([htup_details.h L295](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/include/access/htup_details.h#L295))
flags an UPDATE that didn't touch any logged index — the new tuple
lives on the same page as the old, and the index entries still
point at the old tuple's ctid which chains forward via `t_ctid`.
For CDC purposes, HOT updates carry the same new tuple shape as a
non-HOT UPDATE; the distinction matters for Phase 9's differential
oracle (HOT updates can be skipped from the index-coverage drill)
and potentially for downstream MergeTree partitioning strategies.

Phase 5 surfaces them via `HeapOp::HotUpdate` separately from
`HeapOp::Update` so downstream consumers can pick. Phase 7's
emitter today would treat them identically and that's fine; future
optimisation has a clean handle.

### Dropped columns → `ColumnValue::Null`, advance cursor

`pg_attribute.attisdropped = true` columns retain `attlen` / `typalign`
/ `typbyval` per
[catalog convention](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/commands/tablecmds.c#L7610)
so older heap pages still walk correctly after a `DROP COLUMN`. The
decoder treats them as taking byte positions but emits `Null` — same
shape PG presents to a SELECT after the drop.

The alternative (skip them entirely) breaks cursor accounting: the
next column's alignment is relative to the previous column's end,
and treating a dropped column as zero-width would desync everything
that comes after.

### Decoder sink errors don't poison the stream

PG `WalStream` poisons on filter or segment-sink errors because
those signal byte-level corruption — every subsequent record after
a bad page or a failed segment write is meaningless. Decoder
semantic errors are different in kind: a malformed tuple on one
catalog miss doesn't compromise the next 1000 records.

`DecoderSink::on_record` absorbs `Decode` / `Catalog` errors into
`DecoderStats` (and only on observer-side errors propagates as
`SinkError::Other`). PLAN.md doesn't pin a poison policy for the
decoder side; this picks the looser policy that matches the
expected error shapes (catalog-miss races, type-matrix gaps).

## Wire-format quick reference

PG WAL record layout for the four ops Phase 5 handles. All numbers
little-endian on every supported PG architecture.

### INSERT (info & 0x70 == 0x00)

```
main_data:  xl_heap_insert            { offnum:u16, flags:u8 }                              [3 bytes]
block 0 data:
            xl_heap_header            { t_infomask2:u16, t_infomask:u16, t_hoff:u8 }        [5 bytes]
            bitmap[ceil(natts/8)]                                                            (only if HEAP_HASNULL)
            pad to t_hoff
            col data                                                                         [variable]
```

### DELETE (info & 0x70 == 0x10)

```
main_data:  xl_heap_delete            { xmax:u32, offnum:u16, infobits_set:u8, flags:u8 }   [8 bytes]
            [xl_heap_header (5) + bitmap + col data]                                         (only if flags & XLH_DELETE_CONTAINS_OLD)
block 0:    no tuple data
```

### UPDATE / HOT_UPDATE (info & 0x70 == 0x20 / 0x40)

```
main_data:  xl_heap_update            { old_xmax:u32, old_offnum:u16, old_infobits:u8,
                                        flags:u8, new_xmax:u32, new_offnum:u16 }            [14 bytes]
            [xl_heap_header (5) + bitmap + old key tuple bytes]                              (only if flags & XLH_UPDATE_CONTAINS_OLD)
block 0:    [prefixlen:u16]                                                                  (only if XLH_UPDATE_PREFIX_FROM_OLD)
            [suffixlen:u16]                                                                  (only if XLH_UPDATE_SUFFIX_FROM_OLD)
            xl_heap_header
            bitmap + pad
            col data, covers logical range [t_hoff + prefixlen .. t_len - suffixlen]
```

## Type matrix coverage

Tier 1 (fixed-width):

| OID | type | Rust variant | `attlen` | `typalign` |
|---|---|---|---|---|
| 16 | `bool` | `Bool(bool)` | 1 | `c` |
| 18 | `char` | `Char(i8)` | 1 | `c` |
| 21 | `int2` | `Int2(i16)` | 2 | `s` |
| 23 | `int4` | `Int4(i32)` | 4 | `i` |
| 20 | `int8` | `Int8(i64)` | 8 | `d` |
| 700 | `float4` | `Float4(f32)` | 4 | `i` |
| 701 | `float8` | `Float8(f64)` | 8 | `d` |
| 26 | `oid` | `Oid(u32)` | 4 | `i` |
| 1082 | `date` | `Date(i32)` | 4 | `i` |
| 1083 | `time` | `Time(i64)` | 8 | `d` |
| 1114 | `timestamp` | `Timestamp(i64)` | 8 | `d` |
| 1184 | `timestamptz` | `TimestampTz(i64)` | 8 | `d` |
| 1266 | `timetz` | `TimeTz { micros, tz_seconds }` | 12 | `d` |
| 2950 | `uuid` | `Uuid([u8;16])` | 16 | `c` |

Tier 2 (length-prefixed mechanical):

| OID | type | Rust variant | header |
|---|---|---|---|
| 17 | `bytea` | `Bytea(Vec<u8>)` | 1B or 4B varlena |
| 25 | `text` | `Text(String)` | 1B or 4B varlena, UTF-8 |
| 1042 | `bpchar` | `Text(String)` | 1B or 4B varlena, UTF-8 |
| 1043 | `varchar` | `Text(String)` | 1B or 4B varlena, UTF-8 |
| 19 | `name` | `Name(String)` | fixed 64-byte (NUL-padded), `attlen = 64` |

OIDs from
[pg_type.dat](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/include/catalog/pg_type.dat).

Anything else → `ColumnValue::Unsupported { type_oid, raw }`. The raw
bytes are kept so Phase 9's Tier-3 codec rollout can compile-time
test against the same wire bytes.

## Files touched

```
walshadow/src/lib.rs                  +4 / -1    (heap_decoder + decoder_sink module decls)
walshadow/src/heap_decoder.rs         +786       (new)
walshadow/src/decoder_sink.rs         +266       (new)
walshadow/src/bin/stream.rs           +35 / -9   (DaemonSinks composite, status line)
walshadow/plans/PLAN.md               status list (Phase 5)
walshadow/plans/PHASE5.md             new (this doc)
```

No new runtime crates; no dev-dep additions. The decoder reads from
the existing `wal_rs::pg::walparser::XLogRecord` struct, the catalog
from `crate::shadow_catalog::RelDescriptor`, and emits per-record
events that match PHASE5's PLAN.md surface.

## Live-cluster observations (deferred)

The decoder hasn't yet been exercised against a live PG capture —
unit tests fully cover the wire-format machinery, and the live test
would need either a `ShadowCatalog` stub or co-locating a shadow PG
boot in the integration harness. The latter exists at
[`tests/wal_stream_e2e.rs`](../tests/wal_stream_e2e.rs); plugging
the decoder into that flow is mechanical follow-up. Phase 8's e2e
DDL drill bundles this naturally.

## Deviations from [PLAN.md Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix)

* [PLAN.md](PLAN.md) sized Phase 5 at "≈700 LOC decoder + ~150 LOC
  async-sink refactor + Default-PK fetch ~30 LOC". Async sink + PK
  fetch landed under PRE5 prereqs (see
  [PHASE5_prereq_async_sink.md](PHASE5_prereq_async_sink.md),
  [PHASE5_prereq_default_pk.md](PHASE5_prereq_default_pk.md),
  [PHASE5_prereq_fpi.md](PHASE5_prereq_fpi.md)). This commit's
  decoder lands at ~790 LOC + ~270 LOC decoder-sink adapter ≈ +1060
  LOC of decoder code, slightly over budget. Overflow is roughly
  evenly split between the ColumnValue enum (more types in the matrix
  than the plan counted), the partial-decode logic for prefix/suffix
  UPDATE (PLAN.md described it but didn't size it), and the
  decoder-sink stats / observer trait surface (PLAN.md didn't
  enumerate the sink shape).
* PLAN.md's relreplident matrix says `UsingIndex` ⇒ "decode subset;
  non-indexed attnums emit as `None` in `old`". The implementation
  emits `Some(ColumnValue::Null)` for non-indexed columns instead,
  because that's what PG's `ExtractReplicaIdentity` writes into the
  WAL bytes — `heap_form_tuple(desc, values, nulls)` with `nulls[i]
  = true` for non-indexed positions, which surfaces as a bitmap NULL.
  Phase 7's emitter applies `is_replica_identity_attr` to distinguish
  decoded-and-explicitly-NULL columns from non-RI columns whose value
  was simply not logged. Documented in the new helper's doc comment.
* PLAN.md doesn't mention `ColumnValue::ExternalToast(ToastPointer)`
  — Phase 5 surfaces TOAST pointers as a structured value rather
  than blanket `Unsupported`. This makes Phase 6's TOAST reassembly
  a pure consumer of the decoded output (no need to re-parse the
  pointer); minor scope addition, ~30 LOC.
* PLAN.md mentions `_commit_ts` as `NULL` for Phase 5 emissions
  ([Phase 7 wire convention](PLAN.md#phase-7--ch-native-emitter-via-clickhouse-c-rs)).
  Phase 5's `DecodedHeap` doesn't carry a `commit_ts` field at all —
  the emitter slot is built in Phase 7, when the synthetic columns
  land alongside the CH translation. The bridge through `xid` is
  documented in the module header.

## Followups & known gaps

- `Decision::Drop`-only gating in `DecoderSink::on_record`: catalog
  heap writes (`Decision::Keep`) bypass the decoder. Correct for
  Phase 5 (catalog records ride the shadow-replay path), but it
  means the decoder never sees `pg_class` / `pg_attribute` row
  shapes. Phase 9's differential oracle may want a separate
  catalog-decoder mode; flag with a `decode_catalog_records: bool`
  knob then.
- `EnsureOpen` race: `DecoderSink::on_record` takes the catalog
  mutex per record. Under the daemon's 16 MiB segment-burst pattern
  this can serialize ~10k catalog lookups against a single
  `tokio::sync::Mutex<ShadowCatalog>`. Phase 5 accepts the
  serialization because the cache-hit rate is high and
  `relation_at` is cheap on a hit. The [PRE5b7
  Arc<Mutex<_>>](pre5/PRE5b7.md) wrap is the documented step;
  swapping to `RwLock` or sharded locks lands when measurement asks.
- `decode_heap_record` parses Tier 1/2 in line with the segment
  sink's burst dispatch — i.e., catalog lookups arrive at segment
  cadence (~once per segment per relation), not per record. Per-
  record latency is dominated by the catalog lock, not the decoder
  arithmetic. Documented for Phase 7 / 9 perf tuning to find later.
- `XLOG_HEAP_INPLACE` (0x70) ships system-catalog updates that
  bypass MVCC. Phase 5 skips silently (counted in
  `DecoderStats.skipped_op`). PG's `heap_inplace_update`
  ([heapam.c L6300](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/backend/access/heap/heapam.c#L6300))
  is catalog-only; user tables don't trip it. If shadow PG ever
  needs to reflect inplace writes on a user-tracked relation, we
  revisit.
- `XLOG_HEAP2_MULTI_INSERT` (0x50, RmId::Heap2) — bulk insert path
  used by `COPY` and `INSERT ... SELECT`. The wire format
  ([heapam_xlog.h L181](https://github.com/postgres/postgres/blob/REL_18_STABLE/src/include/access/heapam_xlog.h#L181))
  is per-tuple `xl_multi_insert_tuple` records inside one block-0
  data area. Phase 5 skips with `skipped_op`. Phase 6's xact buffer
  needs the offset array from main_data to fan out properly, so
  decoder support lands there.
- Composite types (`pg_type.typtype = 'c'`) are decoded as
  `Unsupported` today. Phase 5 doesn't need them — user-table heap
  records reference base or row types from a user namespace.
  Composite types in catalog use are out of scope (catalog writes
  skip the decoder entirely).
