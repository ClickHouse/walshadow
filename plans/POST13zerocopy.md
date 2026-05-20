# POST13zerocopy — cut hot-path allocations + segment-buffer churn

Reduce per-record and per-segment allocations in the streaming pipeline
that PHASE13 wired up. walshadow runs on no-overcommit hosts, so every
`Vec::with_capacity` is a real VM charge, not free address space — the
daemon's RSS ceiling matches its virtual size

Audit done after PHASE13 landed (commit `a365832`), against the
streaming-fed shadow pipeline now in `src/streaming_walker.rs`,
`src/wal_stream.rs`, `src/shadow_stream.rs`

Goal: every record that fits on a single page travels from socket to
sink without a heap allocation for its bytes; the 16 MiB segment
buffer is allocated once for the daemon's lifetime instead of once per
segment

## Scope summary

| # | Hole                                                  | Per     | Shape of fix                              |
|---|-------------------------------------------------------|---------|-------------------------------------------|
| 1 | `drain_records` double-clones `logical_bytes`         | record  | consume owned Vec                         |
| 2 | `StreamingWalker::take_segment` reallocates 16 MiB    | segment | reuse buffer, hand sinks `&[u8]`          |
| 3 | `CompletedRecord.logical_bytes` copies bytes in buf   | record  | `Cow<'_, [u8]>` borrowing into `walker.buf` |
| 4 | `ShadowStreamSink::on_wire_chunk` 3-way amplifies     | rec×con | encode frame into queue directly          |
| 5 | `SpillWriter::write` double-allocs per entry          | spill   | append into one buffer                    |
| 6 | `Pending.accumulated` duplicates cross-page bytes     | x-page  | drop the Vec, carry only `byte_ranges`    |
| 7 | `ch_emitter::route` `format!` per row                 | row     | cache qualified name on `RelDescriptor`   |
| 8 | `toast_chunk_from_decoded` clones bytea               | toast   | `mem::take` the column out                |
| 9 | wal-rs parser `to_vec()` on every block/main_data     | record  | borrowing `XLogRecord<'a>`                |

Items 1, 4, 5, 7, 8 are local single-file edits. Item 2 ripples through
`WalStream::flush_segment` + `close`. Item 3 is the largest local
behaviour shift and depends on item 2 landing first. Item 9 is the
floor on what items 3 + 6 can reach, and on wal-rs's general
allocation profile; walshadow owns wal-rs, so it lands in this plan

## Strategy

Land in three passes:

1. **Cheap wins** (items 1, 4, 5, 7, 8). Each is a small diff inside
   one file, no API change. Cumulative effect: roughly halves
   per-record allocations on the streaming path; removes the per-row
   `format!` on the CH emit path
2. **walshadow API shifts** (items 2 + 3 + 6 together).
   `StreamingWalker`'s public surface changes — `take_segment`
   returns `&[u8]` + `reset()`, `CompletedRecord` either drops
   `logical_bytes` or wraps it in `Cow`. Cascade-affects
   `WalStream::flush_segment`, `WalStream::close`, `drain_records`,
   every `SegmentSink` impl
3. **wal-rs refactor** (item 9). Borrowing `XLogRecord<'a>` removes
   the parser's per-block + per-main_data heap allocations entirely.
   Done after passes 1–2 so the walshadow side is already shaped
   around borrows

## 1. `drain_records` double-clones the completed record's bytes

[`src/wal_stream.rs:613-668`](../src/wal_stream.rs)

```rust
let final_bytes: Vec<u8>;
if decision == Decision::Drop {
    let mut buf = completed.logical_bytes.clone();   // clone #1 — already-owned Vec
    noop_replace(&mut buf)?;
    self.walker.rewrite_record(&completed.byte_ranges, &buf);
    final_bytes = buf;
} else {
    final_bytes = completed.logical_bytes.clone();   // clone #2 — pure waste
}
```

`completed.logical_bytes` is an owned `Vec<u8>` returned by
`try_next`; nothing else holds it. Consume it in both branches:

```rust
let mut bytes = completed.logical_bytes;
if decision == Decision::Drop {
    noop_replace(&mut bytes)?;
    self.walker.rewrite_record(&completed.byte_ranges, &bytes);
}
```

Two `Vec<u8>` allocations gone per record. One-line diff. No semantics
change (rewrite was already in place via `walker.rewrite_record`; the
local `buf` was only used as the scratch for `noop_replace`)

## 2. `StreamingWalker::take_segment` reallocates 16 MiB per segment

[`src/streaming_walker.rs:185-198`](../src/streaming_walker.rs):

```rust
pub fn take_segment(&mut self) -> Vec<u8> {
    let mut new_buf = Vec::new();
    new_buf.reserve_exact(self.seg_size);
    let out = std::mem::replace(&mut self.buf, new_buf);
    ...
    out
}
```

Returns a 16 MiB `Vec<u8>` and allocates a fresh 16 MiB on the spot.
Under steady-state replication (`pg_settings` default → segment fills
≈25 / s on busy DBs), this is >800 MiB / s of allocator churn plus
matching free traffic from whichever sink drops the Vec. Each fresh
reservation is a real commit charge on no-overcommit

The only consumer is `WalStream::flush_segment`
([`src/wal_stream.rs:684`](../src/wal_stream.rs)) which immediately
passes the bytes to `SegmentSink::on_segment(bytes: &[u8], ...)`. The
sink borrows; ownership transfer is wasted

Refactor:

```rust
impl StreamingWalker {
    pub fn segment_bytes(&self) -> &[u8] { &self.buf }
    pub fn reset_segment(&mut self) {
        self.buf.clear();
        // ...all the cursor/page_* resets from take_segment
    }
}
```

`WalStream::flush_segment` then dispatches `self.walker.segment_bytes()`
and calls `self.walker.reset_segment()` after the sink returns. The
walker's 16 MiB allocation lives for the daemon's lifetime

`WalStream::close` ([`src/wal_stream.rs:733`](../src/wal_stream.rs))
calls `take_segment` too; refactor to use `segment_bytes()` + a local
zero-pad scratch sized to the segment-tail gap. Close fires once per
shutdown, so the allocation there is acceptable

Single biggest VM win in this audit

## 3. `CompletedRecord.logical_bytes` copies bytes already in the walker buffer

[`src/streaming_walker.rs:53-66`](../src/streaming_walker.rs),
materialised at L459-468 (single-page) and L471-482 (cross-page)

Single-page records — the overwhelming common case — already sit
contiguously inside `self.buf`. The walker copies them into a fresh
`Vec<u8>` so the caller can hand the bytes to `parse_record_from_bytes`,
`noop_replace`, and the bytes sink. After item 2 lands the buffer
lives forever, so a borrow into `self.buf` is sound for the record's
parse + decide step

The complication: `WalStream::drain_records` calls
`self.walker.rewrite_record(...)` afterwards, which mutates `self.buf`
through `&mut self`. Holding a `&[u8]` into the buffer across that
call is a borrow violation

Two options:

* **Reorder**: parse + decide + build the final byte image into a
  small scratch Vec, then call `rewrite_record`. The bytes sink fires
  *after* rewrite anyway, so the only borrow lifetime is the parse +
  filter step. Doable, but the scratch Vec for the Drop path (NOOP
  rewrite needs a mutable buffer) re-introduces one allocation. Net
  win is only the Keep path

* **`Cow<'_, [u8]>`**: `try_next` returns `CompletedRecord<'a>` with
  `logical_bytes: Cow<'a, [u8]>`. Borrowed for contiguous (single-page)
  records, owned for cross-page (where we already assemble). The
  caller calls `.into_owned()` when they need a mutable copy (Drop
  path) and reads `&*logical_bytes` when they don't (Keep path).
  Borrow is released before `rewrite_record` because `drain_records`
  pulls one record at a time and finishes with it before iterating

Cow is the cleaner shape. `CompletedRecord` becomes generic over the
walker's borrow:

```rust
pub struct CompletedRecord<'a> {
    pub logical_bytes: Cow<'a, [u8]>,
    pub byte_ranges: ByteRanges,
    pub start_offset: usize,
    pub page_magic: u16,
}
```

Single-page hit returns `Cow::Borrowed(&self.buf[off..off+len])`

Even cheaper alternative: keep the API owned but switch the `Vec` to
`SmallVec<[u8; 256]>` so records under the typical xact/heap size
(40–200 bytes) stay on the stack. Pays a 256-byte stack cost per
in-flight record (one at a time in the streaming path → negligible)

## 4. `ShadowStreamSink::on_wire_chunk` makes three copies per (record × connection)

[`src/shadow_stream.rs:228-306`](../src/shadow_stream.rs):

```rust
let ids: Vec<u64> = state.connections.keys().copied().collect();   // (1)
for id in ids {
    ...
    let frame = wrap_copy_data(&encode_wal_data_frame(
        frame_lsn,
        state.server_wal_end,
        to_send,
    ));                                                              // (2) + (2b)
    if state.enqueue(id, frame) {                                    // (3)
        ...
    }
}
```

Stacked allocations per (record × connection):

1. `ids` Vec collected so `enqueue` can re-borrow `state` mutably.
2. `encode_wal_data_frame` returns a fresh `Vec<u8>` per call.
2b. `wrap_copy_data` clones that Vec into a new envelope.
3. `enqueue` calls `extend_from_slice` into a per-connection queue
   Vec, copying the framed envelope again.

For `N` active shadow connections, that's `3N` copies of the same
record bytes per record

Fixes, ordered by impact:

* Give `wal-rs` an `encode_wal_data_frame_into(out: &mut Vec<u8>, ...)`
  shape (mirroring the keepalive variant), and have `shadow_stream`
  build the CopyData envelope by writing the `'d'` + length placeholder
  + body directly into the per-connection queue, back-patching the
  length. Removes (2), (2b), and (3) — bytes never leave the queue
* Header bytes are identical across connections except for the 8-byte
  send timestamp (per `pg_replication_protocol`). Encode once into a
  scratch, splice the timestamp per connection. Saves the
  `bytes.len()` byte copy per additional connection — useful when an
  operator runs 2-3 shadow replicas, irrelevant for single-shadow
* `ids` Vec: restructure the borrow so we iterate `state.connections`
  directly. Holding `&mut state` across the loop body needs `enqueue`
  inlined or split into a `enqueue_locked(&mut self, ...)` helper that
  takes a connection reference. Or accept the `Vec<u64>` cost (it's
  `8 * N` bytes, allocator can fast-path the `SmallVec<[u64; 4]>`
  equivalent)

`on_segment_boundary` (L269-305) has the same `ids` collection +
keepalive framing pattern; fix together

## 5. `SpillWriter::write` double-allocates per entry

[`src/spill.rs:177-196`](../src/spill.rs):

```rust
pub async fn write(&mut self, entry: &SpillEntry) -> Result<()> {
    let mut body = Vec::with_capacity(128);
    match entry {
        SpillEntry::Heap(h) => {
            body.push(0u8);
            let inner = encode_heap(h);                     // alloc + grow
            push_u32(&mut body, inner.len() as u32);
            body.extend_from_slice(&inner);                 // copy inner → body
        }
        SpillEntry::Chunk(c) => { ... same shape ... }
    }
    self.file.write_all(&body).await?;
    ...
}
```

`encode_heap` allocates `inner` and likely re-grows several times
(starts at 64 — see [`spill.rs:307`](../src/spill.rs)). Then the whole
encoded blob is copied byte-for-byte into `body`. Two Vec allocations,
one full copy, per spill write — hit on every eviction loop pass

Refactor `encode_heap` / `encode_chunk` / `encode_value` etc. to take
`out: &mut Vec<u8>` and append in place. Caller's shape:

```rust
let mut body = Vec::with_capacity(128);
match entry {
    SpillEntry::Heap(h) => {
        body.push(0u8);
        let len_off = body.len();
        body.extend_from_slice(&[0u8; 4]);                  // length placeholder
        encode_heap_into(&mut body, h);
        let inner_len = (body.len() - len_off - 4) as u32;
        body[len_off..len_off + 4].copy_from_slice(&inner_len.to_le_bytes());
    }
    SpillEntry::Chunk(c) => { ... }
}
self.file.write_all(&body).await?;
```

One Vec, one back-patch, no copy. Halves spill-path allocator traffic

## 6. `Pending.accumulated` duplicates cross-page record bytes

[`src/streaming_walker.rs:72`](../src/streaming_walker.rs)

Cross-page records hold both the bytes in `self.buf` (where every
walker-extend put them) *and* in `Pending.accumulated`. Once item 3
lands, the `Cow::Owned` cross-page path is the one consumer of
`Pending.accumulated` — and even then, we could assemble lazily from
`byte_ranges` at `fully_loaded()` time, never holding a duplicate Vec
during the stitching window

Marginal versus item 3 because cross-page records are uncommon
(record sizes are well below 8 kB the vast majority of the time), but
the fix is a few lines once `byte_ranges` is the sole source of truth

```rust
struct Pending {
    start_offset: usize,
    total_len: Option<u32>,
    byte_ranges: ByteRanges,
    accumulated_len: usize,            // replaces .accumulated.len()
    page_magic: u16,
}
```

Logical bytes assembled at completion: `byte_ranges.iter().flat_map(|&(o, l)| &buf[o..o+l]).collect()`

## 7. `ch_emitter::route` `format!`s a fresh String per row

[`src/ch_emitter.rs:1025`](../src/ch_emitter.rs):

```rust
let key = format!("{}.{}", rel.namespace_name, rel.name);
```

Allocates a `String` per emitted tuple just to look up the table
mapping. On a write-heavy workload that's one allocation per row, on
top of all the per-column ones

`RelDescriptor` (see [`src/shadow_catalog.rs`](../src/shadow_catalog.rs))
should cache the qualified name. Add `pub qualified_name: Arc<str>`
populated at descriptor build time. The mapping `HashMap` keys can
move to `Arc<str>` too, or stay `String` and the lookup uses
`map.get(rel.qualified_name.as_ref())`

`drain_xact` ([`src/ch_emitter.rs:1118`](../src/ch_emitter.rs)) has
the same shape — `keys.cloned().collect()` per xact. Switch to
`self.tables.drain()` and consume the keys; clears in the same step

## 8. `toast_chunk_from_decoded` clones bytea unnecessarily

[`src/xact_buffer.rs:880-910`](../src/xact_buffer.rs):

```rust
let chunk_data = match new.columns[2].as_ref()? {
    ColumnValue::Bytea(b) => b.clone(),
    ColumnValue::Text(s) => s.as_bytes().to_vec(),
    _ => return None,
};
```

Caller (`BufferingDecoderSink::on_record`,
[`xact_buffer.rs:851-857`](../src/xact_buffer.rs)) holds `decoded`
only to extract the chunk and then drops the heap. The clone is
because `&decoded` is borrowed. Refactor to take `&mut DecodedHeap`
(or `DecodedHeap` by value) and `mem::take` the column out:

```rust
fn toast_chunk_from_decoded(d: &mut DecodedHeap, rel: &RelDescriptor) -> Option<ToastChunk> {
    ...
    let chunk_data = match d.new.as_mut()?.columns.get_mut(2)?.take()? {
        ColumnValue::Bytea(b) => b,
        ColumnValue::Text(s) => s.into_bytes(),
        _ => return None,
    };
    ...
}
```

For TOAST-heavy workloads (~2 KiB chunks, many per large `bytea`)
this halves the chunk-path byte volume. Minor cleanups in the same
file:

* [L592](../src/xact_buffer.rs) `let p_clone = p.clone();` — 16-byte
  struct, exists only to release the `col` borrow before mutating.
  Refactor `detoast_tuple` so the borrow shape doesn't need it
* [L630](../src/xact_buffer.rs) `reassemble`'s `concat: Vec<u8>` has
  no capacity hint; precompute `map.values().map(Vec::len).sum()` and
  `with_capacity` it
* [L650](../src/xact_buffer.rs) `vec![0u8; raw_len]` zero-fills before
  `pglz::decompress_into` overwrites every byte; check if the pglz
  crate accepts a `Vec::with_capacity` + `set_len` after the
  decompress reports `n`

## 9. wal-rs `XLogRecord` becomes `XLogRecord<'a>`

[`wal-rs/src/pg/walparser/parse.rs:351-385`](../wal-rs/src/pg/walparser/parse.rs),
[`wal-rs/src/pg/walparser/types.rs:201-205`](../wal-rs/src/pg/walparser/types.rs)

The parser allocates one `Vec<u8>` per block image, one per block
data, and one for `main_data` — all populated via `to_vec()` off the
input slice:

```rust
b.image = head.to_vec();
b.data = head.to_vec();
...
Ok(head.to_vec())                     // main_data
```

A record with `N` block references costs `2N + 1` heap allocations on
top of the per-record `Vec<XLogRecordBlock>`. Most heap UPDATE
records are `N = 1`, so this is 3 allocations / record minimum,
copying every WAL byte once even before walshadow touches it

This is the allocation floor on items 3 and 6. After items 2 + 3 land,
`WalStream` holds the record bytes in `walker.buf` (16 MiB segment
buffer, lives forever). The wal-rs parser sits between the buffer
slice and the consumer — it is the only thing left that copies

### Refactor shape

```rust
pub struct XLogRecord<'a> {
    pub header: XLogRecordHeader,
    pub origin: u16,
    pub main_data: &'a [u8],
    pub blocks: SmallVec<[XLogRecordBlock<'a>; 2]>,
}

pub struct XLogRecordBlock<'a> {
    pub header: XLogRecordBlockHeader,
    pub image: &'a [u8],
    pub data: &'a [u8],
}

pub fn parse_record_from_bytes<'a>(
    data: &'a [u8],
    page_magic: u16,
) -> Result<XLogRecord<'a>, ParseError>;
```

Every consumer of `block.data` / `block.image` / `record.main_data`
in walshadow already reads them as slices — see [`fpi.rs:54-94`](../src/fpi.rs),
[`catalog_tracker.rs:222`](../src/catalog_tracker.rs),
[`heap_decoder.rs:391-405`](../src/heap_decoder.rs),
[`pg_class_decoder.rs`](../src/pg_class_decoder.rs), and the
`main_data::relation_for_empty` path in
[`main_data.rs`](../src/main_data.rs). None of them need ownership;
the shift is invisible at every read site, modulo adding `'a` to
function signatures that pass `XLogRecord` around

### Cascading borrow lifetime

The lifetime `'a` ties the parsed record to the input slice. In the
streaming path:

* `StreamingWalker::buf: Vec<u8>` — lives the daemon's lifetime
  after item 2 lands
* `try_next()` returns `CompletedRecord<'a>` borrowing from `buf`
  (item 3)
* `parse_record_from_bytes(&completed.logical_bytes, ...)` returns
  `XLogRecord<'a>` borrowing transitively from `buf`
* `Filter::decide(&XLogRecord)`, `decode_heap_record(&XLogRecord, ...)`,
  every read site reads `&[u8]` slices and is `'a`-clean

The borrow is released the moment `drain_records` finishes with the
record (parse → decide → bytes_sink dispatch → record_sink dispatch),
before the next `walker.try_next()` call or the eventual
`walker.rewrite_record()`. The xact buffer (item 8 above) is the only
consumer that needs to outlive the record's borrow — it consumes the
decoded `DecodedHeap` (already owned, with `Vec<u8>` Bytea / `String`
Text), not the raw `XLogRecord`

### Sinks that store records

[`CollectingRecordSink`](../src/wal_stream.rs#L213) and
[`CollectingBytesSink`](../src/wal_stream.rs#L337) store `Record`
([`wal_stream.rs:109`](../src/wal_stream.rs)) which embeds
`XLogRecord`. With the borrowing parser these become
`Record<'a>`, which test sinks can't store across iterations without
materialising. Two paths:

* Test sinks call an explicit `record.to_owned()` that does the Vec
  allocations the parser used to do automatically. Cost lives only
  in tests
* Provide a `to_owned()` helper on `XLogRecord<'_>` returning an
  `XLogRecordOwned` mirror. Same shape, only used by tests + sinks
  that genuinely need ownership (none in production)

Production sinks (`DecoderSink`, `BufferingDecoderSink`,
`MetricsRecordSink`, `XactRecordSink`, the bytes sinks) all read the
`Record` inside their `on_record` future and never store it. No
ownership transition needed

### Other wal-rs allocation review

Drastic-changes-OK gives room to address a few related items in the
same wal-rs pass:

* [`walparser/types.rs:201`](../wal-rs/src/pg/walparser/types.rs)
  `XLogRecord.blocks: Vec<XLogRecordBlock>`. Records average 0–2
  blocks. `SmallVec<[_; 2]>` keeps the common case stack-resident.
  Cargo-feature-gated to avoid forcing the smallvec dep on
  non-walshadow users
* [`parse.rs:339`](../wal-rs/src/pg/walparser/parse.rs)
  `record.blocks.push(...)` reserves on every push. With smallvec the
  reserves stay on stack until block 3 (never, in practice)
* The `record.blocks` walk runs *twice* — once in
  `read_block_header_part` to discover block IDs, once in
  `read_block_data_and_images` to attach payloads. Could be merged
  into a single pass since block IDs arrive in order; current shape
  is a leftover from the wal-g port. Removes a `Vec<_>` iteration
  pass per record

### Risk surface

Lifetime-poisoning is the obvious cost: every `fn` that names
`XLogRecord` gains an `'a`. Worth auditing how `XLogRecord` shows up
in trait objects — `RecordSink::on_record(&Record<'a>)` and
`TupleObserver::on_tuple(&CommittedTuple)` should both be fine since
they're already `&'a` shaped on the future return type, but
`Box<dyn RecordSink + Send>` becomes
`Box<dyn for<'a> RecordSink<'a> + Send>` (HRTB). One pass through the
sink trait wiring to confirm

The blast radius is large in line count, but each individual site is
a mechanical `&` insertion. The semantic test is whether the existing
unit tests still pass without `Vec<u8>` copies — they should, since
every assertion is on byte slices, not on `Vec` identity

### Validation for the wal-rs pass

`criterion` bench against a known WAL segment (use the
`tests/fixtures/` payloads or capture one from the demo cluster):

* `parse_record_from_bytes` allocations per call: target 0 (was 3+
  per record)
* RSS during a full-segment walk: should drop by the byte volume of
  all `image` + `data` + `main_data` per record (varies, but for a
  100k-record heap-INSERT segment ≈ 200 MB → ≈ 0)
* Decode throughput: expect 1.5–3× improvement from the dropped
  allocator pressure alone, even before the smallvec change

## Validation

Each item above is independently measurable. A `criterion` benchmark
over `tests/integration_streaming.rs`'s 100k-record fixture run
through `WalStream::push` should track:

* allocations per record (use `dhat-rs` or `jemalloc_pprof`)
* peak RSS during the segment-fill window
* commit-charge ceiling (RSS + reserved address space)

Item 2's regression risk is the smallest of the three API-touching
items — `take_segment`'s only behavioural promise is "give me the
segment bytes + reset", and the new shape covers both. Item 3 needs
a careful sweep of `RecordBytesSink` / `SegmentSink` impls to confirm
no downstream caller relies on owning the bytes past the sink call

## Retro

What landed vs what the plan called for, plus the surprises picked
up while plumbing the lifetime through walshadow's record sinks

### §1 — `drain_records` double-clones

Landed as a 6-line diff in [`src/wal_stream.rs`](../src/wal_stream.rs).
Both clones gone; the Drop path moves `completed.logical_bytes` into
the scratch and `noop_replace` mutates it in place. Tests untouched

### §2 — `StreamingWalker::take_segment` → `reset_segment`

Landed as [`StreamingWalker::reset_segment`](../src/streaming_walker.rs)
— `buf.clear()` retains capacity for the daemon's lifetime, and the
walker's `buffer()` accessor returns `&[u8]` directly. `WalStream::
flush_segment` calls `segment_sink.on_segment(self.walker.buffer(),
…)` then `reset_segment()`; `close` zero-pads into a fresh local Vec
since shutdown allocates once

The plan called the new accessor `segment_bytes()`; the existing
`buffer()` already covered the same shape so the rename never landed

### §3 — `CompletedRecord` Cow-vs-stitched

First swing made `logical_bytes: Cow<'a, [u8]>` and added `<'a>` to
`CompletedRecord` and `try_next`. NLL choked on the resulting
`&mut self` return type: every loop iteration in `try_next` reborrowed
`self.pending`, `self.page_cursor`, `self.buf` while the prior
iteration's potential `CompletedRecord<'_>` return value was held to
own a `&mut self` borrow for the full function. Polonius would fix
this; stable Rust does not

Second swing kept `CompletedRecord` owned but replaced `logical_bytes:
Vec<u8>` with `stitched_bytes: Option<Vec<u8>>` — `None` for the
single-page case (caller reads bytes via `walker.buffer()` at
`byte_ranges[0]`), `Some` only for cross-page stitching. New
`CompletedRecord::logical_bytes(&self, walker_buf: &[u8]) -> &[u8]`
helper hides the distinction. Zero allocation for the single-page
common case; the cross-page `Vec` is the same one item 6 produces via
`Pending::materialise`. The byte savings on a typical heap-INSERT
stream are the dominant case

Tests that call `walker.try_next()` and later mutate the walker
(`rewrite_record`) must `clone()` the `byte_ranges` and drop the
`CompletedRecord` first — the borrow into `walker.buffer()` is
explicit. The drip-feed and cross-page tests added an explicit
`stitched_bytes.is_some()` / `is_none()` check so the byte
materialisation is visible in the test surface

### §4 — `ShadowStreamSink` encode-into-queue

Landed via two new `encode_*_into(&mut Vec<u8>, …)` siblings in
[`wal-rs/src/pg/replication/stream.rs`](../wal-rs/src/pg/replication/stream.rs)
(`encode_wal_data_frame_into`, `encode_keepalive_frame_into`) plus a
local `ShadowStreamState::enqueue_copy_data_with(id, |out| …)` helper
in [`src/shadow_stream.rs`](../src/shadow_stream.rs). The helper
writes `'d'` + a u32 BE length placeholder + body + back-patches the
length. Per (record × connection) the wire envelope is built once,
directly into the connection's send queue Vec — gone are the
`encode_wal_data_frame` Vec, the `wrap_copy_data` Vec, and the
`enqueue(Vec)` copy

The plan also called out the `ids: Vec<u64>` collected per dispatch
to free the `&state` borrow. Left in place — `SmallVec<[u64; 4]>`
would be a measurable win only at 4+ active shadow connections and
the `8 * N` byte alloc is fast-path

### §5 — `SpillWriter::write` single-buf

Landed as `encode_heap_into` / `encode_chunk_into` (`encode_value`
already took `&mut Vec<u8>`). `SpillWriter::write` pushes the tag,
writes a u32 LE length placeholder, calls the `_into` encoder, then
back-patches the inner length. One Vec per write, one back-patch, no
copy from a scratch Vec — half the spill-path allocator traffic gone

### §6 — `Pending` accumulated → byte_ranges only

[`Pending`](../src/streaming_walker.rs) drops `accumulated: Vec<u8>`
and tracks `accumulated_len: usize` instead. `Pending::
try_resolve_total_len(&self.buf)` walks the first 4 bytes through
`byte_ranges` to read `xl_tot_len`; `Pending::materialise(&self.buf)`
assembles the cross-page record into one Vec at completion time.
Cross-page records pay one allocation at the end (same as the
`Cow::Owned` shape from §3); the duplicated mid-stitch Vec is gone

### §7 — qualified-name cache on `RelDescriptor`

`pub qualified_name: Arc<str>` populated at descriptor build via
`RelDescriptor::build_qualified_name(ns, name)`. `ch_emitter::route`
looks up via `rel.qualified_name.as_ref()` — no per-row `format!`.
The `tables: HashMap<String, _>` keys stay `String` since the
emitter owns them lifetime-wise across xact boundaries; the lookup
threads `&str` through

`drain_xact` now consumes the table map via `std::mem::take(&mut
self.tables)` and iterates `(key, encoder)` pairs directly. Dropped
the old `finish_table(&str)` shim — its only caller was `drain_xact`,
and the new `finish_table_owned(_key, encoder)` consumes the encoder
in one move

### §8 — `toast_chunk_from_decoded` mem::take

Signature flipped to `fn toast_chunk_from_decoded(mut d: DecodedHeap,
rel: &RelDescriptor)`. The `chunk_data` column extracts via
`new.columns[2].take()`; the byte-volume halving lands. Caller in
`BufferingDecoderSink::on_record` passes `decoded` by value (it had
no other use past the chunk extraction)

`detoast_tuple`'s `p_clone` is gone — `ToastPointer` now derives
`Copy` (it's 16 bytes of plain integers) and the `*p` read frees the
borrow on the column slot. `reassemble`'s `concat: Vec<u8>` precomputes
`map.values().map(Vec::len).sum()` and `Vec::with_capacity`s. The
pglz `vec![0u8; raw_len]` zero-fill was not changed — pglz's
`decompress_into` API takes `&mut [u8]`, not a writer, so the zero
fill is the same cost as `set_len` + write. Skipped

### §9 — borrowing `XLogRecord<'a>`

Landed in `Cow<'a, [u8]>` form rather than the plan's `&'a [u8]`.
`XLogRecord<'a>` and `XLogRecordBlock<'a>` hold `main_data` and
per-block `image` / `data` as `Cow<'a, [u8]>`. The parser populates
`Cow::Borrowed(slice)` views straight off the input — every
`head.to_vec()` in `read_block_data_and_images` and
`read_xlog_record_main_data` is gone. Defaults use `Cow::Borrowed(&[])`
(static-empty), `into_owned()` materialises to `'static`

Cow keeps the owned-vs-borrowed distinction off the type surface for
test sinks: a `Record<'static>` constructed via `Cow::Owned(vec![…])`
in tests sits in the same shape as a `Record<'_>` parsed from a live
buffer. `state.rs` (the batch WAL-file walker, not on the streaming
hot path) calls `into_owned()` immediately after parse so its
returned `XLogPage<'static>` outlives the scratch buffer it parsed
from. `filter_segment` does the same — `ParsedRecord.record:
XLogRecord<'static>`

The smallvec change for `XLogRecord.blocks` — plan called out
`SmallVec<[_; 2]>` for the average 0–2 blocks per record — did not
land. Cow already delivers the dominant byte-traffic win; the Vec
remains. Re-evaluate after a criterion benchmark surfaces the alloc
shape

### Surprises

1. **Lifetimes through `RecordSink::on_record` work, but require
   `Record<'a>` and `&'a Record<'a>` with the same `'a`.** Tried
   first with two lifetimes (`<'a, 'b>` and `'b: 'a`) — the HRTB
   shape multiplies callsite friction at every sink impl. Collapsing
   to one lifetime kept the trait signature usable across the dozen+
   impls in the crate (`MetricsRecordSink`, `CompositeRecordSink`,
   `XactRecordSink`, `BufferingDecoderSink`, `DecoderSink`, plus
   integration-test sinks). `Cow`'s covariance over `'a` keeps the
   coercion ergonomic
2. **The Drop path forces a `parsed.into_owned()` even with the Cow
   parser.** `parse_record_from_bytes` returns slices borrowing from
   `walker.buf`; then `noop_replace` + `walker.rewrite_record` mutates
   `walker.buf` at the same byte ranges — which would invalidate
   `parsed.main_data` / `parsed.blocks[…].data`. So `drain_records`
   materialises `parsed.into_owned()` between filter decision and
   rewrite. The Keep path could stay zero-copy, but the trait shape
   forces a single owned type at the dispatch boundary; both branches
   pay one `into_owned()` per record dispatched to `record_sink`.
   The wins live in the parser-internal `to_vec()` removal — the
   parser no longer allocates during the parse phase itself
3. **`Cow<'_, [u8]>` mutation needs `to_mut()`.** Tests under
   `catalog_tracker` and `main_data` that mutated `r.main_data` in
   place (truncate, splice bytes) needed `r.main_data.to_mut()`. One
   `sed -i` round
4. **Test sinks store `Record<'static>`, paying the same allocations
   the parser used to.** `CollectingRecordSink` calls
   `record.parsed.clone().into_owned()` in `on_record`; the production
   sinks (decoder, emitter, xact buffer) consume `parsed` for the
   duration of one future and never store. Net production effect:
   the parser-internal allocations are gone, the boundary `into_owned`
   adds them back at one specific site (the Drop dispatch path) but
   only for filtered records. Keep records on a non-storing sink ride
   the full zero-copy path
5. **State.rs's `XLogPage<'static>` cost is wal-rs's batch-walker
   tax.** `parse_records_from_page` materialises every record before
   pushing into the returned Vec. That path is not on walshadow's
   streaming hot path; it's used by `extract_locations_from_wal_file`
   and `wal-rs`'s own tests. The owned shape there is the right
   default

### Validation

- `cargo test --workspace --lib` — 258 walshadow + 186 wal-rs unit
  tests green
- `cargo test --workspace` — every integration test still passes
  (`phase8_e2e`, `bin_stream_e2e`, `walsender_pg18_walreceiver`,
  `multi_segment_filter`, `wal_stream_chunk_boundary`, the spill
  + cursor + emitter coverage)
- `criterion` benchmark deferred — the plan asked for `dhat-rs` /
  `jemalloc_pprof` counts. Worth landing as a follow-up to confirm
  the predicted RSS drop (≈200 MB → ≈0 for a 100k-record
  heap-INSERT segment) and the 1.5–3× decode throughput shift
- `XLogRecord.blocks` smallvec, header-walk single-pass merge, and
  the pglz `vec![0u8; n]` zero-fill skip from §8 left for a
  follow-up — none are dominant on the byte-traffic numbers the rest
  of this audit eliminates
