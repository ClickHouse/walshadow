# xact — per-xid buffer, spill backend, subxact tracker

Component lives in [`src/xact_buffer.rs`](../src/xact_buffer.rs) +
[`src/spill.rs`](../src/spill.rs). Sits between the decoder fan-out
(see [decoder.md](decoder.md)) and the commit-drain observer (see
[emitter.md](emitter.md)). Source record stream feeding it: see
[source.md](source.md)

## Purpose

Buffer every decoded heap tuple + reassembled TOAST chunk per xid from
first heap touch until the matching `XLOG_XACT_COMMIT` / `_PREPARED`
drains it; abort discards. Mirrors PG `ReorderBuffer` shape for the
same problem in logical decoding, minus snapshot building (catalog
state already lives in shadow PG, see [shadow.md](shadow.md))

Three responsibilities collapse into one struct:

- per-xid sub-buffer for heaps + chunks, ordered by `source_lsn`
- spill-to-local-disk backend when in-memory budget breaches
- subxact tracker that hints early eviction & funnels commit drain
  across `top + subxids`

Catalog access is lazy: only heaps where any column is
`ColumnValue::ExternalToast` hit `ShadowCatalog::relation_at`, and only
at drain. Per-xact descriptor cache was tried then removed — duplicated
shadow's own LRU surface

## Buffer shape

`XactBuffer { config, store, inflight: HashMap<u32, XactState>,
bytes_in_memory, stats }`. One `XactState` per inflight xid:

```text
XactState {
    first_lsn,                              // sticky filename anchor
    in_mem: Vec<SpillEntry>,                // pending memory→spill, WAL order
    in_mem_bytes,                           // approximate accounting
    spill: Option<SpillWriter>,             // None until first eviction
    spill_bytes,                            // mirrors writer.byte_count()
    catalog_events: Vec<(u64, SchemaEvent)>, // lsn-stamped
}
```

`SpillEntry` is a tagged union of `Heap(Box<DecodedHeap>)` and
`Chunk(ToastChunk)`. Spill collapses both into a single per-xid file
because PG's `toast_save_datum` writes chunks in the same xact as the
referring tuple — cross-xact chunk references don't exist outside
`streaming=on` mode walshadow doesn't implement

`commit_lsn` rides through `CommittedTuple { decoded, commit_ts,
commit_lsn }` to the observer. `commit_lsn` is the LSN of the
`XLOG_XACT_COMMIT` record itself, used by the emitter's ack tracker.
`commit_ts` is parsed off `xl_xact_commit.xact_time` at the head of
`main_data` (i64 le)

## Eviction policy

`maybe_evict` runs after every `absorb` while
`bytes_in_memory > config.xact_buffer_max` (default
`DEFAULT_XACT_BUFFER_MAX = 64 MiB`, matches PG's
`logical_decoding_work_mem` default). Picks the largest *in-memory*
xact and flushes its `in_mem` vec to the spill writer, mirroring PG's
`ReorderBufferLargestTXN`

Largest-xact-first is correct because:

- small xacts evicted would bounce back on the next record, freeing
  nothing
- the heaviest xact frees the most bytes per file write
- the policy is xid-keyed, so a subxact's buffer evicts independently
  of its parent — long-lived tops with many subxacts shed memory
  evenly across the family

Catalog events are *not* spilled. A handful of DDL events per xact
stays in-memory; spilling would require encoding `RelDescriptor`
snapshots which duplicates [decoder.md](decoder.md)'s shape

Earlier impl used `min_by_key` linear scan over `inflight`. Lifted to
keep an explicit ordering when the inflight count grew past
O(thousands) under pgbench — see `inflight_snapshot` for the
diagnostic surface

## TOAST reassembly

Decoder sink ([decoder.md](decoder.md)'s `BufferingDecoderSink`)
recognises INSERTs into `pg_toast.pg_toast_<rel>` (three-column shape:
`chunk_id oid, chunk_seq int4, chunk_data bytea`) and reshapes them
into `ToastChunk` keyed on the toast relation's *pg_class OID* (not
relfilenode — `va_toastrelid` on the referring `ToastPointer` is an
OID, the two diverge after `VACUUM FULL` / `CLUSTER`)

Chunks ride the same `XactState.in_mem` deque as heaps. At drain, the
k-way merge accumulates them into `HashMap<(toast_relid, value_id),
BTreeMap<chunk_seq, Vec<u8>>>`; `reassemble` walks the BTreeMap
checking `seq == expected` and concatenates. Compression decoded
inline: `TOAST_COMPRESSION_PGLZ` via `pglz::decompress_into`,
`TOAST_COMPRESSION_LZ4` via `lz4_flex::decompress`. Method tag lives
in the top bits of `va_extinfo` past `VARLENA_EXTSIZE_BITS = 30`

Missing chunk surfaces as `XactBufferError::MissingToastChunk
{ toast_relid, value_id, missing }` rather than silent loss. Malformed
chunk shape (wrong column count, wrong types) bumps
`DecoderStats.toast_chunks_malformed`; the malformed counter is wired
into the decoder fan-out so silent toast loss is visible on the status
line

## Subxact tracker

`SubxactTracker { parent: HashMap<u32, u32>, children: HashMap<u32,
Vec<u32>> }`. Both directions kept so `forget_tree(top_xid)` runs O(k)
over actual children rather than scanning every `parent` entry.

Populated from `XLOG_XACT_ASSIGNMENT` (info `0x50`) via
`parse_xact_assignment` reading `(xtop: u32, nsub: i32, xsub[nsub])`
off `main_data`. Tracker is a HINT — PG batches the first
`PGPROC_MAX_CACHED_SUBXIDS` (= 64) subxacts under the top without
emitting an explicit ASSIGNMENT. Authoritative subxact list arrives
inline on the commit / abort record itself

`parse_xact_payload(info, main_data)` walks the tail in PG-source
order matching `xactdesc.c::ParseCommitRecord` / `ParseAbortRecord`:

```text
xact_time (i64)
[xinfo (u32) if info & XLOG_XACT_HAS_INFO]      // 0x80
dbinfo  (8 bytes)   if xinfo & HAS_DBINFO       // 1<<0
subxacts (i32 n + n×u32)
                    if xinfo & HAS_SUBXACTS     // 1<<1
relfilelocators (i32 n + n×12)
                    if xinfo & HAS_RELFILELOCATORS  // 1<<2
dropped_stats (i32 n + n×16)
                    if xinfo & HAS_DROPPED_STATS   // 1<<8
invals (i32 n + n×16) if xinfo & HAS_INVALS        // 1<<3
twophase (u32 xid)  if xinfo & HAS_TWOPHASE        // 1<<4
gid (cstr, NUL term) if xinfo & HAS_GID            // 1<<7
origin (8+8)        if xinfo & HAS_ORIGIN          // 1<<5
```

Short-read at any tail position degrades to
`XactCommitPayload::default()` (xact_time + no subxacts), so the
decoder doesn't poison the stream over a single bad record

`XactBuffer::abort(xid, abort_lsn, subxids)`: drops `xid`'s
`XactState` (top or standalone-subxact rollback) plus every subxid in
the slice. Each dropped state unlinks any spill file via
`SpillWriter::unlink`. Standalone subxact rollback for a sub of a
still-open top: top's pre-savepoint entries stay keyed on top_xid in
`inflight` and flush at top's COMMIT — drain-time merge across `top +
remaining_subxids` produces the correct survivor set

`XactBuffer::commit(top_xid, commit_ts, commit_lsn, subxids, catalog,
observer)`: pulls every xid in `(top_xid, ..subxids)` out of
`inflight`. For each: drains spill file (sequential read), unlinks,
appends `in_mem`. Result is one `VecDeque<SpillEntry>` per xid already
sorted ASC by `source_lsn`. K-way merge across `k = 1 + nsubxacts`
buffers using a linear-scan head pick (k typically ≤ 4, beats a heap
at that size)

`subxact_tracker.forget_tree(top)` runs after commit / abort drains,
dropping every edge rooted at the family. Cheap O(k) cleanup

## Spill backend

[`src/spill.rs`](../src/spill.rs). One file per xid under
`{data_dir}/spill/`, name `xid-{xid:010}-{first_lsn:016X}.bin`. LSN
suffix mirrors PG's `pg_replslot/<slot>/xid-*.snap` shape; without it,
two streams that picked up the same xid value after a slot rebuild or
post-restart could collide

File layout:

```text
[2 bytes "WS" magic = SPILL_MAGIC]
[u16 LE version = SPILL_VERSION = 2]
repeating:
  [u8 tag]
  [u32 LE inner_len]
  [body of inner_len bytes]
    tag=0 → SpillEntry::Heap   (encoded DecodedHeap)
    tag=1 → SpillEntry::Chunk  (encoded ToastChunk)
```

`SpillReader::check_header()` runs lazily on first `next()`: rejects
wrong magic with `SpillError::Format { offset: 0, detail: "bad
magic …" }`, wrong version with same shape at offset 2. Reader is
fail-fast — a corrupt body's inner_len lets the caller skip it on
principle, but v1 propagates as `SpillError::Format` because the xact
is unrecoverable anyway

`HeapOp` encodes as `0=Insert, 1=Update, 2=HotUpdate, 3=Delete,
4=Truncate`. **`HeapOp::Truncate` tag-4 was added without bumping
`SPILL_VERSION`** — academic because the resume contract wipes the
spill dir on startup ([`SpillStore::clear`]) and the cursor file
guarantees on-disk state is always "drained into CH" or "replayable
from `decoder_lsn`". Documented in
[future/parked.md](future/parked.md) for a future bump

`spill_backend` config knob was reserved at design time for a
CH-as-scratch v2; the enum + config surface were NOT shipped in v1.
ClickHouse-as-scratch path was rejected on three grounds:

- commit-drain latency: ms × n_toast per round trip vs µs sequential
  read
- 2× wire bandwidth: same TOAST bytes ingress CH twice
- MergeTree hygiene: short-lived staging is the canonical anti-pattern

`src/spill_ch.rs` placeholder was never created. Future diskless
operator wanting this gets a fresh config-surface decision

## Drain shape

`XactBuffer::commit` calls `observer.on_tuple(&CommittedTuple)` per
drained tuple in source_lsn order, then `observer.on_xact_end(commit_lsn)`
once. The latter returns the durable `ack_lsn` from the downstream
sink. Stats:

- `drain_lsn` = max commit_lsn passed to commit (advances FIRST so an
  observer failure leaves `drain_lsn > emitter_ack_lsn`, exactly the
  gap the cursor file surfaces)
- `emitter_ack_lsn` = max ack_lsn returned by observer (lags
  `drain_lsn` whenever the CH emitter holds rows in open INSERTs under
  `flush_timeout > 0`)

Both snapshot back into the cursor file maintained by
[ops.md](ops.md). `advance_idle(lsn)` lifts both ceilings when
`xacts_active == 0` so source's slot can recycle past trailing
RUNNING_XACTS / CHECKPOINT WAL during quiescence

`XactBufferStats::summary` renders `xact_active`, `bytes_in_mem`,
`spill_active`, `spill_bytes`, `commit`, `abort` always; appends
`evictions`, `commit_unk`, `abort_unk` only when non-zero. Matches
[decoder.md](decoder.md)'s `DecoderStats::summary` convention

## DrainEntry::{Tuple, Catalog}

Drain queue is an interleaved sequence over `(tuple, schema_event)`,
lifted from earlier `Vec<DecodedHeap>` shape. Catalog events
arrive via `BufferingDecoderSink::drain_schema_events` after every
`relation_at` and via `XactRecordSink::route_pending_schema_events`
after every `ShadowCatalog::sweep_dropped`. Both push into
`XactState.catalog_events` keyed on the same `(xid, source_lsn)` the
triggering record carried

Drain interleaving: k-way merge picks catalog events FIRST on lsn
ties because PG always writes the catalog mutation BEFORE the heap
write that depends on it. When the decoder stamps a schema event with
the triggering heap's `source_lsn` (catalog refetch is lazy), the
event sorts in front so the applicator's `ALTER` lands on CH before
the dependent INSERT encodes against the post-DDL shape

Drain implementation: collect catalog event positions as
`(heap_index_event_sorts_before, SchemaEvent)`; main dispatch loop
flushes pending events via `observer.on_schema_event(&ev)` before
each `observer.on_tuple(&committed)` whose index it sorts in front
of; trailing events (no heap after) flush at the tail

Cross-link: [shadow.md](shadow.md) `SchemaEvent` channel, fed by
`ShadowCatalog` on Added / Changed / Dropped catalog state

## Two-phase commit

`XLOG_XACT_PREPARE` is ignored. The sink leaves it untouched; the
xact buffer keeps its state alive until `XLOG_XACT_COMMIT_PREPARED`
(info `0x30`) or `XLOG_XACT_ABORT_PREPARED` (info `0x40`) arrives,
both of which route through the same `parse_xact_payload` + drain /
discard path as plain COMMIT / ABORT

Gap: `PREPARE` followed by daemon restart loses the prepared writes —
buffer state is process-local, `clear_spill_dir` wipes the inflight
spill on boot, and no replay-from-WAL reconstruction of prepared
xacts exists. Operator-visible 2PC users (XA transaction managers,
distributed-commit drivers) will silently lose prepared writes
across a walshadow restart between `PREPARE` and `COMMIT PREPARED`.
Cross-link [future/two_phase_commit.md](future/two_phase_commit.md)
for the recovery shape

`XactRecordSink` does process `COMMIT_PREPARED` / `ABORT_PREPARED`
inline today — the gap is only the cross-restart case

## Cross-links

- [decoder.md](decoder.md) — `DecodedHeap` producer + `BufferingDecoderSink`
- [source.md](source.md) — `Record` stream entry, classifier
- [emitter.md](emitter.md) — `TupleObserver` impl consuming commit drain
- [shadow.md](shadow.md) — `ShadowCatalog`, `SchemaEvent` channel
- [ops.md](ops.md) — `--spill-dir`, cursor file `(drain_lsn,
  emitter_ack_lsn)`
- [future/two_phase_commit.md](future/two_phase_commit.md) — PREPARE ↔
  COMMIT PREPARED across restart
- [future/parked.md](future/parked.md) — `SPILL_VERSION` bump for
  HeapOp::Truncate tag-4
