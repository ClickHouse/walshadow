# xact — per-xid buffer, spill backend, subxact tracker

Lives in [`src/xact_buffer.rs`](../src/xact_buffer.rs) +
[`src/spill.rs`](../src/spill.rs). Sits between decoder fan-out (see
[decoder.md](decoder.md)) and commit-drain observer (see
[emitter.md](emitter.md)). Source record stream: see [source.md](source.md)

## Purpose

Buffer every decoded heap tuple, TOAST birth/death, and commit-time raw
record per xid from first touch until matching `XLOG_XACT_COMMIT` /
`_PREPARED` drains it; abort discards. Mirrors PG `ReorderBuffer` shape
for the same problem in logical decoding, minus snapshot building
(catalog state already lives in shadow PG, see [shadow.md](shadow.md))

Three responsibilities collapse into one struct:

- per-xid sub-buffer for heaps, chunks, tombstones, and raw records,
  ordered by `source_lsn`
- spill-to-local-disk backend when in-memory budget breaches
- subxact tracker hinting early eviction & funneling commit drain
  across `top + subxids`

Catalog access is lazy: only heaps where any column is
`ColumnValue::ExternalToast` hit `ShadowCatalog::relation_at`, and only
at drain. No per-xact descriptor cache — it would duplicate shadow's
own LRU surface

![xact](../architecture/xact.svg)

## Buffer shape

`XactBuffer` owns inflight xid state, global generation markers,
commit-time stash resolutions, pending-durable accounting, memory gauges,
and spill backend. One `XactState` per inflight xid:

```text
XactState {
    first_lsn,                              // sticky filename anchor
    in_mem: Vec<SpillEntry>,                // pending memory→spill, WAL order
    in_mem_bytes,                           // approximate accounting
    spill: Option<SpillWriter>,             // None until first eviction
    spill_bytes,                            // mirrors writer.byte_count()
    events: Vec<(u64, DrainEntry)>,          // catalog/config/toast barriers
    stash_rfns: HashSet<RelFileNode>,        // commit-time resolution set
}
```

Spill collapses heaps + chunks into one per-xid file because PG's
`toast_save_datum` writes chunks in the same xact as the referring
tuple — cross-xact chunk references don't exist outside `streaming=on`
mode walshadow doesn't implement

`commit_ts` parsed off `xl_xact_commit.xact_time` at head of `main_data`
(i64 le)

## Eviction policy

64 MiB default matches PG's `logical_decoding_work_mem`. Largest-first
is correct because:

- small xacts evicted would bounce back on next record, freeing nothing
- heaviest xact frees most bytes per file write
- policy is xid-keyed, so a subxact's buffer evicts independently of
  its parent — long-lived tops with many subxacts shed memory evenly
  across the family

Drain events are *not* spilled. Catalog and config events carry typed
state; toast barriers carry only relation + marker identity. Practical
case is a handful of control events per xact

`inflight_snapshot` is the diagnostic surface for "a commit for this
xid never arrived" investigations — heap / chunk / event / spill
counters per xid

## TOAST reassembly

Decoder sink ([decoder.md](decoder.md)'s `BufferingDecoderSink`)
recognises INSERTs into `pg_toast.pg_toast_<rel>` (three-column shape:
`chunk_id oid, chunk_seq int4, chunk_data bytea`) and reshapes them
into `ToastChunk` keyed on toast relation's *pg_class OID* (not
relfilenode — `va_toastrelid` on referring `ToastPointer` is an OID,
the two diverge after `VACUUM FULL` / `CLUSTER`)

Chunks ride same `XactState.in_mem` deque as heaps. At drain, k-way
merge accumulates them into `HashMap<(toast_relid, value_id),
BTreeMap<chunk_seq, Vec<u8>>>`; `reassemble` walks BTreeMap checking
`seq == expected`, concatenates. Compression decoded inline:
`TOAST_COMPRESSION_PGLZ` via `pglz::decompress_into`,
`TOAST_COMPRESSION_LZ4` via `lz4_flex::decompress`. Method tag lives in
top bits of `va_extinfo` past `VARLENA_EXTSIZE_BITS = 30`

A gap in the xact's own chunk maps surfaces as
`XactBufferError::MissingToastChunk { toast_relid, value_id, missing }`
rather than silent loss (a store-side miss instead superseded-fills,
[TOAST.md](TOAST.md)). Malformed chunk shape (wrong column count,
wrong types) bumps `DecoderStats.toast_chunks_malformed`; malformed
counter wired into decoder fan-out so silent toast loss is visible on
status line

## Commit-time stash

Records on a filenode `relation_at` cannot resolve at record time (the
creating xact's pg_class row is MVCC-invisible until commit: rewrite
generations, same-xact CREATE/TRUNCATE + INSERT) ride the same per-xid
spill as `SpillEntry::Raw` — raw rmgr/info + main data + blocks with
images + record LSN — so subxact merge ordering and abort discard come
for free. Admission is gated on the filenode's `XLOG_SMGR_CREATE`
marker (observed pre-route-gate, global by filenode since the record
can precede xid assignment); once a filenode is a stash candidate the
per-record replay-gated lookup is skipped — the xact's own records can
never resolve. At commit `resolve_stash` resolves each candidate with
`relation_at(rfn, commit_lsn)`, installs per-filenode verdicts the
drain merge consumes (toast → decode like live chunks, ordinary heap →
fenced skip, unresolvable → counted discard), and queues a
`DrainEntry::ToastBarrier` at commit LSN per marker-proven toast
generation ([TOAST.md](TOAST.md)). Lifting the ordinary-heap fence:
[future/xact_stash.md](future/xact_stash.md)

## Subxact tracker

`SubxactTracker { parent: HashMap<u32, u32>, children: HashMap<u32,
Vec<u32>> }`. Both directions kept so `forget_tree(top_xid)` runs O(k)
over actual children rather than scanning every `parent` entry

Populated from `XLOG_XACT_ASSIGNMENT` (info `0x50`) via
`parse_xact_assignment` reading `(xtop: u32, nsub: i32, xsub[nsub])`
off `main_data`. Tracker is a HINT — PG batches first
`PGPROC_MAX_CACHED_SUBXIDS` (= 64) subxacts under the top without
emitting an explicit ASSIGNMENT. Authoritative subxact list arrives
inline on commit / abort record itself

`parse_xact_payload(info, main_data)` walks tail in PG-source order
matching `xactdesc.c::ParseCommitRecord` / `ParseAbortRecord`:

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
`XactCommitPayload::default()` (xact_time + no subxacts), so decoder
doesn't poison the stream over one bad record

Standalone subxact rollback for a sub of a still-open top: top's
pre-savepoint entries stay keyed on top_xid in `inflight` and flush at
top's COMMIT — drain-time merge across `top + remaining_subxids`
produces correct survivor set

## Spill backend

[`src/spill.rs`](../src/spill.rs). File name
`xid-{xid:010}-{first_lsn:016X}.bin` mirrors PG's
`pg_replslot/<slot>/xid-*.snap` shape; without LSN suffix, two streams
that picked up same xid value after a slot rebuild or post-restart
could collide

File layout:

```text
[2 bytes "WS" magic = SPILL_MAGIC]
[u16 LE version = SPILL_VERSION = 4]
repeating:
  [u8 tag]
  [u32 LE inner_len]
  [body of inner_len bytes]
    tag=0 → SpillEntry::Heap        (encoded DecodedHeap)
    tag=1 → SpillEntry::Chunk       (encoded ToastChunk, TID + record LSN)
    tag=2 → SpillEntry::ToastDelete (TID-keyed, a store tombstone at drain)
    tag=3 → SpillEntry::Raw         (rmgr/info/main data/blocks + images)
```

`SpillReader::check_header()` runs lazily on first `next()`: rejects
wrong magic with `SpillError::Format { offset: 0, detail: "bad
magic …" }`, wrong version with same shape at offset 2. Reader is
fail-fast — a corrupt body's inner_len lets caller skip it on principle,
but v1 propagates as `SpillError::Format` because the xact is
unrecoverable anyway

`HeapOp` encodes as `0=Insert, 1=Update, 2=HotUpdate, 3=Delete,
4=Truncate`. v2 added `Truncate`; v3 added chunk TIDs + `ToastDelete`;
v4 added raw stashed records.
Version mismatch is near-academic anyway: resume contract wipes spill
dir on startup
([`SpillStore::clear`]) and cursor file guarantees on-disk state is
always "drained into CH" or "replayable from `decoder_lsn`"

Spill backend is local disk only; no `spill_backend` config knob or
enum surface for a CH-as-scratch alternative. ClickHouse-as-scratch is
rejected on three grounds:

- commit-drain latency: ms × n_toast per round trip vs µs sequential
  read
- 2× wire bandwidth: same TOAST bytes ingress CH twice
- MergeTree hygiene: short-lived staging is canonical anti-pattern

There is no `src/spill_ch.rs`. A diskless operator wanting this faces a
fresh config-surface decision

## Drain shape

Two consumers exist for commit drain: parallel pipeline's reorder
coordinator (`pipeline/reorder.rs`, with `--ch-config`) pulls bounded
`DrainedBatch` slices from `CommittedDrain`, dispatching heaps to decode
pool while applying store rows and ordered barriers; ack collector tracks
durability (see [emitter.md](emitter.md)). Serial
`XactRecordSink` → `TupleObserver` path (metrics-only runs, inproc
harness). The observer-ack semantics below describe the serial path;
the pipeline replaces them with the contiguous-done watermark

`drain_lsn` advances BEFORE `on_xact_end` ack so an observer failure
leaves `drain_lsn > emitter_ack_lsn`, exactly the gap cursor file
surfaces. `emitter_ack_lsn` lags whenever the CH emitter holds rows
buffered under `flush_timeout > 0`. Both snapshot back into cursor file
maintained by [ops.md](ops.md)

Idle ticks keep the ack moving without a commit: `advance_idle(lsn,
ack_ceiling)` lifts `drain_lsn` to the dispatched `lsn` but
`emitter_ack_lsn` only to `min(lsn, ack_ceiling)` — the observer's
durable horizon (`idle_ack_ceiling`), so a quiescent nudge can't promote
the ack past rows still buffered in the emitter. `note_idle_durable(lsn)`
folds a deadline-triggered close's durable LSN into `emitter_ack_lsn`
alone. Both no-op while a xact is in flight

`XactBufferStats::summary` renders `xact_active`, `bytes_in_mem`,
`spill_active`, `spill_bytes`, `commit`, `abort` always; appends
`evictions`, `commit_unk`, `abort_unk` only when non-zero. Matches
[decoder.md](decoder.md)'s `DecoderStats::summary` convention

## Drain entries and batch cursors

Catalog events arrive via `BufferingDecoderSink::drain_schema_events`
after every `relation_at` and via
`XactRecordSink::route_pending_schema_events` after every
`ShadowCatalog::sweep_dropped`. Both push into
`XactState.events` as `DrainEntry::Catalog`, keyed on same
`(xid, source_lsn)` triggering record carried. Runtime config changes
use `DrainEntry::Config`; rewrite generations close with
`DrainEntry::ToastBarrier`

Tie-break rule (catalog before tuple) matters because when decoder
stamps a schema event with triggering heap's `source_lsn` (catalog
refetch is lazy), the two share an LSN; routing catalog first lands
applicator's `ALTER` on CH before dependent INSERT encodes against
post-DDL shape

Parallel drain records each event as `OrderedEvent { heap_idx, row_idx,
event }` in `DrainedBatch.ordered_events`. `heap_idx` orders control
events against heaps; `row_idx` orders TOAST mirror births/deaths before
each control event. `truncate_rows` supplies equivalent row cursors for
`HeapOp::Truncate`. Serial path flushes pending events via
`observer.on_schema_event(&ev)` before each
`observer.on_tuple(&committed)` whose index it sorts in front of;
trailing events (no heap after) flush at tail. Pipeline path walks the
same positions as barrier segments, puts store rows through each cursor,
then fences and applies each event (see [emitter.md](emitter.md))

Cross-link: [shadow.md](shadow.md) `SchemaEvent` channel, fed by
`ShadowCatalog` on Added / Changed / Dropped catalog state

## Two-phase commit

`XLOG_XACT_PREPARE` is ignored. Sink leaves it untouched; xact buffer
keeps its state alive until `XLOG_XACT_COMMIT_PREPARED` (info `0x30`)
or `XLOG_XACT_ABORT_PREPARED` (info `0x40`) arrives, both route through
same `parse_xact_payload` + drain / discard path as plain COMMIT / ABORT

Gap: `PREPARE` followed by daemon restart loses prepared writes —
buffer state is process-local, `clear_spill_dir` wipes inflight spill
on boot, no replay-from-WAL reconstruction of prepared xacts exists.
Operator-visible 2PC users (XA transaction managers, distributed-commit
drivers) will silently lose prepared writes across walshadow restart
between `PREPARE` and `COMMIT PREPARED`. Cross-link
[future/two_phase_commit.md](future/two_phase_commit.md)

`XactRecordSink` processes `COMMIT_PREPARED` / `ABORT_PREPARED`
inline — the gap is only cross-restart

## Cross-links

- [decoder.md](decoder.md) — `DecodedHeap` producer + `BufferingDecoderSink`
- [source.md](source.md) — `Record` stream entry, classifier
- [emitter.md](emitter.md) — pipeline reorder coordinator consuming
  commit drain (serial `TupleObserver` path on metrics-only runs)
- [shadow.md](shadow.md) — `ShadowCatalog`, `SchemaEvent` channel
- [ops.md](ops.md) — `--spill-dir`, cursor file `(drain_lsn,
  emitter_ack_lsn)`
- [future/two_phase_commit.md](future/two_phase_commit.md) — PREPARE ↔
  COMMIT PREPARED across restart
