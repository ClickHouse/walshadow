# xact — per-xid buffer, spill backend, subxact tracker

Lives in [`src/xact/xact_buffer.rs`](../src/xact/xact_buffer.rs) +
[`src/xact/spill.rs`](../src/xact/spill.rs). Sits between decoder
fan-out (see [decoder.md](decoder.md)) and the reorder coordinator's
commit drain (see [emitter.md](emitter.md)). Source record stream: see
[source.md](source.md)

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

Descriptor access is a wait-free interval lookup against the durable
log ([desc_log.md](desc_log.md)); heaps with `ColumnValue::ExternalToast`
resolve their owner descriptor at drain the same way

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
merge folds them into `ChunkRefMap` (`(toast_relid, value_id) →
ValueRef`: dense contiguous spool run + out-of-pattern tail). Bodies
are shared `Bytes` — resolution map and mirror row hold one
allocation, charged once — kept memory-resident until
`toast_body_mem_max` cumulative bytes per drain, appended once to the
drain's `toastbody-*` body spool past it with `BodyRef` ranges
resident. Ref metadata caps at `toast_index_mem_max`, breach is typed
non-retryable `ToastIndexOverflow`. `reassemble_value_ref` walks a
value's run + tail checking `seq == expected`, concatenates.
Compression decoded inline:
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

Records the decoder cannot safely decode at record time ride the same
per-xid spill as `SpillEntry::Raw` — raw rmgr/info + main data + blocks
with images + writer xid + record LSN — so subxact merge ordering and
abort discard come for free. Three admission gates, in order:

- `defer_catalog_decode` from the filter's dirty tree
  ([filter.md](filter.md)): once a xact family touches the catalog,
  every subsequent ordinary heap record in that family stashes raw —
  a Present predecessor descriptor doesn't prove decodability inside
  the dirty interval
- known-invisible filenode: `XLOG_SMGR_CREATE` marker (observed
  pre-route-gate, global by filenode since the record can precede xid
  assignment) or a prior stash on the same rfn; the xact's own records
  can never resolve, so the per-record lookup is skipped
- spanned lookup miss: `descriptor_at_spanned(rfn, lsn)` answering
  NotCovered past the coverage horizon, Dropped, or Ambiguous defers
  to commit-time resolution; ForeignDb / pre-horizon NotCovered /
  Retired are counted skips (rows can't outlive the commit)

At commit `resolve_stash` resolves each candidate against the log at
the commit's `next_lsn` (capture ran inside the boundary hold, so the
batch is already indexed) and installs per-filenode `StashOutcome`
verdicts the drain merge consumes:

- Present toast → `Toast(rel)`: decode stashed chunks like live ones;
  marker-proven generation without its chunks fails closed
  (`IncompleteToastGeneration`), and each proven generation queues a
  `DrainEntry::ToastBarrier` at commit LSN ([TOAST.md](TOAST.md))
- Present ordinary → `Ordinary(rel, valid_from)`: raw records re-run
  the heap decoder at drain under the commit-resolution descriptor,
  fanning rows out through the merge's pending queue (MULTI_INSERT
  yields per-tuple rows in tuple order, same-LSN events order before
  the fanout); this is what delivers `BEGIN; CREATE TABLE; COPY;
  COMMIT` rows
- Ambiguous → fatal `XactBufferError::StashAmbiguous`: no descriptor
  proven safe, neither decode nor discard is sound; operator takes a
  fresh snapshot
- Dropped / Retired / NotCovered → discard, end-state-neutral.
  NotCovered with a marker means born + gone inside the xact family
  (capture tombstones only predecessors; a commit-time survivor is
  always Present), so no surviving rows
- ForeignDb → counted once per filenode
  (`stash_foreign_db_skipped`)

Raw decode at drain is bounded and deterministic: operation policy
rejects image-only INSERT/UPDATE (`FailClosedReason::ImageOnly`) and
unknown heap ops; DELETE decodes, LOCK/CONFIRM and Heap2 page
maintenance no-op. Per-op counters split live vs raw decode
(`raw_decode_{toast,ordinary,rows}_ops`, [ops.md](ops.md))

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

Short-read at any tail position is a typed `XactPayloadError`. Filter
classification poisons on it (a silent partial parse could drop the
inval marking a boundary); decoder-side consumers degrade to
`XactCommitPayload::default()` (xact_time + no subxacts) so one bad
record doesn't poison the drain. Inval messages classify during the
walk: relcache invals enumerate affected rels, pg_namespace catcache /
whole-catalog invals mark capture-all ([desc_log.md](desc_log.md));
`XLOG_XACT_INVALIDATIONS` (0x60, wal_level=logical command boundaries)
walks the same message encoding filter-side

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
[u16 LE version = SPILL_VERSION = 6]
repeating:
  [u8 tag]
  [u32 LE inner_len]
  [body of inner_len bytes]
    tag=0 → SpillEntry::Heap        (u32 dict id + encoded DecodedHeap)
    tag=1 → SpillEntry::Chunk       (encoded ToastChunk, TID + record LSN)
    tag=2 → SpillEntry::ToastDelete (TID-keyed, a store tombstone at drain)
    tag=3 → SpillEntry::Raw         (rmgr/info/main data/blocks + images + xid)
    tag=4 → descriptor dictionary   (u64 valid_from + descriptor)
```

Heap bodies reference their descriptor by dictionary id; the writer
assigns ids in first-use order keyed on `(rfn, valid_from)` log
identity, the reader folds tag-4 records back into descriptors in
write order (they never surface as entries). One descriptor encodes
once per file however many heaps share it

`SpillReader::check_header()` runs lazily on first `next()`: rejects
wrong magic with `SpillError::Format { offset: 0, detail: "bad
magic …" }`, wrong version with same shape at offset 2. Reader is
fail-fast — a corrupt body's inner_len lets caller skip it on principle,
but v1 propagates as `SpillError::Format` because the xact is
unrecoverable anyway

`HeapOp` encodes as `0=Insert, 1=Update, 2=HotUpdate, 3=Delete,
4=Truncate`. v2 added `Truncate`; v3 added chunk TIDs + `ToastDelete`;
v4 added raw stashed records; v5 added the descriptor dictionary; v6
added writer xid to heap + raw bodies (raw fanout stamps `_xid` from
the stashing subxact, not the draining top).
Version mismatch is near-academic anyway: resume contract wipes spill
dir on startup
([`SpillStore::clear`]) and manifest guarantees on-disk state is
always "drained into CH" or "replayable from the floor"

Sibling file families share the dir and the startup wipe: per-drain
TOAST body spools (`toastbody-{xid:010}-{commit_lsn:016X}.bin`, raw
concatenated bodies, no framing — `BodyRef` ranges are process-local)
and deferred-record spools ([`src/spool.rs`](../src/spool.rs), own
"WD" magic + version so a cross-read fails as `SpillError::Format`).
Disk is unmetered — spilling is the pressure release for the memory
budget, so it must always succeed; sizing the spill volume is an
operator concern. Each file's disk gauge rides its shared owner: an
unlinked body spool stays charged while decode/store readers hold its
fd, releasing with the last holder

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

One consumer exists for commit drain: the pipeline's reorder
coordinator (`pipeline/reorder.rs`) pulls bounded `DrainedBatch` slices
from `CommittedDrain` and feeds them through the transaction planner —
plan first (side-effect-free: detoast, route, raw decode, all
input-derived failures surface here), then `execute_plan` replays the
sealed plan through barrier ordering to the decode pool and DDL
applicator; ack collector tracks durability (see
[emitter.md](emitter.md)). Metrics-only runs use the same coordinator
over a null tail; backup gap replay drives `drain_committed` through
its own serial `ReplaySink`. Every consumer walks a slice via
`DrainedBatch::into_walk` — the single implementation of the
events/truncate cursor interleave

Each slice seals accumulated chunks into an immutable
`ChunkGeneration` shared by `Arc` (the drain and every later slice's
decode job re-reference all generations sealed so far). A generation
carries its resident-gauge share and, under an active budget, its
admission permit — both release at last-holder drop, container
hand-off is not release ([emitter.md](emitter.md) Memory budget).
`drain_resident_bytes` gauges ownership with
`drain_chunk_resident_bytes` / `drain_row_resident_bytes` shares;
`toast_xact_spool_bytes` gauges body-spool disk. `finish()` unlinks
spill + spool files after dispatch; an error path drops instead,
leaving files for inspection (startup wipe + redecode-from-ack cover
replay)

`drain_lsn` advances at `drain_committed`, ahead of durability — the
gap against the ack collector's `emitter_ack` (which lags by up to one
flush window) is exactly what the manifest surfaces. Both snapshot into
the manifest maintained by [ops.md](ops.md)

Idle ticks keep the marks moving without a commit: `advance_idle(lsn)`
lifts `drain_lsn` to the dispatched `lsn`; the ack side rides the
reorder's `AckHandle::trailing`, which advances only when every
registered seq is done and the buffer is empty. Both no-op while a
xact is in flight

`XactBufferStats::summary` renders `xact_active`, `bytes_in_mem`,
`spill_active`, `spill_bytes`, `commit`, `abort` always; appends
`evictions`, `commit_unk`, `abort_unk` only when non-zero. Matches
[decoder.md](decoder.md)'s `DecoderStats::summary` convention

## Drain entries and batch cursors

Catalog events arrive from descriptor capture at boundary time
([desc_log.md](desc_log.md)), pushed as `DrainEntry::Catalog` keyed
`(drain_xid, valid_from)`; the worker pushes `DrainEntry::Config` at
observe order. Two producers means arrival order is not LSN order —
drain open stable-sorts each xact's events by LSN. Rewrite generations
close with `DrainEntry::ToastBarrier`

Tie-break rule (catalog before tuple) matters because when decoder
stamps a schema event with triggering heap's `source_lsn` (catalog
refetch is lazy), the two share an LSN; routing catalog first lands
applicator's `ALTER` on CH before dependent INSERT encodes against
post-DDL shape

Drain records each event as `OrderedEvent { heap_idx, row_idx, event }`
in `DrainedBatch.ordered_events`. `heap_idx` orders control events
against heaps; `row_idx` orders TOAST mirror births/deaths before each
control event. `truncate_rows` supplies equivalent row cursors for
`HeapOp::Truncate`. `DrainedBatch::into_walk` compiles the three
cursors into one `WalkStep` sequence (`Rows` seals store rows before
each `Event`/`Truncate`, once at the tail); the reorder barrier fences
and applies each step (see [emitter.md](emitter.md)), gap replay ships
`Heap` steps and ignores catalog/config events

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

`ReorderSink` processes `COMMIT_PREPARED` / `ABORT_PREPARED`
inline — the gap is only cross-restart

## Cross-links

- [decoder.md](decoder.md) — `DecodedHeap` producer + `BufferingDecoderSink`
- [source.md](source.md) — `Record` stream entry, classifier
- [emitter.md](emitter.md) — pipeline reorder coordinator consuming
  commit drain (null tail on metrics-only runs)
- [shadow.md](shadow.md) — `ShadowCatalog`, `SchemaEvent` channel
- [ops.md](ops.md) — `--spill-dir`, manifest (`drain`, `emitter_ack`,
  floor)
- [future/two_phase_commit.md](future/two_phase_commit.md) — PREPARE ↔
  COMMIT PREPARED across restart
