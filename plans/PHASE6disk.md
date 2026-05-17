# PHASE6disk — xact buffer + TOAST spill backend (design)

Design layer for [Phase 6](PLAN.md#phase-6--toast-reassembly--xact-buffer)'s
unbounded buffers. Compares local-disk spill against ClickHouse-as-scratch
and recommends local disk with a config knob reserved for the diskless case.
Lands in the same commit as the buffer itself, not as a separate phase.

## Problem

Phase 6 introduces two data structures that grow unbounded by xact size.

* **Xact buffer.** Holds every `DecodedHeap` per `xid` from first heap
  touch until `XLOG_XACT_COMMIT` / abort. A bulk `INSERT … SELECT FROM
  big_table` or multi-million-row `UPDATE` accumulates the buffer until
  the commit record lands. WAL-stream order must be preserved on drain.
* **TOAST reassembly buffer.** Keyed by `(toast_relid, va_valueid)`. A
  single `varatt_external` addresses up to ~1 GiB of logical bytes split
  into chunks of `TOAST_MAX_CHUNK_SIZE` ≈ 1996 bytes
  (`~/s/postgresql/src/include/access/heaptoast.h:84`), so ~500 k chunks
  per max-size value. Chunks may arrive before or after referring tuples;
  both halves must outlive each other until commit.

## Memory amplification

`DecodedHeap` (in [`src/heap_decoder.rs`](../src/heap_decoder.rs)) is fatter
than the raw WAL bytes it came from:

```rust
pub struct DecodedHeap {
    pub rfn: RelFileNode, pub xid: u32, pub source_lsn: u64,
    pub op: HeapOp,
    pub new: Option<DecodedTuple>, pub old: Option<DecodedTuple>,
}
pub struct DecodedTuple {
    pub columns: Vec<Option<ColumnValue>>, pub partial: bool,
}
pub enum ColumnValue {
    /* … */ Bytea(Vec<u8>), Text(String), Unsupported { type_oid: u32, raw: Vec<u8> }, /* … */
}
```

Each tuple is one heap-allocated outer `Vec` plus a heap allocation per
varlena column plus ~32-byte enum tag overhead per cell. Against PG's
dense 5-byte `xl_heap_header` + bitmap + packed payload, expect **3–5×
amplification** for narrow rows; wide rows dominate by content.

PG faced the same problem in logical decoding:
`logical_decoding_work_mem` defaults to **64 MiB** per slot
(`~/s/postgresql/src/backend/utils/misc/guc_tables.c:2611`), and
reorderbuffer picks the largest in-flight xact and spills when over budget
(`~/s/postgresql/src/backend/replication/logical/reorderbuffer.c` —
`ReorderBufferLargestTXN` at L3789, `ReorderBufferCheckMemoryLimit`
nearby).

## Why shadow catalog can't help

`ShadowCatalog` is `pg_class` / `pg_attribute` / `pg_type` / `pg_index`
state, not a data sink. Shadow PG filters out user-heap WAL by design
([PLAN.md "What 'replay only catalog' filters in"](PLAN.md#what-replay-only-catalog-filters-in)),
so it has no user-heap files to park buffered tuples against. Reintroducing
them defeats the schema-only premise that keeps shadow at MiB scale.

Shadow does keep `RM_CLOG_ID` / `RM_XACT_ID` and `pg_prepared_xacts` for
2PC, useful for *querying* whether an xid committed but mute on *what data*
it touched. Decoder still has to hold data somewhere outside shadow.

## Spill options

### A. Local disk

Per-xact append-only file under `{data_dir}/spill/xid-{xid}-{first_lsn}.bin`,
length-prefixed serialised `DecodedHeap` plus reassembled TOAST chunks.
Commit drain reads sequentially and unlinks; abort just unlinks.

Mirrors PG's `pg_replslot/<slot>/xid-*.snap` shape. Local filesystem is
built for this: append-only, short-lived, written-once-read-once-deleted.

### B. CH as scratch, walshadow reassembles

Chunks + tuples land in a CH staging table
(`(toast_relid UInt32, value_id UInt32, chunk_seq UInt16, chunk_data
String) ENGINE = MergeTree`) as they arrive. At commit, drain pass
`SELECT`s back, reassembles, emits final tuple. `DELETE` / TTL the staging
rows afterwards.

Cost surface:

* **Commit-drain latency.** CH query latency is multi-ms per round trip.
  Xact with 100 TOASTed columns pays 100× that on drain. Local disk is
  microsecond-scale.
* **2× wire bandwidth.** Same TOAST bytes traverse the wire twice — once
  into staging, once into the destination column on emit. 10 GiB bulk
  INSERT of `text` becomes 20 GiB CH ingress.
* **MergeTree hates short-lived data.** Lots of small inserts followed by
  deletes is the canonical anti-pattern: part churn, mutation overhead,
  async deletes that don't free disk until merge. TTL "1 hour from `_lsn`"
  is sloppy; explicit DELETE per aborted xid is another round trip on the
  abort path.
* **CH unavailability blocks the decoder's commit drain.** Local disk
  decouples: decode keeps running while CH is paused, emitter retry loop
  covers the gap.

### C. CH as final storage, never reassemble

Destination table replaces TOASTed columns with `value_id UInt32`
pointers; chunks live in a sibling `toast_chunks` table; reads JOIN on
`value_id`.

Cost surface:

* **Breaks operator query model.** Phase 7's pitch is `RelDescriptor →
  destination table with per-column TypeAst`. Users expect `text` on PG
  → `String` on CH. Forcing
  `arrayStringConcat(groupArray(chunk_data ORDER BY chunk_seq))`
  GROUP BYs on every read is not a CDC tool.
* **Loses MergeTree skip indices / projections.** Predicate pushdown
  doesn't reach the chunk table cleanly through the JOIN.
* **Dedup harder.** `ReplacingMergeTree(_lsn)` can't dedupe the chunk
  table the way it does the main table; same `value_id` appears in many
  `_lsn`s after walshadow replay-after-crash.

Niche viability: archival-only flows where chunked bytes never get
queried. For typical CDC-to-analytics targets, non-starter.

### D. Hybrid threshold

Small TOAST (`va_rawsize < N`) in-memory; large TOAST → CH stash (Shape B).
Bounds RAM regardless of single-value size.

Cost surface: same network / bandwidth / MergeTree-churn issues as B on
the long tail; two code paths to maintain; threshold tuning is
workload-specific.

## Comparison

| concern | A (local disk) | B (CH staging) | C (CH primary) |
|---|---|---|---|
| commit-drain latency | µs (append + sequential read) | ms × `n_toast` (CH query per value) | n/a (no reassembly) |
| TOAST bytes on the wire | 1× to CH | 2× (stash + emit) | 1× (chunked) |
| abort cleanup | `unlink(file)` | DELETE + merge churn | n/a (TTL or leak) |
| CH downtime impact | decoder drains; emitter waits | decoder blocks on commit | decoder blocks on writes |
| CH part hygiene | none | MergeTree merges short-lived parts | acceptable |
| operator query model | unchanged | unchanged | breaks (forced JOINs) |
| operator-visible state | one scratch dir | staging rows across crashes | n/a |

## When CH stash actually fits

Containerised walshadow with no writable local disk, unbounded CH ingest
budget, no large TOASTs in the workload. Spill-via-CH is fallback
configuration, not default. Plumb as
`spill_backend = "local_disk" | "clickhouse"` (analogous to
[BASEBACKUP.md](BASEBACKUP.md)'s `bootstrap.ch_initial_load` knob),
`"local_disk"` the default.

## Recommendation

**Local disk (Option A), with a config knob for the diskless case.**

Spill primitive shape:

```
src/spill.rs                       new — ~250 LOC
  SpillWriter { file: tokio::fs::File, xid, byte_count,
                 lsn_index: BTreeMap<Lsn, u64> }
  SpillReader  — streams DecodedHeap back in WAL order
  SpillStore   — directory manager, evict-largest policy

config.toml
  xact_buffer_max = "64MiB"          # matches PG's logical_decoding_work_mem
  spill_dir       = "{data_dir}/spill"
  spill_backend   = "local_disk"     # "clickhouse" reserved, unimplemented v1

src/spill_ch.rs                    placeholder, not implemented v1
```

Eviction policy mirrors PG: largest-xact-first. Small xacts stay in RAM
(no point evicting — they'd bounce back); evicting the heaviest frees
the most memory per file write.

Status-line additions: `spill_bytes_active`, `spill_xacts_active`,
`spill_evictions_total`.

Crash recovery: spill dir scan at startup. Cleanest is to clear the entire
spill dir on start and re-stream from cursor's `decoder_lsn`; the cursor
file already commits atomically post-drain so the on-disk state is always
"either drained and visible in CH, or replayable from WAL".

## Phase 6 sizing impact

PLAN.md sizes Phase 6 at ≈600 LOC. With the spill primitive folded in:

```
xact buffer + TOAST reassembly + commit drain      ~500 LOC
src/spill.rs (local disk)                          ~250 LOC
crash recovery + cursor integration                ~100 LOC
config knob + status metrics                       ~50 LOC
tests (spill + drain + crash recovery)             ~250 LOC
```

Net: ≈900 LOC src + ~250 LOC tests. ~50% larger than the bare buffer-only
sizing, paid once.

## Why spill lands inside Phase 6, not later

Without spill, Phase 6's xact buffer is correctness-correct but OOMs the
daemon under any bulk INSERT / UPDATE. Acceptance criterion §1
(`pgbench -T 30 -c 8`) doesn't exercise large xacts, so CI passes silently
while production fails in hours.

Retrofitting spill onto an in-memory-only buffer means rewriting the
storage abstraction (the `XactState` type changes shape, the drain
iterator changes shape, the eviction callback didn't exist). Cheaper to
design the trait surface right from the first commit.

Subxact lineage (PG's reorderbuffer threads sub-xact change lists onto the
top-level xact at commit) is the one piece reasonable to defer; cheap to
bolt on once the spill primitive exists.

## What this defers

* **Subxact lineage.** Phase 6 ships top-level-xact-only; subxact aborts
  inside a top-level commit need to drop their changes without dropping
  siblings. Lands as a Phase 6 follow-up when PG savepoints in workloads
  measure non-negligible.
* **Streaming mid-xact.** PG's `streaming=on` mode emits uncommitted
  changes to subscribers. Walshadow can't, because abort → ghost rows in
  CH and `ReplacingMergeTree(_lsn)` has nothing to retract with.
  Commit-buffer is the only correctness-safe model; spill, don't stream.
* **CH-stash backend** (`spill_backend = "clickhouse"`). Surfaced as a
  config knob, unimplemented in v1. Lands only when a diskless-walshadow
  operator asks.

## Related

* [BASEBACKUP.md "Path 1B + 2A — streamed page-walk"](BASEBACKUP.md#path-1b--2a--streamed-page-walk)
  already mentions "+~150 LOC TOAST buffer + spill-to-scratch" for the
  bootstrap path. Phase 6's spill primitive serves both: steady-state WAL
  hot path and tar-decode bootstrap. One implementation, two consumers.
* `~/s/postgresql/src/backend/replication/logical/reorderbuffer.c` is the
  reference implementation. Walshadow's spill is the same idea minus the
  in-memory-only catalog-aware bits PG carries for snapshot building.
