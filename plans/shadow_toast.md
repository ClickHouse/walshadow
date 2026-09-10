# Read TOAST values from shadow PostgreSQL

Evaluate keeping physical TOAST heaps and indexes in shadow, then using
PostgreSQL to fetch external values. Current ClickHouse store remains available
until this alternative proves correct and useful. Current behavior is explained
in [value architecture](../architecture/values.md)

This changes shadow from catalog-only storage to catalog plus large-value
storage. Measure disk, replay I/O, lookup latency, and memory against current
backend before choosing it for production

## Prove historical reads first

Build an isolated experiment showing exactly which old chunks PostgreSQL can
read after insert, delete, value-ID reuse, and physical cleanup. Use original
TOAST pointer metadata and referring WAL position to validate fetched value
Current-state SQL alone cannot prove an older record's value

Keep same-transaction chunk reconstruction as fast path. For shadow lookup,
wait for required replay, validate value identity, sequence, and stored size,
then decompress. A missing or ambiguous value is fatal unless supersession is
independently proven. Never silently fall back to a backend with different
history

## Preserve physical storage

Seed complete TOAST heap and index files, including forks and relation segments
Track creation, rewrite, truncate, and drop by full physical identity. Replayed
WAL remains sole writer; do not add walshadow-owned rows to a physical standby

Route original TOAST WAL to shadow while retaining records required by
same-transaction reconstruction. Keep ordinary user heaps filtered out
Bootstrap must provide physical files before replay and delay dependent rows
until their TOAST data is readable. Schema-only seeds cannot enable this mode
Coordinate non-default layouts with [tablespace support](tablespaces.md)

## Fence reclamation

Enumerate every WAL operation that can destroy or detach old chunks: pruning,
vacuum, index cleanup, rewrite, truncate, relation replacement, and relation,
database, or tablespace drop. Audit supported PostgreSQL majors and reject
unclassified destructive operations on retained storage

Do not let shadow reclaim a value while any queued, active, spilled, deferred,
or restartable work can still fetch it. Before releasing a destructive record,
drain or durably preserve affected work and persist restart evidence proving
older reads cannot recur. Highest decoded or acknowledged commit alone is not
that proof

Gate both live WAL and archive publication. Holding a socket is insufficient
if restore_command can read same destructive record from a completed segment
Bound staged bytes and persist enough publication state to recover after crash
Reject startup if shadow has already reclaimed beyond durable safe boundary

Check for deadlocks between a reclamation fence and catalog replay required to
finish older transactions before integrating this gate into production pipeline

## Proposed interfaces and routing

Separate fetch capability from accepting decoded chunk writes. A proposed
`ShadowToastStore` reads physical storage and must never receive `ToastRow`
births/tombstones intended for ClickHouse backend. Use reader/writer capabilities
or explicit backend variants, and pass shadow connection settings from pipeline
and bootstrap configuration. Preserve same-transaction chunk decoding in both
modes; skip ClickHouse chunk DDL and persistent chunk writes in shadow mode

Fetch request carries full relation/generation identity, value ID, stored/raw
sizes, compression method, referring record LSN, and minimum replay LSN. These
last two positions express different constraints. Thread commit visibility into
detoast where needed; a replay wait alone never establishes historical identity
Use dedicated bounded extension connections, avoid holding catalog-client mutex
across large-value reads, and budget bytes as well as request count

Prototype version-pinned extension returning stored payload via PostgreSQL TOAST
fetch machinery and `SnapshotToast`. Validate TOAST relation/index, sequence,
chunk lengths, and total stored size; decompress and validate raw size in Rust
Prove deleted chunks remain readable until cleanup and value-ID reuse cannot
select another generation. `HeapTupleSatisfiesToast` and value-ID existence checks
are investigation anchors, verify behavior on each supported major

Seed synchronous tracker with TOAST heaps and their indexes. Track speculative
creation, rewrite, abort, and drop before commit visibility, including CREATE
TABLE plus large INSERT in same transaction. Use explicit dual route carrying
original bytes, never rebuild shadow WAL from decoded tuples

| Physical owner/operation | Shadow delivery | Decoder work |
|---|---|---|
| Ordinary user heap/index | Filtered no-op | Decode user heap only |
| TOAST heap insert/update | Original WAL | Populate transaction chunk map |
| TOAST heap delete/prune/vacuum | Original WAL behind reclamation decision | Retain lifecycle evidence as needed |
| TOAST index insert/split | Original WAL | Track identity when needed |
| TOAST index cleanup, rewrite, truncate, drop | Original WAL behind reclamation decision | Track generation and affected readers |

Classify complete records before publication, including heap/index records that
cross segments. Every shadow-delivered record needs matching physical base files

## Durable release and bootstrap sequence

Define `resume_safe_lsn` as boundary below which no restartable work can need an
older TOAST fetch, and `toast_reclaim_lsn` as highest destructive boundary released
Require `toast_reclaim_lsn <= resume_safe_lsn`, interpreted on validated lineage
Before releasing boundary D: withhold record, finish or durably materialize
dependent work, fsync restart proof covering D, then publish original bytes
Include [bootstrap carry](bootstrap.md) and future destination queues in proof

Stage archive segments and manifest durably before atomic publication. On
restart, recover manifest or rescan staged records, validate against cursor and
shadow replay, and publish only safe segments. No alternate restore path may
bypass gate. Test crashes before cursor fsync, after fsync/before live release,
after release/before archive publication, and during atomic publication

Bootstrap ordering: finish physical file pump, hydrate backup WAL, start shadow
recovery, replay through required backup boundary, mark store ready, resolve
deferred main-heap rows, then finish destination tail. Preserve heap/index forks,
segments, and [tablespace mapping](tablespaces.md). Page walk may overlap replay,
but readiness cannot precede complete seed. Bound deferred pointers on disk

Implement in stages: prove extension reads, audit destructive WAL per major,
track/route physical relations, seed bootstrap, integrate reader, then compose
live/archive fences. Keep configuration unavailable until composition passes
recovery tests. Resolve catalog-wait/reclamation deadlocks before integration,
as described in [shared constraints](coordination.md)

## Completion

Prove reads and reclamation on each supported PostgreSQL major, then test live
and archived replay with vacuum, rewrite, truncate, drop, value-ID reuse, and
transactions spanning bootstrap. Cover source and shadow restart and crashes
around each persistence and publication boundary

Compare emitted values with current backend and source. Expose fetch failures,
replay waits, fence stalls, staged bytes, retained storage, and durable/released
positions. Require a recovery procedure for missing module, incomplete physical
seed, lost staging state, and disk exhaustion before enabling configuration
