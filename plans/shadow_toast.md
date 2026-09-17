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

### Test results

Implementation includes `WS_OP_FETCH_TOAST` in `pgext/toast.c`,
`Bridge::fetch_toast`, read-only `ShadowToastStore`, and
`tests/shadow_toast_reads.rs`. Tests on PostgreSQL 18 found:

- `HeapTupleSatisfiesToast` can read exact bytes after referring row version
  dies because it ignores `xmax`
- **VACUUM is not required to remove chunks.** Opportunistic pruning removes
  them as soon as cleanup horizon passes deleting transaction. On a primary,
  a 240,000-byte value returned only its final 480-byte chunk without an
  explicit VACUUM. An open repeatable-read snapshot kept complete value
- Standby preserves value until it replays an `XLOG_HEAP2_PRUNE` record because
  standby does not prune locally. Reclamation fence must therefore handle
  prune records as well as vacuum records
- Density plus total size is sufficient validation: it rejects a partly pruned
  run and interleaved generations under a reused id with one rule. True value-id
  reuse was not reproduced in a test (PostgreSQL refuses direct writes to a TOAST
  relation and the OID counter cannot be rewound), so that case rests on the
  rule rather than on a measurement
- One round trip for 8 values and 720,000 stored bytes took 3.7 ms total, or
  457 us per value. ClickHouse mirror round trip took 71.9 ms

Worker returns stored chunks without decompressing them. Daemon already handles
pglz/lz4 and `va_tcinfo`, so decompression and raw-size validation remain in one
place.

### Implementation

`[toast] backend = "shadow"` enables backend for greenfield bootstrap and live
CDC. Tests are in `tests/bootstrap_toast_shadow_ch.rs` and
`tests/toast_shadow_backend_e2e.rs`.

**Shadow is the only value server and starts during bootstrap.** Bootstrap
copies source TOAST heaps and indexes into shadow's data directory. PostgreSQL
recovery then replays through `end_lsn`, adding chunks written during backup
that are absent from copied files and page images. Recovery also repairs torn
pages and recomputes their checksums.

`filter_landed_wal` must rewrite `pg_wal` before shadow recovery starts, while
backup WAL processing needs original bytes. `wal_landing::copy_window_segments`
copies original segments to `--spill-dir` so both operations can proceed in
required order.

Supporting changes:

- Defer values when `!resolver.fill_on_miss()`, not when `stores_chunks()`.
  Deferral requires a backing store but does not require that store to accept
  writes
- The staged set is explicit (`toast_staging::StagedRels`), not derived from
  `CatalogMap::is_toast`. It has to include the toast **index**, which is a
  relation for which catalog seed has no descriptor. Its filenode comes
  from a `pg_class`/`pg_index` query on the source. The indexes also join
  `tap_filenodes` so page walk reaches staging hook
- `Route::ToBoth` plus `Filter::keep_user_rels` deliver those relations'
  records to shadow and decoder. Explicit set covers relations present at
  backup time. `route_user_to_shadow` remains only as no-bootstrap fallback
- `filter_landed_wal` runs once, keeping those relations' records rather than
  rewriting them to NOOPs
- `run()` adopts shadow process started by bootstrap instead of starting another
  postmaster on the same data dir
- Bootstrap oracle remains responsible only for tier-3 type conversion

### Rejected designs

Three designs were implemented and removed:

1. **Support backend only for live CDC.** This does not remove bootstrap cost.
2. **Serve bootstrap values from the bootstrap oracle.** It is already running
   with a bridge, and `pg_dump --binary-upgrade` gives it the source's OIDs
   and relfilenodes, so the files belong at the paths they already have. It
   cannot replay source WAL, so it can only hold backup copy, which
   does not contain values written during the backup. It also needed
   `pg_resetwal -x` to keep clog reads in range, `REINDEX` to rebuild indexes
   over the landed heaps (a full scan), a second copy of the corpus, and
   `ignore_checksum_failure`, because a raw backup copy contains torn pages
   whose checksums do not match.
3. **Repair copy from backup window's full-page images**
   (`ToastPagePatcher`, first image per block). A page gets an image
   only on its first post-checkpoint touch, so chunks appended later in the
   window appear in no image. Same issue prevents copying a B-tree
   index this way: a page split relocates entries onto a newly initialized
   page logged `REGBUF_WILL_INIT`, which carries no image at all. Under real
   redo applies split record and does not have this limitation.

### PostgreSQL behavior observed in tests

`pg_walinspect` showed following results for a backup with concurrent TOAST churn,
35 records touching the toast relation, 5 carrying a page image:

| source of a TOAST page image | emitted? |
|---|---|
| `Heap INSERT`, first post-checkpoint touch of an existing page | yes |
| `Heap INSERT` into a freshly initialised page | no (`REGBUF_WILL_INIT`) |
| `Heap DELETE` of pre-window chunks | no image on the record itself |
| `XLOG_FPI_FOR_HINT` on that same page | yes, image arrives here |
| a plain read of the toasted value | no: `SnapshotToast` never consults clog, so there is no hint bit to set |

**This exposed a ClickHouse backend bug fixed in this branch:**
`harvest_toast_images` was called only from the Heap/Heap2 branch of
`on_record`, so mirror missed every `XLOG_FPI_FOR_HINT` image. As table shows,
these records contain images for pages holding values created before backup.
Mirror now processes XLOG page images as well.

### The fence cannot be delegated to PostgreSQL

PostgreSQL standby conflict handling pauses replay of a record carrying a
`snapshotConflictHorizon`
while a query holds an older snapshot, and `max_standby_archive_delay = -1`
makes it wait rather than cancel. Measured on a real standby:

- A snapshot opened *before* replay of referrer deletion pauses reclamation and
  preserves complete value
- A snapshot opened *after* it does not, and the value is gone

Conflict resolution protects tuples visible to held snapshot. A reader that
needs values from before backup is always in second case, so walshadow must
implement its own reclamation fence.

walshadow supplies shadow WAL through walsender and archive directory used by
`restore_command`, so it can delay destructive records before publication.
`BoundaryHoldSink` already blocks successor bytes on both paths in `on_record`;
archive path must use same gate. Staging must keep pump running so
`resume_safe_lsn` can advance. If daemon crashes, no WAL is shipped and replay
cannot pass staged record. Catalog boundaries after staged record must wait for
ClickHouse-paced `resume_safe_lsn`. `hold_timeout` turns prolonged ClickHouse
delay into DDL failure instead of unbounded DDL lag.

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
Include [pending bootstrap rows](bootstrap.md) and future destination queues in proof

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
