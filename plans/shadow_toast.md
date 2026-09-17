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

### Measured answer

Built: `WS_OP_FETCH_TOAST` in `pgext/toast.c`, `Bridge::fetch_toast`, a read-only
`ShadowToastStore`, and `tests/shadow_toast_reads.rs`. Results on PG 18:

- A value whose referring row version is dead reads back byte-exact, so
  `HeapTupleSatisfiesToast` ignoring xmax is confirmed as usable
- **VACUUM is not the reclaimer.** Opportunistic pruning takes the chunks as
  soon as the cleanup horizon passes the deleting transaction. On a primary
  that is immediate and unprompted: a 240,000-byte value read back as 480
  bytes, its final chunk, with no VACUUM issued. Pin the horizon with an open
  repeatable-read snapshot and the same value reads back whole
- On a standby the property holds as the design needs it to. Nothing prunes
  locally, so the value stays whole until an `XLOG_HEAP2_PRUNE` record is
  replayed, and replaying the reclamation then takes it. That makes the fence
  primarily about prune records, not vacuum records
- Density plus total size is sufficient validation: it rejects a partly pruned
  run and interleaved generations under a reused id with one rule. True value-id
  reuse is not reproducible in a test (PG refuses direct writes to a toast
  relation and the OID counter cannot be rewound), so that case rests on the
  rule rather than on a measurement
- Latency: 8 values / 720,000 stored bytes in one round trip, 3.7 ms total,
  457 us per value. The ClickHouse mirror's round trip measured 71.9 ms flat

Chunks come back stored, not decompressed: the daemon already owns pglz/lz4 and
the `va_tcinfo` prefix, and raw-size validation stays where the decompressor is.

### Wired up

`[toast] backend = "shadow"` selects it, for a greenfield bootstrap and for
live CDC. Covered by `tests/bootstrap_toast_oracle_ch.rs` and
`tests/toast_shadow_backend_e2e.rs`.

**Shadow is the only value server, and it starts during bootstrap.** The
source's TOAST heaps *and their indexes* are landed into its data dir, and it
reaches `end_lsn` by ordinary recovery. That is what makes a value written
during the backup readable at all — its chunks are appended mid-window, so no
file copy and no page image holds them. Recovery also repairs torn pages
correctly, restoring each image into a buffer it marks dirty so the checksum
is recomputed.

The one ordering constraint: `filter_landed_wal` rewrites `pg_wal` in place and
must precede shadow's recovery, but the backup-window leg needs those bytes
raw. `wal_landing::copy_window_segments` gives the leg its own copy under
`--spill-dir`, which is what lets shadow start ahead of it.

Supporting changes:

- Deferral keys on `!resolver.fill_on_miss()`, not `stores_chunks()`: "a store
  owes this value" is the condition; "the store also takes writes" is not
- The staged set is explicit (`toast_staging::StagedRels`), not derived from
  `CatalogMap::is_toast`. It has to include the toast **index**, which is a
  relation the catalog seed holds no descriptor for, and whose filenode comes
  from a `pg_class`/`pg_index` query on the source. The indexes also join
  `tap_filenodes`, or the walk declines them before the staging hook
- `Route::ToBoth` plus `Filter::keep_user_rels` deliver those relations'
  records to shadow as well as the decoder. Exact for everything that existed
  at backup time; the blanket `route_user_to_shadow` survives only as the
  no-bootstrap fallback, because over-routing costs disk while under-routing
  loses a value silently
- `filter_landed_wal` runs once, keeping those relations' records rather than
  rewriting them to NOOPs
- `run()` adopts the shadow bootstrap started, rather than starting a second
  postmaster on the same data dir
- The bootstrap oracle is untouched: provisioned only for tier-3 type
  conversion, and no part of the value path

### What was tried and removed

Three designs were built and discarded, each for a reason worth keeping:

1. **Bootstrap refuses the backend; live CDC only.** Useless — bootstrap is
   where the cost is.
2. **Serve bootstrap values from the bootstrap oracle.** It is already running
   with a bridge, and `pg_dump --binary-upgrade` gives it the source's OIDs
   and relfilenodes, so the files belong at the paths they already have. It
   cannot redo source WAL, so it can only ever hold the backup *copy* — which
   does not contain values written during the backup. It also needed
   `pg_resetwal -x` to keep clog reads in range, `REINDEX` to rebuild indexes
   over the landed heaps (a full scan), a second copy of the corpus, and
   `ignore_checksum_failure`, because a raw backup copy contains torn pages
   whose checksums do not match.
3. **Repair the copy from the window's full-page images**
   (`ToastPagePatcher`, first image per block). Unsound: a page gets an image
   only on its first post-checkpoint touch, so chunks appended later in the
   window appear in no image. The same argument rules out landing a btree
   index this way — a page split relocates entries onto a newly-initialised
   page logged `REGBUF_WILL_INIT`, which carries no image at all. Under real
   redo none of this applies, because the split record is applied too.

### Measured PostgreSQL behaviour worth not rediscovering

From `pg_walinspect` over a real backup window with concurrent TOAST churn —
35 records touching the toast relation, 5 carrying a page image:

| source of a TOAST page image | emitted? |
|---|---|
| `Heap INSERT`, first post-checkpoint touch of an existing page | yes |
| `Heap INSERT` into a freshly initialised page | no (`REGBUF_WILL_INIT`) |
| `Heap DELETE` of pre-window chunks | no image on the record itself |
| `XLOG_FPI_FOR_HINT` on that same page | yes — and this is where it arrives |
| a plain read of the toasted value | no: `SnapshotToast` never consults clog, so there is no hint bit to set |

**This exposed a live bug in the ClickHouse backend, fixed here:**
`harvest_toast_images` was called only from the Heap/Heap2 branch of
`on_record`, so the mirror missed every `XLOG_FPI_FOR_HINT` image — which per
the table above is exactly where the images for pages holding pre-window
values arrive. The mirror's torn-page repair was missing the pages it existed
for. It now runs on XLOG page images too.

### The fence cannot be delegated to PostgreSQL

Tested, because it would have removed most of the work below. PG's standby
conflict machinery parks replay of a record carrying a `snapshotConflictHorizon`
while a query holds an older snapshot, and `max_standby_archive_delay = -1`
makes it wait rather than cancel. Measured on a real standby:

- A snapshot opened *before* the referrer's delete replays does park the
  reclamation, and the value stays whole behind it
- A snapshot opened *after* it does not, and the value is gone

Conflict resolution protects tuples the held snapshot can see. A reader owing
pre-window values is always in the second case, so PG will not hold the line for
it. The fence has to be walshadow's own.

What makes that tractable: walshadow *is* shadow's WAL supply, over the
walsender and the archive directory `restore_command` reads. Withholding a
destructive record is therefore a publication decision, not new machinery —
which is why `BoundaryHoldSink` already withholds successor bytes from both
paths by blocking in `on_record`, and why gating the archive separately is not
optional. Staging differs from that hold only in keeping the pump running
instead of parking it, so `resume_safe_lsn` can still advance. It is also
fail-safe on daemon crash: nothing ships, so replay cannot pass the staged
record. The cost is that a catalog boundary above a staged record waits for
`resume_safe_lsn` to reach it, which is ClickHouse-paced — normally seconds,
but `hold_timeout` turns a ClickHouse slowdown into a DDL outage rather than
DDL lag.

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
