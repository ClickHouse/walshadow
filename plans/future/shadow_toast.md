# shadow-backed TOAST chunk store

Status: future, experimental follow-up to [TOAST.md](../TOAST.md)

## Decision

Keep ClickHouse chunk store as current baseline. Add `ToastMode::Shadow` only
after proving exact reads and crash-safe reclamation fencing

`ShadowCatalog` currently shadows metadata, not TOAST data. Shadow mode changes
that contract: retain PostgreSQL catalog relations plus TOAST heaps and indexes,
then replay source WAL into those files. Source WAL remains sole writer

Do not write walshadow-owned chunk rows into shadow PostgreSQL. Continuous
recovery is read only, and local writes would diverge from source WAL history

## Goals and non-goals

Goals:

- resolve external values from physical TOAST relations in shadow PostgreSQL
- preserve same-transaction `ChunkMap` fast path and R2 superseded-value behavior
- use native PostgreSQL handling for delete, vacuum, rewrite, truncate, and drop
- avoid ClickHouse TOAST tables and per-chunk lifecycle rows in shadow mode
- keep ClickHouse backend available until shadow mode passes full recovery matrix

Non-goals:

- retain ordinary user heaps or build a full user-data replica
- support local writes, unlogged TOAST, temporary TOAST, or multi-database routing
- replace ClickHouse backend in first implementation
- treat a current-state SQL query as a historical read

## Current gap

[TOAST.md](../TOAST.md) stores chunk births and tombstones in ClickHouse with WAL
LSNs. Fetch uses `(toast_relid, value_id, max_lsn)`, so result is explicitly
bounded by referring record's history

Shadow catalog exposes relation metadata through
`ShadowCatalog::toast_descriptor_for`, but physical chunks are absent:

- filter sends user relation WAL, including `pg_toast` heaps and indexes, to
  decoder path
- decoder consumes heap records and sends NOOP bytes to shadow recovery
- bootstrap lander keeps catalog filenodes and skips user filenodes
- schema-only bootstrap deliberately omits user data

Shadow mode must change retained relation set, WAL routing, bootstrap, read
visibility, and recovery contract together

## Invariants

1. Build TOAST files from source backup bytes and source WAL redo only
2. Retain catalog relations, persistent TOAST heaps, their indexes, and transient
   TOAST relations created by rewrites, keep ordinary user heaps absent
3. Decode TOAST inserts into transaction-local `ChunkMap`, resolve from that map
   before querying shadow
4. Keep old physical chunks readable until every older referring row is durably
   resolved
5. Reject ambiguous misses or generations, return NULL only for independently
   proven superseded values
6. Preserve ClickHouse mode and its explicit-LSN history as independent backend

Waiting for replay through a target LSN does not create an as-of snapshot. Shadow
can already be ahead. Old chunk tuples must remain physical while older decoder
work remains

Value ID and stored size are not a historical key. PostgreSQL may reuse value ID
after physical cleanup, and different generations can have equal compressed
length. Exactness depends on fencing cleanup, not only validating assembled size

## Target flow

```text
source backup
  catalog files ----------------------> shadow data directory
  TOAST heap and index files ---------> shadow data directory
  main user heap pages ----------------> page walker -> ClickHouse rows

source WAL
  catalog record ----------------------> shadow recovery
  main user heap record ---------------> decoder
  TOAST heap insert -------------------> shadow recovery + decoder ChunkMap
  destructive TOAST record ------------> reclaim fence -> shadow recovery
  TOAST index record ------------------> shadow recovery

external pointer
  transaction ChunkMap -> shadow extension -> R2 superseded handling
```

## Relation tracking and routing

Routing needs a synchronous live set, not an eventually consistent query of
shadow catalogs

### Attach seed

Read source catalogs at attach snapshot and collect:

- `pg_class` rows with TOAST relkind and current relfilenodes
- `pg_index.indexrelid` rows belonging to each TOAST heap
- current index relfilenodes
- database and tablespace identity needed to construct paths

Store relation kind with every filenode. Filter must distinguish TOAST heap
records requiring dual delivery from index records requiring shadow delivery

### Live changes

Extend narrow catalog WAL decoder to recognize:

- newly created TOAST heaps and indexes
- relfilenode replacements and truncate-created relfilenodes
- transient rewrite heaps and indexes
- drops, including database removal

Track speculative changes by transaction and discard them on abort. Publish
committed changes before routing later records

Same-transaction `CREATE TABLE` plus large `INSERT` is critical. First TOAST
insert can arrive before corresponding catalog rows become queryable in shadow,
so routing cannot wait for commit visibility

### Route matrix

| Record owner | Shadow bytes | Decoder record | Notes |
| --- | ---: | ---: | --- |
| catalog relation | yes | tracker when needed | preserve catalog replay |
| ordinary user heap | NOOP | yes | keep user heap absent |
| ordinary user index | NOOP | no | no physical base relation |
| TOAST heap insert/update | yes | yes | redo tuple, fill `ChunkMap` |
| TOAST heap delete/prune/vacuum | fenced yes | metadata if needed | protect old chunks |
| TOAST index insert/split | yes | no | keep fetch index valid |
| TOAST index cleanup | fenced yes | no | protect old search path |
| rewrite TOAST heap/index | fenced yes | tracker when needed | preserve swap semantics |

Represent dual delivery explicitly, for example `Route::ToBoth`. Shadow recovery
must receive original WAL bytes, not a post-decoding approximation

Decide route after assembling complete WAL record. Keep physical base page for
every record sent to shadow. Test TOAST heap and index records crossing WAL
segment boundaries

## Read path

### PostgreSQL extension

Ordinary SQL uses current MVCC and hides deleted TOAST tuples. Use a required,
version-pinned extension exposing PostgreSQL TOAST visibility semantics instead

`HeapTupleSatisfiesToast` ignores tuple `xmax`, so deleted chunks remain readable
until physical cleanup. `toastrel_valueid_exists` uses `SnapshotAny`, so value ID
cannot be reused while an old physical tuple remains

Provisional API:

```sql
walshadow_fetch_toast(
    toast_relid oid,
    value_id oid,
    stored_size integer
) returns bytea
```

Implementation should:

1. validate relation kind and `pg_toast` namespace
2. select valid TOAST index using PostgreSQL internals
3. fetch with `SnapshotToast`, preferably through
   `table_relation_fetch_toast_slice`
4. validate chunk sequence, chunk sizes, and total stored size
5. return stored payload without applying pointer's logical decompression

Rust validates pointer metadata, decompresses payload, and validates raw size.
Revoke public execution and use a dedicated least-privilege role

### Replay and lookup

Fetch request needs both:

- value LSN, semantic upper bound from referring WAL record
- visibility LSN, minimum shadow replay point needed for value to exist

Thread `DecodeJob.commit_lsn` into detoast path and wait for replay through that
point before extension fetch. Same-transaction `ChunkMap` hit skips wait

Use a dedicated libpq connection or small pool. Do not hold `ShadowCatalog`
client mutex across multi-megabyte reads. Bound concurrent requests and bytes in
flight

Resolve in this order:

1. transaction-local `ChunkMap`
2. shadow extension after replay wait
3. R2 nullable fill only when row is independently proven superseded

Treat missing chunks, malformed sequence, size mismatch, extension failure, and
unexpected relation identity as fatal for a live value

## Reclamation fence

`SnapshotToast` preserves deleted tuples only until physical cleanup. Shadow
cannot replay destructive WAL ahead of unresolved decoder work

### Record classification

Audit PostgreSQL resource managers for every record that can make an older TOAST
value unreadable, including:

- heap prune, vacuum cleanup, page rewrite, and truncate
- btree deletion, vacuum, page deletion, and cleanup
- `VACUUM FULL`, `CLUSTER`, and rewriting `ALTER TABLE`
- relfilenode replacement
- relation, database, or tablespace drop

This list is not final classifier. Build a source-backed record matrix for each
supported PostgreSQL major version and fail closed on any unclassified record
touching retained TOAST storage

### Durable rule

Define:

```text
resume_safe_lsn = highest LSN whose decoder effects are durably complete and
                  require no earlier TOAST fetch after restart

toast_reclaim_lsn = highest destructive boundary released to shadow

toast_reclaim_lsn <= resume_safe_lsn
```

`resume_safe_lsn` must include queued, active, reordered, spilled, deferred, and
emitter work. It is not highest decoded commit LSN

Before releasing destructive record at `D`:

1. stop shadow publication before record bytes
2. drain or persist all decoder work below `D`
3. fsync cursor state proving `resume_safe_lsn >= D`
4. release original WAL bytes to shadow
5. expose resulting reclaim watermark in diagnostics

This is scalar durability coupling, not a per-TID journal. It activates only at
records capable of physical reclamation

### Live and archive gates

Current WAL flow can forward bytes to shadow before record sink sees parsed
record. Move classification ahead of shadow publication or stage full record
until route and fence decision are known. Pause only at record boundary

Gating live path is insufficient because `restore_command` may consume a
completed segment containing same destructive record. Stage completed segments
before archive publication. Keep a segment unpublished until every destructive
boundary in it satisfies durable fence

Fsync staged segment and manifest before publication. On restart, validate or
rebuild manifest by scanning staged WAL, compare boundaries with durable cursor,
and publish only proven-safe segments. Recovery must have no alternate archive
path that bypasses fence

Crash tests must cover:

- before cursor fsync, record and archive remain withheld
- after cursor fsync but before live release, replay may retry safely
- after live release but before archive publication, publication may retry
- during archive publication, restore sees absence or complete segment
- cursor behind shadow replay at restart, startup rejects violated invariant

Shadow promotion remains unsupported, shadow is disposable and rebuildable

## Bootstrap

Extend backup relation map to include TOAST indexes, not only relkind `t` heaps.
Land TOAST heap and index files in shadow data directory with catalog files.
Preserve forks, relation segments, and tablespace mapping

Do not page-walk TOAST heaps into ClickHouse rows in shadow mode. Continue walking
main user heaps and defer rows containing external pointers

Required ordering:

```text
finish backup pump
  -> hydrate backup WAL
  -> start shadow recovery
  -> catch up through backup end LSN
  -> mark ShadowToastStore ready
  -> resolve deferred main rows
  -> finish ClickHouse bootstrap tail
```

Page walk and shadow catch-up may overlap, but deferred resolution waits for
complete base files and required replay. Bound deferred memory with existing spill
infrastructure or a restart-safe pointer spool

Reject shadow mode with schema-only initialization. Physical bootstrap must
preserve source OIDs, relfilenodes, database identity, and supported tablespace
mapping

Preflight verifies extension availability and version, PostgreSQL major version,
TOAST heap/index completeness, system identifier, backup LSNs, and WAL source
identity

## Rust interfaces

Current `ChunkStore { put, fetch }` assumes every reader accepts decoded chunk
rows. Split capabilities so shadow backend cannot receive `ToastRow` writes

Possible shape:

```rust
enum ToastBackend {
    Disabled,
    ClickHouse(Arc<ClickHouseChunkStore>),
    Shadow(Arc<ShadowToastStore>),
}

impl ToastBackend {
    fn can_fetch(&self) -> bool;
    fn accepts_chunk_rows(&self) -> bool;
    fn keeps_physical_toast(&self) -> bool;
    fn decodes_toast_pages(&self) -> bool;
}
```

Prefer separate `ChunkReader` and `ChunkWriter` traits if call sites remain
ambiguous. Fetch request carries TOAST relation identity, value ID, stored and raw
sizes, compression method, value LSN, and minimum replay LSN

Pass shadow connection configuration from pipeline and bootstrap configuration,
not emitter configuration

Shadow capabilities:

- `can_fetch = true`
- `accepts_chunk_rows = false`
- `keeps_physical_toast = true`
- `decodes_toast_pages = false` during bootstrap
- live inserts still decode enough to populate transaction `ChunkMap`

Skip steady-state `ToastRow` births, tombstones, ClickHouse TOAST DDL, and
per-commit chunk writes. Keep `ToastDelete` spill data only if classifier needs it

## Lifecycle, failures, and observability

Once routing and fence hold, PostgreSQL redo owns lifecycle. Delete preserves old
chunks through `SnapshotToast`; vacuum, truncate, rewrite, and drop take effect
only after older work is safe. Shadow mode needs no ClickHouse tombstone
propagation or explicit TOAST GC phase

Fail startup when extension is absent, physical seed is incomplete, tracker
cannot classify retained storage, archive can bypass fence, or shadow replay is
already beyond durable reclaim watermark

Stop ingestion on corrupt or ambiguous fetch, stalled replay, cursor persistence
failure, disk exhaustion, or recovery error. Do not silently switch backends

Add metrics for:

- fetches, misses, errors, bytes, latency, and `ChunkMap` hits
- replay waits and requested LSN lag
- reclaim fence count, stall duration, and pending bytes
- staged archive segments and bytes
- durable resume-safe and released reclaim LSNs
- retained TOAST heap/index bytes
- deferred bootstrap rows and spool bytes
- extension connection saturation

Shadow mode is not a full replica, but it is not schema only. TOAST payload and
bloat may dominate database size. Benchmark local storage, recovery I/O, dual WAL
processing, libpq payload copies, and archive staging against ClickHouse backend

## Implementation phases

### Phase 0: prove extension semantics

- build version-pinned extension function
- seed one TOAST heap and index into disposable PostgreSQL instance
- fetch after insert and delete, then verify failure after physical cleanup
- verify compressed payload and Rust decompression
- demonstrate wrong generation is possible after value ID reuse without fence

### Phase 1: classify destructive WAL

- enumerate reclaiming or detaching records for supported PostgreSQL versions
- define relation identity and safe boundary for each record
- add parser and cross-segment fixtures
- fail closed on unclassified records touching retained TOAST storage

### Phase 2: track and route physical relations

- seed TOAST heap/index live set from source catalogs
- track same-transaction creation, abort, truncate, rewrite, and drop
- add dual-delivery route using original bytes
- replay physical WAL while retaining insert decode for `ChunkMap`
- verify every shadow-routed record has matching base file

### Phase 3: retain TOAST during bootstrap

- land heap/index files, forks, segments, and tablespace mappings
- skip ClickHouse chunk extraction in shadow mode
- catch shadow up before deferred detoast
- bound or spool deferred rows
- reject schema-only bootstrap

### Phase 4: integrate `ShadowToastStore`

- add dedicated extension client pool
- thread commit and value LSNs into fetch request
- wait for replay, validate pointer and payload, then decompress
- enforce fatal miss policy before R2 superseded handling
- use same reader for bootstrap and streaming

### Phase 5: fence reclamation and archives

- connect classifier to durable resume-safe cursor
- gate live WAL before destructive record boundary
- stage and gate completed archive segments
- persist and recover pending manifests
- reject cursor/replay invariant violations
- inject crashes around every fsync and publication boundary

### Phase 6: validate and expose

- cover vacuum, rewrite, truncate, drop, restart, and supported PostgreSQL versions
- compare emitted logical values byte-for-byte with ClickHouse backend
- add experimental `toast.mode = shadow`, preflight, metrics, and operations docs
- keep ClickHouse mode default until production evidence supports changing it

## Acceptance matrix

Required end-to-end cases:

- pre-window value during bootstrap
- same-transaction value and A to B update
- delete before and after lazy or aggressive vacuum
- forced value ID reuse with equal stored size
- `VACUUM FULL`, `CLUSTER`, and rewriting `ALTER TABLE`
- `TRUNCATE`, table drop, and database drop
- same-transaction `CREATE TABLE` plus large `INSERT`
- TOAST heap and index records crossing WAL segment boundary
- shadow replay ahead of decoder
- decoder backpressure while destructive WAL arrives
- crash before and after reclaim cursor fsync
- crash during archive publication
- restart with cursor behind shadow replay
- missing or incompatible extension
- corrupt or missing TOAST index
- multi-megabyte compressed and uncompressed values under concurrency
- non-default tablespace

For every success case, compare ClickHouse output with source PostgreSQL logical
values. For unsupported cases, fail before emitting ambiguous rows

## Rejected shortcuts

- current `pg_toast` SQL query, current MVCC hides deleted tuples
- replay wait without fence, replay can already have reclaimed old generation
- stored-size validation alone, generations can have equal compressed length
- live-stream gate alone, restore command can consume archived segment
- TOAST heaps without indexes, native fetch needs valid index
- local auxiliary history in shadow, local writes violate recovery contract
- immediate ClickHouse backend replacement, shadow adds storage, ABI, recovery,
  archive, and cursor coupling

## Dependencies and open questions

Dependencies:

- R2 and transaction `ChunkMap` behavior from [TOAST.md](../TOAST.md)
- durable cursor proving completion across spill, reorder, bootstrap, and emitter
- extension packaging for supported PostgreSQL majors
- backup routing for TOAST indexes and tablespaces
- archive staging that restore command cannot bypass

Resolve before implementation:

- which cursor proves no work remains below reclaim boundary, and which state must
  join its fsync transaction?
- can current archive layout publish complete segment atomically with no second
  visible path?
- should extension return assembled payload or validated chunks to reduce peak
  memory?
- how should extension choose valid index during rewrite recovery?
- which PostgreSQL versions enter initial support set?
- should deferred bootstrap reuse spill format or use dedicated pointer spool?
- how should source tablespace paths map into shadow data directory?
- is recovery pause under prolonged ClickHouse backpressure acceptable?

Cursor and archive answers determine whether reads are historical and exact.
Resolve both before work moves beyond proof of concept

## Exit criteria

Call `ToastMode::Shadow` production-ready only when:

- source WAL is sole physical writer
- bootstrap retains complete TOAST heap and index set
- same-transaction and pre-window values resolve
- extension uses PostgreSQL TOAST visibility semantics
- live and archived destructive WAL obey same durable fence
- crash tests preserve reclaim invariant
- equal-size value ID reuse cannot return wrong generation
- lifecycle matrix passes for supported PostgreSQL versions
- storage, replay, and backpressure costs are observable
- ClickHouse comparison emits identical logical rows

Until then, treat shadow-backed TOAST as experimental alternative, not replacement
for explicit-LSN ClickHouse chunk history
