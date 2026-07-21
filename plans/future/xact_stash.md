# xact stash: generic raw-record decode from catalog history

Status: future, extends commit-time stash in [xact.md](../xact.md) and
[TOAST.md](../TOAST.md)

## Decision

Do not generalize toast stash with per-commit resolution bundles

Build one durable, LSN-indexed catalog history and make every WAL heap decoder
read descriptors from it. Capture full relation descriptors from shadow at
catalog commit boundaries, persist each capture before publishing successor
WAL, then answer `descriptor_at(rfn, lsn)` from bounded intervals rather than
current shadow state

Treat generic stash decode as first consumer of this history, not as owner of
another consistency protocol. Same source must serve live rows, delayed rows,
spilled rows, toast ownership, schema events, restart replay, and future
prepared-xact or rewrite consumers

Five changes form one dependency chain:

1. replace independent restart and GC calculations with one durable manifest
2. collapse serial and pipeline drain consumers into one ordered apply path
3. capture durable descriptor history at catalog boundaries
4. put routing and shape config in WAL order, split descriptor and route stages
5. enable ordinary raw decode only after all preceding prerequisites land

Reject prior `ResolutionBundle` design. A stash-only fence leaves identical
cold-cache race on live rows, while bundle contents grow toward a per-commit
snapshot of every decode input. One sparse history closes both paths and
removes bundle identity, lineage-proof, retention, and resolver-lane protocols

## Current gap

`StashOutcome::Skip` drops every non-toast stash candidate. Committed rows do
not reach ClickHouse for common cases:

- `BEGIN; CREATE TABLE; COPY; COMMIT`
- existing-table `TRUNCATE` plus reload in one xact

Directly changing `Skip` to ordinary decode is unsafe. Correct tuple decode at
record LSN `L` requires `descriptor(rfn, L)`. `relation_at(rfn, L)` only waits
for `shadow_replay >= L`, then queries current catalog state at some unbounded
later position. Worker lag, cache eviction, or restart can therefore substitute
post-`ALTER` shape for pre-`ALTER` row

Race affects live path too:

1. row for existing relation appears before `ALTER`
2. decoder worker falls behind
3. shadow replays through `ALTER`
4. descriptor cache is cold or evicted
5. lookup for old row fetches post-`ALTER` descriptor

Carrying fetched descriptor downstream prevents later re-resolution, but does
not make initial fetch correct. Name, replica identity, dropped positions,
column overrides, and encoding plan can all change, not only tuple width

Current inputs also follow four clocks:

- shadow replay position
- decoder and drain position
- config state from WAL rows, boot seed, and SIGHUP
- live Oracle conversion time

Replay stability needs one WAL-positioned ordering domain, not pairwise bridges
between clocks

## Scope

This plan covers:

- descriptor history for every live and stashed heap record
- ordinary `INSERT`, `MULTI_INSERT`, `UPDATE`, `HOT_UPDATE`, and `DELETE`
  operations already supported by heap decoder
- same-xact `CREATE` plus writes
- same-xact `TRUNCATE` plus writes when resulting generation is catalog-visible
- toast relation identity and main-relation descriptor lookup
- schema-event and routing order
- spill xid fidelity and multi-insert fanout
- restart, source identity, retention, and fail-closed behavior

Keep outside scope:

- main-heap rewrite FPI decode
- generation never visible in catalog at any capture boundary
- mapped materialized-view replacement semantics
- PREPARE-durable xact buffering
- arbitrary historic MVCC snapshots inside PostgreSQL

Never-visible generations remain fail closed until FPI capture supplies tuple
and lineage data. Examples include rewrite output, intermediate generations
superseded inside one xact, and some materialized-view refresh paths

## Architecture

```text
source wire -> pump/classifier ---- bytes ----> shadow
                  |                              ^
                  | catalog boundary             | hold after commit EndRecPtr
                  v                              |
          catalog capture lane -----------------+
                  |
                  v
       durable metadata journal + manifest
          | descriptor intervals
          | schema changes
          | WAL config changes
          v
decoder -> xact buffer -> merged drain -> DescribedHeap
                                         |
                              apply preceding metadata/config
                                         |
                                         v
                                      RoutedHeap -> emit
```

Catalog capture is only live shadow lookup in row-decode architecture. Heap
decode, detoast, schema apply, routing, plan construction, and replay consume
durable metadata history

Hold shadow publication only at catalog-mutating commits. DML-only commits do
not park. Capture cost remains tied to DDL rate and one local batched snapshot,
never ClickHouse latency or drain backlog

## Invariants

1. `descriptor_at(rfn, L)` is durable pure function of source identity,
   timeline, database, and WAL position for every decodable generation
2. Decoder never reads descriptor version whose interval begins after row's
   effective schema position
3. Successor WAL cannot reach shadow or archive until catalog-boundary capture
   covering preceding commit is durable
4. Schema and route inputs apply in WAL order, route attaches only after all
   preceding metadata and config events apply
5. WAL replay emits byte-identical rows for equal source records; live Oracle
   output and placeholder fallback never feed replayable rows
6. Unknown invalidation, ambiguous interval, missing history, corrupt history,
   or never-visible generation fails closed before partial emit
7. One manifest computes actual restart point and one GC cut for every durable
   artifact family
8. Publication hold never waits for ClickHouse, committed drain, or queued
   barrier work

## WAL positions

### PostgreSQL EndRecPtr

Keep record start and PostgreSQL next-record position distinct:

```rust
struct Record {
    source_lsn: u64,
    next_lsn: u64,
    wire_end_lsn: u64,
    // ...
}
```

`source_lsn` remains row version and ordering position. `next_lsn` must match
PostgreSQL `XLogReaderState::EndRecPtr`, not last physical record byte:

- ordinary record advances by aligned total length
- page-spanning record advances from continuation-page address by aligned
  remaining length
- segment-spanning record follows same continuation semantics
- `XLOG_SWITCH` advances to segment end

Keep `wire_end_lsn` only for byte framing. Never use it for replay comparison

All capture boundaries use `next_lsn`:

- wait until `pg_last_wal_replay_lsn() >= commit.next_lsn`
- deliver bytes through commit, hold every successor byte
- stamp capture batch with commit start and `next_lsn`
- compare crash recovery against exact `next_lsn`

Port arithmetic from PostgreSQL `xlogreader.c`, keep version-specific tests
beside WAL walker

### Effective schema position

Each descriptor version carries two positions:

```rust
struct CatalogVersion {
    valid_from: WalPosition,
    captured_at: WalPosition,
    value: CatalogValue,
}

enum CatalogValue {
    Present(Arc<RelDescriptor>),
    Dropped { oid: Oid },
}
```

`captured_at` is catalog commit `next_lsn`, point where shadow snapshot became
visible and journal batch became eligible for publication

`valid_from` is earliest proven relation-specific schema position:

- bootstrap baseline uses bootstrap handoff LSN
- new filenode uses observed `XLOG_SMGR_CREATE` marker
- existing relation change uses earliest relation-specific catalog mutation or
  invalidation position
- drop uses relation-specific drop observation

Capture final committed descriptor only. If one xact produces multiple
incompatible layouts for same generation and user rows overlap intermediate
layouts, no single sampled version proves decode. Mark interval ambiguous and
fail closed. Permit compatible transitions only through explicit physical
compatibility predicate, including attnum-preserving rename, replica-identity
change, and append-only column addition with complete dropped-slot layout

Never guess `valid_from` from commit time when same-xact rows precede commit. If
parser can identify changed relation but not exact change position, publish
post-commit version for future rows and fail closed for overlapping rows

### Catalog frontier and decode admission

Pump owns monotonic catalog frontier: highest dispatched WAL position with no
unresolved committed catalog boundary before it. Publication hold advances
frontier through catalog commit only after journal batch becomes durable

Record dispatch after catalog boundary therefore proves required history is
present. Handle records inside catalog-changing xact separately:

- first catalog record marks top xid catalog-dirty before later user record can
  decode
- `XLOG_SMGR_CREATE` marks new rfn immediately
- user heap record after dirty mark stays `SpillEntry::Raw`, even when old
  descriptor exists for same rfn
- commit drain waits for catalog frontier through commit `next_lsn`, then
  selects captured interval and decodes raw records
- user record before first catalog mutation may decode against predecessor
  interval
- abort clears dirty state and raw records without journal append

When relation identity for dirty catalog records is not known yet, defer all
later user heap records in that top xid. Prefer extra raw buffering over stale
decode. Capture batch later narrows affected relations; unrelated deferred rows
decode through unchanged intervals

Multiple catalog changes after dirty mark may expose intermediate layout which
final snapshot cannot recover. Compatibility check or ambiguity record decides,
never timing or cache state

## Catalog-boundary detection

Track catalog mutation by top xid in pump classifier, including assigned
subxids. Existing `CatalogTracker` and `pg_class_decoder` provide starting
signals, but capture admission must not depend on decoder-worker cache state

Build affected-relation observations from:

- relcache invalidation messages in commit records carrying `HAS_INVALS`
- `XLOG_XACT_INVALIDATIONS` records
- decoded `pg_class` identities already available to `CatalogTracker`
- relation identifiers from other catalog tuples needed for exact change
  position, especially `pg_attribute` and `pg_index`
- `XLOG_SMGR_CREATE` markers for new filenode generations
- `XLOG_SMGR_TRUNCATE` and heap `TRUNCATE` OIDs where relevant

Decode `SharedInvalidationMessage` with explicit PostgreSQL-major layouts.
Unknown tag, short payload, unsupported major, or unresolved database identity
must never mean no change

Fallback for incomplete affected-rel enumeration:

1. capture all catalog-visible user relations in affected database
2. diff against durable preceding snapshot to find changes and drops
3. retain conservative post-commit versions
4. fail closed for user records in any interval whose exact `valid_from` cannot
   be proved

Full scan is acceptable as rare DDL-rate fallback. It preserves future rows
without claiming correctness for ambiguous intra-xact history

## Publication hold

At each catalog-mutating top-level commit:

1. register result-bearing boundary waiter before commit can leave pump
2. forward commit bytes through `next_lsn` to shadow
3. enqueue commit for decoder path
4. force `QueueingRecordSink::flush()` so commit cannot remain stranded inside
   current source chunk
5. wait for shadow replay through exact `next_lsn`
6. capture affected descriptors on dedicated shadow connection
7. append one checksummed journal batch, fsync data, atomically advance manifest
   frontier, fsync directory
8. release successor-byte publication on `Ok`
9. terminate pump on error

Waiter selects between capture result and terminal transport or worker-health
signal. Channel closure, worker panic, replay timeout, catalog error, journal
error, manifest error, and permanent walreceiver loss wake waiter with `Err`

Do not wait for decoder to process commit. Forced flush proves delivery to
queue, health signal proves task remains viable. Capture lane never calls
ClickHouse and never queues behind `flush_due_retires`

Crash behavior follows hold position:

- before journal fsync, successor bytes were not published, restart replays and
  recaptures boundary
- after journal fsync but before release, duplicate capture is idempotent by
  source identity, timeline, commit `next_lsn`, and batch digest
- after release, durable batch already covers every published successor record
- partial tail without valid batch footer or checksum is ignored, then same
  boundary is recaptured before publication resumes

### Live wire requirement

Whole archive segments cannot stop after a mid-segment commit and later append
original successor bytes. Catalog-boundary hold therefore requires active
walreceiver, while archive remains post-release recovery path

Enforce capability, not flag value:

- reject `--walsender-connect-timeout=0`
- fail startup if walreceiver does not attach before configured timeout
- during hold, reconnect live wire before catalog timeout or fail boundary
- prevent `restore_command` from observing segment containing unreleased bytes

Positive timeout without attachment must fail startup, not warn and continue
archive-only

## Durable metadata journal

Store one append-only journal beside cursor state. Journal header binds data to:

- source system identifier
- PostgreSQL major
- timeline
- source database OID
- WAL segment size
- format version

Reject source replacement, incompatible major, timeline mismatch without
declared history transition, or checksum failure. Never load files solely by
xid or filenode

Each catalog batch contains:

- commit start and `next_lsn`
- affected relation observations and trigger positions
- complete new descriptor versions
- explicit drop versions
- old and new descriptor digests
- deterministic schema intent derived from durable predecessor
- toast main/relation ownership intervals
- ambiguity records for intervals that must fail closed
- batch checksum and completion footer

Key descriptor lookup by `(source, timeline, db_node, spc_node, rel_node)` and
search version intervals by WAL position. Retain relation OID as identity and
lineage field, not lookup substitute, because filenodes rotate

Schema intent must not depend on volatile `ShadowCatalog::prev_known`.
Derive `EnsureRelation`, `Changed`, or `Dropped` from preceding journal version
and descriptor digest. Replaying journal produces same event at same
`valid_from` position

Config events may share journal framing so compaction can checkpoint one ordered
metadata state. They still originate only from WAL-decoded config rows

### Capture snapshot

Use one SQL statement or explicit read-only repeatable-read transaction after
shadow reaches boundary. Batched capture must include:

- `pg_class`, `pg_namespace`, and relation identity
- every positive `pg_attribute.attnum`, including dropped positions
- type metadata needed for physical decode
- `pg_index` data for primary and replica-identity keys
- relation persistence and relkind
- toast ownership in both directions

Preserve foreign-database guard before rfn lookup. `rel_node` is unique only
inside database identity

Query dropped attributes without `pg_type` inner join. PostgreSQL can zero
`atttypid` while retaining physical `attlen` and `attalign`; descriptor must
keep one slot per positive attnum and consume bytes for dropped positions

Serialize complete versioned `RelDescriptor` and `RelAttr`, including:

- namespace, relation name, OID, rfn, relkind, persistence
- attnum, name, dropped flag, nullability, missing value
- type OID/name, length, alignment, by-value, storage, typmod
- replica identity mode, index OID, key attnums
- toast relation identity

Do not reconstruct any field from live catalog downstream

### Bootstrap and migration

Greenfield bootstrap captures complete catalog baseline at bootstrap handoff
LSN before streaming starts. Decoder never requests history before that point

Existing deployments enabling catalog history need one explicit transition:

- drain to durable boundary, capture complete baseline, start new history epoch
- or resnapshot

Do not seed current descriptors and silently claim coverage for earlier replay
range. Explicit `--start-lsn` before retained baseline must fail startup

## One manifest and one GC cut

Replace independent retention formulas with one crash-safe manifest and one
shared helper used by startup and pruning. Manifest owns at least:

```text
source identity
timeline
effective durable resume LSN
decoder floor
catalog frontier
shadow recovery floor
GC cut
metadata checkpoint generation
immutable routing seed digest and contents
```

Compute effective resume with exact startup rules, including segment alignment,
cursor `emitter_ack_lsn`, `filter_durable_lsn`, sealed-archive clamp, bootstrap
handoff, and explicit override validation. Pruner must call same helper rather
than duplicate formula

Compute one conservative cut from every durable floor. Apply cut to artifact
families with format-specific mechanics only:

- archive and spool segments remove complete units ending at or below cut
- xid spill removes only transactions proven below cut and no longer live
- toast-retire entries compact below cut
- metadata journal compacts old batches into checkpoint, retaining predecessor
  descriptor/config version needed to answer first position at cut

Persist new checkpoint and manifest before deleting old journal segments.
Crash between manifest fsync, deletion, and directory fsync must load either
old complete generation or new complete generation

Required archive-clamp case:

1. cursor ack reaches segment `N+2`
2. sealed archive ends at `N`
3. metadata version needed by replay lies in `N+1`
4. GC runs
5. restart clamps to archive end
6. version remains available and replay succeeds

Reject operator rewind below GC cut. Never treat missing history as cache miss
eligible for current-state lookup

## Descriptor and route stages

Split row context at actual ordering boundary:

```rust
struct DescribedHeap {
    decoded: DecodedHeap,
    descriptor: Arc<RelDescriptor>,
}

struct RouteSnapshot {
    mapping: Arc<TableMapping>,
    column_overrides: Arc<ColumnOverrides>,
    row_encoding: Arc<RowEncodingSnapshot>,
}

struct RoutedHeap {
    described: DescribedHeap,
    route: Arc<RouteSnapshot>,
}
```

`MergedDrain` decodes raw records with catalog history and yields
`DescribedHeap`. Reorder coordinator then:

1. applies preceding schema and config entries
2. resolves route for following WAL interval
3. snapshots every encoding-relevant mapping field
4. constructs `RoutedHeap`
5. dispatches trailing segment

Route must include column overrides consumed by `TablePlan`, namespace defaults,
auto-create result, destination, and any other state capable of changing row
values or uncompressed CH row encoding. `TableMapping` alone is insufficient

Detoast and physical tuple decode use `DescribedHeap`. Encoder plan uses
`RoutedHeap`. No stage performs another catalog or live-mapping lookup

Account retained descriptors and route snapshots in spill, queued-batch, and
resident-memory budgets

## Routing and config clock

Make routing and shape config pure function of WAL position:

- persist immutable TOML/CLI routing seed in manifest when history epoch starts
- reject changed routing seed on restart until operator starts new drained epoch
  or resnapshot
- apply table, namespace, column, mapping, and shape changes only through
  WAL-ordered config overlay after pump starts
- limit SIGHUP to operational knobs whose value cannot change logical row
  contents, schema, or destination, such as budgets, compression, retry, and
  observability
- apply each WAL config entry through same reorder site as schema entry

Boot source-table seed must correspond to baseline LSN and become durable before
streaming. Later source config changes come only from decoded WAL, never fresh
side query on restart

Append committed config events to metadata journal before first event from that
xact applies. Config-only commits need no shadow publication hold: stable boot
seed plus retained source WAL can reproduce append after crash. Identify event
by source, timeline, commit LSN, record LSN, and ordinal, not xid alone

Journal compaction folds config events below GC cut into checkpointed config
state. Replayed event append is idempotent and must match stored digest

This removes per-commit config-version references. Route at `L` derives from
durable seed plus ordered config entries through `L`

## Replay and Oracle policy

Keep byte-identical replay invariant. `_lsn` is ReplacingMergeTree version, but
equal-version rows with different bodies do not have a safe deterministic
winner. Dedup-sufficiency therefore cannot permit value drift

WAL row decode may use only deterministic in-process codecs. `PgPending`,
`Unsupported`, compressed varlena without local decoder, or any value requiring
live `walshadow_decode_disk` fails closed before row dispatch

Remove placeholder fallback from replayable WAL path. Oracle may remain for
diagnostics or explicitly non-replayable tooling, never for rows governed by
cursor rewind

Expand local codecs independently. Do not persist per-value Oracle output in
metadata journal, that recreates per-row bundle state and couples catalog hold
to value volume

## Raw substrate

Complete both repairs before ordinary raw verdict is enabled:

- spill v5 persists original xid in `RawRecord`; reconstructing record with
  `xact_id = 0` corrupts `_xid`
- `MergedDrain` owns pending decoded-item queue because one
  `XLOG_HEAP2_MULTI_INSERT` yields multiple heaps

Queue every tuple from one raw record before merge advances beyond record LSN.
Preserve event-before-heap tie break at equal LSN and include queued decoded
memory in budget accounting

Raw ordinary decode resolves descriptor from journal, produces
`DescribedHeap`, and enters same committed-tuple path as live decode. Do not
maintain separate stash emitter

Resolve toast ownership before main tuple detoast for generations created in
same xact. Ownership comes from descriptor intervals, not current catalog or
per-commit bundle

### TRUNCATE

Existing-table `TRUNCATE` plus reload works when final new generation is
catalog-visible: old generation and truncate event come from history, SMGR
marker anchors new generation, final capture supplies new descriptor

`CREATE; INSERT; TRUNCATE; INSERT` contains first generation never visible at a
commit boundary. Fail closed until explicit lineage/FPI work can prove first
rows are safely superseded. Do not revive discard-proof taxonomy in stash

Preserve TRUNCATE record LSN and resolve OIDs through durable catalog history so
wipe barrier remains ordered before post-truncate rows

### Image-only records

Logical ordinary INSERT/COPY keeps tuple block data with
`REGBUF_KEEP_DATA`, including checkpoint-mid-COPY cases. True image-only source
is rewrite path using `HEAP_INSERT_NO_LOGICAL`

Until main rewrite FPI admission lands, image-only ordinary record fails closed.
Synthetic image-only test must carry image without block data. Do not use
checkpoint-mid-COPY as proxy

## One drain consumer

Remove duplicate ordered apply implementations. Run serial
`XactRecordSink` behavior as degenerate pipeline configuration with one worker
and synchronous observer. Keep one merge, one schema/config apply site, one
route snapshot point, and one barrier implementation

Metrics-only and test paths use same ordering engine. Do not preserve alternate
catalog-event semantics for convenience

## Fail-closed boundary

Stop before any partial xact emit when:

- descriptor interval is absent or ambiguous
- generation never became catalog-visible
- affected-relation invalidation cannot be decoded safely
- descriptor journal or manifest is missing, corrupt, or source-mismatched
- dropped-column physical metadata is incomplete
- operation has image only
- relation is mapped materialized view awaiting replacement semantics
- deterministic codec is unavailable
- explicit rewind precedes retained baseline

Abort and subxact abort discard buffered raw records, observations, and pending
metadata events. Never publish descriptor from aborted catalog transaction

Unmapped relation with complete descriptor remains normal route discard at
record position. Missing descriptor is not equivalent to unmapped

## Delivery order

Treat order as dependencies, not independently shippable row features

1. unify manifest and effective-resume/GC helper
2. collapse serial drain into pipeline ordering path
3. add exact `next_lsn`, boundary hold, worker health propagation, and active
   walreceiver enforcement without changing row verdicts
4. add journal format, bootstrap seed, affected-rel capture, dropped-slot
   fidelity, source validation, compaction, and crash recovery
5. switch all live descriptor lookup and schema events to journal, remove
   generation epoch, lazy invalidation, `prev_known`, and volatile schema-event
   channel
6. persist routing seed, constrain SIGHUP, journal WAL config, add staged
   `DescribedHeap`/`RoutedHeap`, and enforce deterministic codec policy
7. add spill v5 xid and multi-insert pending queue
8. atomically enable ordinary raw `INSERT`, `MULTI_INSERT`, `UPDATE`,
   `HOT_UPDATE`, and `DELETE`
9. generalize stash metrics and documentation
10. later, admit rewrite FPI to cover never-visible generations

Keep ordinary raw decode behind one feature gate until steps 1 through 7 pass.
Do not enable insert family before descriptor fidelity, config ordering, and
fail-closed policy

## Acceptance

### EndRecPtr and hold

- unaligned single-page record computes PostgreSQL `EndRecPtr`
- page-spanning and segment-spanning records compute exact next position
- record ending on page boundary remains correct
- `XLOG_SWITCH` advances to segment end
- replay equal to exact commit `next_lsn` permits capture
- decoder batch size greater than one, commit mid-CopyData chunk, forced flush
  prevents deadlock
- capture error, journal error, worker error, worker panic, channel closure, and
  walreceiver loss wake waiter with `Err`
- positive walreceiver timeout with no attachment fails startup
- DML-only commit does not park
- bare DDL with no replicated rows parks once and releases after durable capture

### Catalog history

- existing table, cold cache, pre-`ALTER` row, later same-rfn `ALTER`, forced
  worker lag, old row uses old descriptor
- same sequence after restart and cache eviction produces identical row bytes
- CREATE plus plain/toasted COPY captures descriptor at marker and emits rows
- existing-table TRUNCATE plus reload resolves final generation
- relation rename, replica-identity change, and column override bind to correct
  WAL interval
- dropped column retains physical slot and subsequent values decode correctly
- batched snapshot preserves primary and replica-identity index attnums
- foreign database rfn never resolves through local database query
- schema `Ensure`/`Changed`/`Dropped` event replays from journal without
  `prev_known`
- multiple incompatible layouts inside one xact fail closed when intermediate
  shape was never captured
- unknown invalidation tag triggers full-scan fallback or fail closed, never
  false no-change

### Raw operations

- CREATE, INSERT, UPDATE, COMMIT emits final row with source xid
- CREATE, INSERT, DELETE, COMMIT emits ordered tombstone
- TRUNCATE, INSERT, UPDATE, COMMIT preserves wipe ordering
- multi-insert emits every tuple before merge advances
- multi-insert followed by later update preserves LSN order
- replica identity default, full, and index changes decode old image correctly
- subxact update/delete followed by subxact abort emits nothing from aborted
  branch
- unsupported no-payload heap operation remains explicit fail-closed result

### Routing

- unmapped CREATE plus INSERT discards at route stage, counted by reason
- config-table opt-in before row in same xact routes row
- row before opt-in discards, trailing row routes
- auto-create schema event applies before route snapshot
- route snapshot includes column override consumed by `TablePlan`
- restart with changed TOML mapping seed refuses stream until new epoch
- SIGHUP operational knob applies, SIGHUP mapping/shape mutation is rejected

### Replay codecs

- locally decoded values replay byte-identically after crash
- `jsonb`, array, domain, custom type, and compressed varlena without local codec
  fail closed before any xact row dispatch
- shadow extension present, absent, changed, or injected Oracle error cannot alter
  WAL output because WAL path never queries Oracle

### Durability and GC

- crash before batch footer ignores tail and recaptures boundary
- crash after journal fsync but before release accepts idempotent duplicate
- crash after release reloads descriptor and schema event from journal
- checksum, unknown version, source-system mismatch, and timeline mismatch fail
  closed
- cursor in `N+2`, archive end in `N`, required metadata in `N+1`, GC then
  restart retains metadata and replays successfully
- crash before and after checkpoint rename, manifest fsync, old-segment delete,
  and directory fsync always loads one complete generation
- explicit start LSN before baseline or GC cut is rejected

### Fail closed

- markerless replicated generation without history
- CREATE, INSERT, TRUNCATE, INSERT where first generation never becomes visible
- TRUNCATE, reload, rewriting ALTER where intermediate generation disappears
- mapped materialized-view refresh
- synthetic image-only ordinary record

## Observability

- `catalog_boundary_holds_total{result}` and hold duration
- walreceiver attachment and reconnect state
- capture rows, full-scan fallbacks, snapshot duration, affected-rel count
- journal frontier, versions, bytes, checkpoint generation, and GC cut
- descriptor lookups by hit, missing, ambiguous, corrupt, and source mismatch
- schema events by deterministic intent
- fail-closed rows and xacts by reason
- raw decoded records and rows by operation
- pending multi-insert rows and bytes
- manifest effective resume, archive clamp, decoder floor, and shadow recovery
  floor

Rename remaining `toast_stash_*` metrics only after generic path lands. Keep
toast-specific counters where behavior remains toast-only

## Removed mechanisms

Delete after journal path becomes authoritative:

- per-commit `ResolutionBundle`
- `FenceRequest` keyed by top xid
- bundle resolver lane, bundle loader, and bundle pruner
- stash discard-proof taxonomy
- live `relation_at` calls from row decode and detoast
- generation epoch and lazy descriptor eviction as correctness mechanisms
- volatile `prev_known` schema baseline
- route lookup inside decode worker
- serial ordered-event apply path

Keep shadow catalog client only for boundary capture, bootstrap validation, and
operational inspection

## Rejected

- stash-only publication fence, leaves live cold-cache race intact
- per-commit bundles, input closure expands to routing, Oracle, schema baseline,
  source identity, and lineage while adding another GC protocol
- top-xid bundle identity, xids wrap and do not identify source timeline or
  commit
- current-state lookup plus descriptor ownership, preserves wrong initial
  descriptor
- per-record shadow lockstep, correct but serializes decoder behind libpq and
  defeats queued pump
- catalog tuple reconstruction from WAL, duplicates MVCC visibility and
  version-sensitive catalog layout; boundary sampling lets PostgreSQL interpret
  its own catalog
- current PG catalog time travel, standby exposes moving current state and
  cannot retain arbitrary historic snapshots
- non-WAL mapping reload during streaming, route becomes wall-clock dependent
- attach route before ordered events apply, cannot represent auto-create or
  mid-xact opt-in
- weaken replay to dedup-sufficiency, equal `_lsn` with different bodies has no
  safe deterministic winner
- persist per-value Oracle output, creates row-volume durability and capture
  coupling
- discard never-visible generation based on marker inference, marker proves
  observation start but not identity or lineage
- independent artifact retention formulas, startup archive clamp already proves
  they diverge
- archive-only boundary hold, whole segment publication cannot stop at commit

## Future consumers

- rewrite FPI walk can add descriptor-backed tuples for generations never
  visible at commit and retire largest fail-closed class
- [two_phase_commit.md](two_phase_commit.md) can persist PREPARE buffers while
  reusing catalog history unchanged
- audit tooling can inspect relation shape and routing state at any retained WAL
  position
