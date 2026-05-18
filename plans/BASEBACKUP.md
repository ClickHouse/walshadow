# BASEBACKUP — base-backup-driven initial load (evaluation)

Evaluates using PG's `BASE_BACKUP` replication command (the protocol
wal-g and `pg_basebackup` ride on, exposed in
`~/s/wal-rs/src/pg/replication/base_backup.rs`) to bootstrap two
things walshadow does not currently solve well: shadow PG's data
directory, and ClickHouse's initial user-heap state. Status:
**evaluation, not committed work**. Outcome feeds a decision at the
top of [Phase 7](PLAN.md#phase-7--ch-native-emitter-via-clickhouse-c-rs)
or a dedicated new phase if BASE_BACKUP is the chosen bootstrap path.

**Hard constraint, applies to every option below.** Shadow PG is
catalog-only by design. Any path that lands source-scale user heap
on shadow's data dir (transiently or otherwise) is out of scope —
if walshadow wanted a full physical replica, the answer would be a
normal standby, not a shadow. Bytes pass *through* the daemon during
bootstrap; they do not settle on shadow.

**Sourcing bias.** wal-rs already speaks the wal-g object-store
layout (`pg/backup/fetch.rs`, `pg/wal/fetch.rs`). Production sources
typically already run wal-g for DR; pulling the consistent point
from S3 is preferred over a direct `BASE_BACKUP` against source, which
costs source CPU/IO. Direct-from-source stays as a fallback for
greenfield deployments without wal-g infra.

## Why ask the question now

Two unrelated gaps converge on the same protocol.

### Gap 1: shadow data-dir filenode skew

[Phase 3](PLAN.md#phase-3--shadow-pg-lifecycle) bootstraps shadow by
`initdb`-ing an empty cluster and applying a `pg_dump --schema-only`
payload via `Shadow::apply_schema_dump`. After that step shadow's
catalog filenodes are fresh-initdb assignments, *not* source's
filenodes.

* **Mapped catalogs** (`pg_class`, `pg_attribute`, `pg_type`,
  `pg_proc`, …) ride `pg_filenode.map`. Source's filenode for
  `pg_class` after a prior `VACUUM FULL pg_class` could be `>= 16384`;
  shadow's stays at its fresh initdb value. Heap-WAL records target
  source's filenode, recovery creates a fresh file at that path, and
  shadow's own `pg_class` (still at its initdb filenode) never sees
  the post-replay state — `ShadowCatalog` reads stale rows. The
  schema-only path has no fix. [PRE5 item 3 / PRE5b2](pre5/PRE5b2.md)
  seeds walshadow's runtime `CatalogTracker` from source so the WAL
  filter classifies records on the rotated filenode correctly, but
  does not touch shadow's on-disk `pg_filenode.map`; the
  ShadowCatalog-side skew remains.
* **Non-mapped catalogs** (`pg_depend`, `pg_index`, `pg_constraint`,
  …) have the same skew when source has rotated them via
  `VACUUM FULL` / `REINDEX` before walshadow attaches. The
  schema-only path has *no* fix for this either. [PRE5 item 2](pre5/PRE5.md#2-pg_class-heap-write-decoding)
  catches future rotations from WAL, but the initial state is left
  to luck: shadow's fresh `pg_depend` filenode happens to match
  source's only if source has not rotated it.

### Gap 2: ClickHouse initial load

`PLAN.md`'s pipeline assumes walshadow attaches at the source's tail
and CH starts empty. Existing rows on source are invisible to CH
unless an operator manually backfills. No phase addresses this gap.
Production CDC against a populated source needs an initial-snapshot
path.

### What BASE_BACKUP gives both gaps

`BASE_BACKUP` produces a consistent on-disk image of the source
cluster at `(start_lsn, end_lsn)`. That image carries:

* Catalog files at source's current filenodes — closes Gap 1 for
  mapped *and* non-mapped catalogs in one step.
* User-heap files for every relation — feeds Gap 2.
* `clog/`, `pg_multixact/`, `pg_filenode.map`, `pg_control` — all the
  recovery prerequisites shadow needs.
* Tablespace map listing user-tablespace `(oid, location)` pairs.

One protocol, two consumers.

## What wal-rs already gives us

`~/s/wal-rs/src/pg/replication/base_backup.rs` (929 LOC) implements
the issuer side:

* `run_base_backup(conn, BaseBackupOpts, events_tx)` — async task
  owning a `ReplicationConn`, issues
  `BASE_BACKUP (LABEL …, CHECKPOINT 'fast', WAL false,
  TABLESPACE_MAP true, MANIFEST 'no')` on PG ≥ 15 or the PG 14
  syntax form. Emits `BackupEvent::{Start, Archive, Finish}` over
  an mpsc channel.
* `StartInfo { start_lsn, timeline, tablespaces }` /
  `EndInfo { end_lsn, timeline }` — the LSN pair recovery needs.
* `BackupEvent::Archive { meta, body }` carries a per-archive
  `ChannelReader` implementing `AsyncRead`. Feeds straight into a
  tar parser without materialising the whole archive.
* Handles both PG14 (per-tablespace CopyOut sessions) and PG15+
  (singleton CopyOut with tagged CopyData `'d'`/`'p'`/`'n'`/`'m'`).

`~/s/wal-rs/src/pg/backup/fetch.rs` (401 LOC) implements the
object-store consumer side: walks delta chains, applies tablespace
symlinks (`restore_tablespace_symlinks` honouring caller-supplied
`tablespace_mappings`), unpacks tar parts into a target directory,
applies incremental-backup deltas via `apply_increment_in_place`.

`~/s/wal-rs/src/pg/backup/tar_streamer.rs` (1073 LOC) plus the rest
of `pg/backup/` (push, list, delete, increment) round out the
object-store side.

Reuse: ~1300+ LOC of protocol + recovery glue walshadow doesn't
need to write.

## Library surface to shape on wal-rs

wal-rs is co-evolved with walshadow, so the boundary is negotiable.
Walshadow's needs decompose into "already exposed", "small lift" and
"new contour". The ones below are scoped to land in wal-rs as part
of the Phase 6.5 work, not as separate prior phases.

### Already exposed and usable verbatim

* `pg::replication::base_backup::run_base_backup` plus
  `BaseBackupOpts`, `BackupEvent`, `StartInfo`, `EndInfo`,
  `ArchiveMeta`, `Tablespace`, `ChannelReader`. Direct-from-source
  path (Use Case 1 + Sourcing §1) drops in as-is.
* `pg::replication::conn::ReplicationConn::{connect, send_query,
  expect_copy_both_open, recv_message, send_copy_data}` — walshadow
  already drives these for `START_REPLICATION PHYSICAL`.
* `pg::backup::parse_pg_lsn`, the LSN parser walshadow reuses for
  `pg_last_wal_replay_lsn` strings.
* `pg::backup::fetch::handle_with_args` and `FetchArgs` for the
  object-store consumer path, as far as the existing "extract
  everything" semantics go.

### Small lifts (wal-rs additions, each <100 LOC)

* **Tar-entry path filter on `fetch::unpack_part`.** Currently
  `unpack_manual<R>` extracts every entry. Walshadow needs to skip
  `pg_replslot/**`, `pg_stat_tmp/**`, `pg_logical/**`,
  `pg_dynshmem/**`, `pg_subtrans/**`, `pg_notify/**`, `temp_*`,
  `pgsql_tmp/**`, `pg_serial/**`, `pg_snapshots/**`, **and every
  user-heap file**. The last bullet is load-bearing, not optional:
  it is what keeps shadow catalog-scale after a base-backup fetch.
  Shape:

  ```rust
  pub trait EntryFilter: Send + Sync {
      fn keep(&self, rel: &std::path::Path) -> EntryAction;
  }
  pub enum EntryAction { Keep, Skip }
  ```

  Wire through `FetchArgs::entry_filter: Option<Arc<dyn EntryFilter>>`
  and pass into `unpack_manual`. Default `None` preserves today's
  behaviour. Same callback shape reusable on the direct path
  (walshadow's own tar driver over `BackupEvent::Archive.body`).

* **Slot create co-located with BASE_BACKUP.** Walshadow needs to
  ensure a physical slot exists at `start_lsn`. Two options on
  source: operator pre-creates, or `CREATE_REPLICATION_SLOT
  <slot> PHYSICAL [RESERVE_WAL]` in the same session as
  BASE_BACKUP. Expose
  `ReplicationConn::create_physical_slot(slot, reserve_wal: bool)
  -> Result<Lsn>` returning the consistent point — wal-rs already
  speaks the protocol internally for its own slot management.

* **Path constants.** `pg::backup::SYSTEM_DIRS_DENYLIST: &[&str]`
  shipping the canonical list above. Both wal-rs's own restore and
  walshadow's denylist point at the same source-of-truth slice.

* **`BackupEvent::Archive` is currently the only "we have a stream"
  carrier.** That's fine for direct-from-source. For object-store
  with an inline filter, walshadow should be able to feed
  per-entry decisions without copying the full tar to disk first.
  `fetch::unpack_part` already streams; the filter knob above is
  enough.

### New contours (only if 2A or 2C land)

* **Tar-entry random-access reader.** Use Case 2A needs to find a
  specific `(dbid, filenode)` heap file within the BASE_BACKUP tar
  stream without unpacking the whole archive. Either: (a) build the
  index online during fetch (record `(path → offset, len)` for each
  entry) and re-open the local tar afterwards, or (b) extract every
  heap file to disk first and have 2A operate on disk. (b) is
  trivial and already what fetch does today.

* **Snapshot export / `pg_export_snapshot()`.** Use Case 2C needs a
  source-side snapshot id co-issued around BASE_BACKUP's start
  checkpoint. wal-rs would expose
  `ReplicationConn::start_backup_with_snapshot(label) -> (StartInfo,
  String)` returning the snapshot id alongside the start info, then
  let the BASE_BACKUP and the snapshot live on parallel sessions.
  Skip until 2C is chosen.

* **Live retention / archive lookahead.** If shadow stalls waiting
  for WAL between start_lsn and end_lsn (Pitfall #10), wal-rs's
  `pg::wal::fetch` could feed walshadow's `restore_command`-equivalent
  directly. Already exists for wal-g's own restore; thin shim to
  expose. Only needed if `START_REPLICATION` from start_lsn turns
  out to be insufficient in practice.

Aggregate wal-rs lift for the recommended filtered-fetch + 2C path:
EntryFilter trait + denylist constant + slot-create method. About
150 LOC of upstream change, no churn on existing call sites.

## Use case 1 — shadow PG data-dir bootstrap

Replaces the [Phase 3](PLAN.md#phase-3--shadow-pg-lifecycle) "initdb
+ apply_schema_dump" pair with a single "fetch filtered BASE_BACKUP
into data_dir" pass. The filter is what keeps the bootstrap inside
the catalog-only constraint at the top of this doc. Pseudo-flow:

```
1. Source = ReplicationConn::connect(source)            // direct
       OR  FetchArgs::object_store(...)                  // S3 via wal-rs
2. (slot already created or co-created on source PG)
3. drive run_base_backup / fetch::handle_with_args with
   entry_filter = CatalogOnly { whitelist: catalog_tracker_seed,
                                denylist: SYSTEM_DIRS_DENYLIST }
4. for event in rx:
     Start(info)          -> backup_start_lsn = info.start_lsn
     Archive { meta, body } -> tar-extract body with filter
                               • catalog file (oid < 16384 or in whitelist)
                                   -> shadow.data_dir / pg_tblspc/<oid>/
                               • user-heap file
                                   -> drop
                               • denylist dir (pg_replslot/, …)
                                   -> drop
     Finish(info)         -> backup_end_lsn = info.end_lsn
5. shadow.enable_standby_recovery()
6. shadow.start()
7. shadow.wait_for_replay(backup_end_lsn, …)
       // WAL catch-up sourced from object-store archive when
       // available, else from the live slot
```

### Wins over schema-only

* `pg_dump` privileges not required on source; replication grant is
  enough.
* Mapped catalog filenodes on shadow match source at start_lsn —
  closes the on-disk half of Gap 1 (the schema-only path leaves
  shadow's `pg_filenode.map` skewed). [PRE5b2](pre5/PRE5b2.md)'s
  `seed_from_source` stays load-bearing on the walshadow runtime
  tracker side; BASE_BACKUP populates shadow, not walshadow.
* Non-mapped catalog filenodes on shadow match source — closes a
  class of skew the schema-only path has no fix for.
* `clog/` and `pg_multixact/` are pre-populated; catalog tuple
  visibility resolves at start_lsn without a separate xact-status
  pre-load.
* `pg_control` carries source's start_lsn; shadow recovery anchors at
  the right LSN without a manual override.

### Disk discipline

Shadow stays catalog-scale by construction. The `EntryFilter` lift
above is applied during tar extraction: every `base/<dbid>/<filenode>`
entry whose filenode is not in the catalog whitelist is dropped on
the floor before it hits disk. User-heap bytes never settle on
shadow.

Recovery between `start_lsn` and `end_lsn` references no user-heap
files because walshadow's WAL filter already drops user-heap records
in steady state — the same filter runs over the catch-up window,
fed via `restore_command` from walshadow's filtered segment
directory. Source-side `vacuum_defer_cleanup_age`-style hazards do
not arise because shadow never reaches for those filenodes.

Whitelist input is the same `CatalogTracker` seed that PRE5b2 already
needs (`oid < FirstNormalObjectId OR catalog-tracker hit`); the fetch
side reuses it as `EntryFilter::keep(&path)`.

No post-fetch prune. No transient source-scale window. If the
extraction can't proceed without writing user heap (e.g. because
`pg_filenode.map` recovery wants a heap page touched before catalog
visibility resolves — currently no known case), the fetch fails
loudly rather than degrading silently to a source-scale shadow.

### LSN alignment

BASE_BACKUP returns `(start_lsn, end_lsn)`. PG's recovery rule:
standby must replay from start_lsn up through end_lsn to reach
consistency. After that point shadow accepts queries.

walshadow's `SourceFeed` needs to issue
`START_REPLICATION PHYSICAL <slot> <start_lsn>` so the filter sees
the same WAL shadow replays. Slot must exist on source by the time
BASE_BACKUP runs (operator pre-creates, or wal-rs's
`CREATE_REPLICATION_SLOT` co-creates in the same session before
BASE_BACKUP).

CH initial-load tagging: every tuple emitted from the snapshot
carries `_lsn = end_lsn`. Subsequent WAL-replayed tuples carry their
record's source LSN, which is `> end_lsn` by construction. CH
`ReplacingMergeTree(_lsn)` dedup then resolves any same-PK collision
in favour of the WAL-derived row.

### Migration shape

`Shadow::initdb` and `Shadow::apply_schema_dump` stay — useful for
the test scaffolding under `tests/shadow_lifecycle.rs` and for
environments without REPLICATION grant on source. New
`Shadow::restore_from_base_backup(source, opts) -> Result<EndInfo>`
runs the pump and writes the *filtered* contents into `data_dir`,
where `source` is either a live `ReplicationConn` (direct) or an
object-store `FetchArgs` (S3-via-wal-rs). Both feed
`wal_rs::pg::backup::fetch::unpack_part` with the catalog-only
`EntryFilter`. Roughly 200 LOC of shadow-side glue.

## Use case 2 — ClickHouse initial heap load

Two consumers of the BASE_BACKUP output for the CH side. A third
(COPY from a shadow that holds user heap) is incompatible with the
catalog-only constraint above and is not listed.

### 2A. Page-walk decoder against the tar stream

For each tracked user relation, open its tar entry as it flows past
the daemon (S3 read or direct CopyData), walk 8 KiB pages, iterate
`ItemIdData` slots, decode `HeapTupleHeader` + payload through the
Phase 5 decoder. Emit
`Tuple { rfn, xid: xmin, op: Insert, new, old: None }` per visible
tuple. Visibility folds into the WAL hot path: emit from
`start_lsn`-state pages, then drive the Phase 5/6 WAL decoder
across the `start → end` window. WAL commits/aborts in that window
run through the standard decoder; ReplacingMergeTree dedups via
`_lsn`. Bytes flow daemon → CH; nothing lands on shadow.

Pros: zero source-PG round-trips beyond BASE_BACKUP itself (and
zero source round-trips at all if the backup is read from S3),
parallelisable across relations, reuses the Phase 5 heap-tuple
projection logic, no shadow-disk pressure.

Cons:
* TOAST chunks live in a separate relation. Need to follow
  `va_valueid` references into the corresponding TOAST table's
  pages, also from the tar stream. Buffer `pg_toast_<relid>` chunks
  keyed by `(chunk_id, chunk_seq)`, drain on main-heap decode.
  Bounded per-relation; spill-to-scratch covers TOAST that exceeds
  RAM. Cross-archive lookup the WAL decoder's same-stream TOAST
  handling ([Phase 6](PLAN.md#phase-6--toast-reassembly--xact-buffer))
  doesn't have.
* Torn pages. BASE_BACKUP captures pages mid-write; PG recovery
  normally applies WAL FPIs between `start_lsn` and `end_lsn` to
  make them consistent. The page-walk decoder needs to buffer the
  page subset that the WAL window touches (MiB out of GB/TB),
  apply FPIs in-memory, walk the patched copy. Sliver of recovery
  on the page subset that needs it, not a full re-implementation.
* Catalog-before-heap ordering. Resolving `base/<dbid>/<filenode>`
  to a relation kind requires `pg_class`. Tar order is not
  catalogs-first by spec. Two-pass fetch: pass 1 lands catalogs
  (filtered) to shadow data_dir, spin shadow read-only for
  `pg_class` query, pass 2 streams user heap with the filenode map
  in hand. Trivial when sourcing from S3; on direct-from-source
  it costs one extra BASE_BACKUP duration.

### 2C. Parallel COPY against source at a `pg_export_snapshot()` boundary

BASE_BACKUP's start phase runs a `START_BACKUP` checkpoint. Source
can export a snapshot id around the same checkpoint via
`pg_export_snapshot()`; walshadow opens N parallel libpq sessions
against source, each `SET TRANSACTION SNAPSHOT '…'`, and COPYs
disjoint relations in parallel. Independent of the BASE_BACKUP
fetch path on the wire. Shadow is untouched.

Pros: maximum parallelism, source PG does visibility filtering, no
on-shadow heap concerns, zero new decoder code (existing Phase 7
emitter consumes the COPY stream the same way it consumes WAL-
derived tuples).

Cons:
* Source CPU/IO doubles during initial bootstrap (BASE_BACKUP + N
  COPYs). Counter: the BASE_BACKUP half can come from S3 instead
  of source, removing one of the two pressures — net source load
  is just the N parallel COPYs.
* Snapshot export is scoped to a single source xact; the session
  must stay open for the entire COPY duration. Cancellation /
  reconnect handling gets gnarly.
* Two coordination points (BASE_BACKUP start + snapshot export
  start) need to line up. Resolvable but adds protocol surface.

### Recommendation per case

Bootstrap pairs with the catalog-only Use Case 1 unconditionally.
For the CH side:

* **2C default.** Zero new decoder code. With BASE_BACKUP sourced
  from S3 the source-CPU concern collapses to "source runs the N
  parallel COPYs", which is already the cost shape of any pg_dump-
  parallel bootstrap. Pairs naturally with `pg_export_snapshot()`
  semantics operators already understand.
* **2A** when the source cluster cannot afford the N parallel COPYs
  (locked under tight QoS, or replicas-only access). Pays ~400 LOC
  of in-flight page-walk + FPI replay + cross-archive TOAST
  bookkeeping on the walshadow side to spare source.
* **Shadow-as-source path explicitly out.** Any framing that has
  shadow holding user heap so a COPY can run off shadow violates
  the catalog-only constraint at the top of this doc.

Trade-off table (only viable cells):

| Use Case 1 (filtered fetch, catalog-only on shadow) |
|---|
| 2A page-walk: works; ~400 LOC decoder lift; zero source COPY load |
| 2C source-side COPY: works; zero decoder lift; N source connections |

Recommended pairing: **filtered Use Case 1 + 2C**, fallback to **2A**
when source CPU is constrained.

## Sourcing the backup

Two paths, both already wired through wal-rs. Preference is
**object-store first** — most production sources already run wal-g
for DR, and walshadow inherits the existing backup pipeline for
free.

### From wal-g object store (preferred)

When source is already backed up by wal-g (or any compatible tool
writing the same on-disk layout), walshadow uses
`wal_rs::pg::backup::fetch` to pull the latest backup from S3/GCS,
runs filtered extraction (`EntryFilter` drops user heap + denylist
dirs) into shadow's `data_dir`, then drives shadow's recovery using
WAL pulled from the same archive via `wal_rs::pg::wal::fetch` until
`end_lsn` is reached (or the live `START_REPLICATION` stream catches
up). Source cluster isn't touched for either the backup payload or
the WAL catch-up window.

### Direct from source (fallback)

For greenfield deployments without wal-g infra, walshadow opens a
replication-mode `ReplicationConn` and runs `BASE_BACKUP` against
source itself. Trades source-cluster CPU/IO for the backup duration
(minutes for small clusters, hours for TB-scale). Same filtered
extraction on the receiving side; user heap streams in over the
wire and is dropped before disk.

Both paths land at the same on-disk shape (catalog-only) and the
same `EndInfo` LSN pair. One-call-site difference. Config:

```toml
[bootstrap]
mode = "base_backup"                # or "schema_only" (Phase 3 path)
base_backup_source = "object_store" # or "direct"

[bootstrap.object_store]            # when base_backup_source = "object_store"
storage_url = "s3://bucket/walg/"
```

Default: `object_store` when storage credentials are configured,
`direct` otherwise.

## What this doesn't help with

* **Shadow PG bloat from continuous DDL.** Steady-state
  catalog-index growth is unchanged. [PLAN.md pitfall #3](PLAN.md#3-catalog-index-bloat)
  still stands.
* **Decode oracle (Phase 9).** Shadow still hosts typsend/typoutput;
  BASE_BACKUP doesn't change the oracle surface.
* **Filter logic.** Catalog whitelist computation, CRC rewrite, NOOP
  padding all unchanged.
* **[PRE5 item 2](pre5/PRE5.md#2-pg_class-heap-write-decoding) (`pg_class`
  heap-write decoder).** Still needed for steady-state VACUUM FULL
  detection on non-mapped catalogs. BASE_BACKUP fixes only the
  *initial* skew; ongoing rotations after attach remain a
  WAL-stream concern.
* **Two-phase commit recovery.** Backup snapshot at start_lsn may
  straddle a `PREPARE TRANSACTION`; `pg_twophase/` files come down
  with the tar and feed shadow's prepared-xact table. Decoder's
  two-phase buffer ([PLAN.md pitfall #6](PLAN.md#6-two-phase-commit))
  sees the same xact later via COMMIT PREPARED — unchanged.
* **Source primary failover.** [PLAN.md pitfall #9](PLAN.md#9-source-primary-failover).
  BASE_BACKUP buys a fresh starting LSN if walshadow has to
  re-bootstrap against a promoted standby; doesn't avoid the
  re-bootstrap.

## Pitfalls

### 1. Tablespace symlinks

User tablespaces live at `data_dir/pg_tblspc/<oid>` as symlinks to
operator-chosen locations on source. BASE_BACKUP emits a
`tablespace_map` file listing `(oid, location)`.
`wal_rs::pg::backup::fetch::restore_tablespace_symlinks` handles
this with a `tablespace_mappings: Vec<(from, to)>` knob. Shadow runs
in a sandboxed data root that almost certainly does not have
source's `/srv/pg/ts/…` paths available, so mappings are mandatory
for any source using user tablespaces. Surface as
`Shadow::tablespace_mapping(from, to)` builder method.

### 2. `pg_control` write ordering

BASE_BACKUP's `pg_control.tar` is sorted last in
`wal_rs::pg::backup::fetch::list_tar_parts` so an interrupted fetch
can't leave a stale control file behind. Same discipline needed in
the direct-from-source flow: defer writing `pg_control` until every
other archive is durable. wal-rs's object-store fetch enforces this;
the direct flow needs to mirror it explicitly.

### 3. System directory denylist

Source's replication slots, stats files, logical-decoding state,
temp scratch, dynshmem segments, and similar runtime-state
directories all ship in the tar. None are useful on shadow; the
replication-slot copy is actively harmful (shadow would advertise
itself as the source of the same slot, confusing monitoring).
Canonical denylist:
`pg_replslot/`, `pg_stat_tmp/`, `pg_logical/`, `pg_dynshmem/`,
`pg_subtrans/`, `pg_notify/`, `pg_serial/`, `pg_snapshots/`,
`pgsql_tmp/`, `temp_*`. Surface as the wal-rs
`SYSTEM_DIRS_DENYLIST` constant (see "Library surface") so both
wal-rs's own restore and walshadow's filter pull from the same
slice.

### 4. (folded into Pitfall 3)

### 5. WAL during backup

`BASE_BACKUP (WAL false)` skips the included WAL stream and trusts
the operator to provide WAL between start_lsn and end_lsn through
the archive (or replication). walshadow always streams from source
via `START_REPLICATION PHYSICAL`, so this is fine — provided the
slot is created and positioned at-or-before start_lsn *before*
BASE_BACKUP issues, so source retains WAL from start_lsn forward.

If `WAL true` is preferred (carry needed WAL in the BASE_BACKUP tar
itself, no slot needed during bootstrap), the singleton CopyOut on
PG15+ emits a `pg_wal.tar` archive that needs to be extracted into
shadow's `pg_wal/` before recovery starts. Two new code paths; one
extra archive to handle. Skip unless the slot-management cost
actually shows up.

### 6. CHECKPOINT 'fast' vs 'spread'

`fast_checkpoint: true` forces an immediate checkpoint; backup_lsn
is closer to "now" but source eats a checkpoint storm. Acceptable
for one-shot bootstrap. Default `spread` if walshadow has to coexist
with a busy source on a tight degradation budget. Explicit knob in
`BaseBackupOpts`.

### 7. PG version skew

BASE_BACKUP protocol differs between PG 14 and PG 15+. wal-rs
handles both branches; walshadow inherits. Shadow's PG major still
must match source per [PLAN.md pitfall #7](PLAN.md#7-shadow-pg-version-skew).
Walshadow already rejects source < PG 16; both branches of wal-rs's
BASE_BACKUP code are exercised by PG 16+ (PG 15 in tolerated mode).

### 8. Source-side privileges

Direct BASE_BACKUP needs `REPLICATION` plus
`pg_read_server_files`-equivalent for `pg_basebackup`-style runs.
Object-store path needs neither — pre-existing wal-g job has them,
walshadow only reads from S3/GCS. Surface clearly in operator docs:
"BASE_BACKUP from source needs REPLICATION; from object store needs
storage credentials".

### 9. Interrupted bootstrap

Fetch crashes mid-tar. wal-rs's object-store fetch path writes
straight to disk; partial state on disk is unrecoverable in place
(no resume of an interrupted tar). Restart logic for walshadow:

* `.walshadow-backup-end-lsn` marker written atomically after the
  *last* archive (including `pg_control`) lands.
* On restart: marker present → skip BASE_BACKUP, resume at standby
  start. Marker absent → `rm -rf` data_dir, re-fetch.
* No partial-tar resumption attempted.

### 10. Backup-end-LSN replay deadline

Shadow has to replay up through end_lsn before accepting queries.
If the slot doesn't carry WAL up to end_lsn yet (BASE_BACKUP finished
faster than the live stream has caught up to its own end_lsn —
typical for a busy source), shadow stalls in recovery waiting for
the next segment. Surface as a `wait_for_replay(end_lsn, deadline)`
gate; deadline configurable. Same primitive Phase 3 already ships.

## Sequencing in PLAN.md

Two reasonable insertion points.

**A. New Phase 6.5 between TOAST/xact and CH emitter.** BASE_BACKUP
lands as a Phase-7-friendly precondition for "first row in CH".
[Phase 7](PLAN.md#phase-7--ch-native-emitter-via-clickhouse-c-rs)
then takes initial-load output as the first INSERT block per
relation, then switches to WAL-driven flushes.

**B. New Phase 11 after Phase 10 (operational).** Treat BASE_BACKUP
as production-ready bootstrap rather than v1 hot path. v1.0 ships
with the schema-only + [PRE5](pre5/PRE5.md) fixes path; BASE_BACKUP
arrives in v1.1 alongside the differential oracle.

Default: **A**. Initial CH load is a hard correctness gap, not an
operational nicety. Without it, [Phase 7](PLAN.md#phase-7--ch-native-emitter-via-clickhouse-c-rs)'s
emitter has nothing to write for any pre-existing source data.
[Phase 8](PLAN.md#phase-8--end-to-end-ddl-drill)'s E2E drill works
because the script `CREATE TABLE`s the destination from empty; real
CDC against a populated source needs initial-load before v1.0.

## Estimate

walshadow side (default: filtered fetch + 2C COPY):

```
src/shadow_basebackup.rs   new — ~300 LOC  BASE_BACKUP pump (direct +
                                            object-store), filtered extraction,
                                            denylist + heap-skip EntryFilter
src/shadow.rs              +~60         Shadow::restore_from_base_backup,
                                            tablespace mapping
src/ch_initial_load.rs     new — ~250 LOC  snapshot-export coordinator +
                                            per-relation COPY against source
src/source_feed.rs         +~30         slot create before BASE_BACKUP (uses
                                            wal-rs's new create_physical_slot)
tests/base_backup_e2e.rs   new — ~250 LOC  live source + shadow + CH
fixtures/wal/base_backup/   new           capture script + sentinel fixture
PLAN.md                    add Phase 6.5
BASEBACKUP.md              this doc
```

Total walshadow (2C default): ~640 LOC src + ~250 LOC tests.

For 2A fallback (constrained source), add:

```
+~400 LOC  in-memory FPI replay over buffered page subset
+~150 LOC  TOAST buffer + spill-to-scratch for cross-archive lookup
+~100 LOC  start→end WAL replay driver as visibility end-cap
+~150 LOC  tests
```

2A adds ~650 LOC behind a config knob; only paid by deployments that
flip to `bootstrap.ch_initial_load = "tar_decode"`.

wal-rs side (coupled upstream, lands first or co-lands):

```
pg/backup/mod.rs           +~30   SYSTEM_DIRS_DENYLIST constant + EntryAction enum
pg/backup/fetch.rs         +~50   EntryFilter wiring through FetchArgs and
                                  unpack_manual
pg/replication/conn.rs     +~60   create_physical_slot(slot, reserve_wal) →
                                  Lsn return
tests                      +~80   filter denial behaviour, slot-create round-trip
```

Total wal-rs: ~140 LOC src + ~80 LOC tests. Existing call sites
untouched (filter defaults to `None`, slot-create is new method).

Combined (2C default): ~780 LOC src + ~330 LOC tests across both
repos, on top of the ~1300+ LOC of base-backup machinery already in
wal-rs. 2A adds ~650 LOC src + ~150 LOC tests if/when enabled.

## Recommendation

1. Adopt **Use Case 1 (filtered BASE_BACKUP → shadow data dir)** as
   the default bootstrap, replacing `Shadow::apply_schema_dump` for
   any source reachable via REPLICATION *or* via a wal-g object
   store. Keep `apply_schema_dump` as a fallback for environments
   with neither access path. The filtered extraction (heap-skip
   `EntryFilter` plus the system-dirs denylist) closes shadow's
   on-disk filenode skew for mapped *and* non-mapped catalogs in
   one step without growing shadow past catalog-scale.
   [PRE5b2](pre5/PRE5b2.md)'s `seed_from_source` stays load-bearing
   on the daemon side (walshadow's runtime `CatalogTracker` is
   distinct from shadow's on-disk state — BASE_BACKUP does not seed
   it).
2. Adopt **Use Case 2C (parallel source-side COPY at a
   `pg_export_snapshot()` boundary)** for CH initial load by
   default. Zero new decoder code; pairs naturally with the
   `pg_export_snapshot()` LSN that bridges into the WAL pump's
   `--start-lsn`. Holds shadow at catalog-scale.
3. Provide **Use Case 2A (page-walk over the BASE_BACKUP tar
   stream)** as an opt-in alternative for deployments where source
   CPU is constrained and an extra ~650 LOC of in-flight decoder
   work on the walshadow side is preferable. Especially attractive
   when sourcing from S3, since the tar stream then incurs zero
   source-side load.
4. Prefer **object-store** sourcing whenever wal-g (or compatible)
   infra exists; fall back to **direct** for greenfield clusters.
   Same `data_dir` shape, same `EndInfo` LSN pair.
5. Insert as **Phase 6.5** between [Phase 6](PLAN.md#phase-6--toast-reassembly--xact-buffer)
   and [Phase 7](PLAN.md#phase-7--ch-native-emitter-via-clickhouse-c-rs).
   Acceptance criterion (gates v1.0): a source pre-populated with
   `pgbench -i -s 10` is fully reflected in CH after a single
   `walshadow bootstrap` followed by steady-state replication, with
   row counts and checksums matching, and shadow `data_dir` stays
   under a configurable ceiling (catalog-scale, MiB-order) across
   the whole bootstrap.

Config knob:

```toml
[bootstrap]
mode = "base_backup"                # or "schema_only" (Phase 3 path)
base_backup_source = "object_store" # or "direct"
ch_initial_load = "source_copy"     # or "tar_decode"
```

Defaults: `object_store` + `source_copy` when wal-g infra is
configured; `direct` + `source_copy` for greenfield; flip
`ch_initial_load = "tar_decode"` when source CPU during bootstrap
is the binding cost.

### What this leaves out

* **Shadow as a COPY source.** Any framing where shadow holds user
  heap so a `COPY ... TO STDOUT` can run off shadow violates the
  catalog-only constraint at the top of the doc. Removed
  unconditionally.
* **Post-fetch prune passes.** Filtered extraction makes prune
  obsolete; nothing to clean up.
* **Replicating the full source cluster.** If a deployment ever
  wants that shape, the right tool is a normal physical standby,
  not walshadow.

### Notes carrying forward

* [PRE5b2](pre5/PRE5b2.md)'s `seed_from_source` is independent of
  the 2x choice and remains walshadow's source-of-truth for the
  runtime `CatalogTracker` under every bootstrap mode.
* Phase 6.5 acceptance criterion applies to whichever 2x path is
  configured; pick one for CI (default `source_copy`), gate the
  other as its own acceptance job.
