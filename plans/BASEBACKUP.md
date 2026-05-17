# BASEBACKUP — base-backup-driven initial load (evaluation)

Evaluates using PG's `BASE_BACKUP` replication command (the protocol
wal-g and `pg_basebackup` ride on, exposed in
`~/s/wal-rs/src/pg/replication/base_backup.rs`) to bootstrap two
things walshadow does not currently solve well: shadow PG's data
directory, and ClickHouse's initial user-heap state. Status:
**evaluation, not committed work**. Outcome feeds a decision at the
top of [Phase 7](PLAN.md#phase-7--ch-native-emitter-via-clickhouse-c-rs)
or a dedicated new phase if BASE_BACKUP is the chosen bootstrap path.

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
  schema-only path has no fix. [PRE5 item 3 / PRE5b2](PRE5b2.md)
  seeds walshadow's runtime `CatalogTracker` from source so the WAL
  filter classifies records on the rotated filenode correctly, but
  does not touch shadow's on-disk `pg_filenode.map`; the
  ShadowCatalog-side skew remains.
* **Non-mapped catalogs** (`pg_depend`, `pg_index`, `pg_constraint`,
  …) have the same skew when source has rotated them via
  `VACUUM FULL` / `REINDEX` before walshadow attaches. The
  schema-only path has *no* fix for this either. [PRE5 item 2](PRE5.md#2-pg_class-heap-write-decoding)
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
  `pgsql_tmp/**`, `pg_serial/**`, `pg_snapshots/**`, and any
  user-heap file when running Use Case 1B. Shape:

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

Aggregate wal-rs lift for the recommended 1A→1B + 2B path:
EntryFilter trait + denylist constant + slot-create method. About
150 LOC of upstream change, no churn on existing call sites.

## Use case 1 — shadow PG data-dir bootstrap

Replaces the [Phase 3](PLAN.md#phase-3--shadow-pg-lifecycle) "initdb
+ apply_schema_dump" pair with a single "fetch BASE_BACKUP into
data_dir" pass. Pseudo-flow:

```
1. ReplicationConn::connect(source)
2. (slot already created or co-created in this session)
3. run_base_backup(conn, opts, tx)
4. for event in rx:
     Start(info)          -> backup_start_lsn = info.start_lsn
                             backup_end_lsn   = (filled by Finish)
     Archive { meta, body } -> tar-extract body
                               • data dir   -> shadow.data_dir
                               • tablespace -> shadow.data_dir/pg_tblspc/<oid>/
                                 (or skip, see "Disk budget")
                               filter out pg_replslot/, pg_stat_tmp/, temp_*
     Finish(info)         -> backup_end_lsn = info.end_lsn
5. shadow.enable_standby_recovery()
6. shadow.start()
7. shadow.wait_for_replay(backup_end_lsn, …)
```

### Wins over schema-only

* `pg_dump` privileges not required on source; replication grant is
  enough.
* Mapped catalog filenodes on shadow match source at start_lsn —
  closes the on-disk half of Gap 1 (the schema-only path leaves
  shadow's `pg_filenode.map` skewed). [PRE5b2](PRE5b2.md)'s
  `seed_from_source` stays load-bearing on the walshadow runtime
  tracker side; BASE_BACKUP populates shadow, not walshadow.
* Non-mapped catalog filenodes on shadow match source — closes a
  class of skew the schema-only path has no fix for.
* `clog/` and `pg_multixact/` are pre-populated; catalog tuple
  visibility resolves at start_lsn without a separate xact-status
  pre-load.
* `pg_control` carries source's start_lsn; shadow recovery anchors at
  the right LSN without a manual override.

### Disk budget

A schema-only shadow is MiB-scale. A BASE_BACKUP'd shadow is
source-scale. Two strategies:

**A. Keep user heap on disk.** Disk grows from MiB to full source
cluster size. Shadow's filter NOOPs all user-heap WAL, so user-heap
files stay frozen at start_lsn forever — wasted space scaling with
source's data footprint. Acceptable only as a transient state when
Use Case 2B (below) needs the heap to drive CH initial load.

**B. Strip user heap post-fetch.** After the fetch but before
`enable_standby_recovery`:

* Spin shadow read-only in normal mode (no `standby.signal`).
* Query `pg_class` over libpq: `SELECT oid, relfilenode, reltablespace,
  relkind FROM pg_class`.
* Cross-reference against the catalog whitelist
  (`oid < FirstNormalObjectId OR catalog-tracker hit`).
* Stop shadow; on the data dir, `unlink` each non-catalog
  `base/<dbid>/<filenode>` plus `_fsm`, `_vm`, segno suffixes.
* Touch sentinel `.walshadow-pruned` so re-runs detect prior state.

Risk: deleting a file that recovery later wants to touch. Mitigated
by walking only `oid < FirstNormalObjectId OR is-catalog-tracked` —
non-tracked filenodes by definition don't appear in heap-WAL the
filter keeps, so recovery never reaches for them.

Default: **B**, unless Use Case 2B is chosen, in which case A
persists for the duration of the CH initial-load pass and B runs
once CH has acked the snapshot.

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
`Shadow::restore_from_base_backup(conn, opts) -> Result<EndInfo>`
runs the pump and writes into `data_dir`. Roughly 200 LOC of
shadow-side glue plus 150 LOC of prune pass.

## Use case 2 — ClickHouse initial heap load

Three plausible consumers of the BASE_BACKUP output for the CH side.

### 2A. Page-walk decoder against the tar stream

For each tracked user relation, open its tar entry, walk 8 KiB pages,
iterate `ItemIdData` slots, decode `HeapTupleHeader` + payload
through the Phase 5 decoder. Emit
`Tuple { rfn, xid: xmin, op: Insert, new, old: None }` per visible
tuple. Visibility filter mirrors `HeapTupleSatisfiesMVCC` at
start_lsn against the CLOG/multixact files shipped in the same
BASE_BACKUP.

Pros: zero source-PG round-trips beyond BASE_BACKUP itself,
parallelisable across relations, the decoder code path reuses Phase
5's heap-tuple projection logic.

Cons:
* TOAST chunks live in a separate relation. Need to follow
  `va_valueid` references into the corresponding TOAST table's
  pages, also from the tar stream. Doable; adds a cross-archive
  lookup that the WAL decoder's same-stream TOAST handling
  ([Phase 6](PLAN.md#phase-6--toast-reassembly--xact-buffer))
  doesn't have.
* On-disk heap holds *uncommitted* tuples too (xmin in-progress at
  start_lsn). Backup's CLOG resolves them, but the post-backup WAL
  stream that lands the commit must be drained before deciding
  visibility — i.e. recovery must reach end_lsn first. Easier to
  let shadow do that work than to re-implement on top of the tar.
* Visibility code path diverges from the WAL decoder's "every kept
  record is a committed insert/update/delete" model. Two code paths,
  two regression surfaces.

### 2B. COPY from shadow once consistent

After shadow reaches end_lsn (and Use Case 1's prune step is
postponed), for each tracked relation issue
`COPY <rel> TO STDOUT WITH (FORMAT binary)` against shadow PG. Wire
the COPY output through the [Phase 7](PLAN.md#phase-7--ch-native-emitter-via-clickhouse-c-rs)
emitter to CH.

Pros: PG handles visibility, TOAST de-toasting, type formatting.
Shadow PG already speaks libpq via `ShadowCatalog`. One code path
per type (the existing decoder for the WAL stream re-used as the
CH-side projection).

Cons:
* Requires Use Case 1A (keep user heap on shadow) for the duration
  of initial load. Disk-budget cost is transient but real.
* COPY is sequential per relation; large relations bottleneck on
  shadow's single-connection throughput. Parallelisable across
  relations via N libpq sessions — `tokio_postgres` makes that
  cheap.

### 2C. Parallel COPY against source at a `pg_export_snapshot()` boundary

BASE_BACKUP's start phase runs a `START_BACKUP` checkpoint. Source
can export a snapshot id around the same checkpoint via
`pg_export_snapshot()`; walshadow opens N parallel libpq sessions
against source, each `SET TRANSACTION SNAPSHOT '…'`, and COPYs
disjoint relations in parallel. Independent of the BASE_BACKUP
fetch path on the wire.

Pros: maximum parallelism, source PG does visibility filtering, no
on-shadow heap concerns.

Cons:
* Source CPU/IO doubles during initial bootstrap (BASE_BACKUP + N
  COPYs).
* Snapshot export is scoped to a single source xact; the session
  must stay open for the entire COPY duration. Cancellation /
  reconnect handling gets gnarly.
* Two coordination points (BASE_BACKUP start + snapshot export
  start) need to line up; not impossible, but adds protocol surface.

### Recommendation per case

**2B for the first cut.** Pairs with Use Case 1A (keep user heap on
shadow during initial load, prune after CH has acked the snapshot).
Trades a transient large shadow-disk window for protocol simplicity
and code reuse. Revisit 2B vs 2C once shadow's COPY throughput is
measured on a real workload.

**2A** is the path that's mechanically cleanest but tangles the
decoder with snapshot-visibility logic that doesn't appear in the
WAL hot path. Defer indefinitely.

Trade-off table:

| 1A keep heap | 1B prune heap |
|---|---|
| 2A page-walk | works (over-storing on shadow) | works (independent of shadow disk) |
| 2B COPY from shadow | natural pairing | unavailable |
| 2C COPY from source | wastes shadow heap | works |

Recommended pairing: **1A + 2B → 1B (post-CH-ack prune)**.

## Sourcing the backup

Two paths, both already wired through wal-rs.

### Direct from source

walshadow opens a replication-mode `ReplicationConn` and runs
`BASE_BACKUP` against source itself. Trades source-cluster CPU/IO for
the backup duration (minutes for small clusters, hours for TB-scale).
Operator does not need any storage infrastructure beyond what source
already runs.

### From wal-g object store

When source is already backed up by wal-g (or any compatible tool
writing the same on-disk layout), walshadow uses
`wal_rs::pg::backup::fetch` to pull the latest backup from S3/GCS,
restore into shadow's `data_dir`, then walks the WAL archive forward
until end_lsn (or the live `START_REPLICATION` stream catches up).
Source cluster isn't touched.

Both paths land at the same on-disk shape and the same `EndInfo`
shape. One-call-site difference. Config:

```toml
[bootstrap]
mode = "base_backup"           # or "schema_only" (Phase 3 path)
base_backup_source = "direct"  # or "object_store"

[bootstrap.object_store]       # only when base_backup_source = "object_store"
storage_url = "s3://bucket/walg/"
```

Default greenfield: `direct`. Default brownfield with wal-g infra:
`object_store`.

## What this doesn't help with

* **Shadow PG bloat from continuous DDL.** Steady-state
  catalog-index growth is unchanged. [PLAN.md pitfall #3](PLAN.md#3-catalog-index-bloat)
  still stands.
* **Decode oracle (Phase 9).** Shadow still hosts typsend/typoutput;
  BASE_BACKUP doesn't change the oracle surface.
* **Filter logic.** Catalog whitelist computation, CRC rewrite, NOOP
  padding all unchanged.
* **[PRE5 item 2](PRE5.md#2-pg_class-heap-write-decoding) (`pg_class`
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
with the schema-only + [PRE5](PRE5.md) fixes path; BASE_BACKUP
arrives in v1.1 alongside the differential oracle.

Default: **A**. Initial CH load is a hard correctness gap, not an
operational nicety. Without it, [Phase 7](PLAN.md#phase-7--ch-native-emitter-via-clickhouse-c-rs)'s
emitter has nothing to write for any pre-existing source data.
[Phase 8](PLAN.md#phase-8--end-to-end-ddl-drill)'s E2E drill works
because the script `CREATE TABLE`s the destination from empty; real
CDC against a populated source needs initial-load before v1.0.

## Estimate

walshadow side:

```
src/shadow_basebackup.rs   new — ~300 LOC  BASE_BACKUP-from-source wrapper,
                                            tar → data_dir, denylist filter
src/shadow.rs              +~80         Shadow::restore_from_base_backup,
                                            prune pass, tablespace mapping
src/ch_initial_load.rs     new — ~250 LOC  per-relation COPY pump → emitter
src/source_feed.rs         +~30         slot create before BASE_BACKUP (uses
                                            wal-rs's new create_physical_slot)
tests/base_backup_e2e.rs   new — ~250 LOC  live source + shadow + CH
fixtures/wal/base_backup/   new           capture script + sentinel fixture
PLAN.md                    add Phase 6.5
BASEBACKUP.md              this doc
```

Total walshadow: ~660 LOC src + ~250 LOC tests.

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

Combined: ~800 LOC src + ~330 LOC tests across both repos, on top of
the ~1300+ LOC of base-backup machinery already in wal-rs.

## Recommendation

1. Adopt **Use Case 1 (BASE_BACKUP → shadow data dir)** as the
   default bootstrap, replacing `Shadow::apply_schema_dump` for any
   source reachable via REPLICATION. Keep `apply_schema_dump` as a
   fallback for environments without REPLICATION grants and for
   test scaffolding. Closes shadow's on-disk filenode skew for
   mapped *and* non-mapped catalogs in one step; the schema-only
   path leaves both unaddressed. [PRE5b2](PRE5b2.md)'s
   `seed_from_source` stays load-bearing on the daemon side
   (walshadow's runtime `CatalogTracker` is distinct from shadow's
   on-disk state — BASE_BACKUP does not seed it).
2. Adopt **Use Case 2B (COPY from shadow once consistent)** for CH
   initial load. Pair with Use Case 1A transiently (keep user heap
   on shadow until CH has acked the snapshot), then run the prune
   pass to drop back to MiB-scale shadow disk.
3. Defer **direct vs object-store** sourcing to a runtime config
   knob; both land at the same `data_dir` shape and `EndInfo` LSN
   pair.
4. Insert as **Phase 6.5** between [Phase 6](PLAN.md#phase-6--toast-reassembly--xact-buffer)
   and [Phase 7](PLAN.md#phase-7--ch-native-emitter-via-clickhouse-c-rs).
   Acceptance criterion (gates v1.0): a source pre-populated with
   `pgbench -i -s 10` is fully reflected in CH after a single
   `walshadow bootstrap` followed by steady-state replication,
   with row counts and checksums matching.
5. **Use Case 2A** is reframed in the counter-proposal below as the
   only path that keeps shadow at catalog-scale without source-side
   double-load. Visibility folds into the Phase 5/6 WAL hot path
   rather than diverging from it. Pick over 2C when source CPU is
   tight; pick over 2B when shadow disk is.

## Counter-proposal: shadow disk budget as hard constraint

The recommendation above (1A+2B) assumes shadow can absorb full
source disk transiently. That assumption holds only when source <
shadow VM disk. The stated design target — dinky shadow VM (e.g.
50 GB) against a TB-scale source — breaks it; 1A's prune pass cannot
run if the fetch exhausts disk before completing. This section flips
the default by shadow-disk budget.

### What forces user heap onto shadow disk

Only 2B. PG's `COPY ... TO STDOUT` reads from a running shadow's
on-disk heap, so 2B inherently requires 1A. Every other CH-side
option (2A page-walk, 2C source-side COPY) is indifferent to shadow
heap state. The tar bytes themselves already stream —
`BackupEvent::Archive.body` is an `AsyncRead` (see "What wal-rs
already gives us"). Nothing in the protocol forces landing user heap
to disk.

### Three regimes

| Path | Shadow heap on disk | Source CPU during bootstrap | Decoder LOC delta | When |
|---|---|---|---|---|
| 1A + 2B (doc default) | source-scale until pruned | 1× (BASE_BACKUP only) | baseline | source < shadow VM disk |
| 1B + 2C | catalog-scale (MiB) | 2× (BASE_BACKUP + N COPY) | ~0 | dinky shadow, source has CPU headroom |
| 1B + 2A | catalog-scale (MiB) | 1× (BASE_BACKUP only) | +~400 LOC | dinky shadow, tight source |

### Path 1B + 2C — source-side parallel COPY

Co-issue `pg_export_snapshot()` with BASE_BACKUP's start checkpoint.
Open N libpq sessions against source, each `SET TRANSACTION SNAPSHOT
'<id>'`, COPY disjoint relations directly into the Phase 7 emitter.
Shadow stays catalog-scale; decoder is unchanged from the
steady-state WAL path — source PG handles visibility, TOAST, type
formatting.

Cost: source CPU/IO doubles for the bootstrap window; snapshot
session must stay open for full COPY duration; cancellation /
reconnect handling on the snapshot session adds protocol surface.

### Path 1B + 2A — streamed page-walk

For each tracked user relation, the tar entry feeds a page-walk
decoder inline: walk 8 KiB pages, iterate `ItemIdData` slots, decode
`HeapTupleHeader` + payload through the Phase 5 decoder, emit
`Tuple { rfn, xid: xmin, op: Insert, new, old: None }` per visible
tuple, drop the bytes. Catalogs and recovery prereqs land on shadow
as in 1B; user heap bypasses disk entirely.

Engineering surface — re-examines the doc's original 2A cons:

* **Torn pages.** BASE_BACKUP captures pages mid-write; PG recovery
  applies WAL FPIs between `start_lsn` and `end_lsn` to make them
  consistent. Decoder buffers the page subset that WAL window
  touches (MiB out of GB/TB), applies FPIs in-memory, walks decoded.
  Sliver of recovery on the page subset that needs it, not a full
  re-implementation.

* **Catalog-before-heap ordering.** Resolving
  `base/<dbid>/<filenode>` to relation kind requires `pg_class`. Tar
  order is not catalogs-first by spec. Two-pass fetch: pass 1 lands
  catalogs to shadow data_dir, spin shadow read-only for `pg_class`
  query, pass 2 streams user heap with the filenode map in hand.
  Trivial on object-store; adds one BASE_BACKUP duration on
  direct-from-source.

* **TOAST cross-archive lookup.** Buffer `pg_toast_<relid>` chunks
  keyed by `(chunk_id, chunk_seq)`, drain on main-heap decode.
  Bounded per-relation; spill-to-scratch covers TOAST that exceeds
  RAM. Scratch is bounded per-relation, not source-scale.

* **Visibility folds into the WAL hot path.** Doc's original framing
  called 2A's visibility code path divergent. It need not be: emit
  tuples from start_lsn-state heap pages, then drive the Phase 5/6
  WAL decoder across the start→end window. WAL commits/aborts in
  that window run through the standard decoder; ReplacingMergeTree
  dedups via `_lsn`. Same record stream, same code path —
  BASE_BACKUP becomes "WAL prefix initialised from heap pages".

Code-size delta vs 2B:

```
+~400 LOC  in-memory FPI replay over buffered page subset
+~150 LOC  TOAST buffer + spill-to-scratch
+~100 LOC  start→end WAL replay driver as visibility end-cap
-~250 LOC  ch_initial_load.rs (no shadow-COPY path)
+~150 LOC  tests
```

Net: ~400 LOC src above the original 1A+2B estimate. Shadow stays
dinky-feasible without source-side double-load.

### Revised recommendation

Bootstrap path is conditional on shadow disk budget:

* **Source < shadow VM disk** (small source, beefy shadow, test
  harness): **1A + 2B**. Simplest decoder.
* **Source >> shadow VM disk, source has CPU headroom**
  (dinky-shadow default when operator's source is overprovisioned):
  **1B + 2C**. Catalog-scale shadow, zero new decoder code, pays
  source-side double-load.
* **Source >> shadow VM disk, source is tight on CPU**: **1B + 2A**.
  Catalog-scale shadow, single BASE_BACKUP on source, ~400 LOC of
  in-flight decoder work absorbs the cost on the walshadow side.

Ship `bootstrap.ch_initial_load = "shadow_copy" | "source_copy" |
"tar_decode"` as a config knob, default `"source_copy"` (1B+2C):
zero decoder cost, catalog-scale shadow, source-side load is
operator infrastructure that's already provisioned for any
production PG cluster. Operators flip to `"tar_decode"` (1B+2A) when
source CPU is constrained, or to `"shadow_copy"` (1A+2B) when shadow
disk is unconstrained and decoder simplicity dominates.

### What this reorders in the doc

* [PRE5b2](PRE5b2.md)'s `seed_from_source` is independent of the
  1x/2x choice and remains walshadow's source-of-truth for the
  runtime `CatalogTracker` under every bootstrap mode.
* Phase 6.5 acceptance criterion (`pgbench -i -s 10` round-trip)
  applies to whichever 2x path is configured; pick one for CI, gate
  the others as their own acceptance jobs.
* Estimate (line 568) is for 1A+2B. Add ~400 LOC src + ~150 LOC
  tests for 1B+2A; subtract ~250 LOC (`ch_initial_load.rs`) and add
  ~100 LOC (snapshot-session coordinator) for 1B+2C.
