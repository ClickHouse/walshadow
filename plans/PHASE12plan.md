# PHASE12plan — backfill bridge via file-streaming source trait

Concrete plan for Phase 12. Supersedes the
[PLAN.md §Phase 12](PLAN.md#phase-12--backfill-bridge) two-shape sketch
and the [PHASE12experiments](PHASE12experiments.md) Default-greenfield
(H+E+G) recommendation. Flips the default from 2C (per-relation COPY)
to **2A (BASE_BACKUP page-walk)**: the rationale, restated, is that
2A's deferred work (FPI replay, TOAST chunk decode, on-disk page →
tuple projection) is **WAL-decoder work walshadow has to land anyway**,
not 2A-specific cost. 2C's per-OID binary-COPY adapter list is the
only net-new codec, and it has to be carried forever. One decoder vs
two.

Co-evolves with `~/s/wal-rs/` Phase 6.5 surface
(`EntryFilter`/`TapSink` at `pg/backup/mod.rs`,
`FetchArgs::entry_filter`/`entry_tap` at `pg/backup/fetch.rs`,
`run_base_backup` at `pg/replication/base_backup.rs`). That surface is
load-bearing for both source impls but lives **below** the trait
defined here.

## Goal

One file-level streaming abstraction over base-backup bytes that:

1. Emits per-file events (`begin`/`chunk`/`end`) with cluster-relative
   paths, independent of wire encoding.
2. Has two production impls today (Direct, ObjectStore) and a future
   third (LocalDir) that drops in without trait change.
3. Pipes user-heap file bodies into the **same** Phase 5 heap-tuple
   decoder the WAL hot path uses — zero codec drift across the
   backfill / WAL boundary, by construction.
4. Composes with existing walshadow Phase 3-7 lifecycle: catalog files
   land on shadow's `data_dir`; user-heap rows flow through `Emitter`
   to CH; `_lsn` tagging meshes with `ReplacingMergeTree(_lsn)` dedup
   against the WAL tail.

## Non-goals for V1

- **FPI replay over backup pages.** WAL `[start_lsn, end_lsn]` carries
  FPIs that PG recovery normally applies to torn / mid-write pages
  captured by BASE_BACKUP. Skipped in V1; the cost is a brief window
  of duplicate rows when a WAL record updates a backup-time tuple
  before `end_lsn`. `ReplacingMergeTree(_lsn)` collapses these.
  Documented as accepted, not unbounded — pages with
  `pd_lsn >= start_lsn` need no patch by construction. WAL-side FPI
  replay stays required (already in [`fpi.rs`](../src/fpi.rs)).
- **Cross-archive TOAST reassembly.** V1 decodes main-heap tuples
  whose columns are entirely inline (no `va_external`). TOAST-spilled
  columns surface as `ColumnValue::PgPending` — same shape Phase 9
  already produces. Full chunk-and-assemble decoder lands when WAL
  records targeting `pg_toast_<relid>` need the same projection (that
  work is WAL-shared, not 2A-specific).
- **LocalDir source.** Trait shape allows it; impl deferred until a
  use case demands.
- **Resume mid-bootstrap.** V1 is single-shot per
  [PLAN.md §Phase 12 out-of-scope](PLAN.md#phase-12--backfill-bridge).
  Crash semantics: wipe `data_dir`, wipe CH dest, retry. Per-relation
  / per-chunk resume is the PHASE12 follow-up
  ([PHASE12experiments](PHASE12experiments.md) G + E synthesis).
- **2C orchestrator as a fallback path.** Per-relation COPY is the
  PHASE12experiments default but the codec-drift cost is structural.
  V1 ships 2A only; 2C lives only as a "constrained-operator opt-in"
  follow-up if measurement demands.

## Trait surface — file-level, above tar

Lives in `src/backup_source.rs` (new). Trait names + types:

```rust
/// Filesystem object kind. Tar-driven sources translate tar entry
/// types to this; LocalDir reads from inode metadata; the trait does
/// not expose tar at all.
pub enum FileKind {
    File,
    Dir,
    Symlink { target: PathBuf },
}

/// One file's metadata. Path is cluster-relative — never absolute,
/// never contains parent-dir traversals (sources sanitise before
/// emitting).
pub struct FileMeta {
    pub path: PathBuf,    // "base/5/16400", "global/1259", "pg_control",
                          // "pg_tblspc/16384" (Symlink), …
    pub size: u64,
    pub mode: u32,
    pub kind: FileKind,
}

/// Sink decision per file at `begin()` time.
pub enum FileAction {
    /// Source writes File / Dir / Symlink under caller-supplied
    /// `data_dir`. `chunk()` is not called.
    Keep,
    /// Drop body unread (or in the Dir / Symlink case, don't create
    /// anything). `chunk()` is not called.
    Skip,
    /// `chunk()` fires with body bytes; nothing lands on disk under
    /// `data_dir`. Symlinks tap as zero-length. Dirs tap as empty.
    Tap,
}

pub trait BackupSink: Send {
    /// Fired once before any file event. Carries source-supplied
    /// invariants (`start_lsn`, timeline, tablespace list). Sink uses
    /// this to prepare any pre-pump state — e.g. logging the
    /// consistent-point LSN before any `_lsn`-tagged row ships.
    fn start(&mut self, _info: &StartInfo) -> io::Result<()> { Ok(()) }

    /// Returns the routing decision for this file. For `Tap`, the
    /// sink will receive subsequent `chunk()` calls. For `Keep` the
    /// source writes through to `data_dir`; the sink still sees
    /// `end()` so it can update bookkeeping.
    fn begin(&mut self, meta: &FileMeta) -> io::Result<FileAction>;

    /// Body bytes. Called zero or more times between `begin()`
    /// returning `Tap` and `end()`. Bytes are presented in source
    /// order; sink owns any per-page / per-chunk framing.
    fn chunk(&mut self, bytes: &[u8]) -> io::Result<()>;

    /// Closes the current file. Always fires once per `begin()`,
    /// regardless of action.
    fn end(&mut self) -> io::Result<()>;

    /// Fired once after the last file event. `end_lsn` is the
    /// recovery target the WAL pump uses for handoff.
    fn finish(&mut self, _info: &EndInfo) -> io::Result<()> { Ok(()) }
}

pub trait BackupSource {
    /// Pump every file in the backup through `sink`. `data_dir` is
    /// where `Keep`d bodies land. Returns the (start, end) LSN pair
    /// the caller uses for shadow recovery + WAL-pump handoff.
    async fn run(
        self,
        data_dir: &Path,
        sink: Arc<Mutex<dyn BackupSink>>,
    ) -> Result<(StartInfo, EndInfo)>;
}
```

`StartInfo` / `EndInfo` mirror wal-rs's
`pg::replication::base_backup::{StartInfo, EndInfo}` so callers that
already speak wal-rs's types don't translate:

```rust
pub struct StartInfo {
    pub start_lsn: u64,
    pub timeline: u32,
    pub tablespaces: Vec<Tablespace>,    // re-exported from wal-rs
}

pub struct EndInfo {
    pub end_lsn: u64,
    pub timeline: u32,
}
```

### Source contracts

Every `BackupSource` impl guarantees:

1. **`start()` fires before any `begin()`** with `start_lsn` and the
   tablespace list.
2. **Symlinks for user tablespaces emit before any file under their
   `pg_tblspc/<oid>/` subtree.** This lets sinks materialise the
   symlink target before tar entries that ride through the link land.
   (Option (b) from the design conversation: symlinks are files of
   `kind = Symlink`, not a parallel orchestrator concern.)
3. **`pg_control` emits last.** PG's BASE_BACKUP protocol natively
   ships pg_control after stop_backup completes; wal-rs's
   `list_tar_parts` (at `pg/backup/fetch.rs:290`) sorts
   `pg_control.tar` last. Both impls honour this without extra logic;
   the trait pins it so future impls (LocalDir) inherit the contract.
4. **`finish()` fires after the last `end()`** with `end_lsn`.
5. **Paths are cluster-relative**, sanitised against `..` /
   absolute-root traversal at the source impl boundary.

### Tap & Keep semantics — who writes to disk

`Keep` makes the **source** write the body under `data_dir`. Source
impls share a helper `write_kept(meta, body_reader, data_dir)` so the
disk-landing logic exists once. This puts symlink creation,
permissions, and dir handling out of the sink trait and keeps sinks
focused on routing + decode.

`Tap` flips the polarity: bytes flow to the sink, nothing to disk.
The sink owns whatever buffering / page-walk machinery it needs.

`Skip` returns the source to "drain body unread" — exactly
`wal_rs::pg::backup::EntryAction::Skip`'s semantics at the underlying
tar layer.

Decisions are per-`begin()`. Same-named files in different archives
(e.g. user-heap segments at `.0`, `.1`) make independent decisions.

## Source impl: Direct

`DirectSource` wraps wal-rs's `run_base_backup` to drive the trait.
Single tokio task drives the `BackupEvent` mpsc; each `Archive` body
is tar-parsed in-place; per tar entry the source calls
`sink.begin()` + (for Keep) writes to `data_dir` + (for Tap) replays
chunks through `sink.chunk()`.

```text
ReplicationConn ──BASE_BACKUP──> wal-rs run_base_backup
                                    │
                                    ▼ BackupEvent mpsc
                                  DirectSource::run
                                    │
                                    ├─ Start  → sink.start(info), emit
                                    │           Tablespace symlinks as
                                    │           FileMeta::Symlink
                                    │
                                    ├─ Archive(body) →
                                    │     tar::Archive on body bytes,
                                    │     per entry:
                                    │       Dir   → emit FileMeta::Dir
                                    │       Sym   → emit FileMeta::Symlink
                                    │       File  → emit FileMeta::File,
                                    │               sink.begin()
                                    │               match action:
                                    │                 Keep → write to
                                    │                   data_dir/meta.path
                                    │                 Tap  → sink.chunk(...)
                                    │                 Skip → drop body
                                    │               sink.end()
                                    │
                                    └─ Finish → sink.finish(info), return
```

Tar parsing inside `Archive(body)` runs on `tokio::task::spawn_blocking`
because `tar::Archive` is sync. `ChannelReader` from wal-rs (already
implements `AsyncRead`) bridges via `tokio_util::io::SyncIoBridge`
(same pattern wal-rs's `fetch::unpack_part` uses at
`pg/backup/fetch.rs:320`).

Source-CPU + IO load: BASE_BACKUP duration on source. No re-stream
needed (single-pass design). Tablespace map handling is upstream
already; tracking `is_default()` decides whether to emit a symlink at
all.

## Source impl: ObjectStore

`ObjectStoreSource` wraps wal-rs's `pg::backup::fetch` primitives:
`resolve_name` → `fetch_sentinel` (for `StartInfo`/`EndInfo`/tablespace
spec) → `list_tar_parts` → fan-out fetch+decompress+tar-parse per
part.

```text
DynStorage ──list──> [part_000.tar.zst, part_001.tar.zst,
                     pg_control.tar.zst]
              │
              ▼
            Sort: pg_control last
              │
              ▼
            Parallel fan-out (bounded, default 4):
              part_NNN.tar.* → storage.get → decompress → tar parse
                              │
                              ▼ per entry: same begin/chunk/end as Direct
              ──────────────────────────────────────────────
            Barrier: all part_NNN drained
              │
              ▼
            pg_control.tar.* (single-task)
              │
              ▼
            sink.finish(end_info)
```

Parallelism is internal to this impl. Workers feed the shared
`Arc<Mutex<dyn BackupSink>>` — lock contention is at file boundaries
(begin/end); intra-file `chunk()` is uncontested because each worker
owns its current file's bytes. Concurrency knob:
`ObjectStoreSource::parallelism: usize` (default `min(4, num_cpus)`).

`pg_control` is a hard barrier: every other part drains before
pg_control opens. This matches PG recovery's requirement that
`pg_control` reflects state *after* every other file landed. The
barrier is one `Vec::pop_back()` after the sort + a sequential second
phase.

`StartInfo` is derived from the leaf sentinel
(`BackupSentinelDtoV2.sentinel.backup_start_lsn`, plus
`TablespaceSpec` for tablespaces). `EndInfo` from
`backup_finish_lsn`. Timeline lives in the backup name
(`base_TTTTTTTTLLLLLLLLSSSSSSSS`, parsed via
`pg/backup/mod.rs::format_backup_name` inverse).

### Delta-chain handling

`fetch::build_chain` already walks delta backups leaf→root. V1 reuses
that walk: each chain step gets fanned out the same way. Incremented
files (`apply_increment_in_place` at `pg/backup/increment.rs`) need a
disk-resident base to overlay onto — this works for `Keep` entries
naturally (write the base, then apply the increment) but **breaks for
`Tap`** because Tap entries never land on disk to be incremented.

Decision: V1 supports full backups only on the ObjectStore path. Delta
chain detection (`chain.len() > 1`) returns a hard error pointing the
operator at the full base. Production wal-g shops who only have delta
backups would need either (a) the page-walk to internally drive the
increment overlay (substantial work), or (b) a "rehydrate to scratch
first" mode (defeats the streaming win). Defer.

## Future source impl: LocalDir

Walks a directory rooted at `data_dir_snapshot/`, synthesising
`FileMeta` from inode metadata. `StartInfo`/`EndInfo` carried in a
sidecar `backup_label`-style file the operator writes before invoking
the source. Useful for: dev-loop scratch snapshots, NFS-mounted
backups, ad-hoc cold restores.

Not implemented in V1. Trait shape supports it without change.

## Sink impl: DiskLanderSink

Routes `base/<dbid>/<filenode>` files for catalogs (filenode <
`FIRST_NORMAL_OBJECT_ID` = 16384, or in `catalog_tracker_seed`
whitelist) to **Keep**. Routes user-heap to **Skip** when used alone.
Routes `pg_tblspc/<oid>` symlinks, `global/`, `pg_xact/`,
`pg_multixact/`, `pg_filenode.map`, `tablespace_map`, `pg_control`,
`backup_label` to **Keep**. Routes `pg_replslot/`, `pg_stat_tmp/`,
`pg_logical/`, `pg_dynshmem/`, `pg_subtrans/`, `pg_notify/`,
`pg_serial/`, `pg_snapshots/`, `pgsql_tmp/` to **Skip** (mirrors
`wal_rs::pg::backup::SYSTEM_DIRS_DENYLIST`).

System-dir directory entries themselves are **Keep** as empty dirs.
PG's recovery refuses to start without them.

## Sink impl: PageWalkSink

Taps **user heap only** (`base/<dbid>/<filenode>` with filenode >=
16384, not in TOAST namespace). Page-walks the body 8 KiB at a time,
iterating `ItemIdData` slots, projecting through the **same Phase 5
heap decoder** the WAL hot path uses. Salvaged largely as-is from
PHASE12experiments worktree D's `backfill_tar.rs`:

- `PageWalker::walk_page` (D, lines 338-410) — iterates `LP_NORMAL`
  slots, dispatches to `decode_heap_tuple`.
- `decode_heap_tuple` (D, lines 432-481) — reshapes
  `HeapTupleHeaderData`-prefixed tuple bytes into the
  `xl_heap_header`-prefixed shape `decode_block_data_for_test`
  consumes. Requires the test-only shim moved to `pub(crate)` on the
  main heap decoder.

CatalogMap (per-`(db_node, rel_node)` → `Arc<RelDescriptor>`) is
populated **before** `source.run` by querying source PG's
`pg_class`/`pg_attribute`/`pg_type`/`pg_namespace` for
`oid >= 16384`. The query rides on the sidecar SQL client at
`source_feed.rs` (worktree D path).

Race: pg_class on source could change between the seed query and
BASE_BACKUP's checkpoint. PLAN.md §Phase 12 out-of-scope already
states "DDL during backfill — operator must quiesce DDL for the
backfill window". The seed query happens *immediately before*
`source.run` (sub-second gap) so the window is operationally
indistinguishable from the BASE_BACKUP issue itself.

Emitter handoff: every decoded tuple feeds `Emitter::push_backfill_row`
tagged with `_lsn = start_lsn`. The emitter is `!Send`
(PHASE12experiments §"Emitter::!Send constraint"); PageWalkSink holds
`Arc<Mutex<Emitter>>` and drains tuples through one shared lock — the
same shape every PHASE12experiments prototype landed on.

### TOAST handling

V1 limit: only inline-stored varlena columns decode through Phase 5's
existing matrix (`heap_decoder` Tier 1/2). Columns whose tuple bytes
carry an `ExternalToast` pointer (`va_external` set) surface as
`ColumnValue::PgPending` — Phase 9's existing carrier for unresolved
varlena. Per-row stats track how many columns spilled. Acceptance
criterion §1's `pgbench -i` data has zero TOAST'd columns, so this
limit doesn't gate v1.0.

Full TOAST chunk decoder lands as part of the WAL-side
`pg_toast_<relid>` work in Phase 5 follow-ups, **not** as 2A-specific.

## Sink impl: MultiplexSink

Composes DiskLanderSink (`Keep` catalogs + system files) and
PageWalkSink (`Tap` user heap). One `begin()` dispatches to whichever
inner sink wants this path; same-file `chunk()`/`end()` route to the
chosen inner.

Dispatch (in priority order):

1. `pg_replslot/*`, `pg_stat_tmp/*`, etc. (`is_system_dir_path`) →
   inner = DiskLander, action = Skip files inside; Keep the dir entry
   itself so PG sees an empty dir.
2. `pg_tblspc/<oid>` symlink → inner = DiskLander, action = Keep
   (creates the symlink under data_dir via the source's `write_kept`
   helper).
3. `base/<dbid>/<filenode>` with `filenode < 16384` OR in
   catalog-tracker-seed whitelist → DiskLander, Keep.
4. `base/<dbid>/<filenode>` user heap → PageWalk, Tap.
5. Everything else (`global/`, `pg_xact/`, …) → DiskLander, Keep.

This is the only sink users construct directly. DiskLander and
PageWalk are public for testing in isolation but the production
greenfield path always wires them through Multiplex.

## Bootstrap orchestrator

New module `src/backfill_bootstrap.rs`. Replaces
`Shadow::apply_schema_dump` on the greenfield path:

```rust
pub async fn run_greenfield_bootstrap(
    cfg: &BootstrapConfig,
    shadow: &Shadow,
    emitter: Arc<Mutex<Emitter>>,
) -> Result<EndInfo> {
    // 0. Source-side prep
    let source = open_source_connection(&cfg.source).await?;
    source.ensure_physical_slot(&cfg.slot_name).await?;       // (a)
    let catalog_map = seed_catalog_map_from_source(&source)   // (b)
        .await?;

    // 1. Initialize empty shadow data_dir
    fs::create_dir_all(&cfg.shadow_data_dir).await?;

    // 2. Drive the source through the multiplex sink
    let disk_lander = DiskLanderSink::new(
        cfg.shadow_data_dir.clone(),
        cfg.catalog_seed_filenodes.clone(),
    );
    let page_walk = PageWalkSink::new(
        catalog_map,
        emitter.clone(),
        cfg.start_lsn_placeholder,    // overwritten in sink.start()
    );
    let mux = Arc::new(Mutex::new(
        MultiplexSink::new(disk_lander, page_walk)
    ));

    let source_impl = build_source(cfg)?;     // Direct | ObjectStore
    let (start, end) = source_impl
        .run(&cfg.shadow_data_dir, mux.clone()).await?;

    // 3. Append shadow's standby config, recovery_target_lsn = end_lsn
    shadow.write_base_conf()?;
    shadow.enable_standby_recovery_with_target(end.end_lsn)?;

    // 4. Start shadow, wait for replay to end_lsn
    shadow.start()?;
    shadow.wait_for_replay(end.end_lsn, cfg.replay_timeout)?;

    Ok(end)
}
```

(a) Source-side physical slot pre-created (PHASE12experiments Pitfall
#5): the slot reserves WAL from `start_lsn` forward so the WAL pump's
`START_REPLICATION PHYSICAL <slot> <end_lsn>` finds its target after
recovery completes.

(b) Catalog seed query: `SELECT n.nspname, c.relname, c.oid,
c.relfilenode, c.relnamespace, c.relkind, c.relpersistence,
c.relreplident, ... FROM pg_class c JOIN pg_namespace n ON
c.relnamespace = n.oid WHERE c.oid >= 16384`. Fan-out to
`pg_attribute` for column types per relation. Builds the
`CatalogMap` PageWalkSink consumes.

Daemon binary (`src/bin/stream.rs`) gains a `--bootstrap-mode`
flag with values `none` (legacy), `direct`,
`object_store=<storage-url>`. CLI follow-up; orchestrator is callable
without CLI changes for tests.

## Module layout

New modules:

| Module | Purpose | Approx LOC |
|---|---|---|
| `backup_source.rs` | Trait + types | ~120 |
| `backup_source/direct.rs` | DirectSource impl | ~220 |
| `backup_source/object_store.rs` | ObjectStoreSource impl | ~250 |
| `backup_source/tar_helper.rs` | Shared tar→file translation | ~150 |
| `backup_sink_disk.rs` | DiskLanderSink | ~140 |
| `backup_sink_pagewalk.rs` | PageWalkSink + PageWalker + CatalogMap | ~520 (mostly salvaged from worktree D `backfill_tar.rs`) |
| `backup_sink_mux.rs` | MultiplexSink | ~80 |
| `backfill_bootstrap.rs` | Orchestrator | ~180 |

Salvaged from worktree D essentially as-is:
- `PageWalker` + `decode_heap_tuple` + `parse_base_path` + page header
  iteration (D `backfill_tar.rs:317-554`).
- `CatalogMap` (D `backfill_tar.rs:268-308`).
- `BackfillTuple` carrier (D `backfill_tar.rs:107-117`).

Discarded from worktree D:
- `EntryFilter`-tied `CatalogOnlyFilter` — replaced by Multiplex
  dispatch.
- `TapSink`-tied `PageWalkTap` — replaced by PageWalkSink (same logic,
  different trait).
- `ConstrainedSourceBootstrap` two-pass machinery — single-pass via
  source-side catalog seed.
- `shadow_basebackup.rs` orchestrator — replaced by
  `backfill_bootstrap.rs`.
- `FpiReplayer` stubs — V1 doesn't replay backup-page FPIs; the
  primitive can land separately if measurement shows duplicate volume
  matters.

One main-branch lift:
- `heap_decoder::decode_block_data_for_test` → drop the `_for_test`
  suffix, make it `pub(crate)`. V1's PageWalker needs it as a
  production entry point, not just a test shim.

## Acceptance + tests

### Unit (in-crate)

1. `tests::trait_routes_files_in_synthetic_source` — `MockSource`
   emits a curated `FileMeta` stream (denylist + symlink + catalog +
   user heap + pg_control). MultiplexSink routes each correctly.
   Catalog files land under tmpdir. User-heap bytes captured by a
   recording-PageWalkSink stand-in. pg_control received last. Denylist
   not written to disk.
2. `tests::page_walker_emits_single_tuple_from_hand_crafted_page` —
   salvaged from worktree D (D `backfill_tar.rs:799-814`). Builds an
   8 KiB page in memory with one INT4 tuple, walks it, asserts the
   `BackfillTuple` matches.
3. `tests::page_walker_handles_empty_page` — fresh-init page (lower =
   header end, upper = page end). Walker emits zero tuples cleanly,
   stats reflect one page walked.
4. `tests::object_store_source_rejects_delta_chain` — synthetic
   `DynStorage` with `increment_from` set on the sentinel; source
   errors out with operator-actionable message.
5. `tests::direct_source_archive_translation` — feed a synthetic
   `BackupEvent` mpsc into DirectSource, assert tar entries from one
   archive translate to the expected `FileMeta` sequence.
6. `tests::catalog_seed_from_pg_class_row_shape` — fixture row
   matching `pg_class` column shape on PG 16/17/18 decodes into a
   `RelDescriptor`. Compile-time stable.

### Integration (`tests/phase12_*.rs`)

7. `phase12_direct_e2e` (live source PG required, gated by env-var):
   - Source: `CREATE TABLE t (id int, v text); INSERT 10k rows`.
   - Run `run_greenfield_bootstrap` with DirectSource.
   - Assert shadow's `pg_class` post-recovery has source's filenode
     for `t`.
   - Assert CH `t` has 10k rows with `_lsn = start_lsn`.
   - Start WAL pump at `end_lsn`; INSERT 1k more rows on source.
   - Assert CH `t` has 11k rows after drain.
8. `phase12_object_store_e2e` (requires wal-g'd snapshot under a
   local `file://` `DynStorage`): same shape as 7 but ObjectStoreSource.

Integration tests gate v1.0 `pgbench -i` acceptance criterion (§1 in
[PLAN.md §Acceptance criteria](PLAN.md#acceptance-criteria)).

## Open items / risks

- **Catalog-seed snapshot binding.** V1 queries source PG immediately
  before BASE_BACKUP. PHASE12experiments §"LSN handoff" notes a
  cleaner shape: `wal_rs::pg::replication::base_backup` exposes a
  `start_backup_with_snapshot(label) -> (StartInfo, String)` that mints
  a snapshot id atomically with the checkpoint. Tighter coupling but
  adds ~80 LOC on wal-rs. V1 punts; trait isn't affected.
- **Sink mutex contention under parallel ObjectStore fan-out.**
  V1's `Arc<Mutex<dyn BackupSink>>` serialises sink callbacks across
  workers. File-level granularity should keep contention bounded but
  not measured. If parallelism > 4 turns into a lock-thrash floor,
  the follow-up is per-worker sinks + a merge step (or a sink trait
  that's `&self` + interior-locking).
- **Source-side catalog seed assumes pg_class layout per PG major.**
  pg_class added `relreplident` in PG 9.4; PG 16 introduced no
  changes the seed query cares about; PG 17/18 ditto. Query stays
  stable.
- **Object_store path requires source PG reachable for catalog seed.**
  An air-gapped backup (S3 only, source PG offline) can't seed the
  CatalogMap. Operationally rare. Workaround: a sidecar
  `pg_catalog.json` dumped alongside the BASE_BACKUP at backup-time
  (wal-g extension), consumed at restore-time. Defer; documented.
- **No `pg_dump` schema fallback.** The legacy
  `Shadow::apply_schema_dump` path stays callable for operators with
  neither REPLICATION grant nor storage credentials but is removed
  from the greenfield boot orchestrator. Pre-existing tests
  exercising `apply_schema_dump` continue to pass.

## Co-evolving wal-rs surface

No required wal-rs changes for V1. The existing
`EntryFilter`/`TapSink` surface (`pg/backup/mod.rs:65-77`) used by
worktree D's `unpack_manual` integration is **not** consumed by the
file-level trait — both source impls tar-parse internally and call
`sink.begin/chunk/end` directly.

Open question for wal-rs hygiene: with no walshadow consumers of
`EntryFilter`/`TapSink`, are they worth keeping as a public surface?
Two options:

1. Keep them. They're additive, well-tested, and a future caller may
   want filter-driven extraction without going through the file-level
   trait.
2. Demote to `pub(crate)`. Forces the file-level trait to be the only
   walshadow consumer surface; tighter API.

Defer to wal-rs maintainer; doesn't gate Phase 12.

## Out-of-band notes

- **2C orchestrator stays archived in PHASE12experiments worktrees.**
  If 2A measurement on a real source shows unacceptable cost (page
  decode CPU, FPI replay miss rate), 2C is the documented fallback;
  it composes onto the same `BackupSink` trait by having a 2C
  source-impl that emits `BackupEvent::Archive`-style tar streams
  synthesised from COPY BINARY output. Not pursued.
- **Cursor + resume (worktree G synthesis).** The Phase 11 cursor
  primitive (`src/cursor.rs`) covers WAL-side durability. A per-file
  / per-page resume cursor is orthogonal Phase-12 follow-up: store
  the last fully-emitted `(rfn, block_no)` to scratch; on restart,
  skip ahead. Out of V1 scope; trait doesn't need changes.
