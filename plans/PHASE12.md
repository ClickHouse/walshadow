# PHASE12 — backfill bridge via file-streaming source trait (retro)

Closes [PHASE12plan.md](PHASE12plan.md). Supersedes
[PHASE12experiments.md](PHASE12experiments.md)'s recommended
greenfield composite (H+E+G — 2C-shaped) with the 2A-shaped design
the plan named: one file-level trait, two source impls (Direct +
ObjectStore), shared decoder with the WAL hot path, parallel S3
fan-out, future LocalDir slot reserved.

## What landed

Six new modules under `src/`:

| Module | LOC | Tests | Role |
|---|---:|---:|---|
| `backup_source.rs` | 656 | 3 | Trait + types (`FileMeta`, `FileKind`, `FileAction`, `StartInfo`, `EndInfo`, `BackupSink`, `BackupSource`), `pump_tar_to_sink` helper, `RecordingSink` for tests |
| `backup_source_direct.rs` | 136 | 0 | Wraps `wal_rs::pg::replication::base_backup::run_base_backup`; per-archive async tar parse via `tokio_tar` |
| `backup_source_object_store.rs` | 399 | 5 | Wraps `wal_rs::pg::backup::fetch` primitives; bounded-parallel fan-out via `futures::StreamExt::buffer_unordered`; pg_control as last-task barrier |
| `backup_sink.rs` | 510 | 5 | `DiskLanderSink` (catalog Keep / denylist Skip / user-heap Skip), `MultiplexSink<T>` (lander + tap composition), `CatalogFilenodes` whitelist |
| `backup_page_walk.rs` | 773 | 8 | `PageWalker` (8 KiB heap page → `BackfillTuple` via Phase 5 decoder), `CatalogMap`, `PageWalkSink` (Tap user heap, ship over unbounded mpsc) |
| `backfill_bootstrap.rs` | 619 | 2 | Orchestrator: `seed_catalog_from_source` (source-PG sidecar SQL), `seed_in_snapshot` (REPEATABLE READ wrapper), `spawn_greenfield_bootstrap` (pump task yielding rx + JoinHandle), `run_greenfield_bootstrap` (test wrapper collecting Vec<BackfillTuple>), `drain_backfill` (BackfillTuple → CommittedTuple synthesis + TupleObserver hand-off) |
| `bin/stream.rs` (Phase-12 hunks) | ~180 | covered by `phase12_object_store_e2e` | `--bootstrap-mode={off,direct,object_store}` CLI plumbing, `run_bootstrap` helper, `write_standby_config` |
| `tests/phase12_object_store_e2e.rs` | 327 | 1 (live PG) | Full pipeline drill: live source PG → `wal_rs::pg::backup::push::handle` → FsStorage → ObjectStoreSource → orchestrator → drain → `CollectingTupleObserver`. Self-hosted; no `wal-g` binary needed. |
| **Total** | **~3520** | **23** (lib) + **1** (live-PG e2e) | |

Plus one small main-branch lift:
- `heap_decoder::decode_block_data` exposed as `pub(crate)` (was
  `decode_block_data_for_test` in worktree D). PageWalker reshapes
  on-disk `HeapTupleHeaderData`-prefixed tuples into the
  `xl_heap_header`-prefixed shape Phase 5's decoder consumes, then
  calls through — zero codec drift between WAL and backup paths by
  construction.

All 23 new lib tests pass; full lib suite at **202 tests** (was 179
pre-Phase-12), all green. The `phase12_object_store_e2e` integration
test runs a live source PG via `initdb` / `pg_ctl`, drives a real
BASE_BACKUP through wal-rs's push pipeline, and reads it back through
ObjectStoreSource — green on PG 16 / PG 17 hosts that have `initdb` on
PATH (skipped silently otherwise, matching the existing
`phase8_e2e.rs` / `wal_stream_e2e.rs` convention).

## What changed vs the plan

### tokio_tar instead of sync `tar`

Plan called for `tar = "0.4"` + `SyncIoBridge` + `spawn_blocking`
dance. Switched mid-execution to `astral-tokio-tar` (the
astral-sh/tokio-tar fork) on user request. Net win:

- DirectSource's `BackupEvent::Archive.body` is `AsyncRead` already
  (wal-rs `ChannelReader`); tokio_tar consumes it without a sync
  bridge.
- ObjectStoreSource's decompressed reader is `AsyncRead` already; same
  win.
- `pump_tar_to_sink` is `async fn` end-to-end, the sink lock is held
  only across the sync `begin`/`chunk`/`end` calls — async work
  (entry-body reads, disk writes, dir creation) is unlocked.

Cost: one extra dep (`astral-tokio-tar`), zero behavioural difference
in the test surface.

### Unbounded mpsc for page-walk → emitter

Plan called for bounded `mpsc::channel(1024)` with natural
back-pressure. Hit `Sender::blocking_send` panicking inside the tokio
runtime (the sync `BackupSink::chunk` callback fires from an async
context). Switched to `mpsc::unbounded_channel`. The drain task
ahead of the sink should outpace decode in normal operation; if
measurement shows queue build-up, swap to `try_send` + capacity. Noted
in code at `backup_page_walk.rs:331-339`.

### Stats recovery shape

Plan implied direct `Arc<Mutex<dyn BackupSink>>` ownership through the
orchestrator. `Mutex<dyn ?Sized>::into_inner` doesn't compile (unsized
inner). Solved by holding two Arc clones — one typed
(`Arc<Mutex<MultiplexSink<PageWalkSink>>>`) for stats recovery and
one erased (`Arc<Mutex<dyn BackupSink>>`) for the source's call. Both
clone the same `Mutex`. After source returns, `Arc::try_unwrap` on the
typed clone exposes both inner sinks' stats. See
`backfill_bootstrap.rs:124-159`.

### Single-pass design (vs worktree D's two-pass)

Worktree D ran Pass 1 (catalog land, drop user heap) + shadow recovery
+ pg_class read + Pass 2 (re-fetch / re-stream, page-walk user heap).
V1 ships single-pass: source PG sidecar SQL seeds `CatalogMap` for
user relations **before** the source pumps. Catalog filenodes land
synchronously; user-heap pages decode synchronously through the same
pass. The race window (DDL between seed and BASE_BACKUP checkpoint)
is collapsed by PLAN.md §Phase 12's existing "DDL during backfill is
out-of-scope" stipulation. Object-store path inherits the same seed
mechanism — operator must have source PG reachable for the seed query
even if the bulk bytes come from S3. Air-gapped backup is documented
as an open item in PHASE12plan.md §"Open items / risks".

## What stayed deferred (by design)

These are deferred in the plan and stay deferred:

- **FPI replay on backup pages.** Pages with `pd_lsn < start_lsn`
  walk as-shipped; WAL records in `[start_lsn, end_lsn]` that update
  those tuples re-emit at higher `_lsn` and
  `ReplacingMergeTree(_lsn)` collapses the duplicate. WAL-side FPI
  replay (already in `fpi.rs`) handles the steady-state path.
- **TOAST cross-archive reassembly.** Inline-stored varlena decodes
  through Phase 5; external TOAST chunks surface as
  `ColumnValue::PgPending`. The chunk-and-assemble logic is
  WAL-shared, not 2A-specific.
- **LocalDir source.** Trait shape supports it (FileMeta with
  `FileKind::Symlink`, no tar coupling). Build it when a use case
  demands.
- **Delta-chain support on ObjectStoreSource.** V1 errors hard on
  `increment_from` set in the sentinel. Operators who only retain
  delta backups would need either (a) a "rehydrate to scratch first"
  mode or (b) chain-aware page-walk overlay — neither is cheap.
- **Resume mid-bootstrap.** V1 is single-shot per PLAN.md. Per-chunk
  cursor (PHASE12experiments G + E synthesis) is the natural follow-up.

## What landed beyond the original V1 scope

The first cut of this retro flagged three items as deferred. All three
either landed in this commit set or were unblocked:

- **Daemon CLI wiring.** `walshadow-stream` now accepts
  `--bootstrap-mode={off,direct,object_store}` plus
  `--bootstrap-shadow-data-dir`, `--bootstrap-backup-name`,
  `--bootstrap-object-store-parallelism`, `--bootstrap-fast-checkpoint`,
  `--bootstrap-autospawn-shadow`, `--bootstrap-shadow-replay-timeout`.
  The `run_bootstrap` helper in `bin/stream.rs` performs the catalog
  seed, picks the source impl, drives the orchestrator, drains tuples
  through either the transitional CH emitter (when `--ch-config` is
  set — see "Bootstrap rows → CH emitter" below) or
  `MetricsTupleObserver`, writes `standby.signal` + `restore_command`
  to the shadow data dir, optionally auto-spawns shadow PG + waits for
  replay past `end_lsn`, and returns `end_lsn`. The WAL pump's
  start-LSN selection now consults `bootstrap_end_lsn` ahead of the
  cursor file (`--start-lsn` still wins for recovery drills).

- **Bootstrap rows → CH emitter (Solution 2).** New trait
  `walshadow::relation_resolver::RelationResolver` fronts the emitter's
  single catalog dependency (`relation_at(rfn, source_lsn) →
  Arc<RelDescriptor>`). `Emitter` now holds `Arc<dyn RelationResolver>`
  instead of `Arc<Mutex<ShadowCatalog>>`; existing daemon call sites
  auto-coerce because `Mutex<ShadowCatalog>: RelationResolver`. The
  bootstrap path builds a transitional emitter against
  `CatalogMapResolver` wrapping the seeded `CatalogMap` — no live
  shadow PG needed. Synthetic INSERTs flow into CH as the page walker
  produces them; `drain_backfill` closes the per-table INSERT block
  via `on_xact_end` before the transitional emitter is dropped, then
  the daemon's main flow builds a fresh `ShadowCatalog`-backed
  emitter for WAL records. Peak memory during bootstrap is
  `O(tables × byte_budget)` (default 1 MiB per table) rather than
  `O(total backfill rows)`. See [PHASE12bootstrap.md](PHASE12bootstrap.md)
  for the three-way comparison against the spill-to-disk + in-mem
  buffer alternatives.

- **Auto-spawn shadow PG (Solution 3 hand-off).**
  `--bootstrap-autospawn-shadow` (default off) lets the daemon drive
  shadow lifecycle itself via `walshadow::shadow::Shadow::start` +
  `Shadow::wait_for_replay(end_lsn, timeout)`. The sync `pg_ctl` +
  `psql` shells run inside `tokio::task::block_in_place` so the
  multi-threaded runtime keeps other tasks alive while waiting.
  `--bootstrap-shadow-replay-timeout` (default 300 s) bounds the wait.
  Operators with an external supervisor (systemd, k8s) leave the flag
  off and own shadow lifecycle themselves.

- **Live-PG integration test.** `tests/phase12_object_store_e2e.rs`
  brings up a fresh source PG via the existing `Shadow` fixture, loads
  `s12.t` with 64 int4 + text rows, drives `wal_rs::pg::backup::push::handle`
  against an `FsStorage` root in the test's tempdir, then reads the
  whole wal-g-format backup back through `ObjectStoreSource`. Asserts
  cover LSN handoff, disk-lander coverage (>100 catalog files
  landed), denylist skips, pg_control landed last, user-heap
  filenode-by-OID *not* landed on disk, pg_class landed, page-walk
  stats (files_seen / files_walked / pages_walked / tuples_emitted),
  drain count agreement with page-walk emit count, and per-tuple
  shape (`op=Insert`, `commit_ts=0`, `commit_lsn=start_lsn`, int4 id
  range 1..=64).

- **Self-hosted fixtures.** The prior retro called for a "wal-g'd
  snapshot fixture", implying an external `wal-g` binary. Replaced by
  wal-rs's own `pg::backup::push::handle` which produces a
  wal-g-compatible layout on disk via `FsStorage`. The walshadow ↔
  wal-rs round trip validates both ends of the wire without any
  binary tooling outside the workspace. (`~/s/wal-g` is locally
  available if a third-party cross-check oracle is ever wanted; not
  on the V1 critical path.)

- **Concurrent drain shape.** Orchestrator API split into a streaming
  primitive `spawn_greenfield_bootstrap(cfg, source, catalog_map)
  → (UnboundedReceiver<BackfillTuple>, JoinHandle<Result<BootstrapOutcome>>)`
  + a test-only `run_greenfield_bootstrap` wrapper that collects into
  `Vec<BackfillTuple>`. The split is load-bearing: without it the
  unbounded channel queues every BackfillTuple in memory until the
  source completes; with it, drain runs concurrently with the source
  pump and the queue is bounded by emitter throughput.

- **`drain_backfill` helper.** Synthesises one `CommittedTuple` per
  `BackfillTuple` with `op = Insert`, `commit_ts = 0`,
  `commit_lsn = source_lsn`. Hands each through a
  `TupleObserver`. The CLI's `MetricsTupleObserver` consumes them
  for counting; the orchestrator unit test uses
  `CollectingTupleObserver`; the live-PG e2e test does too. The
  emitter-side wiring (`EmitterObserver`) plugs in unchanged when
  shadow PG is up — see "Open items" §V1 CLI limitation below.

## What stayed deferred (intentionally)

Nothing on the original V1 plan's critical path remains deferred.
Both the bootstrap-rows → CH emitter handoff and shadow PG
auto-spawn landed; see "What landed beyond the original V1 scope"
above. Remaining items are genuine follow-ups (TOAST cross-archive
reassembly, LocalDir source, delta-chain support on ObjectStoreSource,
resume mid-bootstrap) and live in "Open items carrying forward" below.

## Surprising findings

### `tar::Builder::set_path` rejects `..`

The path-traversal test couldn't construct an entry with `..` via
`tokio_tar::Header::set_path` — the API refuses to write the bytes.
Worked around by writing the raw 100-byte name slot in the header
directly (`backup_source.rs:560-572`). A hostile / corrupt tar on the
wire would still need defending against; the path-traversal guard in
`tar_entry_meta` covers that.

### `Mutex<dyn ?Sized>::into_inner` doesn't exist

Rust's `Mutex<T: ?Sized>::into_inner()` requires `T: Sized`. Trait
objects (`dyn BackupSink`) are unsized, so the natural
"`Arc::try_unwrap → mutex.into_inner` → recover stats" pattern
doesn't compile. The dual-clone approach (one typed, one erased) is
the standard workaround; documented in code.

### `mpsc::Sender::blocking_send` panics inside tokio context

Discovered when running the orchestrator test: `blocking_send` is
fine **outside** a runtime, but inside the runtime (the source's
async context) it would block a worker thread that needs to drive
other tasks. The panic message is explicit. Unbounded channel
sidesteps; production path may want `try_send` + backpressure
signalling later.

### worktree D's PageWalker salvaged with one shape change

D's `PageWalker::walk_page` took `block_no` as a parameter and an
`Arc<Mutex<>>`-bookended `WalkStats`. V1 simplifies: PageWalker is
borrowed-`&self`, takes `&mut Vec<BackfillTuple>` + `&mut
PageWalkStats` directly. The Arc<Mutex<>> bookkeeping moves to
PageWalkSink (which has it anyway because tap chunks fire serialized
through the sink mutex). Code reads cleaner.

## Open items carrying forward

- **`phase12_direct_e2e.rs`.** Object-store e2e validates the
  orchestrator + drain + page-walk path against a real BASE_BACKUP
  layout; the DirectSource path uses the same machinery (just a
  different `BackupSource` impl), but a dedicated e2e against
  DirectSource would exercise the replication-protocol BackupEvent
  channel under load. Cheap to add — mostly a one-line swap from
  `ObjectStoreSource` to `DirectSource` in the existing e2e fixture.
- **CH-server-backed bootstrap e2e.** `phase12_object_store_e2e`
  exercises the page-walk + drain path against a `RecordingObserver`
  (no live ClickHouse). A variant that spawns a `clickhouse server`
  via the existing `ChServer::spawn` helper (`tests/phase8_e2e.rs`)
  and drives the transitional emitter against a `CatalogMapResolver`
  would close the last hole — proves rows land in CH end-to-end. The
  Solution 2 wiring is already exercised at the unit-test level
  (`relation_resolver::tests`, `backfill_bootstrap::tests::drain_backfill_calls_on_xact_end_*`)
  + at the live-PG level minus the CH socket.
- **Optional spool tee for audit.** PHASE12bootstrap.md describes a
  hybrid: Solution 2 as the live path + Solution 1's spool as a
  side-channel tee for compliance audits. The Solution 1 worktree
  carries a ready-to-plug-in `BootstrapSpillWriter` module. Off by
  default; flag-on when an auditable backfill artifact is required.
- **Page-walk stats observability.** Today exposed on
  `PageWalkSink::stats`; orchestrator's `BootstrapOutcome.page_walk`
  carries them; the CLI logs a one-line summary at INFO. Hook into
  the `metrics` module so the bootstrap gauges show up alongside the
  existing WAL-pump metrics on the Prometheus endpoint.
- **TOAST chunk decoder (shared with WAL path).** When the WAL-side
  `pg_toast_<relid>` projection lands, PageWalkSink's
  `if is_toast { ... }` branch picks it up — currently counts pages
  and skips.
- **Per-chunk resume.** V1 bootstrap is single-shot; on crash mid-pump
  the next boot re-issues BASE_BACKUP from scratch. PHASE12experiments
  G + E synthesis (per-chunk cursor) is the natural follow-up — would
  let the daemon resume a half-finished bootstrap.

## Co-evolution status: wal-rs

V1 landed with zero wal-rs changes. Post-V1 cleanup lifted three
helpers to wal-rs's public surface so the walshadow duplicates can be
deleted:

- `pg::backup::fetch::fetch_sentinel` (was private, copy-pasted in
  walshadow)
- `pg::backup::fetch::list_tar_parts` (same)
- `pg::backup::parse_timeline_from_backup_name` (was private in
  `pg/wal/show.rs` + `pg/wal/verify.rs`; walshadow had a third
  `Result`-returning variant which now wraps the canonical Option-form
  for its error context)

Bundled with that lift, wal-rs absorbed the parallel internal cleanup
(three private `fetch_sentinel` copies → one canonical; two
`all_zero` copies in walparser hoisted to `mod.rs`; a new `pub(crate)
load_json<T>` helper collapses the storage-get → read → parse pattern
shared by `fetch_sentinel`, `fetch_files_metadata`, and
`fetch_incremented_set`). Net: ~80 LOC removed in wal-rs, behaviour
unchanged.

Full set of wal-rs public symbols ObjectStoreSource consumes today:
`pg::backup::{BackupSentinelDtoV2, TablespaceSpec,
tar_partitions_prefix, parse_timeline_from_backup_name}`,
`pg::backup::fetch::{fetch_sentinel, list_tar_parts, resolve_name}`,
`pg::replication::base_backup::Tablespace`, `compression`,
`config::Settings`, `storage::DynStorage`. The plan's "Open question
for wal-rs hygiene" still stands: with no walshadow consumer of
`EntryFilter`/`TapSink`, those traits remain technically dead public
surface. Defer to wal-rs maintainer's call.

## Verifying §1 acceptance is unblocked

PLAN.md §Acceptance Criterion §1 (`pgbench -T 30 -c 8` produces
matching row counts & checksums) was gated on Phase 12 because
`pgbench -i` pre-populates the source and walshadow couldn't see it.
After this commit set:

- Bootstrap loop (catalog-seed + BASE_BACKUP + page-walk + drain) is
  in place as both a library API (`spawn_greenfield_bootstrap` +
  `drain_backfill`) and a CLI mode (`walshadow-stream
  --bootstrap-mode=...`).
- LSN handoff returns `end_lsn`; CLI rebinds WAL pump there ahead of
  any cursor file (`bootstrap_end_lsn` takes precedence over
  cursor-resume in the start-LSN selection chain).
- `_lsn = start_lsn` tagging meshes with `ReplacingMergeTree(_lsn)`
  dedup against post-attach WAL.
- Live-PG e2e (`phase12_object_store_e2e`) proves the wal-rs ↔
  ObjectStoreSource ↔ orchestrator ↔ PageWalkSink ↔ drain ↔
  observer pipeline end-to-end, including LSN agreement,
  catalog/user-heap routing decisions, tuple-shape contracts, and
  the `on_xact_end` close signal the transitional emitter relies on.
- Bootstrap rows route through a transitional `Emitter` against
  `CatalogMapResolver` when `--ch-config` is set, so synthetic
  INSERTs land in ClickHouse during the bootstrap window — no
  buffering, no replay, no on-disk spool.
- `--bootstrap-autospawn-shadow` optionally drives shadow PG
  lifecycle from inside the daemon, removing the manual operator
  step between bootstrap-end and WAL-pump-start.

§1 acceptance is now end-to-end testable from `walshadow-stream`
alone (plus a configured CH server). The remaining CH-server-backed
e2e (open item above) would automate the cross-check, but the wiring
itself is in place.

## File-by-file structure

```
src/
├── backfill_bootstrap.rs            # orchestrator + source-PG seed +
│                                    # drain_backfill
├── backup_page_walk.rs              # PageWalker, CatalogMap, PageWalkSink
├── backup_sink.rs                   # DiskLanderSink, MultiplexSink
├── backup_source.rs                 # trait + types + tar pump helper
├── backup_source_direct.rs          # Direct (replication protocol) impl
├── backup_source_object_store.rs    # ObjectStore (wal-g layout) impl
└── bin/stream.rs (Phase 12 hunks)   # BootstrapMode CLI + run_bootstrap +
                                     # write_standby_config + start-LSN
                                     # ordering with bootstrap_end_lsn

tests/
└── phase12_object_store_e2e.rs      # live source PG → wal-rs push →
                                     # ObjectStoreSource → orchestrator
                                     # → drain_backfill → observer
```

Dependency edges in the new code:

- `backup_source` → wal-rs (`Tablespace` re-export), `tokio_tar`,
  `async-trait`
- `backup_source_direct` → `backup_source`, `wal_rs::pg::replication`
- `backup_source_object_store` → `backup_source`,
  `wal_rs::pg::backup`, `wal_rs::storage`, `wal_rs::compression`,
  `wal_rs::config`, `futures`
- `backup_sink` → `backup_source`, `crate::classify`
- `backup_page_walk` → `backup_source`, `backup_sink`,
  `crate::heap_decoder`, `crate::shadow_catalog`
- `backfill_bootstrap` → all of the above + `tokio_postgres::Client`
  + `crate::decoder_sink::TupleObserver`
  + `crate::heap_decoder::{CommittedTuple, DecodedHeap, DecodedTuple, HeapOp}`
- `bin/stream.rs` → `backfill_bootstrap`, `backup_source`,
  `backup_source_direct`, `backup_source_object_store`,
  `wal_rs::config::Settings`, `wal_rs::pg::backup::BACKUP_NAME_PREFIX`,
  `wal_rs::pg::replication::base_backup::BaseBackupOpts`

No cycles.
