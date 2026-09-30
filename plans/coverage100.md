# Reach 100% line coverage

Drive workspace line coverage to 100%, including product binaries and defensive
paths. Use meaningful behavioral assertions, testable boundaries, and removal of
confirmed dead code. Keep lints and existing zero-coverage-file gate enabled
Never exclude files, modules, or test code from coverage

## Measurement and gate

Regenerate coverage with [development recipe](../docs/development.md#wal-fixtures-and-coverage)
and current [CI matrix](../.github/workflows/ci.yml). Merged job unions DA hits
from every major, so a merged miss is missed on all of 16/17/18/19. Require every
expected major's artifact before accepting merged result. An early-returned
integration test does not establish coverage of its scenario

Merged job caps missed lines, unique `(source file, DA line)` entries with zero
hits, at 3115. Lower ceiling after each measured batch and finish at zero.
Per-major LCOV `LF`/`LH` and native llvm-cov summaries count a larger set than DA
entries, compare DA counts only against ceiling

[PG module coverage](../pgext/README.md#coverage) is gated per major at 100% and
stays out of Rust target

LCOV covers `src/**`, including in-file `#[cfg(test)]` modules; integration
tests under `tests/` are not counted. Generate fresh merged PG16/17/18/19
coverage to establish hit/total counts and rank remaining misses

## Coverage conventions

- Test code counts. In-file `#[cfg(test)]` modules are measured and never
  excluded. Failure paths of an assertion must not own a line: compare whole
  values with `assert_eq!`, use `unwrap`/`expect`/`unwrap_err`, compute message
  args before the assert, drop unused mock methods and helper branches
- Stop spawned daemons with `tools::stop_gracefully` (SIGINT, SIGKILL after
  timeout); `ChildGuard` drop does so. Instrumented binaries write their profile
  only on exit, so SIGKILL is reserved for deliberate crash points (`kill_restart`
  crash step, `bootstrap_object_store_crash_ch` first run,
  `control_plane_e2e::kill_daemon`), whose code is covered by the restarted run
- Lib, `walshadow-stream` and integration test binaries install a no-op
  `tracing_subscriber::registry()` before main, so every callsite is enabled and
  field expressions execute. Spawned daemons keep their own filter
- Remove dead code rather than test it. Guards on internal invariants become
  unrepresentable state where a small type change allows, else stay and get a
  unit test through the narrowest boundary

## Invariant arms requiring type changes

Each needs a type change beyond a local edit:
- bin bootstrap `BootstrapMode::Off` arm, needs start mode carried in `ShadowStart`
- `copy_backfill::note_opt_in` None handling and `backup_backfill::run_pass`
  None/Copy arm, need a backup-only mode type through queue, ledger and trait
- batcher `HeapOp::Truncate` arm, needs a batcher-only op type
- `XactBuffer::stash_raw`/`fold_raw` missing rfn, needs rfn-carrying record type
- `descriptor_at_spanned` Present arms, need a narrower `LookupResult`
- `ops::control` save fragment path check, needs a typed fragment location
- `ch.rs` codec arms reachable only without default features

`WalSegmentRemoved::from_start_replication` is live only against a mock, since
PG raises 58P01 after CopyBoth; cover via mock walsender

## Seams

Existing:
- `mock_feed()` pgwire backend in `source_feed` tests. Lift to a crate-level
  test helper that scripts IDENTIFY_SYSTEM, TIMELINE_HISTORY, CopyData, CopyDone,
  keepalive with reply, ErrorResponse. Unlocks `transition` probe/cross/verify
  and `source_feed` event arms
- `ch::test_support::retry_server` fake CH native server. Extend with scripted
  ServerException codes for `ch_ddl` refusal and passthrough arms, toast truncate
  and backfill publish/swap failures
- `ChunkStore` trait with `MemChunkStore`, `ReadOnly`, `FailPrefetch`
- `RecordSink`, `TupleObserver`, `SegmentSink` failing doubles (`Fail`, `ErrAt`,
  `ErrSegmentSink`)
- `BackupSource` trait, reachable only inside `backup_backfill` via in-file tests
- `ShadowConfig.pg_bin_dir`: empty dir gives MissingBinary, fake `pg_ctl`/`psql`
  scripts give start failure and parse errors
- bridge `fake_worker` for protocol violation arms
- `metrics` `FailAfter` writer

Filesystem faults without a new trait: directory in place of a file, file in
place of a directory, read-only directory. CI runs as non-root, so permission
faults hold

Missing:
- Unix-socket walreceiver client for `shadow_stream` Unix listener
- Storage injection into object-store backfill pass, today built from settings
- Retention trim interval, hard-coded, so the housekeeping loop never iterates in
  a test. Take interval from config or a hidden flag
- Walk checkpoint period knob for `backup_backfill` periodic checkpoint branches
- Hook between COPY chunks to run a rewrite mid-COPY

## Work list by area

Check candidate gaps against fresh merged report before adding tests

### CLI and ops

- `runtime_cfg::seed_runtime_config`: no spawned daemon sets
  `[runtime_config] schema`, and inproc harness only sees rows over WAL. Graceful
  `control_plane_e2e` boot with config rows pre-inserted (global, namespace,
  table with glob match, column) and TOML `initial_load` mappings including a
  bogus mode. Also covers `apply_toml_initial_loads`, bin `source_db` seed and
  opt-in paths. SIGHUP with broken TOML, double SIGINT for forced exit
- `housekeeping` retention trim: needs interval seam, then graceful run with
  `--retention-bytes 1` and a shadow connection drop for reconnect
- bin `bootstrap`: resume past extraction, metrics-only bootstrap, deferred
  referrer handback, archive WAL hydrate, `--bootstrap-wal-from-archive` in direct
  mode. Check coverage collected by graceful teardown before extending tests.
  `resolve_shadow_start` overlap and `paths_overlap` are bin unit tests
- `session`: manifest corrupt/foreign with and without `--ignore-cursor`, sibling
  branch and non-descendant boots, multi-db preflight, crossing retry/park and
  walsender reconnect via `control_plane_e2e` promotion drills. `task_stopped`,
  `SessionTasks::exited` outcomes and `adopt`/`shutdown` panic arms are units
- `ops::control` introspection verbs (`tables`, `schemas`, `columns`,
  `--database`): add missing `ctl` calls to `control_plane_e2e`. Covers
  `ctl` render, `introspect`. Pure `ok_with`, malformed dispatch, `pg_connect`
  empty host are units
- `ops::init`: `remedy` per `PreflightError`, `select_tables`/`resolve_url` under
  non-tty stdin, non-superuser source role in `init_e2e`. Spawn `init` subcommand
  once to cover `InitArgs::into_opts` and dispatch. TTY picker needs a pty
- `ops::oracle` Native block decode validation and cell-count check as units.
  `text_value` via ADD COLUMN with oracle-rendered default
- `tracing_setup` OTLP branch: spawn with an unreachable `--otlp-endpoint`,
  invalid endpoint for parse error
- `shadow_proc` supervise restart/backoff: immediate-stop shadow postmaster under
  graceful run, `probe_blocking` Err/panic as units
- `preflight::bootstrap` window-leg sender check and `wal_from_archive` against CI PG

### Backfill

- Object-store opt-in pass: every `backfill_staging_e2e` opt-in
  lacks a `[backup]` section and falls back to COPY. Give fixture an FsStorage
  backup root, push a backup and WAL past it, opt in above redo LSN with a
  TOAST-external row. Covers `run_object_store_pass`, `walk_and_ship` gap leg,
  `prescan_gap`, `replay_gap`, `drain_deferred`. Then crash after walk and rerun
  for `reopen_walk_spools` and ready-checkpoint resume, or hand-seed checkpoints
  as `backup_checkpoint_e2e` does
- `copy_backfill` pending tables: hold an open xact on target table across a
  base-backup opt-in, commit or abort after walk, for `record_pending` and
  `settle_ended_pending`. `copy_chunk_blocks = 1` on a multi-block table for chunk
  loop and progress ledger. Empty table fast path. Opt-out and CH schema change
  mid-pass for publish/swap edges. Two opt-ins inside coalesce window
- `copy_backfill` units: `wire_kind` per OID, `decode_field` per type and invalid
  UTF-8, ledger version rejection. Ledger persist failures via fs faults
- `wal_replay`: window leg with toasted update, >64 subxacts, VACUUM FULL of
  toasted table, DELETE under append-only mapping, DDL inside window. Extend
  `bootstrap_window_ch`
- `visibility_pending` ship with no mapping, over slab size, disjoint columns,
  settle after pending table dropped. `note_view`, empty `fresh_side` as units
- `bootstrap_marker` `ExtractedCheckpoint` write/read and `resumable_extraction`
  with no pin, mismatched pin, missing spool, valid checkpoint as units
- `backup_page_walk` short page, bad line pointer, bad `t_hoff`, offnum bounds as
  units. `backup_source` tar skip entries and kept symlinks as units
- `opt_in` alternates via `config_table` rows: `initial_load` none and bogus,
  NULL `replicate`, forward-declared row, refused mode for unfollowed database
- `bootstrap_oracle` `present_sockets`, `should_drop_entry` as units

### Pipeline, decode, xact, toast

- `heap_decoder` truncation guards: table-driven units looping `buf[..n]`
  through `decode_one_value`, `decode_varlena` per header form, and hand-built
  records into `decode_heap_record`. Prefix/suffix partial tuples via
  `decode_tuple_payload` directly. `missing_value_from_text` per fixed type and
  `ADD COLUMN "char" DEFAULT` in `add_column_default`
- `xact_buffer`: drive `on_record` against crafted `DescriptorLog` for Retired,
  ForeignDb, NotCovered, Ambiguous with and without marker, no-block heap record.
  `handle_truncate` malformed and foreign/toast relids. `resolve_stash` ambiguity
  and superseded generation. `decode_image_insert` needs a real 8 KiB page
  fixture or VACUUM FULL of a toasted table with a mid-rewrite checkpoint.
  `body_mem_max = 0` for file-backed bodies, `MemChunkStore` Missing/Generation
- `ch_emitter`: config loader errors in tempdirs, TOML table parser rejections,
  `parse_decimal_text` and `timestamp_ticks` bounds, `oracle_cell` mismatches,
  oversized oracle row, `"char"` in native type sweep. `ColumnBuf` Debug in one
  format assert
- `ch_ddl`: DROP COLUMN through applicator without resolver, tier-3 fast default
  with oracle, multi-db target ownership conflict, drop strategy Warn,
  `render_create_table` edge columns. CH refusal codes via `retry_server`
- `reorder`: config reload removing an opted-in table in `runtime_config_e2e`,
  `plan_mem_max = 0` for file-backed plans, plan dir faults, fatal before barrier
- `plan_spool` and `spill` replay corruption: write, patch bytes, replay, in the
  style of existing corruption tests
- pipeline `bootstrap` resumable-walk checkpoint: barrier with a ticker firing
  during walk, extend object-store crash or chunk resume test
- `batcher` oracle frame full and empty-table flush under global byte budget
- `shadow_landing::serves_toast`: runtime opt-in of toasted table under
  `[toast] mode = "shadow"`
- small decoders (`visibility`, `wal_xact`, `fpi`, `jsonb`, `inet`) as crafted units

### Source, catalog, filter

- `transition` probe and cross guards via lifted mock walsender, `CrossingState`
  and `ForkWait` Display as units. Double promotion (TL1 to TL3) for multi-hop
  `branch_history` and missing source history
- `desc_log` decoder rejections: write log, patch magic, tag, CRC, header,
  identity fields, unknown kinds, reopen. Round-trip rare variants
- `shadow_stream` state edges as units on `ShadowStreamState`, slow-client cap
  with small threshold, Unix listener needs Unix walreceiver client
- `catalog::shadow` via fake `pg_bin_dir`, `wait_for_replay` timeout,
  `validate_running` mismatches in `shadow_lifecycle`
- `streaming_walker` ShortRecord, ZeroRecord, zero tail, oversize continuation,
  `truncate_to` below page start with existing page builders
- `queueing_record_sink` worker failure via failing sink doubles, span sampling
  with a span registry
- `catalog_capture` resume below `covered_through` with in-flight command
  boundaries, lost coverage by deleting desc log spill dir
- `source_feed` `check_slot` with logical slot and slot without restart LSN
- `type_bridge` literal and type mapping as one table-driven unit
- `engine` smgr marker eviction, mixed-scope catalog touch across target dbs
- `shadow_catalog` duplicate oid, odd hex, tablespace and no-toast tables

## Sequencing

1. Measure merged coverage, prune covered candidates and rank
   remaining misses; lower 3115 ceiling if measured result permits
2. Pure units: decoders, corruption replays, parsers, Display, CLI args
3. Seam lifts: mock walsender, `retry_server` codes, retention interval, storage
   injection
4. Live scenarios: object-store backfill pass and resume, runtime config seed,
   control verbs, window-leg shapes, pending tables, promotion drills
5. Fault arms left after above, via fs layout faults and new seams
6. Enforce literal zero missed once no DA line remains uncovered

Use [semantic fuzzing](fuzzing.md) to discover interactions, then convert useful
findings into deterministic coverage tests. Coverage proves execution, recovery
and semantic oracles must still prove outcomes
