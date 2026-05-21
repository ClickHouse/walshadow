# walshadow plan index

Mutable list of plan docs under `plans/`. Each phase closes with
`PHASE<N>.md`; pre-phase prep uses `PRE<N><suffix>.md`; evaluation
docs that are not yet committed work sit alongside as peers.

- **Phase 0** — record-classification fixture. [PHASE0.md](PHASE0.md).
- **Phase 1** — WAL filter + CRC rewrite. [PHASE1.md](PHASE1.md).
- **Phase 2** — PG-16-minimum cleanup. [PHASE2.md](PHASE2.md).
- **Phase 3** — shadow PG lifecycle. [PHASE3.md](PHASE3.md).
- **Phase 4** — catalog cache integration. [PHASE4.md](PHASE4.md).
- **Phase 4b** — restart resilience. [PHASE4b.md](PHASE4b.md).
- **Phase 5** — heap-tuple decoder + Tier 1/2 type matrix. [PHASE5.md](PHASE5.md).
- **Phase 6** — TOAST reassembly + xact buffer + local-disk spill.
  [PHASE6.md](PHASE6.md). Design layer: [PHASE6disk.md](PHASE6disk.md).
- **Phase 7** — CH Native emitter via clickhouse-c-rs. Feature-passdown
  shape + emitter scaffold + observer wiring; Tier 1/2 + live-CH drill
  iterate in followups. [PHASE7.md](PHASE7.md).
- **Phase 8** — end-to-end DDL drill: source PG → walshadow filter →
  shadow PG (recovery via `restore_command`, bootstrapped through
  `pg_basebackup`) → heap decoder → xact buffer → CH-Native emitter →
  spawned `clickhouse server`. Two integration tests in
  `tests/phase8_e2e.rs` — INSERT/UPDATE/DELETE on a pre-created table,
  and `ALTER TABLE ADD COLUMN` mid-stream with a mapping that
  pre-declares the post-ALTER shape. Surfaces + fixes four bugs:
  `WalStream::flush_current`'s dispatch order (segment-first so the
  decoder's `relation_at` replay-LSN gate isn't deadlocked against
  the segment write its own caller holds back),
  `clickhouse-c-rs::Client` pins its `Allocator` in a `Box` so the
  C-side `c->al` pointer stays valid after `Client::init` returns,
  `TablePlan::build` no longer rejects mapping attnums absent from
  the catalog descriptor (pre-ALTER xacts are valid), and `ChServer`
  uses `SYSTEM SHUTDOWN` instead of `kill -TERM <pgid>` for clean
  CH-server teardown. DROP TABLE + PG read-time-default replication
  are followups. [PHASE8.md](PHASE8.md).
- **Phase 9** — differential decode oracle + Tier 3 hot types.
  Hybrid scope: `numeric` / `inet` / `cidr` / `interval` decoded
  locally in `src/codecs.rs` (Tier 3 hot types — small, fixed
  layout); `jsonb`, arrays, `tsvector`, ranges, custom domains, …
  surface as `ColumnValue::PgPending` carrying raw on-disk bytes
  and resolve at emit time via a new `walshadow` PG
  extension exposing `walshadow_decode_disk(oid, bytea) -> text`.
  Extension is optional — when absent the emitter falls back to
  writing raw on-disk bytes verbatim. `Oracle` module
  ([`src/oracle.rs`](../src/oracle.rs)) hosts the libpq bridge,
  1-in-N validator sampler, and `OracleObserver` wrapper that
  rewrites `PgPending` → `Text` before the inner observer sees
  the tuple. `walshadow-stream --validate <N>` enables sampling.
  Two integration tests pin the extension-present vs absent
  paths, plus an `OracleObserver` round-trip. Extension ships
  its own pg_regress suite under
  [`pgext/`](../pgext) covering varlena,
  fixed-width by-val 1/2/4/8, by-ref, cstring, STRICT NULL, and
  the two `ereport` branches. CI matrix gains
  `postgresql-server-dev-<major>` and runs the regress suite
  under `--temp-instance`. Surfaces + fixes two bugs:
  `decode_inet` was reading the wire-format `is_cidr | nb`
  bytes that aren't actually present in the heap-tuple body
  (PG's `inet_struct` is `family | bits | ipaddr[nb]`, with
  `is_cidr` encoded at the type-OID level and `nb` implied by
  `family`), and the oracle's `walshadow_decode_disk` SQL
  binding tripped tokio-postgres' "error serializing
  parameter 0" because `oid` is `u32`, not `i32`. Followups:
  local codecs for `jsonb` / arrays if measurement says the
  per-row libpq round-trip is hot; sampler auto-tuning;
  mismatch ring buffer for debugging. [PHASE9.md](PHASE9.md).
- **Phase 10** — operational scaffolding. Pre-flight validators
  (`src/preflight.rs` — aggregated report across version / wal_level /
  REPLICA IDENTITY FULL / slot existence), HTTP/Prom metrics endpoint
  (`src/metrics.rs` — hand-rolled text format over tokio TCP, no new
  observability dep), `tracing_subscriber` pipeline (`RUST_LOG=
  walshadow=debug` surfaces wal-rs's frame-level diagnostics),
  filtered segment retention (`src/retention.rs` — LSN-keyed trim
  against shadow's `pg_last_wal_replay_lsn`), `(write, flush, apply)`
  standby-status triple (`StandbyStatus` in `source_feed.rs`; Phase 11
  fills resume-safe values), SIGHUP reload of `--ch-config` (atomic
  swap of `MappingHandle` at xact boundary), CH-emitter bounded
  reconnect+retry (`Emitter::route_with_retry` /
  `drain_xact_with_retry` + `RetryConfig`). Surfaces + fixes two
  bugs: tokio-postgres rejected `&str → regclass` binding, fix is
  `to_regclass(text)` which also returns NULL on missing relations;
  the Emitter's reconnect drop-order required keeping the existing
  `Pin<Box<PosixIo>>` invariant so the C-side `client→io.state`
  back-pointer stays valid after field reassignment. [PHASE10.md
  ](PHASE10.md).
- **Phase 11** — durability + resume. Cursor file
  ([`src/cursor.rs`](../src/cursor.rs)) at `{spill_dir}/cursor.bin`,
  56-byte atomic-rename writer with magic + CRC32C; filter-segment
  fsync via `OpenOptions+sync_all+rename+dir-fsync`; per-xact
  `commit_lsn` carrier on [`CommittedTuple`](../src/heap_decoder.rs);
  `XactBufferStats::{drain_lsn, emitter_ack_lsn}` monotonic gauges
  set inside [`XactBuffer::commit`](../src/xact_buffer.rs) — the
  single source of truth for "observer.on_xact_end returned Ok".
  Daemon's status loop flips the standby-status triple to durable
  values: `apply = min(shadow_replay, emitter_ack)`. Boot path reads
  cursor before `START_REPLICATION`; `--ignore-cursor` forces
  greenfield. Unblocks acceptance §5 (`kill -9` + restart matches
  uninterrupted CH end-state). Surfaces + fixes three bugs:
  unknown-xid commit/abort paths weren't advancing `emitter_ack_lsn`
  (sustained read-only workload would freeze the slot), atomic-
  rename writer must leave the `.tmp` sidecar tolerable across
  crashes (boot reads `cursor.bin` only, ignores stale `.tmp`), and
  `shadow_replay_lsn == 0` must be treated as "no constraint" not
  literal-min (otherwise retention-disabled deployments pin
  apply_lsn at 0). Per-xact cursor write + spill-replay on boot +
  2PC cursor entries deferred. [PHASE11.md](PHASE11.md).
- **Phase 12** — backfill bridge. `COPY` from source (or shadow,
  opt-in) under `pg_export_snapshot()`, ships pre-existing rows
  through the same per-relation emitter the WAL hot path uses;
  daemon's `--start-lsn` pins to the snapshot LSN so backfill +
  WAL tail meet seamlessly. Unblocks greenfield deployments
  against non-empty source. Not yet committed work; see
  [PLAN.md §"Phase 12"](PLAN.md#phase-12--backfill-bridge).
- **Phase 14** — close v1.0 product gaps + lift integration coverage.
  Read-time defaults (`atthasmissing` / `attmissingval`),
  `XLOG_HEAP2_MULTI_INSERT` per-tuple fan-out (`SmallVec<[_; 1]>`
  on `decode_heap_record`), `TRUNCATE` propagation (single
  `TRUNCATE TABLE <dest>` per relid, no tombstone shape), subxact
  lineage + `ROLLBACK TO SAVEPOINT` via `XLOG_XACT_ASSIGNMENT`
  tracking + commit/abort-payload subxids list,
  `walshadow_shadow_apply_lag_*` metrics on the Prom endpoint,
  plus three integration tests covering v1.0 acceptance §1
  (pgbench + ALTER + CIC), acceptance §5 (kill -9 + restart), and
  the Phase 12 direct + CH-backed bootstrap drill. `DROP TABLE`
  propagation moved into [Phase 15 §6](PHASE15.md) because it
  rides the same `SchemaEvent` channel + xact-buffer drain wiring
  that PHASE15's shape-mutating DDLs need. 2PC / sequence state /
  cross-table ordering / TLS-SCRAM defer to v1.1+ with rationale.
  Split into eight sub-plans; overview at
  [phase14/PLAN.md](phase14/PLAN.md).
- **Phase 15** — propagate source schema changes through to CH.
  `ShadowCatalog` gains a per-relation `SchemaEvent` channel
  (`Added` / `Changed{diff}` / `Dropped`), sourced both from
  descriptor-diff at fetch time and from `pg_class_decoder`'s
  heap_delete branch. New `src/ch_ddl.rs` applicator translates
  events into CH `CREATE TABLE` / `ALTER ... ADD|DROP|RENAME
  COLUMN` / `DROP TABLE` against a PG → CH `type_bridge`, gated
  against the emitter's INSERT path via a per-(rfn, generation)
  ready notify so post-ALTER xacts land on the new CH shape.
  `XactBuffer::commit`'s drain queue lifts to `DrainEntry::{Tuple,
  Catalog}` so DROP TABLE orders correctly against pre-DROP
  writes, gated on `--drop-table-strategy = retain|drop|warn`
  (default `retain`).
  Namespace block (`[namespace."public"] auto_create = true`)
  flips the daemon from operator-pre-declared TOML mapping to
  auto-discovery; per-table TOML stays as override. Type widening
  (`ALTER COLUMN TYPE`), partitioned-table DDL, FK constraints
  defer. [PHASE15.md](PHASE15.md).
- **Phase 13** — sub-segment record latency. Lift the page-by-page
  walker into `WalStream::push` so records reach the decoder on
  page cadence instead of waiting for a 16 MiB segment to fill.
  Catalog `relation_at` gate gets a "cached + no churn" fast path
  so steady-state UPDATEs no longer wait on shadow's replay; cache
  miss falls back to today's `wait_for_replay`. `DirSegmentSink`
  cadence + manifest shape stay segment-aligned (shadow's
  `restore_command` still needs whole segments).
  [PHASE13.md](PHASE13.md).
- **PRE5** — pre-Phase-5 cleanup: streaming filter pipeline
  (`WalStream`, `RecordSink`, `DirSegmentSink`), `SourceFeed`
  (`START_REPLICATION PHYSICAL` pump), `walshadow-stream` binary,
  `pg_class` heap-write decoder, `CatalogTracker::seed_from_source`
  bootstrap, `XLOG_SWITCH` pass-through test. [PRE5.md](pre5/PRE5.md).
- **PRE5b** — close [PRE5](pre5/PRE5.md) silent-correctness gaps before
  [Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix).
  Split into ten sub-phases, each shipped as its own commit; overview
  at [PRE5b.md](pre5/PRE5b.md).
  - **PRE5b1** lift `Filter` to per-stream scope. [PRE5b1.md](pre5/PRE5b1.md).
  - **PRE5b2** wire `seed_from_source` into `walshadow-stream`. [PRE5b2.md](pre5/PRE5b2.md).
  - **PRE5b3** handle `xl_heap_update` prefix/suffix in `pg_class_decoder`. [PRE5b3.md](pre5/PRE5b3.md).
  - **PRE5b4** connect `CatalogTracker` to `ShadowCatalog::invalidate`. [PRE5b4.md](pre5/PRE5b4.md).
  - **PRE5b5** widen `RecordEvent` → `Record` carrying parsed `XLogRecord`. [PRE5b5.md](pre5/PRE5b5.md).
  - **PRE5b6** `CompositeRecordSink` fan-out. [PRE5b6.md](pre5/PRE5b6.md).
  - **PRE5b7** `Arc<Mutex<ShadowCatalog>>` daemon wrap. [PRE5b7.md](pre5/PRE5b7.md).
  - **PRE5b8** `relreplident` + `pg_index` on `RelDescriptor`. [PRE5b8.md](pre5/PRE5b8.md).
  - **PRE5b9** `walshadow-stream` shutdown + memory hygiene. [PRE5b9.md](pre5/PRE5b9.md).
  - **PRE5b10** smaller debts (Empty-bucket audit, FIFO eviction, etc.). [PRE5b10.md](pre5/PRE5b10.md).
- **clickhouse-c-rs** — vendored as workspace member. Provides the
  Native-wire emitter for Phase 7. Not gated by a `PHASE*.md`: the
  crate is upstream code, walshadow just consumes it.
- **BASEBACKUP** — evaluation: use `BASE_BACKUP` to bootstrap
  shadow's data dir (replacing `Shadow::apply_schema_dump`) and to
  seed CH's initial heap load (via `COPY` from shadow at the
  backup's `end_lsn`). Proposes insertion as Phase 6.5 between
  [Phase 6](PLAN.md#phase-6--toast-reassembly--xact-buffer) and
  [Phase 7](PLAN.md#phase-7--ch-native-emitter-via-clickhouse-c-rs).
  Not committed work. [BASEBACKUP.md](BASEBACKUP.md).
- **SEGMENT_COMPRESSION** — evaluation: compressed WAL segment file
  ingestion (`*.zst`, `*.lz4`, `*.gz`, `*.lzma`). wal-rs gets
  `Method::Gz` + `classify_segment_path` + async
  `open_segment_file`; `walshadow-filter` flips to
  `#[tokio::main(flavor = "current_thread")]` and feeds the
  decoder into the existing sync `filter_segment`. Test-local
  `decompress_gz` helpers go away. Sibling of FPI_COMPRESSION;
  independent. [SEGMENT_COMPRESSION.md](SEGMENT_COMPRESSION.md).
- **PHASE6disk** — [Phase 6](PLAN.md#phase-6--toast-reassembly--xact-buffer)
  design layer: xact buffer + TOAST reassembly spill backend. Compares
  local-disk spill (mirrors PG `pg_replslot/<slot>/xid-*.snap`) against
  CH-as-scratch and CH-as-primary; recommends local disk with a
  `spill_backend = "local_disk" | "clickhouse"` knob reserved for the
  diskless case. Lands inside Phase 6's commit, not as a separate phase.
  [PHASE6disk.md](PHASE6disk.md).
- **FUTURE** — evaluation: speculative roles for shadow PG beyond
  CDC. Schema-only restore (ship shadow's catalog as DDL / hollow
  data dir for third-party clusters) and synchronous-commit WAL
  witness (walshadow as RPO=0 durability standby that relays the
  surviving WAL tail to a lagging async standby on primary loss).
  Not committed work. [FUTURE.md](FUTURE.md).
- **FPI_COMPRESSION** — [Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix)
  prerequisite: decompress `wal_compression = pglz|lz4|zstd`
  full-page images via a new `src/fpi.rs` (`restore_block_image`)
  atop the `pglz` / `lz4_flex` / `zstd` crates. Required by Phase 5
  for user-heap records that carry their tuple bytes inside an FPI
  (post-checkpoint hot set). Also unblocks
  [BASEBACKUP](BASEBACKUP.md) 1B+2A and `XLOG_FPI_FOR_HINT`
  handling. Sibling of SEGMENT_COMPRESSION (still evaluation),
  independent. [FPI_COMPRESSION.md](FPI_COMPRESSION.md).
