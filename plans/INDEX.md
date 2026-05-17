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
- **FPI_COMPRESSION** — [Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix)
  prerequisite: decompress `wal_compression = pglz|lz4|zstd`
  full-page images via a new `src/fpi.rs` (`restore_block_image`)
  atop the `pglz` / `lz4_flex` / `zstd` crates. Required by Phase 5
  for user-heap records that carry their tuple bytes inside an FPI
  (post-checkpoint hot set). Also unblocks
  [BASEBACKUP](BASEBACKUP.md) 1B+2A and `XLOG_FPI_FOR_HINT`
  handling. Sibling of SEGMENT_COMPRESSION (still evaluation),
  independent. [FPI_COMPRESSION.md](FPI_COMPRESSION.md).
