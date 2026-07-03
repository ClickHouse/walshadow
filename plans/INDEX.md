# walshadow plan index

Component docs for walshadow's current implementation state. Start at
[overview.md](overview.md) for system shape, then drop into individual
components. Future-work proposals live under [future/](future/INDEX.md).
Cross-doc terminology is collected in [GLOSSARY.md](GLOSSARY.md)

## Components

- [overview.md](overview.md) — system shape, supported PG versions,
  filter contract, ordering invariants, acceptance gates
- [filter.md](filter.md) — WAL filter, CRC rewrite, catalog tracker,
  rmgr-level keep/drop, NOOP-over-fork rationale
- [source.md](source.md) — START_REPLICATION PHYSICAL pump,
  `WalStream`, `StreamingWalker`, fan-out sinks, `QueueingRecordSink`,
  `DecoderSink`, walshadow walsender server
- [shadow.md](shadow.md) — shadow PG lifecycle, `ShadowCatalog` async
  libpq cache, `RelDescriptor`, `SchemaEvent` channel, reconnect
  resilience
- [decoder.md](decoder.md) — heap-tuple decoder, Tier 1/2 codec
  matrix, FPI decompression, `main_data` parsers, `pg_class_decoder`,
  read-time defaults
- [xact.md](xact.md) — `XactBuffer`, `SubxactTracker`, TOAST
  reassembly, local-disk spill, `DrainEntry` ordering
- [TOAST.md](TOAST.md) — full TOAST chunk store (`disabled`/`disk`/
  `clickhouse`), bootstrap tap + defer-resolve, pre-window miss→fetch;
  deferred R1 JOIN mode, chunk GC, streaming reassembly
- [emitter.md](emitter.md) — parallel decode+insert pipeline
  (reorder → decode ×M → batcher → inserter ×N → ack watermark),
  `type_bridge`, synthetic columns, `DdlApplicator`, barrier fence
- [bootstrap.md](bootstrap.md) — greenfield BASE_BACKUP, `BackupSource`
  / `BackupSink` traits, `MultiplexSink`, `PageWalkSink` 2A decoder,
  shared insert tail
- [ops.md](ops.md) — preflight, metrics, retention, cursor file (v2,
  6 LSNs), standby-status triple, kill-restart drill
- [oracle.md](oracle.md) — differential decode oracle, walshadow PG
  extension, `--validate` sampling
- [clickhouse-c-rs Safety model](../clickhouse-c-rs/README.md#safety-model)
  — FFI trust boundary, `Client<'fd>` lifetime shape, `PosixIo`
  `BorrowedFd` discipline, packet-payload union

## Future work

[future/INDEX.md](future/INDEX.md) collects design docs for unbuilt work:
runtime config overlay from source PG, two-phase commit, sequence-state
replication, cross-table ordering, CH-bounce recovery, parked operational
polish. Promote into `plans/` once an item is built

## Architecture diagrams

Live under [architecture/](../architecture/README.md). System-level
SVGs cover overview, internals, shadow communication, bootstrap
timeline, streaming timeline, restart timelines. Per-component SVGs
(one per file in this index, embedded inline) cover filter, source,
shadow, decoder, xact, emitter, bootstrap, ops, oracle. Updated
on architecturally load-bearing changes

## Regenerating diagrams

Each `architecture/<comp>.dot` carries its own regeneration spec as a
header comment (sources of truth, subsumed plan section, quality bar);
shared style invariants live in [`architecture/palette.md`](../architecture/palette.md).
Workflow in [`architecture/README.md`](../architecture/README.md#regenerating-a-diagram).
Use when regenerating a component diagram after material code change
