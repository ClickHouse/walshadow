# walshadow engineering index

Engineering rationale and invariants for walshadow. Start at
[overview.md](overview.md) for system shape, then drop into individual
components. User workflows and supported behavior live under
[`docs/`](../docs/README.md). Future-work proposals live under
[future/](future/INDEX.md). Cross-doc terminology is collected in
[GLOSSARY.md](GLOSSARY.md)

## Components

- [overview.md](overview.md) — system shape, filter contract, ordering invariants
- [filter.md](filter.md) — WAL filter, CRC rewrite, catalog tracker,
  dirty tree, rmgr-level keep/drop, NOOP-over-fork rationale
- [source.md](source.md) — START_REPLICATION PHYSICAL pump,
  `WalStream`, `StreamingWalker`, fan-out sinks, `QueueingRecordSink`,
  `DecoderSink`, walshadow walsender server
- [shadow.md](shadow.md) — shadow PG lifecycle, `ShadowCatalog` async
  libpq client, `RelDescriptor`, reconnect resilience
- [desc_log.md](desc_log.md) — durable descriptor log: boundary
  capture, interval lookups, ambiguity intervals, replay-from-log,
  seed + coverage horizon, GC against the resolved floor
- [decoder.md](decoder.md) — heap-tuple decoder, Tier 1/2 codec
  matrix, FPI decompression, `main_data` parsers, `pg_class_decoder`,
  read-time defaults
- [xact.md](xact.md) — `XactBuffer`, `SubxactTracker`, TOAST
  reassembly, commit-time stash + raw decode, local-disk spill + body
  spool, `DrainEntry` ordering
- [TOAST.md](TOAST.md) — TID-keyed `pg_toast_<relid>` CH mirror
  with delete tombstones + RMT-merge reclaim, as-of fetch,
  superseded-fill miss policy, bootstrap tap +
  defer-resolve; deferred R1 JOIN mode, streaming reassembly
- [emitter.md](emitter.md) — parallel decode+insert pipeline
  (reorder plan → execute → decode ×M → batcher → inserter ×N → ack
  watermark), transaction planner + plan spool, memory budget,
  `type_bridge`, synthetic columns, `DdlApplicator`, barrier fence
- [bootstrap.md](bootstrap.md) — greenfield BASE_BACKUP, `BackupSource`
  / `BackupSink` traits, `MultiplexSink`, `PageWalkSink` 2A decoder,
  shared insert tail, restart source fallback contract
- [ops.md](ops.md) — retention, manifest floor, standby-status triple,
  resume invariants
- [failover.md](failover.md) — source timeline crossing: frozen pause
  frontier, promotion gate, fork proofs,
  pipeline barrier at the fork, committed resume position, fork-segment
  prefix verification, shadow handoff order, lineage-aware resume and
  reconnect, slot proofs, parked refusals
- [oracle.md](oracle.md) — PgPending resolver, walshadow PG
  extension

## Future work

[future/INDEX.md](future/INDEX.md) collects design docs for unbuilt work:
runtime-config signals and net-new knobs, two-phase commit,
sequence-state replication, cross-table ordering, CH-bounce recovery,
parked operational polish. Once built, keep behavior in code and tests,
move user-facing consequences into `docs/`, and retain only rationale or
invariants which code cannot express

## Architecture diagrams

Live under [architecture/](../architecture/README.md). System-level
SVGs cover overview, internals, shadow communication, bootstrap
timeline, streaming timeline, restart timelines. Component SVGs cover
filter, source, shadow, decoder, xact, TOAST, emitter, bootstrap, ops,
and oracle. Updated on architecturally load-bearing changes

## Regenerating diagrams

Each `architecture/<comp>.dot` carries its own regeneration spec as a
header comment (sources of truth, subsumed plan section, quality bar);
shared style invariants live in [`architecture/palette.md`](../architecture/palette.md).
Workflow in [`architecture/README.md`](../architecture/README.md#regenerating-a-diagram).
Use when regenerating a component diagram after material code change
