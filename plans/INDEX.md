# walshadow plan index

Component docs for walshadow's current implementation state. Start at
[overview.md](overview.md) for system shape, then drop into individual
components. Future-work proposals live under [future/](future/INDEX.md).
Cross-doc terminology is collected in [GLOSSARY.md](GLOSSARY.md)

## Components

- [overview.md](overview.md) — system shape, supported PG versions,
  filter contract, ordering invariants, acceptance gates
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
  (`disabled`/`clickhouse`), delete tombstones + RMT-merge reclaim,
  as-of fetch, superseded-fill miss policy, bootstrap tap +
  defer-resolve; deferred R1 JOIN mode, streaming reassembly
- [emitter.md](emitter.md) — parallel decode+insert pipeline
  (reorder plan → execute → decode ×M → batcher → inserter ×N → ack
  watermark), transaction planner + plan spool, memory budget,
  `type_bridge`, synthetic columns, `DdlApplicator`, barrier fence
- [bootstrap.md](bootstrap.md) — greenfield BASE_BACKUP, `BackupSource`
  / `BackupSink` traits, `MultiplexSink`, `PageWalkSink` 2A decoder,
  shared insert tail, restart source fallback contract
- [ops.md](ops.md) — preflight, metrics, retention, manifest (floor,
  6 LSNs), standby-status triple, kill-restart drill
- [control.md](control.md) — in-process control plane: `ctl` unix-socket
  line protocol, base+`conf.d` config merge (API writes only its
  `50-api.toml` fragment), live reload (mappings/budgets/CH-conn/table
  selection/pause) with no restart, config-driven table opt-in, pause as
  `[stream] paused`, `Reloader` (no session lifecycle)
- [oracle.md](oracle.md) — PgPending resolver, walshadow PG
  extension
- [clickhouse-c-rs Safety model](../clickhouse-c-rs/README.md#safety-model)
  — FFI trust boundary, `Client<'fd>` lifetime shape, `PosixIo`
  `BorrowedFd` discipline, packet-payload union

## Future work

[future/INDEX.md](future/INDEX.md) collects design docs for unbuilt work:
runtime-config signals and net-new knobs, two-phase commit,
sequence-state replication, cross-table ordering, CH-bounce recovery,
parked operational polish. Promote built behavior into `plans/`

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
