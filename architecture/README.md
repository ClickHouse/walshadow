# Architecture

walshadow consumes PostgreSQL physical WAL, replays filtered WAL in a shadow
PostgreSQL process, and reconstructs committed rows for ClickHouse

Use [docs](../docs/README.md) for supported behavior and operating procedures,
[plans](../plans/INDEX.md) for unfinished work, and linked source for implementation

## Why a shadow

Physical tuples need catalog state from when they were written. A static schema
snapshot cannot follow DDL or relation rewrites. Shadow replays source catalog
changes using PostgreSQL itself, while walshadow keeps versioned descriptors for
row decoding and uses PostgreSQL conversion for types it cannot decode locally

Filtering preserves WAL positions while replacing unwanted records or blocks
with valid placeholders. This keeps compatibility work in WAL transformation
instead of requiring a PostgreSQL recovery fork. Shadow retains catalogs and
recovery state, not ordinary user-table contents

## Streaming topology

![Streaming topology: original records feed a bounded queue and transaction buffer; filtered WAL feeds shadow; catalog capture supplies descriptor history and schema events to row processing](overview.svg)

`WalStream` retains original records for decoding and rewrites user-table
records to no-ops for shadow replay. Shadow receives filtered bytes through
walshadow's sender, with local segments as archive fallback

`CatalogCapture` holds publication at schema boundaries, reads shadow at an
exact replay position, persists descriptors, and attaches `SchemaEvent` to
`XactBuffer`. Queued row processing uses that history when decoding and
planning committed transactions

## Commit pipeline

![Commit pipeline: bounded DecodeJob queue fans out to M workers, rows merge through one batcher, InsertBatch queue fans out to N inserters, and separate Register, Placed and Acked events advance a contiguous watermark](workers.svg)

`BufferingDecoderSink` and `ReorderSink` share one record-queue worker
`[ch].decoder_pool_size` and `[ch].inserter_pool_size` size downstream pools
Each inserter owns a ClickHouse connection and can take any sealed batch

Sequence numbers identify work slices, not necessarily whole transactions
Only a commit's final slice publishes its LSN, after all earlier work finishes
Bounded queues and a shared payload budget limit work in flight; transaction
and plan data can spill to disk

## Design boundaries

| Diagram | Scope | Implementation |
|---|---|---|
| [Catalog capture and DDL](catalog.md) | Historical tuple layouts and ordered schema effects | [capture](../src/source/catalog_capture.rs), [reorder](../src/emit/pipeline/reorder.rs) |
| [TOAST and type conversion](values.md) | Historical large values and PostgreSQL conversion | [resolver](../src/toast/resolver.rs), [oracle](../src/ops/oracle.rs) |
| [Bootstrap](bootstrap.md) | Backup visibility, concurrent WAL, and initial-load publication | [backup](../src/backfill/backfill_bootstrap.rs), [window](../src/backfill/bootstrap_window.rs) |
| [Restart and cleanup](recovery.md) | Durable progress, retained history, and timeline crossing | [manifest](../src/source/manifest.rs), [status loop](../src/bin/stream.rs) |

Streaming wiring lives in [stream.rs](../src/bin/stream.rs), queue ownership
in [queueing_record_sink.rs](../src/source/queueing_record_sink.rs), and pool
assembly in [pipeline/mod.rs](../src/emit/pipeline/mod.rs)

## Diagram sources

SVGs are editable source. Rectangles identify components, dashed enclosures
identify processes or worker groups, narrow bars identify queues, and cylinders
identify stored state. Solid arrows carry data; dashed arrows carry progress
or control. Labels name messages, protocols, or state transferred

Use dark colors: warm neutral backgrounds and text, blue data paths, orange control
paths, green catalog paths, magenta stored state, and yellow ClickHouse borders

Keep diagrams and high-level explanations together here. Check connections
against linked source before updating diagrams. Leave data structures, protocol
layouts, function walkthroughs, and exhaustive terminology in source
