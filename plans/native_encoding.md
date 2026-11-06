# Encode PostgreSQL values directly into ClickHouse blocks

Streaming commits plan rows straight from WAL bytes into column chunks, and
sealed Native blocks stay compressed until acknowledgement; see
[architecture](../architecture/README.md#commit-pipeline). Remaining work
removes owned cells from load paths and TOAST rows, sends prepared payloads,
and proves resource gains end to end. Follow [shared constraints](coordination.md),
[value coercion](value_coercion.md), and [performance acceptance](performance.md)

## Encode loads directly

COPY and heap-page/bootstrap loads still decode every tuple into `ColumnValue`
cells: [page walk](../src/backfill/backup_page_walk.rs) and
[COPY](../src/backfill/copy_backfill.rs) fill `BackfillTuple` slabs, and
[bootstrap drain](../src/emit/pipeline/bootstrap.rs) and
[gap replay](../src/backfill/wal_replay.rs) stage cell rows through
`RowBuffer`, the batcher's second input next to planned segments

Carry tuple bytes or COPY fields through load queues instead of cells. Read
visibility from tuple headers, which needs no columns. Encode visible rows into
chunks with the shared view encoder; decode cells only where pending rows,
deferred TOAST, and replacement evidence need them, keeping those mechanisms
and staging publication unchanged. Binary COPY fields are big-endian `typsend`
representations: add a field-view decoder beside the heap one, never reuse the
heap byte-copy rule. Then ship load rows as planned segments and delete
`RowChunk`/`RoutedRow` from the batcher

Version the deferred TOAST spool if its tuple format changes. Rewrite bootstrap
tests that inspect routed cells to read chunk columns

## Detoast without cells

Inputs whose images hold external pointers, stash-resolved records, and
directly buffered heaps plan through the owned-cell adapter, since
`detoast_heap` resolves cells. Resolve pointers from tuple views into bounded
scratch, then encode with the view path, preserving size policy, fill modes,
read-ahead transfer, and chunk generation fences. Compressed inline values and
`numeric`/`jsonb` already decode through per-row scratch

## Send prepared payloads

clickhouse-c `chc_block_prepare` / `chc_client_send_prepared`, bound in
clickhouse-c-rs as `BlockBuilder::prepare` and `send_prepared`, write prepared
Data bytes on connections negotiating the same compression and revision. The
clickhouse-c-rs walshadow depends on predates them, so
[sealed blocks](../src/emit/pipeline/sealed.rs) store a Native body compressed
by walshadow and each inserter decodes it back into a block to send,
recompressing on the wire. With prepared sends, seal through
`BlockBuilder::prepare` with the destination's negotiated codec and send
without decoding; rebuild through the decode path only for a connection that
rejects the block, such as a named destination configured differently. Count
rebuilds separately

## Bound memory across owners

Track allocated capacity, not lengths, for raw inputs, open column builders,
oracle cells and responses, sealed blocks, compression scratch, and transport
buffers, and keep them distinct from process RSS. Bound queue bytes as well as
item counts, including spare slab capacity and per-chunk metadata for small
rows across many tables. Oracle cells own one allocation each; pack them into
shared buffers if they dominate

Reserve conversion and compression workspace separately from queued output so
unfinished work cannot block its own completion. Specify acquisition and release
order before changing pools, including incompressible output and oversized rows

Evaluate compressing raw transaction segments if open inputs dominate RSS; keep
spill available regardless of ratio. Defer encoding before commit until
measurements justify its unresolved-cell and rollback state

## Acceptance

| Concern | Required evidence |
|---|---|
| Load coverage | Compare cell and direct output across COPY, direct bootstrap, base backup, object store, pending transactions, deferred TOAST, and restart using existing load fixtures |
| TOAST | Compare cell and view detoast across inline, compressed, external, unchanged, and same-transaction values under every value mode |
| Prepared send | Decode prepared blocks through existing Native reader; verify none/LZ4/ZSTD, mismatched destinations, and retries after an intermediate Data block and after server acceptance before reply |
| Recovery | Restart with open raw transactions, spilled plans, and ambiguous INSERT outcomes; verify exact destination state and resume floor across required destinations |
| Memory progress | Exhaust transaction/output budgets with many tables, a large transaction, and stalled inserters; prove spill reaches COMMIT and compression finishes without circular waits |

Measure narrow fixed rows, short strings, wide JSONB/text, TOAST-heavy updates,
mixed oracle tables, many small transactions, and a spilling large transaction
against `more-types` (`69f1700`) using `xact_spill`, `spool_read`, and
`bootstrap_pump` allocation reports, and source-to-destination with the
[benchmark driver](../bench/README.md). Run with unchanged worker counts,
batching limits, and codec settings. Record rows/s, source WAL bytes/s, CPU,
allocation count, peak/steady RSS, retained bytes by owner, spill volume,
`walshadow_resident_raw_bytes_total` over `walshadow_resident_stored_bytes_total`,
durable lag, blocks per INSERT, and actual ClickHouse part creation, with
compressible and incompressible payloads and both healthy and stalled
destinations. Attribute savings separately to compact transaction storage,
direct encoding, and compression. Choose block size and default compression
from measured CPU/memory/part tradeoffs; do not claim gains from wire
compression alone
