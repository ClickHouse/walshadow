# Encode PostgreSQL values directly into ClickHouse blocks

Commits, loads, and gap replay encode rows from tuple bytes or COPY fields into
column chunks, and sealed Native blocks stay compressed until acknowledgement;
see [architecture](../architecture/README.md#commit-pipeline). Remaining work
bounds memory across owners and proves resource gains end to end. Follow [shared constraints](coordination.md),
[value coercion](value_coercion.md), and [performance acceptance](performance.md)

Share view encoders and column buffers with [container codecs](tier2.md)
Container promotion can retain existing raw pending cells in owned adapters

## Bound memory across owners

Track allocated capacity, not lengths, for raw inputs, open column builders,
oracle cells and responses, compression scratch, and transport buffers, and keep
them distinct from process RSS. Bound queue bytes as well as
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
`walshadow_resident_raw_bytes_total` over `walshadow_resident_stored_bytes_total`
(stored includes allocation slack), durable lag, blocks per INSERT, and actual
ClickHouse part creation, with compressible and incompressible payloads and
both healthy and stalled destinations. Attribute savings separately to compact
transaction storage, direct encoding, and compression. Choose block size and default compression
from measured CPU/memory/part tradeoffs; do not claim gains from wire
compression alone
