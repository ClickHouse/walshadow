# Measure before scaling

Use [benchmark workload](../bench/README.md) to identify whether source transfer,
filtering, catalog replay, decoding, type conversion, or ClickHouse inserts limit
throughput. Record workload, versions, hardware, concurrency, memory, and lag
Do not retain predicted speedups as evidence

Correctness tests should assert outcomes without comparing wall-clock speed
Keep performance runs on dedicated hardware, separate from shared CI runners
Publish repeatable baselines and their variance before choosing regression bands

## Initial load

Initial-load and greenfield-bootstrap workloads already exist, including fresh
table seeding, stage metrics, completion checks, and row verification. Use
[initial-load driver](../bench/src/initial_load.rs) rather than adding another
benchmark framework

Establish comparable baselines for copy, direct backup, and object-store modes
Measure source read cost, transfer volume, resident memory, spill, and insert
saturation alongside current timing reports. Add concurrent-write cases only
with an exact final-state oracle; approximate row counts are insufficient when
replay can temporarily duplicate rows

Use existing stage metrics to locate missing attribution before adding counters
Check process uptime and cumulative series remain valid across phase changes

## Candidate changes

| Measured limit | Candidate |
|---|---|
| Bootstrap page decode cannot feed inserters | Add decode workers behind the per-entry page walk |
| ClickHouse round trips leave workers idle | Tune inserter count and batch size within memory and part-count budgets |
| One hot table saturates batcher | Shard by stable row key, measure extra part cost |
| Catalog-boundary holds dominate | Evaluate [replay callback](custom_rmgr.md) |
| Filter CRC consumes a core | Parallelize independent record work while preserving output order |
| Per-record allocation dominates | Evaluate inline storage for small block-reference lists |
| Catalog refresh work dominates | Narrow invalidation only after measuring recapture and cache cost |
| TOAST fetch or materialization dominates | Compare current store with [shadow proposal](shadow_toast.md) |

Bootstrap workers change ordering: assign acknowledgement sequence before
parallel decode and account for concurrent deferred-TOAST writers. Preserve
bounded queues, shared memory budget, and contiguous durable acknowledgements

DDL barriers currently drain earlier work. Relax barriers by operation only
after demonstrating a throughput problem and proving rows cannot cross an
incompatible schema or destructive operation

WAL parser separates block headers from payloads because record layout does
too. Do not merge passes based only on repeated-loop appearance. Profile actual
cost before changing framing or allocation

## Bootstrap worker shape

Each tap entry decodes its own segment, so add decode workers only after
profiling shows per-entry decode rather than source concurrency as the limit
Keep page framing and slot walk in producer; submit owned tuple bytes, full
physical locator, load boundary, and completion identity to bounded workers
Reuse decode/resolve/route logic where job semantics match. WAL jobs already
containing decoded heaps need not share bootstrap wire shape

Assign sequence and expected completion counts before worker reordering. Current
[bootstrap drain](../src/emit/pipeline/bootstrap.rs) infers sequence boundaries
from consecutive relation locators, which cannot survive out-of-order workers
Account explicitly for filtered, failed, and deferred tuples before closing a
sequence. Compare per-relation versus fixed-row grouping by acknowledgement-state
memory, especially many small relations and wide rows

External-pointer detection happens after decode, so workers become concurrent
writers to deferred storage. Choose serialized spool writer or per-worker spools
with deterministic merge and restart ownership. Preserve [open-xact carry](bootstrap.md)
and [shadow TOAST readiness](shadow_toast.md), bound both queued bytes and jobs

Tune fetch/decompress concurrency, decode workers, and inserter connections
independently. Estimate inserter demand from batch production rate and measured
round-trip latency, then verify part count, memory, and runtime saturation
Parallel spilled transactions need concurrent readers or separate file handles

## Shared ordering and deferred investigations

Re-prove byte-before-record replay reachability when splitting pump and worker
Current independent walsender progress cannot resolve a wait for bytes a new
[replay callback](custom_rmgr.md) or [TOAST fence](shadow_toast.md) withholds

Hot-table sharding must keep stable key ownership and compose inserter completion
counts with future [destination](extensions.md#multiple-clickhouse-destinations)
counts. For weaker DDL barriers, distinguish seal/epoch changes from destructive
drains and prove mapping changes cannot overtake rows under old schema

Measure shadow redo saturation, cache invalidation breadth, filter CRC cost, and
allocation separately. Consider per-relation invalidation and record-parallel CRC
only for measured limits, preserving classification and publication order
Compare bounded parallel filtering before considering maintenance of a PostgreSQL
fork. Avoid carrying unsupported parallel-recovery or predicted-speedup claims

Revisit shared ClickHouse test-server fixture when repeated startup dominates
suite. Preserve per-test database isolation and fault-test ownership; sharing a
server that another test kills would invalidate outage evidence

## Completion

For each optimization, keep one reproducible workload showing bottleneck before
and after, with correctness checks and memory measurements. Repeat apparent
regressions before attributing them to code. Delete task-specific microbenchmarks
once their question is settled unless they remain useful regression workloads
