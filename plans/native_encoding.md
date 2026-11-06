# Encode PostgreSQL values directly into ClickHouse blocks

Build on `more-types` at `69f1700` (`Decode more PostgreSQL fixed-width types
locally`). Replace long-lived decoded row graphs with compact PG inputs and
direct column encoding. Retain transaction coordination, then compress sealed
output blocks while queued or retained for retry.

Keep three changes independently reviewable: transaction storage, transcoding,
and compressed output ownership. Measure each against `more-types` before
combining results. Follow [shared constraints](coordination.md),
[value coercion](value_coercion.md), and [performance acceptance](performance.md).

## Preserve more types behavior

Reuse branch codecs, target mappings, defaults, and type coverage:

| Source types | Existing branch behavior to preserve |
|---|---|
| `oid`, `xid`, `cid` | `UInt32` |
| `oid8`, `xid8`, `pg_lsn` | `UInt64`; retain `oid8` availability on PG 19+ |
| `money` | `Int64` containing source minor units; do not introduce decimal rescaling |
| `name` | Trim NUL padding and preserve existing text behavior |
| `macaddr`, `macaddr8`, `tid` | Render existing PostgreSQL-compatible text |

Keep source type identity beside storage class. An eight-byte value alone
cannot distinguish timestamp epoch conversion, unsigned integers, and money.
Share rendering helpers from branch's `src/decode/codecs/fixed.rs` rather than
copying their logic into new encoders. Preserve COPY coverage without assuming
every newly local heap type also uses binary COPY.

## Replace representation and retain coordination

Current [heap decoder](../src/decode/heap_decoder.rs) constructs owned
`ColumnValue` cells. [Transaction buffer](../src/xact/xact_buffer.rs) retains
decoded heaps and TOAST entries until commit or abort.
[Planner](../src/emit/pipeline/planner.rs) resolves TOAST and routing, then
[plan spool](../src/emit/pipeline/plan_spool.rs) retains or serializes decoded
heaps. [Batcher](../src/emit/pipeline/batcher.rs) traverses rows again to fill
`ColumnBuf` slabs. [Resolver](../src/emit/pipeline/resolver.rs) converts remaining
oracle columns before [inserter](../src/emit/pipeline/inserter.rs) builds and
sends Native blocks. Sealed batches retain uncompressed slabs.

Use this proposed flow:

```text
WAL -> compact PG inputs owned by xid -> commit merge
    -> resolve descriptor, route, TOAST, old and new key
    -> append directly to column chunks in private transaction plan
    -> validate and seal plan
    -> coalesce compatible committed chunks across transactions
    -> resolve oracle columns -> serialize and compress Native blocks
    -> retain compressed blocks through INSERT acknowledgement
```

Encode after commit initially so subtransaction rollback never requires editing
compressed columns, and descriptor/TOAST resolution precedes final encoding.
Compression here reduces retained output, not open transaction inputs. Evaluate
compression of raw transaction segments separately if those inputs dominate RSS;
keep spill available regardless of compression ratio. Defer Native encoding
before commit until measurements justify its unresolved-cell and rollback state.

Keep `XactBuffer` responsible for aborts, subtransactions, WAL ordering, spill,
and resume floor. Replace its heap payload, not its correctness responsibilities.
Keep `ColumnValue` for runtime configuration, SQL defaults, diagnostics, and
temporary adapters where useful. Remove it from ordinary retained row payloads;
deleting enum globally is not a completion requirement.

## Store compact inputs before commit

Introduce an owned heap input containing writer xid, source LSN, physical
identity, descriptor span key when covered, and bytes needed to reconstruct
logical old/new images. Preserve absent versus SQL NULL, dropped attributes,
missing defaults, operation flags, and partial-image diagnostics.

Start from existing `RawRecord` ownership and reconstruction in
[spill codec](../src/xact/spill.rs). Reuse `drop_redundant_images`; retain images
when decoding actually requires them. Do not retain a complete WAL segment or
backup page through a small borrowed tuple slice. Compact further only after
measuring retained bytes against decoded rows, including full-page images and
multi-insert records.

Give covered raw heaps a distinct representation from unresolved catalog stash
records. Existing `SpillEntry::Raw` carries commit-time stash classification;
routing every ordinary heap through that arm would change discard and rejection
semantics. Pin covered descriptor versions exactly as `DescribedHeap` does;
resolve uncovered records through existing fail-closed stash verdicts.

Keep TOAST chunk and delete handling intact during initial migration. Preserve
chunk generation fences, store references, same-transaction births, and mirror
row ordering. Decode small runtime configuration rows through existing adapter
until a separate reader is useful.

Replace speculative owned `Predecoded` row graphs in
[queue](../src/source/queueing_record_sink.rs) and `ReorderSink::predecoder`
with bounded framing or borrowed scans. Preserve TOAST read-ahead for unchanged
external values without retaining decoded copies. Measure lost decode overlap
and read-ahead effectiveness before removing existing path.

## Transcode during transaction planning

Split tuple walking from owned cell construction. Expose short-lived views over
fixed bytes, varlena bodies, SQL NULL, missing attributes, and external pointers.
Borrow from current input or bounded detoast scratch; never let views outlive
their owners. Retain a cell-producing adapter for remaining callers.

Compile conversion decisions once per descriptor version and frozen route,
using source type and destination type together. Keep mapping changes and
schema epochs in cache identity. Reuse existing codecs for epoch shifts, UUID
layout, numeric rendering, JSONB, interval, inet, and branch's added types.
Append formatted output directly where helpers allow it; accept bounded scratch
when a codec needs materialization.

Coordinate [local container codecs](tier2.md) around recursive column builders.
Do not require a retained nested `ColumnValue` graph when array or map support
can append directly; preserve oracle fallback for uncovered source/target pairs.

| Input | Direct encoding rule |
|---|---|
| Compatible fixed heap datum | Copy validated little-endian bytes into matching column storage |
| Binary COPY field | Decode big-endian `typsend` representation; never reuse heap byte-copy rule |
| Inline text or bytea | Strip storage framing and append body after existing validation |
| Compressed or external varlena | Resolve and decompress before encoding; bound scratch and preserve size policy |
| Missing default | Apply shared default conversion with source descriptor and target policy |
| Oracle type or configured composite target | Retain compact raw/text conversion input; preserve oracle fallback |

Do not treat equal width as type compatibility. Preserve configured substitutes
and existing errors; compose outstanding coercion fixes without introducing a
second policy implementation. Keep absence defaults for tombstones distinct
from real source NULL. Preserve NaN payloads and signed zero where existing
fixed-width mappings permit bit-preserving output.

Inspect old and new replica identity before releasing either view. Emit old-key
tombstones for key changes, choose correct image for DELETE, and keep configured
append-only delete policy. Preserve source `_lsn` and writer `_xid`; attach commit
timestamp after commit is known. Keep load-specific synthetic versions unchanged.

Store encoded chunks and oracle inputs in private transaction plan. Seal chunks
at row/byte limits, schema or route changes, and ordered control entries. Preserve
TOAST mirror row positions and truncate fences. Keep all existing planning
rejections before transaction effects; do not send partially planned chunks.
Oracle transport and conversion remain explicit later failure points.

Replace heap frames in plan spool with versioned encoded frames once adapter
parity passes. Retain plan memory/disk caps and bounded replay. Reuse chunk
metadata across rows instead of copying descriptor and routing ownership per
cell. Treat plans and transaction spills as scratch recovered from source WAL,
not as a new durable destination queue.

## Preserve batching across transactions

Accept column chunks in batcher instead of `RoutedRow` graphs. Coalesce adjacent
compatible small chunks by appending fixed bytes and rebasing string offsets,
null maps, and oracle cell ranges. Preserve `(seq, row_count)` contributions.
Keep compatibility keyed by destination, schema epoch, route, and encoding plan.

Separate Native block size from INSERT size. Seal bounded blocks, then group
compatible blocks from multiple transactions into one INSERT under existing row,
byte, and deadline triggers. Do not compress tiny per-transaction blocks and
assume concatenating their byte streams produces one merged Native block.
Send several Data blocks under one query when grouping sealed blocks.

Preserve FIFO `FlushAll`, destructive-operation drains, and configuration
barriers. Never move chunks across DDL or TRUNCATE to improve compression.
Measure actual destination parts: several blocks in one INSERT do not establish
unchanged part creation by themselves.

## Compress sealed output blocks

Resolve oracle columns before final serialization. Serialize final Native block
body and compress once; release column slabs and oracle output after compressed
owner is admitted. Keep schema/route metadata and acknowledgement counts beside
payload. Retain original logical row/byte counts for batching independently of
compressed resident size. Start with existing configured codecs and measure
incompressible expansion as well as useful compression.

Add a narrow prepare/send interface in `clickhouse-c` and `clickhouse-c-rs`.
Current client accepts `BlockBuilder` through `send_data`; it does not expose an
owned prepared payload send API. Keep serialization, compression framing, packet
state, and transport handling in dependency rather than duplicating protocol in
walshadow. Read existing [client](../pgext/pg-clickhouse-c/clickhouse-c/doc/clickhouse-client.md)
and [compression](../pgext/pg-clickhouse-c/clickhouse-c/doc/clickhouse-compression.md)
contracts before choosing interface.

Prepare immutable block payload with explicit codec and protocol compatibility
requirements; generate connection-specific packet framing at send. Validate
negotiated revision and serialization settings on reconnect and across named
destinations. For incompatible connections, reject reuse or rebuild through a
bounded decode/re-encode path. Do not replay captured Query packets, session
state, or unchecked raw socket bytes.

Stream prepared payload through bounded transport buffers. Account for any C
output-buffer copy and retained capacity; adding compressed owner while keeping
slabs or an unbounded transport copy defeats resident-memory goal. If prepared
send cannot land first, allow storage compression with one bounded decompression
buffer per inserter as an intermediate step, and measure its CPU cost separately.

Retain every block in an INSERT until `EndOfStream` from every required
destination. Retry entire INSERT after ambiguous failure using unchanged rows,
versions, block order, and metadata. Acknowledge its combined sequence counts
only after success. Keep counts logical, independent of compression frames.
Compression, cancellation, and transport errors must neither drop ownership nor
advance contiguous acknowledgement.

## Bound resident memory and preserve progress

Track allocated capacity for raw inputs, open column builders, oracle inputs and
responses, sealed compressed blocks, compression scratch, and transport buffers.
Keep decoded/encoded sizes distinct from compressed allocation and process RSS.
Bound queue bytes as well as item counts; include spare slab capacity and
per-chunk metadata when measuring small rows and many tables.

Preserve separate transaction spill and output admission mechanisms. If retaining
an open transaction would exhaust resident allowance, spill and continue reading
WAL; never wait for its own COMMIT while blocking ingestion. Reserve bounded
conversion/compression workspace separately from queued output so memory held by
unfinished work cannot prevent its completion. Specify acquisition and release
order before changing pools, including incompressible output and oversized rows.

Keep startup scratch cleanup and source-WAL recovery. Version changed spill and
plan formats; never interpret old frames under new layout. Retain first-record
resume floor until corresponding transaction output is durable. Test restart
from recorded source positions rather than treating a compressed buffer as proof
of persistence.

## Implementation sequence

1. Record baseline on `more-types`. Extend existing `xact_spill`, `spool_read`,
   and `bootstrap_pump` benchmarks with retained allocation measurements and
   exact result comparison. Establish source-to-destination baseline using
   existing benchmark driver.
2. Add borrowed tuple views and direct column append behind internal adapters.
   Keep production ownership unchanged while comparing against existing encoder.
   Cover branch's added types and configured target overrides first.
3. Replace ordinary transaction heap storage with owned PG inputs. Reuse spill,
   keep covered descriptors separate from unresolved stash, and remove retained
   predecoded copies. Preserve temporary cell adapter at commit during migration.
4. Fuse commit planning and encoding. Store column chunks in private plan,
   replace typed heap frames, and accept chunks in batcher. Delete superseded
   row materialization paths after parity; avoid a permanent second pipeline.
5. Introduce prepared payload support in dependencies, then compressed block
   ownership and multi-block INSERTs. Validate retries and memory handoff before
   enabling resident compression by default.
6. Apply shared direct append to COPY and heap-page/bootstrap loads. Keep
   visibility, pending rows, deferred TOAST, replacement evidence, and staging
   publication unchanged. Stop retaining enum rows in ordinary load queues.
7. Reconcile end-to-end results and publish per-stage resource comparisons.
   Update settled architecture and operator documentation; remove completed
   proposal sections and retain unresolved acceptance work.

## Acceptance

Extend existing behavioral suites rather than creating a parallel harness:

| Concern | Required evidence |
|---|---|
| Type fidelity | Compare old/direct output across `types_sweep`, `emitter_native_types`, oracle and bootstrap type suites; include branch types, defaults, NULL, empty values, signed boundaries, float bits, timestamp epochs, UUIDs, numeric, and JSONB |
| Source representation | Compare WAL/heap and binary COPY outputs; exercise inline, compressed, external, and unchanged TOAST values |
| Transaction outcomes | Extend `xact_buffer`, `subxact`, and bootstrap crossing-transaction cases with interleaved parent/subxid rows, savepoint rollback, abort after spill, and late subxid assignment |
| Ordering | Extend grouping/spill, schema evolution, parallel DDL, runtime config, and truncate tests with chunk boundaries on both sides of controls |
| Planning failures | Force missing descriptor, malformed input, missing TOAST, and spool-cap failure after earlier chunks have sealed; observe no transaction effects from rejected plan |
| Oracle coexistence | Mix direct and oracle columns, oracle retries, local literal resolution, and conversion errors without rebuilding retained row graphs |
| Compressed output | Decode prepared blocks through existing Native reader; verify none/LZ4/ZSTD, incompressible data, large values, nullable strings, frame boundaries, and multiple blocks per INSERT |
| Retry and cancellation | Disconnect after an intermediate Data block and after server acceptance before reply; cancel compression, queue handoff, and send; verify replay, ownership cleanup, and pinned acknowledgement |
| Recovery | Restart with open raw transactions, spilled plans, and ambiguous INSERT outcomes; verify exact destination state and resume floor across required destinations |
| Memory progress | Exhaust transaction/output budgets with many tables, a large transaction, and stalled inserters; prove spill reaches COMMIT and compression can finish without circular waits |
| Load coverage | Exercise COPY, direct bootstrap, base backup, object store, pending transactions, deferred TOAST, and restart using existing load fixtures |

Measure narrow fixed rows, short strings, wide JSONB/text, TOAST-heavy updates,
mixed oracle tables, many small transactions, and a spilling large transaction.
Run with unchanged worker counts, batching limits, and codec settings. Record
rows/s, source WAL bytes/s, CPU, allocation count, peak/steady RSS, retained bytes
by owner, spill volume, compression ratio, durable lag, blocks/INSERT, and actual
ClickHouse part creation. Compare compressible and incompressible payloads with
both healthy and stalled destination.

Require exact reconciliation and no new progress stalls before accepting resource
gains. Attribute savings separately to removal of enum rows, compact transaction
storage, and compression. Choose block size and default compression from measured
CPU/memory/part tradeoffs; do not claim gains from wire compression alone.
