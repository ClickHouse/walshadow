# Pipeline backpressure and scaling

Remaining axes for the pooled decode+insert pipeline (M decoders / N
inserters). Pipeline substrate in [emitter.md](../emitter.md), bootstrap
tail wiring in [bootstrap.md](../bootstrap.md).

Target ClickHouse Cloud with SharedMergeTree. Per-INSERT cost is mostly
RTT plus object-storage part commit, so throughput comes from keeping
many INSERTs in flight; CPU parallelism matters only once decode cannot
feed inserters. Out-of-order INSERTs are fine — `_lsn` plus
`ReplacingMergeTree(_lsn)` makes final state order-independent
([[project_walshadow_eventual_consistency]]) — but slot feedback still
needs the strict contiguous durable watermark.

## Pump→worker bound (implemented — hard cap)

The pump→worker channel (`queueing_record_sink.rs`) is hard-bounded: a
`mpsc::channel(max_records / batch_size)` whose `send().await` blocks the
pump when full. Real backpressure — a sustained CH-slower-than-WAL run
parks the pump on the source socket (TCP backpressure → with a physical
slot + the `flush_lsn` cap, the source holds WAL on its disk, see
[source.md](../source.md)) instead of growing walshadow RAM without limit.

Deadlock-safe, even though `on_record` and shadow-wire delivery run
lockstep in `wal_stream.rs::drain_records` and
`ReorderSink::maybe_sweep_dropped` → `ShadowCatalog::wait_for_replay`
couples decode to shadow apply. Two facts break the feared deadlock:
`on_wire_chunk` fires *before* `on_record` per record, and the walsender
feeds shadow on an **independent** per-connection task (+keepalive-on-idle,
`shadow_stream.rs`), not from the pump. So a parked pump only withholds
*future* wire (`> resume`); every LSN a worker `wait_for_replay` can target
is a dispatched record's own `source_lsn`, always ≤ the highest wire
already enqueued (dispatch is FIFO ascending, wire precedes dispatch), and
that wire reaches shadow independently → replay advances → the wait clears.
The bytes always lead the wait target; the wait is never for bytes shadow
hasn't received.

The other two ingress channels need nothing: bootstrap page-walk→drain
is bounded (`BOOTSTRAP_TUPLE_CHANNEL_CAP`, a full channel parks the page
walk and the source fetch), and the ack-events channel stays unbounded
by design — the collector is pure-sync `state.apply`, strictly faster
than any producer, and bounding it would force `ack.acked` off its
non-blocking fire-and-forget on the inserter hot path.

## Bootstrap decode pool (Option B)

Gated on measurement: build only if page-walk decode cannot feed N
inserters. Bootstrap's heavy `decode_block_data` runs inside
`PageWalkSink::chunk` under the shared sink mutex, so even with
concurrent part fetch the tuple decode is serialized. The object-store
source likely hits this wall first — fetch/decompress/tar-parse already
fan out (`buffer_unordered`), decode is the next serial stage — so
raising `--bootstrap-object-store-parallelism` past ~1-2 yields little
until decode moves off the sink task.

Shape: `PageWalkSink::chunk` stops decoding, does only cheap framing
(8 KiB page slicing + `ItemIdData` slot walk), and emits raw
`(rfn, tuple_bytes)` units into a job channel; a bootstrap decode pool
runs `decode_block_data` + resolve + route. That job shape differs from
WAL's `DecodeJob` (already-decoded heaps), so either a bootstrap-specific
job variant or a shared `decode_and_route` — the latter requires
abstracting `detoast_heap` over a resolver trait first (it takes
`&Arc<Mutex<ShadowCatalog>>` today and needs only `relation_at`; detoast
no-ops when `chunks` is empty). See catalog-access notes in
[bootstrap.md](../bootstrap.md).

Unlocks the full S3 pipeline, three stages tuned independently against
bucket GET latency, page-walk CPU, and Cloud INSERT RTT:

```text
S3 parts (P concurrent fetch/decompress) -> page framing (sink, cheap)
  -> [decode x M] -> batcher -> [insert x N] -> CH Cloud
```

## DDL-type-aware barrier (out of scope)

`run_barrier` freezes the reorder task on `wait_all_durable` for the
whole pre-DDL backlog while the source advances; the cost scales with
in-flight backlog, not DDL frequency, and grows with N. ADD COLUMN needs
only seal plus epoch bump before post-ALTER rows decode; the full drain
is required only for destructive DDL (DROP/RENAME column, TRUNCATE).
Take up only if a measured workload shows the coarse drain binding
throughput at higher N.

## Hot-table batcher sharding

Shard `InsertBatcher` by `hash(pk)` only if one batcher bottlenecks.
Improves batcher parallelism but increases part count.

## Destination composition

The ack collector's per-seq refcount axis is per inserter;
[DESTINATIONS.md](DESTINATIONS.md) N:M routing adds a per-destination
axis that must compose with it before shipping.

## Sizing

* **N inserters** ~= `insert_round_trip / time_to_seal_one_batch`,
  enough to keep Cloud latency from binding throughput
* **M decoders** ~= enough that decode throughput >= aggregate insert
  throughput; keep M below N unless measurement shows a CPU bottleneck
* No client-side pipelining: Native protocol is request/response, one
  connection stays in query state until `EndOfStream`; concurrency means
  more connections

## Open questions

* Hot-table skew: intra-table sharding vs part-count cost
* Spill reads: parallel decode of large spilled xacts may need
  concurrent or per-xact-file spill readers
* Load test: many concurrent `AsyncClient`s on one runtime at high N
* Bootstrap decode bound: page-walk decode throughput vs N inserters,
  measured per source (object-store likely differs from direct)
* Bootstrap seq granularity: per-rfn vs per-N-rows, measured against
  ack-collector in-flight memory for a wide schema with many small rels
