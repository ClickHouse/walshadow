# Parallel decode and insert, pooled decoders feeding pooled inserters

Move serial decode+send tail into two pools:

* M decoders for CPU work: tuple parse, catalog projection, type coercion,
  detoast, Native encoding
* N inserters for ClickHouse IO: one `AsyncClient` connection per task

Target ClickHouse Cloud with SharedMergeTree. Per-INSERT cost is mostly
RTT plus object-storage part commit, so throughput comes from keeping
many INSERTs in flight. CPU parallelism matters only once decode cannot
feed inserters.

`clickhouse-c-rs-async` has landed. Current emitter uses one
`AsyncClient` in `ch_emitter.rs`, so IO no longer parks tokio workers, but
one connection still serializes INSERTs at `EndOfStream`. Remaining work
is fan-out from one connection to N inserter tasks, with shared
`InsertBatcher` stage and cumulative ack accounting.

Out-of-order INSERTs are OK. `_lsn` plus `ReplacingMergeTree(_lsn)` makes
final state order-independent (`ch_ddl.rs`,
[[project_walshadow_eventual_consistency]]). Slot feedback still needs a
strict contiguous durable watermark.

## Status (2026-06-07) — pooled WAL path green at M=1/N=1

Pipeline built in `src/pipeline/` (`mpmc`, `ack`, `batcher`, `inserter`,
`decode`, `reorder`), wired into `bin/stream.rs` behind
`--decoder-pool-size` / `--inserter-pool-size` (default 1/1). With `--ch-config`
set the WAL path always runs the pooled pipeline (`PipelineConfig::spawn`);
bootstrap still routes through the serial `Emitter`.

Done:

* Five P0 correctness invariants in code: TRUNCATE-as-barrier,
  ack-after-drain, contiguous-done watermark, aborts through gate, accurate
  per-seq counts
* Watermark, mpmc, batcher coalescing unit-tested hermetically
* `FlushAll` race fixed: rows + flush share one FIFO `BatcherMsg` channel,
  reorder waits on ack collector's placed-frontier before issuing `FlushAll`.
  Was a nondeterministic hang in the `flush_all` batcher test at
  `flush_timeout=3600s` (caught by codex, not the design review)
* Clean build + clippy, serial/bootstrap path untouched, 376 lib tests plus
  hermetic integration tests green
* `pgbench_acceptance_ddl_intermix` (live PG+CH, pooled WAL path at M=1/N=1,
  ADD COLUMN + CREATE INDEX CONCURRENTLY mid-workload) green 5/5, ~22s.
  Resolves the 2026-06-05 watermark-pin failure. Root cause was per-INSERT RTT
  cost at `flush_timeout=0`: each pgbench xact's four per-table INSERT closes
  cost 4×RTT (~5 xact/s on local CH), so `emitter_ack` could never track the
  ~700 xact/s source. `--ch-flush-timeout-ms 200` coalesces rows into one part
  per window and lets N=1 hold the source rate. Barrier +
  `wait_for_ack_catchup` both pass, so the coarse barrier is not regressing at
  this scale

Ship-blockers remaining:

* Pool sizes >1 have in-process e2e coverage but no live daemon coverage.
  `tests/pipeline_parallel_e2e.rs` (DML) and `tests/pipeline_parallel_ddl_e2e.rs`
  (ALTER ADD COLUMN + TRUNCATE through `run_barrier`) drive the real
  `PipelineConfig` fan-out at M=2/N=2. `pgbench_acceptance_ddl_intermix`, the
  daemon-spawn live test, still hardcodes default 1/1 with no knob. Validate N
  concurrent `AsyncClient`s under the DDL barrier in the spawned daemon too,
  confirming out-of-order INSERTs across connections stay `_lsn`-correct
* Bootstrap/base-backup still routes through serial `Emitter`, bypassing pool,
  retry, ack, backpressure. Target shape in "Base backup through the same
  pipeline" below

Known gaps:

* `tests/pipeline_e2e.rs`, `tests/bootstrap_direct_ch.rs` still exercise the
  serial emitter, not `PipelineConfig`. The pooled path has live daemon-spawn
  coverage via `pgbench_acceptance_ddl_intermix` (M=1/N=1) and in-process
  coverage via `tests/pipeline_parallel_{e2e,ddl_e2e}.rs` (M=2/N=2, including
  the DDL + TRUNCATE barrier)
* Backpressure goal unmet: pump-to-worker still `mpsc::UnboundedSender`
  (`queueing_record_sink.rs`), ack events still `mpsc::unbounded_channel`
  (`ack.rs`)

Next:

1. Daemon e2e with pool sizes >1; confirm out-of-order INSERTs across N
   connections stay `_lsn`-correct through the DDL barrier
2. Move bootstrap onto the shared batcher/inserter/ack tail (see "Base backup
   through the same pipeline")
3. Replace unbounded pump-to-worker + ack channels with bounded ones to close
   the backpressure chain
4. DDL barrier optimization, deferred now that N=1 tracks the source: make it
   DDL-type-aware. ADD COLUMN needs only seal plus epoch bump before post-ALTER
   rows decode, no global `wait_all_durable`. Reserve the full drain for
   destructive DDL (DROP/RENAME column, TRUNCATE)

## Current serial path

* `bin/stream.rs`, multi-thread tokio runtime
* `bin/stream.rs` pump loop, reads WAL chunks, `WalStream::push`
  frames, filters, NOOP-rewrites, clones records to `'static`, queues to
  `QueueingRecordSink`, returns
* `queueing_record_sink.rs`, one worker owns `BufferingDecoderSink`,
  `XactRecordSink`, `Emitter`; decode waits for COMMIT so
  `wait_for_replay` does not stall pump
* `xact_buffer.rs`, per-xid accumulation, spill past `xact_buffer_max`
  (64 MiB), k-way merge by source LSN at commit, abort advances ack
* `ch_emitter.rs`, per-table encoders, seal on `row_budget`,
  `byte_budget`, `flush_timeout`; `close_all_open_inserts` advances
  `last_durable_commit_lsn` after all table buffers drain
* Watermark path: `on_xact_end` -> `last_durable_commit_lsn` ->
  `emitter_ack_lsn` (`xact_buffer.rs`) ->
  `apply_lsn = min(shadow_replay, emitter_ack)` (`bin/stream.rs`) ->
  standby status (`source_feed.rs`) -> slot recycle

Break only serial decode+send tail. Keep framing and commit-order
assignment single-threaded. Preserve contiguous durable watermark.

## Shape

Cloud SMT constraints:

* Use N connections to hide RTT + PUT latency
* Do not shrink batches, each INSERT creates at least one part
* Coalesce rows before sending, reuse `row_budget`, `byte_budget`,
  `flush_timeout`
* Rely on `_lsn` dedup, not ClickHouse block-hash insert dedup

Pipeline:

```text
pump -> reorder -> decode pool -> InsertBatcher pool -> inserter pool -> CH Cloud
              \                                      /
               \-> ack collector -> emitter_ack_lsn /
```

Stages:

* **Pump**, one task, unchanged
* **Reorder**, one task, assigns dense commit-order `seq#` plus
  `commit_lsn`, flags DDL barriers
* **Reorder -> decoders**, mpmc job queue, one job per committed xact
  (`flume` or `async-channel`; tokio mpsc is single-consumer)
* **Decoders -> `InsertBatcher`**, route rows by target table to owning
  batcher
* **`InsertBatcher` -> inserters**, one shared mpmc queue of sealed
  batches; any idle inserter can take any batch, so hot tables can use
  more than one connection
* **Inserters -> ack collector**, report each sealed batch's xact `seq#`
  set after `EndOfStream`

## InsertBatcher

Do not send per-xact blocks directly to inserters. CDC often emits small
xacts; per-xact INSERTs would flood SMT with small parts.
`InsertBatcher` actors own per-table accumulation and emit budget-sized
Native batches. Decoders produce rows; batchers produce INSERT-ready
batches; inserters only send and ack.

Each sealed batch carries set of xact `seq#`s whose rows it contains.
Ack collector uses that set for durability accounting.

## Ack collector

Everything upstream may complete out of order. `emitter_ack_lsn` may not.
Use cumulative ack with selective batch acks.

Rules:

* One committed xact may span many batches, across tables or budget
  boundaries
* Xact is durable after every batch containing its rows has acked
  `EndOfStream`
* Collector keeps refcount per `seq#`; sealing increments, inserter ack
  decrements
* Decoder marks each xact fully placed after routing all rows
* Xact is done when `refcount == 0` and fully placed
* Watermark is highest contiguous done `seq#`; map to `commit_lsn`, then
  `emitter_ack_lsn`
* Aborts, empty xacts, fully filtered xacts get `seq#` with refcount 0
  and immediately done state

`flush_timeout` becomes part of correctness, not just batching policy. A
tiny old batch on a cold table can hold slot feedback for every later
xact, so seal idle batches by deadline per `InsertBatcher`.

Build collector so `DESTINATIONS.md` N:M destination acks can compose
later. Current axis is per inserter; future axis is per destination. See
[DESTINATIONS.md](DESTINATIONS.md).

## DDL barrier

DDL xacts, or `pg_class`/`pg_attribute` writes
([[feedback_pg_version_wal_skew]]), change schema for later decode and
must order ClickHouse `ALTER` after all earlier data.

Barrier sequence:

1. Quiesce decode pool
2. Drain and seal all open batches
3. Wait until watermark reaches barrier LSN
4. Apply DDL to ClickHouse and shadow catalog
5. Resume pipeline

Keep barrier coarse. DDL is rare. Do not optimize away ordering. See
[pinned_ddl_baseline.md](pinned_ddl_baseline.md),
[[reference_pinned_table_ddl_baseline]].

Caveat: coarse cost scales with in-flight backlog, not DDL frequency.
`run_barrier` freezes the single reorder task on `wait_all_durable` for the
whole pre-DDL backlog while the source advances, against the serial path's
surgical per-table wire close. Not a blocker at M=1/N=1 (the live test passes),
but it grows with N and backlog. See Status next-step 4 (DDL-type-aware
barrier).

## Inserter actor

Define inserter behind narrow actor interface:

* Own one `AsyncClient` connection
* Own at most one open INSERT
* Consume sealed batches from shared mpmc queue
* Emit durability acks after `EndOfStream`

Topology, `InsertBatcher`, and collector must not depend on blocking IO.
`N` should not be tied to core count; async tasks make high N viable for
Cloud latency.

Do not add client-side pipelining for this plan. Native protocol is
request/response per query; one connection remains in query state until
`EndOfStream` is consumed. More connections are required for concurrent
INSERTs.

Current async client already covers compression, exception/progress
events, and cancellation by stopping drain. Its per-method futures are
`Send` (no raw FFI pointer crosses an await; a crate guard test asserts
it), so inserter tasks `tokio::spawn` onto the multi-thread runtime
without `spawn_blocking`. Pool work still needs to validate many
concurrent `AsyncClient`s on one runtime, each with its own out buffer.

## Base backup through the same pipeline

Goal: base-backup (greenfield bootstrap) decode feeds the *same*
batcher → inserter → ack machinery as WAL, not a separate serial emitter.
One shipping path means bootstrap inherits the inserter pool, reconnect +
retry, the durable watermark, and backpressure for free, and the
transitional `Emitter` swap at end-of-bootstrap goes away.

Today bootstrap diverges: `drain_backfill` (`backfill_bootstrap.rs`)
synthesises a `CommittedTuple` per page-walk row (`op=Insert`,
`commit_ts=0`, `commit_lsn=source_lsn=BASE_BACKUP start_lsn`) and drives
the serial `Emitter` through `TupleObserver`, hand-flushing on each rfn
flip because CH's Native protocol forbids a new `Query` while an INSERT
stream is open. The batcher already owns per-table INSERT lifecycle, so
that per-rfn flush dance disappears once bootstrap routes through it.

One real blocker: the decode pool's `DecodeCtx` (`pipeline/decode.rs`)
hard-wires `catalog: Arc<Mutex<ShadowCatalog>>`, but bootstrap resolves
relations from a pre-seeded `CatalogMap` before shadow PG exists. The
`RelationResolver` trait (`relation_resolver.rs`) already abstracts exactly
this — `Mutex<ShadowCatalog>` for live WAL, `CatalogMapResolver` for the
bootstrap snapshot — and the serial emitter already consumes it. Swap
`DecodeCtx.catalog` to `Arc<dyn RelationResolver>` and `decode_and_route`
serves both sources unchanged.

Shape:

* Extract the shared tail around `CommittedTuple`/`RoutedRow` +
  `RelationResolver`; feed both WAL and base backup into the same
  batcher/inserter/ack stages (codex review)
* Bootstrap synthesises `DecodeJob`s (or calls `decode_and_route` inline)
  instead of `TupleObserver` calls; rows land in the shared batcher and
  coalesce to budget-sized parts like WAL rows
* Watermark: every bootstrap row carries `commit_lsn = start_lsn`, so the
  collector sees them as one (or few) contiguous-done `seq#`s; assign them
  a pre-WAL seq range the collector clears before the first WAL `seq#`
* TOAST: bootstrap's reassembled values must map onto the decode path's
  `ToastChunks`, or bypass detoast when already inline

## Sizing

* **N inserters** ~= `insert_round_trip / time_to_seal_one_batch`, enough
  to avoid Cloud latency binding throughput
* **M decoders** ~= enough that decode throughput >= aggregate insert
  throughput; keep M lower than N unless measurements show CPU bottleneck

## Backpressure

Use bounded channels everywhere. Current pump-to-worker mpsc is
unbounded; replace it as part of pool work.

Backpressure chain:

```text
inserter queue full -> InsertBatcher stalls -> decoders block -> job queue fills
-> reorder stalls -> pump soft_cap yields -> walsender queues
```

Goal is bounded overlap, not unbounded buffering against slow ClickHouse.

## Build order

1. Add instrumentation: decode CPU, inserter drain wait, pump idle
2. Add inserter pool and ack collector behind existing single decode path
3. Add decode pool only if decode cannot feed N inserters
4. Add hot-table `InsertBatcher` sharding by `hash(pk)` only if one
   batcher bottlenecks

## Open questions

* Measured bottleneck: instrumentation should decide whether decoder pool
  is necessary
* Hot-table skew: intra-table sharding improves `InsertBatcher`
  parallelism but increases part count
* Destination routing: collector must compose with `DESTINATIONS.md`
  before either ack scheme ships
* Spill reads: parallel decode of large spilled xacts may need concurrent
  or per-xact-file spill readers
* Load test: validate N concurrent `AsyncClient`s on one runtime
