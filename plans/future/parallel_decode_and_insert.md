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
  pipeline" below. Two correctness items surfaced while scoping that move,
  both pre-existing and independent of the pool: object_store concurrent-part
  fan-out corrupting the shared page-walk sink state (fixed via per-entry
  `EntryId` keying, Blockers, refined #5), and externally-toasted columns
  erroring rather than shipping (Blockers, refined #2, design in
  [TOAST.md](TOAST.md))

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
   through the same pipeline"): extract `tail::spawn` from
   `PipelineConfig::spawn`, route the page-walk drain through it (Option A,
   decode stays in the sink), synthesize per-rfn seqs, then run the Handoff
   completion sequence at end-of-bootstrap — `BatcherMsg::FlushAll`,
   `wait_through(K)` for bootstrap durability, then advance the persisted resume
   LSN to `end_lsn` (`ack.trailing(end_lsn)` or a final zero-row `end_lsn`
   marker seq). The explicit `end_lsn` bump is correctness, not cosmetics:
   `wait_through(K)` proves durability but leaves published `emitter_ack` at
   `start_lsn` (every bootstrap `commit_lsn` is `start_lsn`). Direct mode applies
   as-is; `object_store` (S3) first needs serialized Tap sink events
   (Blockers, refined #5) before per-rfn seqs are correct. Decode pool for
   bootstrap (Option B) only if page-walk decode can't feed N inserters
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

Goal: base-backup (greenfield bootstrap) feeds the *same* batcher →
inserter → ack machinery as WAL, not a separate serial emitter. One
shipping path means bootstrap inherits the N-connection inserter pool,
reconnect + retry, the durable watermark, and backpressure for free, and
the transitional `Emitter` swap at end-of-bootstrap goes away. Bootstrap
is the *easy* case for the tail: every row is `op=Insert` at one
`_lsn = start_lsn`, there are no aborts, no TRUNCATE, no DDL barriers. The
only reason it doesn't already use the tail is history, not difficulty.

### Today's two serial walls

```text
BackupSource (Direct | ObjectStore)
  -> MultiplexSink { DiskLanderSink (Keep catalogs), PageWalkSink (Tap heap) }
       -> PageWalkSink::chunk   [SYNC, under Arc<std::Mutex<dyn BackupSink>>]
            -> decode_block_data per LP_NORMAL slot   [WALL 1: single-threaded]
            -> BackfillTuple -> mpsc::unbounded
                 -> drain_backfill: BackfillTuple -> CommittedTuple
                      -> serial Emitter (one AsyncClient)   [WALL 2: one INSERT at a time]
                           -> per-rfn force-flush (flush_timeout=0)
```

* **Wall 1, decode under the sink mutex.** Page-walk *and* the per-tuple
  `decode_block_data` run inside `PageWalkSink::chunk`
  (`backup_page_walk.rs`), which every source worker drives while holding
  the one `Arc<Mutex<dyn BackupSink>>`. So even when `ObjectStoreSource`
  fetches + decompresses + tar-parses many parts concurrently
  (`buffer_unordered(parallelism)`), the actual tuple decode is serialized.
  Note the asymmetry with WAL: the WAL hot path's heavy `decode_block_data`
  also runs off-pool (in `BufferingDecoderSink`), and the "decode pool"
  only detoasts + resolves + routes *already-decoded* `DecodedHeap`s. For
  bootstrap the heavy decode is the page walk, and it sits on the sink
  task, not in any pool.
* **Wall 2, one INSERT connection.** The transitional `Emitter` holds a
  single `AsyncClient`, opens one INSERT at a time, and force-flushes on
  each rfn flip because CH's Native protocol forbids a new `Query` while an
  INSERT data stream is open (`run_bootstrap` sets `flush_timeout=0`,
  `bin/stream.rs`). On Cloud SMT this is the dominant cost, the same
  per-INSERT RTT + part-commit wall the WAL pipeline removed with N
  inserters. The batcher already owns per-table INSERT lifecycle, so the
  per-rfn flush dance disappears the moment bootstrap routes through it.

### What's reusable: the tail, not `PipelineConfig::spawn`

`PipelineConfig::spawn` is WAL-shaped: it wires `ReorderSink` (which needs
`XactBuffer`, `SubxactTracker`, `DdlApplicator`, `schema_events`,
`pg_class_delete_epoch`) plus the decode pool, batcher, inserters, ack.
Bootstrap has none of the reorder inputs. The reusable unit is the
**tail = batcher + inserter pool + ack collector**, fed by a
`mpsc::Sender<BatcherMsg>` and an `AckHandle`. Factor it out of
`PipelineConfig::spawn`:

```text
tail::spawn(emitter_cfg, n_inserters, stats, emitter_ack)
  -> (msg_tx: mpsc::Sender<BatcherMsg>, ack: AckHandle, TailHandle)
```

`PipelineConfig::spawn` then composes `tail::spawn` with reorder + decode
pool (no behaviour change for WAL). Bootstrap composes `tail::spawn` with a
page-walk drain. The producer differs; the tail is identical.

### Producer side (Option A, recommended first)

The drain task plays the same role the decode pool plays for WAL: turn a
source row into a `RoutedRow` and report `Placed{seq, rows}`. For bootstrap
that is cheap, no detoast (see blocker below), just resolve + map:

```text
PageWalkSink::chunk (decode stays here, single-threaded)
  -> BackfillTuple -> bounded channel
       -> bootstrap drain task:
            resolve rfn via CatalogMapResolver
            look up TableMapping
            build CommittedTuple { op=Insert, commit_lsn=start_lsn }
            msg_tx.send(BatcherMsg::Row(RoutedRow { seq, rel, mapping, committed }))
       -> shared batcher (coalesce to budget-sized parts)
       -> inserter pool (N connections, out-of-order OK)
       -> ack collector -> emitter_ack
```

This captures the Cloud win (Wall 2) with the smallest change. Decode
(Wall 1) stays single-threaded; remove it only if measurement shows
page-walk decode can't feed N inserters (Option B below). The
`PageWalkSink::chunk` is sync under a `std::Mutex`, so it can't `.await` a
bounded batcher send; the producer→channel→async-drain split stays, with
the channel now bounded (see Backpressure).

### Synthetic seq scheme

The ack collector keys everything on dense `seq`s registered in commit
order, each with a `commit_lsn` monotonic in `seq`. Bootstrap has no commit
boundaries and one uniform `_lsn`, so synthesize seqs. `PageWalkSink` emits
all rows for one rfn contiguously and the drain already detects rfn flips,
so the natural unit is **one seq per rfn**:

* `register(seq, commit_lsn = start_lsn)` at rfn start
* accumulate rows, `placed(seq, rows)` at rfn end (or channel close)
* batcher tags each sealed batch with `per_seq`, inserter acks decrement

Every seq carries the same `commit_lsn = start_lsn`. Two consequences the
cursor handoff below depends on: the contiguous-done *frontier* reaches `K`
only once every bootstrap seq is durable (`wait_through(K)` is the durability
proof), but the *published* `emitter_ack` value saturates at `start_lsn` —
`advance` does `fetch_max(commit_lsn)` over the done prefix (`ack.rs`) and
every bootstrap `commit_lsn` is `start_lsn`. Durability and the persisted
resume LSN are not the same thing here; see "Handoff" for why that matters.
Per-rfn keeps the collector's in-flight set small; sub-chunk a very large rel
into `seq` per N rows if one giant refcount is undesirable (correctness is
identical either way).

### Blockers, refined

1. **`detoast_heap` is concretely typed.** `decode_and_route`
   (`pipeline/decode.rs`) calls `detoast_heap(&mut heap, &chunks, &ctx.catalog)`,
   and `detoast_heap` (`xact_buffer.rs`) takes `&Arc<Mutex<ShadowCatalog>>`.
   The `relation_at` call in `decode_and_route` already dispatches through
   the `RelationResolver` trait (the impl on `Mutex<ShadowCatalog>`, reached
   by `Arc` deref), so only detoast pins the concrete catalog. The plan's
   earlier "swap `DecodeCtx.catalog` to `Arc<dyn RelationResolver>` and
   `decode_and_route` serves both unchanged" understated this. Two ways:
   * Option A sidesteps it: the bootstrap drain builds `RoutedRow` with a
     thin resolve+map fn that never detoasts, matching today's bootstrap
     semantics exactly.
   * To share one `decode_and_route` (needed for Option B), abstract
     `detoast_heap` over `RelationResolver` (it only needs `relation_at`)
     and make detoast a no-op when `chunks` is empty.
2. **TOAST — current behavior is fail, not ship.** Page walk produces
   `ColumnValue::ExternalToast` for externally-stored columns
   (`decode_block_data` → `decode_varlena`, VARTAG_ONDISK), and the
   orchestrator has no `ToastChunks` (V1 observes `pg_toast_*` pages but
   doesn't reassemble, see `backup_page_walk.rs`). Detoast lives only in the
   xact-buffer path (`detoast_heap`, `xact_buffer.rs`); the bootstrap
   `BackfillTuple → Emitter` path never runs it, so an `ExternalToast`
   reaches the emitter and is *rejected* — `ch_emitter.rs` returns
   `EmitterError::UnsupportedValue` ("unresolved TOAST pointer"). So a prior
   claim that bootstrap "ships unreassembled and converges via WAL re-emit"
   is wrong: an externally-toasted column errors the bootstrap. Latent only
   because tested fixtures keep values inline (<~2 KiB). Option A's
   no-detoast route therefore preserves a *failure*, not convergent
   shipping; it must pick an explicit behavior:
   * fail-fast (status quo, but documented and surfaced cleanly), or
   * emit NULL / a raw on-disk marker with documented convergence semantics
     (WAL re-emit then supersedes via `_lsn`), or
   * implement TOAST assembly first: tap `pg_toast_*` filenodes into a
     `ToastChunks` map keyed by `(toast_relid, value_id)` and reuse the
     shared detoast.

   The third is the real fix and is its own work item, independent of this
   push-down; design in [TOAST.md](TOAST.md) (store `pg_toast_*` chunks on
   ClickHouse).
3. **Seq numbering continuity at handoff.** Bootstrap registers seqs
   `[0, K)`. The WAL `ReorderSink` must continue at `K`, not 0
   (`ReorderSink.next_seq` is currently hardcoded to 0). Pass the post-bootstrap
   `next_seq` into `ReorderSink::new`. The collector requires dense seqs
   from 0; bootstrap fills `[0, K)` densely and WAL continues, so
   `all_done`/contiguity hold across the seam.
4. **No reorder inputs.** Bootstrap stands up `tail::spawn` directly. It
   does not touch `XactBuffer`/`DdlApplicator`/`SubxactTracker`.
5. **Sink interleaving across concurrent parts, fixed.** `ObjectStoreSource`
   drains parts under `buffer_unordered(parallelism)` (default `min(4,
   cores)`), and `pump_entry` re-locks the shared sink *per chunk* (the std
   `Mutex` is released across each `body.read().await`), not per file.
   `PageWalkSink`/`MultiplexSink` previously held a single per-entry slot
   (`cur: Option<TapEntry>` / `routed_to_tap: bool`), so two concurrent parts'
   Tap files interleaved `begin`/`chunk` on that one slot: part B's `begin`
   clobbered the entry part A was mid-stream on, misframing pages and decoding
   them against the wrong relation. Pre-existing in the serial object_store→CH
   path too (Keep files were safe since `write_kept` bypasses the sink, and
   the e2e fixture is single-part so it stayed hidden). Fixed by keying
   per-entry sink state on a source-allocated `EntryId` (`begin`/`chunk`/`end`
   all take it; `PageWalkSink.cur` is now `HashMap<EntryId, TapEntry>`,
   `MultiplexSink` a `HashSet<EntryId>`); the mutex still serializes calls,
   the key just stops the logical clobber. Per-rfn seqs are then safe: the
   drain is the single channel consumer, so it assigns seqs by rfn-flips *as
   observed in the channel*, not by file boundary. Interleaving just yields
   more (still dense, still `commit_lsn = start_lsn`) seqs, a given rfn may
   span more than one seq, which the per-seq refcount handles like any two
   rels. Direct mode was never affected (one sequential entry stream).

### Handoff: shared tail, delete the transitional emitter

Stand up `tail::spawn` once. Bootstrap feeds it via the page-walk drain
registering seqs `[0, K)`; on bootstrap completion, hand the same `msg_tx`
and `AckHandle` to the WAL `ReorderSink`, which continues at seq `K`. The
inserter pool, batcher, and ack collector persist across the seam, so the
transitional `Emitter` and its end-of-bootstrap teardown go away (the plan's
stated goal), with no inserter reconnect at the seam.

Completion sequence at end of bootstrap, once the page-walk drain has
`placed` every bootstrap seq:

1. **Prefer an explicit flush.** The tail persists, so dropping one producer
   does *not* fire the batcher's final flush (that runs only when *all*
   `msg_tx` clones drop, `batcher.rs`). Correctness still holds without an
   explicit flush because finite `flush_timeout` seals cold-table batches via
   the deadline ticker, but handoff latency then depends on deadline cadence
   (roughly one extra tick in the worst case). Send `BatcherMsg::FlushAll`
   and await its reply for prompt, deterministic completion, the same drain
   step DDL/TRUNCATE barriers already use.
2. **Wait for durability.** `ack.wait_through(K)` blocks until the
   contiguous-done frontier covers `[0, K)`, i.e. every bootstrap batch has
   acked `EndOfStream`.
3. **Advance the persisted resume LSN to `end_lsn`.** `wait_through(K)`
   proves durability but does *not* move the published `emitter_ack` above
   `start_lsn` — every bootstrap `commit_lsn` is `start_lsn`, so `advance`'s
   `fetch_max` saturates there. The cursor is written from `emitter_ack`
   (`cursor::write`, `bin/stream.rs`), so without an explicit bump it
   persists `start_lsn`, not `end_lsn`. After step 2 call
   `ack.trailing(end_lsn)` — it advances `emitter_ack` to `end_lsn` iff
   `all_done()`, which step 2 guaranteed. (Equivalent: register a final
   zero-row seq `K` at `commit_lsn = end_lsn`, `wait_through(K + 1)`, and
   pass `next_seq = K + 1` into `ReorderSink::new`.)

**Why step 3 is correctness, not cosmetics.** In-process WAL start is already
`end_lsn` (`bootstrap_end_lsn` outranks the cursor in `raw_start`,
`bin/stream.rs`), so a clean run is fine regardless. The hazard is a crash
between bootstrap completion and the first post-`end_lsn` WAL xact going
durable, restarted with `--bootstrap-mode=off`: the resume path then reads
`cursor.emitter_ack_lsn`. Without step 3 that is `start_lsn`, so WAL replays
`[start_lsn, end_lsn]` again. That direction is safe for data (idempotent
under `_lsn` / `ReplacingMergeTree`) but re-decodes that range against the
`end_lsn` shadow catalog, exposing WAL-version skew
([[feedback_pg_version_wal_skew]]) for any in-window DDL — a regression vs
the serial path, which always resumes at `end_lsn`. The dangerous opposite
(resume at `end_lsn` with baseline rows not yet durable) is what step 2
prevents: `ReplacingMergeTree(_lsn)` covers brief duplicates, not missing
baseline rows.

**Overlap vs. `START_REPLICATION`.** Bootstrap and the first WAL INSERTs may
overlap on the shared inserter pool without a crash-safety hole: the
contiguous-done watermark cannot advance `emitter_ack` past a not-yet-done
bootstrap seq `< K`, so a crash mid-overlap leaves the persisted cursor
`<= start_lsn` (safe re-read). Only the step-3 advance to `end_lsn` must wait
on `wait_through(K)`; opening `START_REPLICATION` itself need not. If that
extra concurrency isn't worth reasoning about, the conservative alternative
is to run steps 1–3 fully before opening `START_REPLICATION` — no overlap,
identical durability guarantee.

### Option B: parallel decode (gated on measurement)

If page-walk decode (Wall 1) can't feed N inserters, move decode off the
sink task. `PageWalkSink::chunk` stops calling `decode_block_data`; it does
only the cheap framing (8 KiB page slicing + `ItemIdData` slot walk) and
emits raw `(rfn, lp_normal_tuple_bytes)` units into the job channel. A
bootstrap decode pool runs `decode_block_data` + resolve + route. This is a
*different* job shape than WAL's `DecodeJob` (which carries already-decoded
heaps), so either a bootstrap-specific job variant or a shared
`decode_and_route` reached after the abstracted-detoast change in blocker 1.
Keep this behind the same "only if decode can't feed N" gate the WAL plan
uses (Build order step 3). Option B matters most for the object-store
source, where fetch/decompress is already parallel and decode is the next
serial wall once insert is parallelized.

## Streaming the base backup from object storage (S3)

The source layer is already wire-protocol-agnostic.
`ObjectStoreSource` (`backup_source_object_store.rs`) streams a
wal-g-compatible BASE_BACKUP from a `DynStorage` bucket, decompresses each
tar part, and pumps the *same* `BackupSink` events as the PG-protocol
`DirectSource`. `DynStorage` resolves to `S3Storage` (hand-rolled SigV4,
`AWS_REGION` / `AWS_ENDPOINT_URL` / `WALG_S3_*`), so "stream from S3 instead
of the PG base backup protocol" already works:

```text
--bootstrap-mode=object_store --bootstrap-backup-name {LATEST | base_...}
WALG_* / AWS_* env -> Settings::from_env -> build_storage -> DynStorage
```

Because the source layer is shared, the tail wiring in "Base backup through
the same pipeline" applies to S3 too — the parallel tail does not care
whether bytes came off the replication socket or out of a bucket, and the
concurrent-part sink interleaving that used to make this unsafe is now fixed
(per-entry `EntryId` keying, Blockers, refined #5). What's S3-specific:

* **Fan-out today is fetch/decompress/tar-parse only.** `ObjectStoreSource`
  drains data parts `buffer_unordered(parallelism)` (default `min(4, cores)`,
  `--bootstrap-object-store-parallelism`), with `pg_control` as a hard
  single-task barrier after them. But all those workers funnel through the
  one sink mutex, and decode runs there (Wall 1). So raising
  `--bootstrap-object-store-parallelism` past ~1-2 yields little under
  Option A: decode, not fetch, is the bound. Option B unlocks the real
  S3 shape, a genuine three-stage parallel pipeline:

  ```text
  S3 parts (P concurrent fetch/decompress) -> page framing (sink, cheap)
    -> [decode x M] -> batcher -> [insert x N] -> CH Cloud
  ```

  P, M, N tune independently against bucket GET latency, page-walk CPU, and
  Cloud INSERT RTT.
* **WAL hydrate stays.** Object-store mode pulls WAL covering
  `[start_lsn, end_lsn]` from `wal_005/` into shadow `pg_wal/` after the
  pump (`fetch_wal_into_pg_wal`, `bin/stream.rs`); Direct mode ships it
  inside `base.tar` via `BaseBackupOpts { wal: true }`. Unchanged by the
  tail push-down.
* **Delta chains still unsupported (V1).** A sentinel with `increment_from`
  errors out: incremented files need a disk-resident base to overlay, which
  the streaming page-walk doesn't produce. Orthogonal to this plan.

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

Bootstrap has the same gap and then some: the `PageWalkSink -> drain`
channel (`backup_page_walk.rs::out_tx`) is `mpsc::unbounded` *by necessity*
today, because `chunk()` is sync under a `std::Mutex` and `blocking_send`
panics in the runtime. Bounding it needs a sync `try_send` + capacity (drop
to a parking strategy on full) or moving the page split off the sink task.
Its chain mirrors WAL:

```text
inserter queue full -> batcher stalls -> bootstrap drain blocks
-> PageWalkSink channel fills -> sink chunk() parks -> BackupSource backpressures
   (DirectSource bounded events channel / ObjectStoreSource buffer_unordered)
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
* Bootstrap decode bound: measure page-walk decode throughput vs N
  inserters before committing to Option B; for the object-store source the
  answer likely differs (fetch already parallel, decode the next wall)
* Bootstrap seq granularity: per-rfn vs per-N-rows, measured against
  ack-collector in-flight memory for a wide schema with many small rels
