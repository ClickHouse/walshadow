# Pipeline backpressure and scaling

WAL-pump backpressure and decode/insert scaling for the parallel decode+insert
pipeline (pooled decoders feeding pooled inserters); design reference below.

Thesis — move serial decode+send tail into two pools:

* M decoders for CPU work: tuple parse, catalog projection, type coercion,
  detoast, Native encoding
* N inserters for ClickHouse IO: one `AsyncClient` connection per task

Target ClickHouse Cloud with SharedMergeTree. Per-INSERT cost is mostly
RTT plus object-storage part commit, so throughput comes from keeping
many INSERTs in flight. CPU parallelism matters only once decode cannot
feed inserters.

`clickhouse-c-rs-async` backs the pooled decode→insert pipeline.
WAL and bootstrap both run the pooled tail (M decoders / N inserters, default
1/1); out-of-order INSERTs reconcile by `_lsn` and the contiguous-done
watermark drives slot feedback. The design spans every axis: N concurrent
`AsyncClient` inserters, async IO that never parks tokio workers, bootstrap
through the shared tail, and bounded bootstrap backpressure. Extension axes
live under "Future work" and "Backpressure".

Out-of-order INSERTs are OK. `_lsn` plus `ReplacingMergeTree(_lsn)` makes
final state order-independent (`ch_ddl.rs`,
[[project_walshadow_eventual_consistency]]). Slot feedback still needs a
strict contiguous durable watermark.

## Pooled WAL path at M=1/N=1

Pipeline built in `src/pipeline/` (`mpmc`, `ack`, `batcher`, `inserter`,
`decode`, `reorder`), wired into `bin/stream.rs` behind
`--decoder-pool-size` / `--inserter-pool-size` (default 1/1). With `--ch-config`
set the WAL path always runs the pooled pipeline (`PipelineConfig::spawn`).

Correctness properties:

* Five P0 correctness invariants in code: TRUNCATE-as-barrier,
  ack-after-drain, contiguous-done watermark, aborts through gate, accurate
  per-seq counts
* Watermark, mpmc, batcher coalescing unit-tested hermetically
* `FlushAll` race guarded: rows + flush share one FIFO `BatcherMsg` channel,
  reorder waits on ack collector's placed-frontier before issuing `FlushAll`.
  Without that ordering the `flush_all` batcher test hangs nondeterministically
  at `flush_timeout=3600s`
* Serial/bootstrap path untouched, covered by hermetic integration tests
* `pgbench_acceptance_ddl_intermix` (live PG+CH, pooled WAL path at M=1/N=1,
  ADD COLUMN + CREATE INDEX CONCURRENTLY mid-workload) green 5/5, ~22s.
  Guards against watermark pin under per-INSERT RTT cost: at `flush_timeout=0`
  each pgbench xact's four per-table INSERT closes cost 4×RTT (~5 xact/s on
  local CH), so `emitter_ack` cannot track the ~700 xact/s source.
  `--ch-flush-timeout-ms 200` coalesces rows into one part per window and lets
  N=1 hold the source rate. Barrier + `wait_for_ack_catchup` both pass, so the
  coarse barrier does not regress at this scale

Live daemon coverage extends past M=1/N=1:

* `tests/pipeline_parallel_e2e.rs` (DML) and `tests/pipeline_parallel_ddl_e2e.rs`
  (ALTER ADD COLUMN + TRUNCATE through `run_barrier`) drive the real
  `PipelineConfig` fan-out at M=2/N=2 in-process; `pgbench_acceptance.rs`
  runs the daemon-spawn drill twice via the parametrized `run_ddl_intermix` —
  `pgbench_acceptance_ddl_intermix` at 1/1 and
  `pgbench_acceptance_ddl_intermix_pooled` at 2/2 (disjoint ports, run
  concurrently). The 2/2 variant drives N concurrent `AsyncClient`s for
  bootstrap + WAL under the DDL barrier; the end-state parity oracle confirms
  out-of-order INSERTs across connections stay `_lsn`-correct
* Bootstrap/base-backup routes through the shared tail (Option A), not a serial
  `Emitter`. See "Bootstrap through the shared tail" below. Externally-toasted
  columns error rather than ship (Blockers, refined #2, design in
  [TOAST.md](../TOAST.md)); the fail-fast is explicit at the producer (the
  bootstrap drain), not a generic encoder rejection deep in the tail.

Backpressure is correct per channel, not one uniform "bound everything" goal
(the three ingress channels have different constraints):

* Bootstrap ingress is bounded: `BackupSink` is async end to end and the
  page-walk→drain channel is a bounded `mpsc::channel`
  (`BOOTSTRAP_TUPLE_CHANNEL_CAP`), so a saturated inserter parks the page walk
  and the source fetch instead of buffering a whole relation. The
  ack-events channel (`ack.rs`) stays unbounded by design — the collector is
  pure-sync `state.apply`, strictly faster than any producer, so it cannot
  back up. The pump→worker channel (`queueing_record_sink.rs`) is soft-capped,
  not hard-bounded, on purpose: a hard bound deadlocks shadow apply. A hard
  bound there needs a wire/record split. See "Backpressure"

Future work:

* Bound the pump→worker backpressure by splitting wire delivery from record
  dispatch so the wire side runs ahead (paced by shadow apply) while record
  dispatch blocks on a bounded queue. Architectural, not a channel swap; the
  ack channel stays unbounded by design and the bootstrap ingress is already
  bounded (see "Backpressure")
* Decode pool for bootstrap (parallel-decode Option B) gated on measurement

Out of scope:

* DDL-type-aware barrier optimization. Make `run_barrier` discriminate: ADD
  COLUMN needs only seal plus epoch bump before post-ALTER rows decode, no
  global `wait_all_durable`; reserve the full drain for destructive DDL
  (DROP/RENAME column, TRUNCATE). The coarse barrier holds at the
  validated pool sizes (the 2/2 live test passes with ADD COLUMN + CREATE INDEX
  CONCURRENTLY mid-workload), and the cost scales with in-flight backlog, not
  DDL frequency. Relevant only if a measured workload shows the coarse drain
  binding throughput at higher N. See "DDL barrier" caveat below

## Bootstrap through the shared tail

Bootstrap (greenfield base backup) feeds the same batcher → inserter →
ack tail as WAL, not a serial `Emitter`. Direct and
ObjectStore sources both route through it.

Shape (Option A — decode stays in the page-walk sink):

* `tail::spawn` (`src/pipeline/tail.rs`) factored out of `PipelineConfig::spawn`:
  ack collector + N inserters + batcher, fed by a `mpsc::Sender<BatcherMsg>` +
  `AckHandle`. WAL composes it with reorder + decode pool; behaviour unchanged
  (`pipeline_parallel_{e2e,ddl_e2e}` green).
* `pipeline::bootstrap::drain` (`src/pipeline/bootstrap.rs`) is the bootstrap
  producer: pulls `BackfillTuple`s off the `PageWalkSink` channel, resolves +
  maps (no detoast, no oracle — Option A), builds `CommittedTuple{op=Insert,
  commit_lsn=start_lsn}`, sends `RoutedRow` to the tail. One synthetic seq per
  rfn (register at rfn start, `placed` at rfn flip / channel close).
* `run_bootstrap` (`bin/stream.rs`, `--ch-config` arm) spawns the tail at
  `--inserter-pool-size`, runs the page walk + drain concurrently, then the
  completion sequence: `BatcherMsg::FlushAll` → `ack.wait_through(K)` →
  teardown. The metrics-only (no `--ch-config`) arm still uses `drain_backfill`.

Handoff (resume-LSN correctness): a **separate** tail instance
torn down at end-of-bootstrap, *not* the shared-instance handoff described under
"Handoff: shared tail" below. `run()` stands bootstrap up before shadow-PG spawn
/ `START_REPLICATION`, so holding one tail (and its CH sockets) idle across the
multi-second autospawn is not worth the coupling. This is the plan's own
sanctioned "run steps 1–3 before opening `START_REPLICATION`" conservative
alternative (no overlap, identical durability guarantee). The explicit `end_lsn`
bump (step 3) is realized by **seeding the WAL pipeline's `emitter_ack` atomic to
`bootstrap_end_lsn`** at creation, so the resume cursor persists `end_lsn` (not
`start_lsn`) until the first WAL xact; the tail's `fetch_max` keeps it monotonic
as WAL re-reads `[aligned, end_lsn]`. Because the WAL collector is a fresh
instance starting at seq 0, Blocker #3 (seq continuity at `K`) does not apply.

Coverage:

* `tests/bootstrap_pipeline_ch.rs` — bootstrap drain → tail at N=2 → live CH,
  `row_budget=4` so each rfn's seq spans many batches that fan across both
  connections and ack out of order; `wait_through(K)` reconciles, all rows land.
* `bootstrap_direct_ch.rs` / `bootstrap_object_store_ch.rs` — full daemon-spawn
  bootstrap through the tail (N=1), green.
* `pipeline::bootstrap` unit tests — per-rfn seq counts, unmapped-skip,
  reappearing-rfn (object_store interleave) yields fresh dense seqs.

Open axis: parallel-decode Option B gated on measurement. The bootstrap
page-walk→drain channel is bounded + async (see "Backpressure"), so this
path's backpressure is closed; only the WAL pump→worker bound is open
(Future work). TOAST is an explicit
producer-side fail-fast (Blockers, refined #2).

## Pool-parallel coverage and bootstrap backpressure

The pooled pipeline covers the following invariants, and the bootstrap
ingress backpressure is closed:

* **Live daemon coverage at pool >1.** `pgbench_acceptance.rs` factors the DDL-
  intermix drill into `run_ddl_intermix(ports, decoder_pool, inserter_pool,
  label)` and runs it twice on disjoint ports: `pgbench_acceptance_ddl_intermix`
  (1/1) and `pgbench_acceptance_ddl_intermix_pooled` (2/2). The 2/2 variant
  spawns N concurrent `AsyncClient`s for both bootstrap and WAL, with ADD COLUMN
  + CREATE INDEX CONCURRENTLY mid-workload; the end-state parity oracle (count +
  sum + the `c` column) proves out-of-order INSERTs across connections stay
  `_lsn`-correct through the DDL barrier. Both run green concurrently (~22s).
* **TOAST fail-fast, surfaced cleanly.** See Blockers, refined #2.
* **Bootstrap backpressure, bounded.** `BackupSink` is async end to end (the
  shared sink `Mutex` is a `tokio::sync::Mutex`, all methods `async fn`), so
  `PageWalkSink::chunk` awaits a bounded page-walk→drain channel
  (`BOOTSTRAP_TUPLE_CHANNEL_CAP`) directly. A saturated inserter parks the page
  walk → tar body read → object-store fetch — empty-channel backpressure, no
  whole-relation buffer. See "Backpressure".

Future work axes: the WAL pump→worker bound
(needs a wire/record split, Future work), parallel-decode
Option B (gated on measurement), and TOAST assembly (separate work item,
[TOAST.md](../TOAST.md)). The DDL-type-aware barrier is out of scope.

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
[pinned_ddl_baseline.md](pinned_ddl_baseline.md).

Caveat: coarse cost scales with in-flight backlog, not DDL frequency.
`run_barrier` freezes the single reorder task on `wait_all_durable` for the
whole pre-DDL backlog while the source advances, against the serial path's
surgical per-table wire close. Not a blocker at the validated pool sizes (both
the M=1/N=1 and M=2/N=2 live tests pass with ADD COLUMN + CREATE INDEX
CONCURRENTLY mid-workload), but it grows with N and backlog. The DDL-type-aware
optimization is out of scope (see "Out of scope" above).

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
reconnect + retry, the durable watermark, and backpressure for free, with
no `Emitter` swap at end-of-bootstrap. Bootstrap
is the *easy* case for the tail: every row is `op=Insert` at one
`_lsn = start_lsn`, there are no aborts, no TRUNCATE, no DDL barriers.

### The two serial walls a serial emitter would hit

```text
BackupSource (Direct | ObjectStore)
  -> MultiplexSink { DiskLanderSink (Keep catalogs), PageWalkSink (Tap heap) }
       -> PageWalkSink::chunk   [under Arc<Mutex<dyn BackupSink>>]
            -> decode_block_data per LP_NORMAL slot   [WALL 1: single-threaded]
            -> BackfillTuple -> mpsc::unbounded
                 -> drain_backfill: BackfillTuple -> CommittedTuple
                      -> serial Emitter (one AsyncClient)   [WALL 2: one INSERT at a time]
                           -> per-rfn force-flush (flush_timeout=0)
```

A serial emitter would hit two walls. Wall 2 does not apply — bootstrap routes
through the shared N-inserter tail — and the sink mutex + `chunk` are async over
a bounded channel. Wall 1 (decode single-threaded on the sink task) stands,
gated on measurement (Option B). The bullets below describe both walls a serial
emitter faces, for context.

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
* **Wall 2, one INSERT connection.** A serial `Emitter` holds a
  single `AsyncClient`, opens one INSERT at a time, and force-flushes on
  each rfn flip because CH's Native protocol forbids a new `Query` while an
  INSERT data stream is open (`run_bootstrap` sets `flush_timeout=0`,
  `bin/stream.rs`). On Cloud SMT this is the dominant cost, the same
  per-INSERT RTT + part-commit wall N inserters avoid. The batcher owns
  per-table INSERT lifecycle, so routing bootstrap through it drops the
  per-rfn flush dance.

### What's reusable: the tail, not `PipelineConfig::spawn`

`PipelineConfig::spawn` is WAL-shaped: it wires `ReorderSink` (which needs
`XactBuffer`, `SubxactTracker`, `DdlApplicator`, `schema_events`,
`pending_sweeps`) plus the decode pool, batcher, inserters, ack.
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
            resolve rfn via CatalogMap
            look up TableMapping
            build CommittedTuple { op=Insert, commit_lsn=start_lsn }
            msg_tx.send(BatcherMsg::Row(RoutedRow { seq, rel, mapping, committed }))
       -> shared batcher (coalesce to budget-sized parts)
       -> inserter pool (N connections, out-of-order OK)
       -> ack collector -> emitter_ack
```

This captures the Cloud win (Wall 2) with the smallest change. Decode
(Wall 1) stays single-threaded; remove it only if measurement shows
page-walk decode can't feed N inserters (Option B below). `PageWalkSink::chunk`
is async (the sink `Mutex` is a `tokio::sync::Mutex`), so `ship_tuple`
awaits the bounded page-walk→drain channel directly: a full channel parks the
page walk and the source fetch, no intermediate buffer (see
"Backpressure").

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

1. **`decode_and_route` is concretely typed.** Both its resolve
   (`shadow_catalog::resolve_at_pooled`) and `detoast_heap`
   (`xact_buffer.rs`) take `&Arc<Mutex<ShadowCatalog>>` — no
   `RelationResolver` trait exists in the codebase; the bootstrap
   drain uses its own `CatalogMap` path
   instead. Sharing `decode_and_route` via a "swap `DecodeCtx.catalog` to
   `Arc<dyn RelationResolver>` and `decode_and_route` serves both
   unchanged" route requires introducing that abstraction
   first. Two ways:
   * Option A sidesteps it: the bootstrap drain builds `RoutedRow` with a
     thin resolve+map fn that never detoasts, matching today's bootstrap
     semantics exactly.
   * To share one `decode_and_route` (needed for Option B), abstract
     `detoast_heap` over `RelationResolver` (it only needs `relation_at`)
     and make detoast a no-op when `chunks` is empty.
2. **TOAST — behavior is fail, not ship.** Page walk produces
   `ColumnValue::ExternalToast` for externally-stored columns
   (`decode_block_data` → `decode_varlena`, VARTAG_ONDISK), and the
   orchestrator has no `ToastChunks` (V1 observes `pg_toast_*` pages but
   doesn't reassemble, see `backup_page_walk.rs`). Detoast lives only in the
   xact-buffer path (`detoast_heap`, `xact_buffer.rs`); the bootstrap
   `BackfillTuple → Emitter` path never runs it, so an `ExternalToast`
   reaches the emitter and is *rejected* — `ch_emitter.rs` returns
   `EmitterError::UnsupportedValue` ("unresolved TOAST pointer"). Bootstrap
   does not "ship unreassembled and converge via WAL re-emit": an
   externally-toasted column errors the bootstrap. Latent only
   because tested fixtures keep values inline (<~2 KiB). Option A's
   no-detoast route therefore preserves a *failure*, not convergent
   shipping; the options are:
   * fail-fast (documented and surfaced cleanly), or
   * emit NULL / a raw on-disk marker with documented convergence semantics
     (WAL re-emit then supersedes via `_lsn`), or
   * implement TOAST assembly first: tap `pg_toast_*` filenodes into a
     `ToastChunks` map keyed by `(toast_relid, value_id)` and reuse the
     shared detoast.

   Choice: fail-fast, surfaced cleanly. The bootstrap drain
   (`pipeline/bootstrap.rs::external_toast_block`) scans each *mapped* column
   before routing and returns a precise error — relation + column + attnum —
   when it finds a `ColumnValue::ExternalToast`. This keeps the pointer from
   flowing to the encoder and tripping a generic `EmitterError::UnsupportedValue`
   ("xact buffer should have reassembled") deep in the inserter pool, wording
   that is meaningless for bootstrap (no xact buffer). Unit-tested by
   `external_toast_fails_fast`.

   The third option is the real fix and is its own work item, independent of
   this push-down; design in [TOAST.md](../TOAST.md) (store `pg_toast_*` chunks on
   ClickHouse).
3. **Seq numbering continuity at handoff.** Bootstrap registers seqs
   `[0, K)`. The WAL `ReorderSink` must continue at `K`, not 0
   (`ReorderSink.next_seq` is currently hardcoded to 0). Pass the post-bootstrap
   `next_seq` into `ReorderSink::new`. The collector requires dense seqs
   from 0; bootstrap fills `[0, K)` densely and WAL continues, so
   `all_done`/contiguity hold across the seam.
4. **No reorder inputs.** Bootstrap stands up `tail::spawn` directly. It
   does not touch `XactBuffer`/`DdlApplicator`/`SubxactTracker`.
5. **Sink interleaving across concurrent parts.** `ObjectStoreSource`
   drains parts under `buffer_unordered(parallelism)` (default `min(4,
   cores)`), and `pump_entry` re-locks the shared sink *per chunk* (the std
   `Mutex` is released across each `body.read().await`), not per file.
   A single per-entry slot (`cur: Option<TapEntry>` / `routed_to_tap: bool`)
   on `PageWalkSink`/`MultiplexSink` would let two concurrent parts'
   Tap files interleave `begin`/`chunk` on that one slot: part B's `begin`
   clobbers the entry part A is mid-stream on, misframing pages and decoding
   them against the wrong relation. This clobber applies to the serial
   object_store→CH path too (Keep files are safe since `write_kept` bypasses the
   sink, and a single-part e2e fixture keeps it latent). Keying
   per-entry sink state on a source-allocated `EntryId` (`begin`/`chunk`/`end`
   all take it; `PageWalkSink.cur` is `HashMap<EntryId, TapEntry>`,
   `MultiplexSink` a `HashSet<EntryId>`) avoids it; the mutex still serializes
   calls, the key just stops the logical clobber. Per-rfn seqs are then safe:
   the drain is the single channel consumer, so it assigns seqs by rfn-flips *as
   observed in the channel*, not by file boundary. Interleaving just yields
   more (still dense, still `commit_lsn = start_lsn`) seqs, a given rfn may
   span more than one seq, which the per-seq refcount handles like any two
   rels. Direct mode is unaffected (one sequential entry stream).

### Handoff: shared tail, no separate emitter

Stand up `tail::spawn` once. Bootstrap feeds it via the page-walk drain
registering seqs `[0, K)`; on bootstrap completion, hand the same `msg_tx`
and `AckHandle` to the WAL `ReorderSink`, which continues at seq `K`. The
inserter pool, batcher, and ack collector persist across the seam, so no
separate `Emitter` or end-of-bootstrap teardown is needed, with no inserter
reconnect at the seam.

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
whether bytes came off the replication socket or out of a bucket, and
concurrent-part sink interleaving stays safe via
per-entry `EntryId` keying (Blockers, refined #5). What's S3-specific:

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

## Backpressure

One bottleneck: the CH inserter pool. `clickhouse-c-rs` keeps no internal send
queue — `AsyncClient::send_data` writes to the socket inside the await and
`drain_to_end_of_stream` awaits the server's `EndOfStream` — so each inserter
task's await *is* the drum, and the internal WAL chain
(reorder→decode→batcher→inserter) is bounded end to end: every stage awaits a
bounded `mpmc`/`mpsc`, the terminal await is the CH send itself. Steady-state
memory there is the sum of those bounded buffers.

The three ingress channels are not one problem with one fix (a uniform
"bound every channel" framing is too coarse):

* **Bootstrap page-walk → drain: bounded.** `BackupSink` is
  async end to end (sink `Mutex` is a `tokio::sync::Mutex`, all methods
  `async fn`); `PageWalkSink::ship_tuple` awaits a bounded `mpsc::channel`
  (`BOOTSTRAP_TUPLE_CHANNEL_CAP`). A full channel parks the page walk → the tar
  body read → the object-store fetch. No standing buffer; the bootstrap paces
  to CH. Deadlock-free: bootstrap runs before shadow start, so the drain
  consumes on a separate task with no cycle.
* **Ack events (`ack.rs`): unbounded by design, leave it.** The collector loop
  is pure-sync `state.apply` (BTreeMap bookkeeping), strictly faster than any
  producer, so the channel cannot back up. Bounding it buys zero memory and
  would force `ack.acked` off its non-blocking fire-and-forget on the inserter
  hot path.
* **Pump → worker (`queueing_record_sink.rs`): soft-capped on purpose; a hard
  bound is future work.** `on_record` and shadow-wire delivery are lockstep in
  `wal_stream.rs::drain_records`, and `ReorderSink::maybe_sweep_dropped` →
  `ShadowCatalog::wait_for_replay` couples decode to shadow apply, which needs
  the pump to keep feeding *subsequent* wire bytes (walreceiver flush
  granularity). A hard-bounded blocking `on_record` therefore re-introduces the
  shadow-starvation deadlock the queueing sink exists to avoid; the soft cap
  (yield past `soft_cap`) is the deliberate trade. Under sustained
  CH-slower-than-WAL this buffer can still grow, buffering WAL in walshadow RAM
  rather than letting the PG slot hold it on disk. The fix is to split wire
  delivery (runs ahead, paced by shadow apply) from record dispatch (blocks on
  a bounded queue) — architectural, not a channel swap. Future work.

## Sizing

* **N inserters** ~= `insert_round_trip / time_to_seal_one_batch`, enough
  to avoid Cloud latency binding throughput
* **M decoders** ~= enough that decode throughput >= aggregate insert
  throughput; keep M lower than N unless measurements show CPU bottleneck

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
