# Pipeline microbench — staged throughput/latency breakdown

## Why

We're chasing **low throughput + high latency** in the async pipeline. The
on-CPU profile (`bench/results/run-big/walshadow-async-run/profile`) showed:

- **Not CPU-bound anywhere.** All walshadow logic combined is <5% of on-CPU
  samples (`decode_heap_record` 0.2%, encode 0.9%, **inserter send/drain 0.0%**).
- tokio park + futex + spinlocks + `eventfd_write`/`try_to_wake_up` dominate
  (~25% scheduler/wakeup churn); 73% `[unknown]` (idle park + clickhouse-c
  native).
- Inserters at **0% CPU** = blocked off-CPU on the ClickHouse network
  round-trip.

Conclusion: the wall is **off-CPU** (CH insert RTT) plus **per-item
coordination overhead** — not any CPU hotspot. An on-CPU profile can't measure
it. So we build a **CH-free, in-process microbench** that adds the pipeline
pieces one at a time, to attribute each stage's throughput cost and find the
ceiling once CH is removed.

## Pipeline recap

```
pump → QueueingRecordSink → reorder → [decode ×M] → InsertBatcher
          (1 worker:                                → [inserter ×N] → ClickHouse
           WAL decode)                                            ↘ ack collector → emitter_ack_lsn
```

- The heavy WAL parse (`decode_heap_record`, `BufferingDecoderSink`) is
  **single-threaded on the queueing worker**, before the pool.
- The **decode pool only detoasts + resolves + routes** (`decode_and_route`) —
  it does NOT do the WAL parse. (Naming is misleading.)
- `reorder` is single-threaded: assigns a dense `seq`, drains the xact, and
  dispatches a `DecodeJob`. Cheap dispatcher unless spill/backpressure/barrier.
- ack collector: refcounted **contiguous** watermark → `emitter_ack_lsn`
  (advertised as standby apply_lsn; one stuck seq stalls everything after it).

## Where the bench lives

`tests/wal_stream_throughput.rs`, `#[ignore]` tests. Run **isolated** (cargo
runs test fns in parallel, which contaminates timings):

```
cargo test --release --test wal_stream_throughput <name> \
  -- --ignored --nocapture --test-threads=1
```

## Stages

### Stage 0 — pump + queueing (existing: `pump_throughput_breakdown`)
pump → queueing → counter. The front-end ceiling.
- Raw `CountingRecordSink` ~3.05M rec/s; **pump + queueing ~1.1M rec/s**
  (QueueingRecordSink's clone-into-owned + mpsc + cross-thread wakeup is a
  ~2.5× tax; clone-only is 2.54M, the channel halves it).
- **Verdict: front end is not the bottleneck** (>1M rec/s ≫ any CH rate).

### Stage 1 — batcher + ack, no CH (DONE: `pipeline_tail_breakdown`)
Synthetic, pre-resolved `RoutedRow` → batcher (encode + block build) → a
**test-local null inserter** that drains each sealed batch and `ack.acked()`s
immediately (no CH) → ack collector.
- `run_ack_only`: ack collector alone (register/placed/acked).
- `run_tail`: + batcher encode + block-build + two channel hops + null inserter.

Results (contaminated by concurrent run — rerun isolated):
- **ack only, 1 row/xact: ~840K rows/s.** Per-txn bound: 3 events/xact through
  the single ack actor ≈ 2.5M events/s (~400ns/event).
- **ack only, 1000 rows/xact: ~1B rows/s.** Ack is per-*event*, ~free per-row.
- batcher+ack encode numbers: **TODO — rerun isolated.**

**Finding:** the watermark/ack layer caps **single-row transactions at <1M
txn/s independent of CH** — the coordination tax the profile flagged. Above any
current CH rate, so not today's wall, but the ceiling once CH is fixed.

### Stage 2 — + decode pool (DONE)
Feed `DecodeJob` → `decode::spawn_pool` (`decode_and_route`: detoast +
`resolve_at_pooled` + mapping lookup + oracle + build `RoutedRow`) → batcher →
null inserter → ack. `run_decode_pool(label, n_xacts, rows_per_xact, m)`.

Enabled by an **offline-seedable catalog** (shipped):
- `client: Client` → `Option<Client>` in `shadow_catalog.rs`; `ensure_open`
  errors if `None`.
- `ShadowCatalog::seeded_for_test(descriptors, replay_lsn)` (`#[doc(hidden)]`,
  plain `pub` so integration tests see it) pre-loads `by_filenode` + sets
  `last_replay_lsn` high → `relation_at_pooled` cache-hits, `wait_for_replay`
  fast-paths, `detoast` is a no-op for non-TOAST rows → never touches the
  absent client.

**Results (rows/s, no CH):**

| stage | 1 row/xact | 1000 rows/xact |
|---|---|---|
| ack only | ~840K | ~1B |
| + batcher | 757K | 1.99M |
| + decode M=1 | 286K | 430K |
| + decode M=4 | — | 778K |

**Finding — the decode pool is the dominant non-CH cost and scales badly:**
- +decode collapses 1.99M → 430K at 1000 rows/xact (**4.6×**, ~2.3µs/row) for a
  trivial single-int row.
- M=4 only 1.8× over M=1 (430K→778K, ~45% efficiency). The single batcher does
  ~2M alone, so the decode workers themselves are the cap.
- **Cause:** `decode_and_route` locks the shared `Arc<Mutex<ShadowCatalog>>`
  (`resolve_at_pooled`) **and** the `mapping` `RwLock` **per row**, even when
  every row in the xact is the same `rfn`/table → 1000 locks for one
  descriptor, and M workers serialize on the catalog mutex.
- **Fix:** memoize `(rel, mapping)` per distinct `rfn` within a `DecodeJob`
  (locks per-table-per-xact instead of per-row). Further out: per-worker
  descriptor cache / shard the catalog lock.

### Stage 3 — + reorder (DONE)
Pre-buffer each xact's heaps into an `XactBuffer` (`on_heap`), then drive
`ReorderSink` with a synthetic `XLOG_XACT_COMMIT` `Record` → seq + register +
`drain_committed` + dispatch → Stage-2 decode pool → tail.
`run_reorder(label, n_xacts, rows_per_xact, m)`.

Production change (only one): `DdlApplicator` made offline-constructible —
`client: Option<AsyncClient>` + `DdlApplicator::offline_for_test(mapping)`
(`#[doc(hidden)]`); `execute` panics if a `None`-client path is ever reached
(the bench applies no DDL). The synthetic commit `Record` is built directly in
the test — `XLogRecord`/`XLogRecordHeader` have pub fields + `Default`, so no
in-crate helper or exposed `pub(crate)` constant was needed.

**Results (rows/s, no CH):**

| stage | 1 row/xact | 1000 rows/xact | M=4 (1000/xact) |
|---|---|---|---|
| batcher+ack | 738K | 1.86M | — |
| + decode pool | 296K | 423K | 753K |
| + reorder | 275K | 484K | 743K |

**Finding — reorder is ~free.** `reorder+tail` tracks `decode+tail` (275K vs
296K at 1 row/xact; 743K vs 753K at M=4). The single-threaded commit
coordinator (seq + register + drain + dispatch + buffer absorb/lock) adds only
~7% at 1 row/xact and is within noise at 1000 rows/xact. **It is not the
bottleneck** — a thin dispatcher, as designed. The decode pool's per-row
catalog locking remains the wall. Whole non-CH pipeline (minus WAL parse):
**~275K single-row-txn/s**, gated by the decode pool.

### Stage 4 — + WAL heap decode (`BufferingDecoderSink`) (DONE)
Drive `BufferingDecoderSink` with synthetic **parsed** heap-insert `Record`s
carrying **real tuple bytes** (the missing piece — `build_segment`'s records had
`data_length=0`, nothing to decode) → `decode_heap_record` + buffer absorb run
inline on the single caller (the production queueing worker's serialization
point); the COMMIT then drives `ReorderSink` → Stage-2 decode pool → batcher →
null inserter → ack. `run_wal_decode(label, n_xacts, rows_per_xact, m)` in
`tests/wal_stream_throughput.rs`; isolated targets `wal_decode_1row_m1` / `_m4`.

Driven directly (no `QueueingRecordSink`) to stay comparable with Stage 3's
`run_reorder`; the queueing channel/clone tax is Stage 0. `m` is the *downstream
decode-pool* width (the WAL decode itself is single-threaded), so M=4 tests
whether downstream parallelism rescues the front stage — it doesn't.

Tuple builder `heap_insert_record`: one `int4` `public.t` row, block data =
`xl_heap_header(5) + 1 pad byte + 4-byte int4`, `t_hoff=24`, route `ToDecoder`.
Real `decode_heap_record` path, no TOAST (`ToastResolver::disabled()`).

**Also updated every earlier stage** to the post-TOAST signatures (`stats` on
`batcher::spawn`, `resolver` on `DecodeCtx` / `ReorderSink::new`) — the committed
microbench no longer compiled against `src/`. That sync-up is the bulk of branch
`harshil/update-micro-bench`.

**Results (rows/s, no CH; current tree with mimalloc — whole ladder re-run):**

| stage | 1 row/xact | 1000 rows/xact | M=4 (1000/xact) |
|---|---|---|---|
| ack only | 1.19M | 1.13B | — |
| + batcher | 962K | 3.89M | — |
| + decode pool | 735K | 3.82M | 4.20M |
| + reorder | 516K | 3.54M | 4.35M |
| **+ WAL decode** | **336K** | **0.98M** | **1.02M** |

**Finding — WAL decode collapses throughput, but NOT because decode is
expensive.** Adding the front decode drops single-row 516K→336K and 1000-row
3.5M→1.0M (3.5×), and downstream M=4 can't recover it (+3.6% vs +23% one stage
below) → the single-threaded front stage is the ceiling. BUT the perf profile
(below) shows the cost is **async coordination, not decode CPU**: the extra
`.await` points the front decode adds to the per-row critical path, not
`decode_heap_record` (which is <1% of samples).

## Perf profile of Stage 4 (`wal_decode_1row_m1`, single-row)

Recorded with `perf record -g --call-graph dwarf -F 999` over
`BENCH_ROWS=20000000`, built release + `CARGO_PROFILE_RELEASE_DEBUG=1
RUSTFLAGS="-C force-frame-pointers=yes"`. Needs `kernel.perf_event_paranoid<=1`.

**The single-row path is bound by per-row async coordination, not compute.**
By DSO: ~38% kernel, ~14% libc, the rest in-app — but the in-app hot symbols are
almost all tokio sync primitives, not walshadow logic.

- **Kernel ~38%:** futex 13.4% · schedule 9% · epoll_wait 3.2% · write 2% ·
  wakeups (`try_to_wake_up`/`wake_up_q`) ~5%. Pure tokio park + cross-thread
  wakeup churn.
- **Hot app symbols:** `notify::NotifyGuard::notify_waiters` 5.4% ·
  `decode_and_route` (the decode *pool*, not WAL decode) 4.8% ·
  `ack::AckState::advance` 4.7% (+`apply` 1.6%) · `watch::BigNotify::
  notify_waiters` 2.5% · `batch_semaphore::Acquire::poll` 2.2% · `mpsc Tx::send`
  ~3% · `Rx::pop`/`recv` ~2% · `mi_free` 2.9% · `memmove` ~4.6% ·
  `BTreeMap::insert` (batcher) 1.7%.
- **The Stage-4 front WAL decode is negligible:** `decode_heap_record` 0.3% ·
  `decode_tuple_payload` 0.5% · `relation_at` ~1.6% · `BufferingDecoderSink::
  on_record` 0.6% · `XactBuffer::absorb` 0.6%. Whole front stage ≈ 4%; the byte
  parse itself is <1%. **No catalog-mutex contention hotspot.**

**Refutes the throughput-ladder hypotheses.** Neither `decode_heap_record` (byte
parse) nor a per-record catalog `Mutex` is the cost. Why is wal-decode slower
than reorder single-row, then? The front decode adds two `.await`s per record on
the hot driver task (`catalog.lock().await`, `buffer.lock().await`, plus the
`DecoderXactPair` fan) → more yields → more scheduler/futex round-trips. We added
*scheduling points* to the per-row critical path, not compute. The two biggest
*walshadow* costs (decode pool 4.8%, ack watermark ~9% counting advance+apply+its
watch-notify) are also coordination-adjacent.

**Caveats:** (1) single-row is the pathological coordination-bound case —
profile the 1000-row case to see whether compute ever takes over. (2) Cache-hot
floor (seeded catalog → `relation_at`/`wait_for_replay` fast-path, 1 int4 col →
no detoast, no live ingest) → bounds the *ceiling*, not the production ~30K
rec/s; that gap is live-contention/replay-gating this bench deliberately removes.

## Optimization menu (from the Stage-4 profile)

Tagged **[small]** = single-row / coordination-bound (what we profiled),
**[bulk]** = 1000-row / compute-bound. Impact = expected, given the profile.

### 1. Cut task hops & coordination — biggest lever [small]
Every row crosses ~5 task boundaries; each hop = send + capacity-semaphore +
notify + possible park/wakeup. ~38% kernel + most app self-time lives here.
- **Group-commit small txns into one `DecodeJob`** (High). At 1 row/xact each
  commit dispatches a 1-row job → maximal coordination. Drain N committed xacts
  in the reorder coordinator, dispatch one combined job.
- **Fuse adjacent stages** (High): merge decode pool into batcher (or front
  decode into reorder) so a row skips a channel. Fewer channels = fewer wakeups.
- **Coarser `decoder_batch_size`** at `QueueingRecordSink` (Medium): bigger
  drains per worker wake-up amortize the futex round-trip.
- **Send slices, not items** (High): the `DecodeJob` channel sends one job per
  xact; send `Vec`/chunks like decode→batcher already does (`BatcherMsg::Rows`).
  One semaphore acquire per chunk instead of per row.

### 2. Redesign the ack/watermark — ~9% of CPU [small]
`AckState::advance` 4.7% + `apply` 1.6% + `watch::BigNotify` 2.5% fire **per
event** (register/placed/acked each row).
- **Coalesce acks** (High): accumulate acked seqs, advance on a tick / per-batch.
- **Drop the per-ack watch-notify** (High): the watermark is already persisted on
  an interval by the status loop → replace `notify_waiters` with a plain
  `AtomicU64::fetch_max` + periodic poll.
- **Contiguous fast-path**: when acks arrive in seq order, skip gap-tracking.

### 3. Kill per-row allocation — ~7% (`mi_free` 2.9% + `memmove` ~4.6%) [small+bulk]
Cross-thread alloc-here/free-there thrashes even mimalloc.
- **Pool/reuse buffers**: recycle `Vec<Record>`, `DecodedHeap`, `RoutedRow`,
  column buffers (freelist / object pool) instead of alloc+free per row.
- **`SmallVec` for `DecodedTuple.columns`**: narrow tables get inline storage.
- **Eliminate the pump's `.into_owned()` clone** (Stage 0 ~2.5× tax): pass
  `Arc<[u8]>`/shared buffers or extend the borrow instead of deep-cloning each
  record onto the queueing channel.
- **Intern strings**: `qualified_name`/target names as `Arc<str>`, once/desc.

### 4. Swap the runtime/scheduler model [small]
Cross-thread wakeups are the futex source; the chain is mostly serial.
- **Thread-per-core / `LocalSet`** (High ceiling, large rewrite): pin a stage
  chain to one thread so handoffs are local — no cross-thread futex. Erases most
  of the 38% kernel time for the inline chain (monoio/glommio model).
- **`current_thread` runtime** for the serial worker chain (front decode +
  reorder are single-threaded anyway).
- **Fewer worker threads**: 4–8 workers for a serial chain → migration churn;
  match threads to actual parallel width (decode/inserter pool sizes).
- **Tune cooperative budget / LIFO slot** if it ping-pongs.

### 5. Faster channels [small]
tokio mpsc uses a per-send `batch_semaphore` (2.2% + `add_permits`).
- **Replace hot mpsc/mpmc with `flume`/`kanal`/`crossbeam`** (no async semaphore
  per send), or a **disruptor-style ring buffer** for the row hot path.
- **Larger/unbounded capacity** on the hottest hop to avoid backpressure-
  semaphore churn (trade memory; safe here — downstream keeps up).

### 6. Compute-side wins — matter at bulk, not small [bulk]
Won't move single-row (decode <2% there); help the 1000-row regime.
- **Memoize `(rfn → rel, mapping)` per `DecodeJob`** in `decode_and_route` *and*
  `BufferingDecoderSink` — lock per-table-per-xact, not per-row (the Stage-2
  finding; still valid). `decode_and_route` is 4.8% even single-row.
- **Per-worker descriptor cache / shard the catalog `Mutex`** so pool workers
  don't serialize on it.
- **Faster batcher encode**: `BTreeMap::insert` 1.7% → `HashMap` / pre-sized vec
  keyed by table index; SIMD/bulk encode for fixed-width columns.

### 7. Build-level (free, broad) [small+bulk]
- **LTO + `codegen-units=1` + `target-cpu=native`** in `[profile.release]` (none
  set currently).
- **PGO** trained on the bench.
- `#[inline]` the hot per-row functions across crate boundaries.

### 8. Micro / correctness-neutral
- Ensure `TxnSpanRegistry` is zero-cost when tracing is off (`adopt` 0.25%,
  `new_txn_span`) — gate on a cheap bool, not an `Option` deref chain.
- Arena-free `DecodedHeap` per batch instead of per-row `drop_in_place`.

**Ranked recommendation:** for realistic small/medium txns, do (1) group-commit
batching + (2) ack coalescing / drop per-ack notify + (3) kill per-row alloc —
together ~50%+ of observed CPU. Thread-per-core (#4) is highest-ceiling but
largest rewrite. Compute opts (#6) only pay once you push big transactions. Do
**before** optimizing: profile the 1000-row case to decide §1–5 (coordination)
vs §6 (compute).

## Findings so far

- Front end (pump+queueing): ~1.1M rec/s — fine.
- Tail (ack+batcher): ~2–4M rows/s — fine. Ack alone is per-txn bound (~1.1M
  txn/s, ~free per-row → small-txn coordination tax).
- **Decode pool: ~0.7–4M rows/s.** Per-row catalog-mutex + mapping-RwLock
  locking; M-scaling ~1.5×. Was thought to be the wall — superseded by Stage 4.
- **Reorder: ~free** — tracks decode+tail; the single-threaded coordinator is a
  thin dispatcher, not the bottleneck.
- **WAL decode front (Stage 4): the throughput ceiling** — 1000-row 3.5M→1.0M,
  single-row 516K→336K; downstream M=4 can't lift it. BUT (perf) the cost is
  **async coordination, not decode CPU**: `decode_heap_record` <1%, the front
  stage adds `.await` points to the per-row critical path → scheduler/futex
  churn. ~38% kernel futex/schedule, the rest tokio notify/semaphore/channel.
- Profile: CH off-CPU (latency wall at low volume); no CPU hotspot in walshadow
  logic — confirmed again at the WAL-decode stage.
- Combined: **low-volume latency = CH RTT; small/medium-txn throughput =
  per-row async-coordination tax** (channel hops + ack notify + alloc churn),
  NOT decode compute. Per-row catalog locking only matters at large-txn volume.

## Next

1. **Profile the 1000-row case** (`run_wal_decode` at 1000/xact) — decides
   whether to invest in coordination (§1–5 of the optimization menu) or compute
   (§6). Single-row says coordination; bulk may differ.
2. **Top coordination wins** (see optimization menu): group-commit small txns
   into one `DecodeJob`, coalesce acks / drop the per-ack watch-notify, kill
   per-row allocation. Re-run the ladder after each.
3. Compute quick win (bulk only): memoize `(rel, mapping)` per `rfn` in
   `decode_and_route` + `BufferingDecoderSink`.
4. Cross-check on the live daemon: null-inserter run, watch `emitter_ack` rate
   (`:9484`) + `pump.queue`/`dispatch` spans against these ceilings.

## Before results
     counter + noop bytes_sink          593920 records   233.179ms      2547060 rec/s     137.2 MiB/s                                                                                                                                                                              
  counter + shadow bytes_sink        593920 records   250.289ms      2372933 rec/s     127.9 MiB/s
  ack only: 1 row/xact                  1000000 rows      1.140s       877128 rows/s                                                    
  ack only: 1000 rows/xact              1000000 rows   967.522µs   1033568229 rows/s                                                    
  queueing(counter) + noop           593920 records   672.361ms       883335 rec/s      47.6 MiB/s
  queueing(counter) + shadow         593920 records   608.987ms       975259 rec/s      52.5 MiB/s
  CountingRecordSink                 593920 records   185.175ms      3207345 rec/s     172.8 MiB/s
  clone-only (no channel)            593920 records   228.528ms      2598893 rec/s     140.0 MiB/s
test pump_throughput_breakdown ... ok                               
  batcher+ack: 1 row/xact               1000000 rows      1.394s       717205 rows/s                                                    
  batcher+ack: 100 rows/xact            1000000 rows   571.613ms      1749436 rows/s                                                    
  batcher+ack: 1000 rows/xact           1000000 rows   504.899ms      1980595 rows/s                                                    
  decode M=1: 1 row/xact                1000000 rows      4.009s       249412 rows/s                                                    
test decode_pool_1row_m1 ... ok                                     
  decode M=4: 1 row/xact                1000000 rows      4.022s       248627 rows/s                                                    
test decode_pool_1row_m4 ... ok                                     
  decode+tail M=1: 1 row/xact           1000000 rows      3.462s       288817 rows/s
  decode+tail M=4: 1 row/xact           1000000 rows      3.176s       314821 rows/s
  decode+tail M=1: 1000 rows/xact       1000000 rows      2.396s       417380 rows/s
  decode+tail M=4: 1000 rows/xact       1000000 rows      1.378s       725790 rows/s
  reorder+tail M=1: 1 row/xact          1000000 rows      4.190s       238663 rows/s
  reorder+tail M=1: 1000 rows/xact      1000000 rows      2.124s       470813 rows/s 
  reorder+tail M=4: 1000 rows/xact      1000000 rows      1.309s       763682 rows/s                                                    
