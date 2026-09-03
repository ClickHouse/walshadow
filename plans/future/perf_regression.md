# perf_regression — measurement off CI machines

Performance claims get their own workflow on hardware nobody else is
using. `cargo nextest` (and the coverage job that wraps it) proves
behavior; it never proves speed. This doc sets that boundary, then
specifies the workload `bench/` needs to cover initial load, which is the
largest unmeasured stage

## Why CI cannot hold a ratio

A GitHub-hosted runner is 4 shared vCPUs running the whole suite at
`num-cpus` width, so a timing assertion measures scheduler luck. The
coverage job is worse: instrumentation adds counter writes to every
region, which lands unevenly across a pooled path and moves the ratio
with no code change

Concretely, the bridge worker pool compares 4 concurrent `ENCODE_NATIVE`
over 4 sockets against the same 4 over 1 socket, best of three a side, and
asserts speedup above 1.4. It scores ~2.4x on a 16-core box, ~1.7x on a
4-vCPU runner and 1.24x (90.9ms vs 112.6ms) under `cargo llvm-cov` on a
runner also executing ~1200 other tests. Cores cap the win, so the bar can
only be set for a machine that the suite does not get to pick

Rule: no assertion in the test suite may compare wall clock against a
constant or against another wall clock. Printing timings is fine, the WAL
pump microbenchmark test does exactly that and asserts only record counts

## What the suite keeps

Splitting a perf test means keeping its correctness half, which is most of
it. For the bridge pool that is: pool width equals the requested worker
count and is visible in `stats.pool_size`, every slot completed its own
HELLO, `info()` is populated, each concurrent request returns one offset
per row, and catalog `scan` stays pinned to slot 0 whatever the width.
None of those touch a clock

The ratio moves to the oracle roundtrip stage, whose job already is
pricing one batch alone and with the pool saturated. Report core count
beside the ratio, since that is the term that sets the ceiling

## bench/ is durable, benches/ is not

Two homes, different lifetimes:

* `bench/` is a deployed workload engine: a crate with shapes, an
  instrument split (`count_all` cadence for throughput, `count_id` probes
  for latency), a `Destination` abstraction covering ClickHouse and a PG
  standby, plus the EC2 harness that stands up a comparable box. Shapes
  are cross-engine, so they survive any internal refactor
* `benches/*.rs` are task-scoped microbenches. Each exists to settle one
  contested optimisation (pump-vs-drain CPU split, oracle framing passes)
  and stops earning its keep once that question closes. Expect the
  directory to churn and to empty

So the perf workflow keys off `bench/` shapes and never off a `[[bench]]`
target name. Deleting a microbench must stay a file delete plus a manifest
stanza: `docker/Dockerfile.bench` already copies `src` and `bench` without
`benches` and builds `-p walshadow-bench`, so the deployed driver does not
see them. A microbench that does stay must answer `--list` with
`<name>: benchmark`, else `nextest --all-targets` fails listing

## Finding the next bottleneck

Gating answers "did this get worse". Optimisation work asks the other
question, "what owns the time", and that needs a profiler plus one
microbench per contested claim, on the same dedicated box

On-CPU capture: `bench/ec2/profile.sh <streamer> [secs]` starts a capture
beside a run and teardown copies the result into the node folder. The
release profile carries `debug = "line-tables-only"`, so `perf annotate`
and `--sort=srcline` attribute samples to source lines at no runtime cost

Criterion earns its keep where a claim is a per-call cost over a stable
input: statistical comparison, saved baselines, outlier detection. Where
the claim is a staged pipeline needing allocator choice, thread counts and
synthetic segments, the flag-driven `harness = false` shape stays. Either
way the allocator must match the daemon's (mimalloc): the walk allocates
decoded values on one thread and the drain frees them on another, and
glibc's shared arena serializes on that pattern hard enough to invert the
conclusion, reporting a parallel pump as slower than the serial one

Allocation count and peak RSS are a separate instrument from wall clock,
answered post hoc by a counting allocator wrapper plus RSS sampling, per
stage. The zero-copy framing pass predicts a 100k-record heap-INSERT
segment dropping ~200 MB of allocator traffic to ~0, worth 1.5-3x decode
throughput off lost allocator pressure. Unmeasured

Candidates already named, each wanting a number before code:

* **CRC32C rewrite parallelism.** The filter rewrites every kept record's
  CRC on one thread at ~1 ns/byte, so 1 GB/s of WAL saturates a core.
  Records are independent post-classification, see [risks.md](risks.md)
* **`XLogRecord.blocks` allocation.** `Vec<XLogRecordBlock>` per record
  where records average 0-2 blocks; `SmallVec<[_; 2]>` keeps the common
  case stack-resident. Ranked below byte-traffic wins
* **Block-header walk.** The parser walks blocks twice, once pushing
  headers and once attaching image/data slices. Not redundant: a record
  body carries every block header before any block payload, which is why
  PostgreSQL's own `DecodeXLogRecord` splits the same way. Any win here is
  pre-sizing the vector, not merging the passes

## Workflow shape

Trigger on `workflow_dispatch` plus a schedule, never on pull_request.
Runs either on a single-tenant self-hosted runner or by driving
`bench/ec2/stack.sh` (`up <setup>`, `bench run <name>`, `down`), which
already pins instance type and AZ and writes `provenance.txt` (setup,
instance type, AZ, commit) beside the results. Publish numbers as
artifacts; do not fail a PR on them

## Shape: initial load

`bench/` covers steady-state only. Initial load is where BASE_BACKUP wire
throughput, source-PG read cost and ClickHouse insert saturation meet, and
it has no shape

### Two triggers, different cost

Greenfield bootstrap (`--bootstrap-mode=direct|object_store`) runs only
against an empty shadow data dir with no completion marker. Triggering it
means wiping the data volume and restarting the container, lifecycle
control the bench engine does not have by design

Per-table backup backfill (`initial_load='base_backup'|'object_store'`)
drives the same `PageWalkSink` → gate → bootstrap-drain path on a live
daemon. Trigger is an INSERT into `config_table` on the source plus
`pg_switch_wal()`, nothing else

Both cover the page walk, the backup sink and source, and the bootstrap
drain. Per-table needs no restart and no SSH, so it carries the primary
workload; greenfield rides a second shape behind a reset hook

### Assumptions the engine breaks

* `run()` truncates the source table unconditionally. An initial-load
  dataset *is* pre-existing source state, so the clean-slate preamble goes
  per shape: clear the destination, never the source
* `Throughput::from_curve`'s `max_backlog` term is `achieved_rate * at`,
  meaningful only for an interval-driven producer. Against a static source
  it is fabricated, so suppress it. `all_visible_at` and `peak_rate` stay
  valid
* The latency instrument has no meaning here, no commit instant exists.
  `count_id` probes, probe slot states and the probe clock all drop out.
  Throughput instrument only
* Seed into `demo.bl_<runid>`, never the table other shapes truncate. An
  auto-create namespace rule already covers new tables there

### `--bench initial-load`

1. **Seed** `CREATE TABLE demo.bl_<runid>`, fill server-side via
   `INSERT … SELECT FROM generate_series`, rows and width from flags.
   Report seed wall time and `pg_total_relation_size` apart from the
   measurement. Relation size is the MiB/s denominator
2. **Trigger** INSERT into `config_table(namespace, relname, replicate,
   initial_load)`, then `pg_switch_wal()`. `t0` is that commit on the
   driver's own monotonic clock, no cross-host coordination
3. **Watch** reuse the `count_all` sampler at `count_interval_ms`.
   Completion predicate `count_all >= N` is exact while the source stays
   static, no FINAL, matching the no-FINAL discipline elsewhere
4. **Report** elapsed, rows/s, MiB/s over heap bytes, peak sampled rate,
   plus stage attribution when a metrics endpoint is configured

Per-run reset is a fresh table name, not a restart: the spill-dir backfill
ledger marks a qname done and no later boot re-runs it. A fresh table per
run also grows cluster size, which is how `base_backup`'s cluster-sized
bandwidth cost becomes visible

Mode sweep (`copy` vs `base_backup` vs `object_store`) is one flag on the
same shape, under one instrument

### Concurrent-write variant

`--concurrent-rate` inserts during a pass, exercising walk/live-stream
overlap and the staging swap. The completion predicate must then become
`count(distinct id) == N` or FINAL: overshoot past N is legitimate there,
dedup absorbs it at merge

### Stage attribution

`--metrics-url` scrapes the daemon's metrics endpoint on the count cadence
(GET; the existing ClickHouse HTTP client is POST-only, so this needs a
sibling):

* `walshadow_config_backfills_pending{mode="…"}`, the daemon-side
  start/stop bracket for a pass
* `walshadow_bootstrap_decode_seconds_total`,
  `walshadow_bootstrap_tap_seconds_total`,
  `walshadow_bootstrap_channel_block_seconds_total`
* `walshadow_inserter_ch_seconds_total`,
  `walshadow_inserter_encode_seconds_total`
* `walshadow_process_cpu_seconds_total`

Answers which of tap, decode, channel block or ClickHouse insert owns wall
clock at real scale. Unset skips it, so the same shape still runs against
PeerDB's snapshot and a standby's `pg_basebackup`, giving a cross-engine
initial-load comparison

### Daemon prerequisites

* The per-table backfill builds its walk sink without stats and hands the
  source a throwaway `PumpStats`, discarding every stage counter. Thread a
  shared progress handle through the pass context and publish it (same
  series, or `phase=` labelled) or the restart-free workload stays
  wall-clock only
* The bootstrap-phase ticker publishes bootstrap fields over
  `MetricsSnapshot::default()`, so every other series, uptime included,
  reads 0 for the bootstrap duration then jumps when the status loop takes
  over. That reads as a counter reset to any scraper and rules out uptime
  as a `t0` anchor. Merge into the live snapshot instead of replacing it

### `--bench bootstrap` (greenfield)

Keep out of `--suite`: the steady-state pass is self-contained, this is
not. Add `stack.sh bench bootstrap <name>` alongside `run`/`fetch`,
sequencing seed → wipe volume + redeploy → one shape → fetch

Give the shape `--reset-cmd '<shell>'` so the bench owns `t0`: run the
command, anchor at the first metrics scrape that answers *after* a
refusal. A first scrape that already answers means daemon start time is
unknown, so report that instead of anchoring wrong, the same rule the
sustained shape applies when rows go missing. One flag covers local
compose and EC2 SSH without the engine knowing either; SSH stays out of
the engine and the local driver shares the flag

### Relation to the pump microbench

Not redundant, and not a substitute. The microbench isolates pump-vs-drain
CPU on one host with in-memory tars. This workload measures wire
throughput, source-PG cost and destination insert saturation at scale, and
it outlives the optimisation that motivated the microbench

## Calling a regression

Numbers from a shared box are unusable, and numbers from a dedicated box
are only usable with a band. Establish the band before gating anything:
repeat the same shape on the same commit until the spread is known, and
express thresholds in terms of that spread, not a guessed percent

* Report median and best-of together. Best-of hides contention, median
  hides the tail; a move in one only is a machine story
* Require a regression to reproduce on a second pass before it counts
* Key stored baselines by (setup, instance type, shape flags, commit).
  A baseline from another instance type is not a baseline
* Publish first, gate later. A perf gate that fires on variance gets
  disabled, which is worse than no gate
