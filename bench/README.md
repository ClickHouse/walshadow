# Replication benchmarks

## Transaction spill

Run buffer admission, spill encoding, commit drain, and cleanup without servers:

```sh
cargo bench --bench xact_spill -- --buffer-bytes 1048576 --rows 7821
cargo bench --bench xact_spill -- --buffer-bytes 1048576 --rows 781 --transactions 80
cargo bench --bench xact_spill -- --rows 111108 --transactions 2
```

Use `--dir` to select filesystem; default `target`. Avoid tmpfs for disk benchmarks.
Defaults use 1 KiB text payloads and three repeats. Verify every row's payload,
xid, LSN order, descriptor, and file cleanup. Write timing includes allocation
and admission; read timing includes reader setup, verified drain, and unlink.
Report spill bytes alongside throughput: final memory tails can avoid disk.
Compare equal spill bytes when isolating I/O improvements.

Files follow disposable-spill semantics: no fsync, immediate replay, then unlink.
Results include page cache and codec costs; they do not measure sustained device
bandwidth or PostgreSQL-to-ClickHouse lag. Buffer peak reports post-admission
payload estimates; use process RSS to measure allocator and I/O buffer overhead.

## Spool replay

Measure writes and replay for bootstrap deferred spool and emit plan spool:

```sh
cargo bench --bench spool_read -- --rows 200000 --payload-bytes 256
cargo bench --bench spool_read -- --plan-only --planner --mapped --rows 1000 --payload-bytes 0
cargo bench --bench spool_read -- --plan-only --planner --mapped --rows 200000 --payload-bytes 256 --relations 8 --control-every 10000
```

Use `--planner` to run `Planner::plan_batch` over drained slices built before
timing starts. Rows enter as WAL insert records and encode from tuple views. Use
`--mapped` to assign frozen routes over an int4 and a text column. By default,
heaps have no mapping: planning counts and discards them without encoding.
Use `--relations` to cycle through relation descriptors, one per row.
Use `--control-every` to insert unrelated schema controls and clear cached routes.
Use `--plan-only` to skip deferred spool measurements. Plans spill to disk when
encoded chunks exceed default 1 MiB memory limit.

Read `plan_total` for combined write and replay time per sample. For plans in
memory, byte counts report allocated chunk bytes; for spilled plans, encoded
file size. Route caching measures descriptor identity checks and reuse of cached
references. Measurements exclude mapping resolution, WAL draining, TOAST I/O,
control execution, and PostgreSQL/ClickHouse I/O.

## pgbench workload

Small-transaction source load for queueing worker throughput, see
[pgbench/README.md](pgbench/README.md)

## Local runs

Start PostgreSQL, ClickHouse, and walshadow with SQL runtime config installed
Run from repository root, override endpoint flags as needed

```sh
cargo run --release -p walshadow-bench --bin walshadow-local-bench -- \
  --bench initial-load --initial-load-mode base-backup \
  --seed-rows 25000000 --row-width 128 \
  --pg-host 127.0.0.1 --ch-host 127.0.0.1 \
  --initial-load-database demo --count-interval-ms 250 \
  --metrics-url http://127.0.0.1:9484/metrics \
  --initial-load-timeout-secs 1800 | tee /tmp/initial-load.txt
```

Choose `copy`, `base-backup`, or `object-store` with `--initial-load-mode`
For object-store, configure daemon archive source and pass
`--prepare-cmd /path/to/create-backup.sh` to capture seeded table before opt-in
Load benchmarks require daemon stage metrics and support walshadow only

Each run creates and seeds a fresh table, drains seed WAL, then opts into replication
Results include time to visible rows, time to settlement, and row verification
Use `--target-load-secs 300` to report a suggested seed count, then fix rows and
width across comparisons

For streaming workloads, use `--bench single-row`, `sustained`, `interleaved`,
or `interleaved-long` against an already replicated `demo.users` table
Use `--help` for endpoint and workload flags

## Greenfield bootstrap

```sh
cargo run --release -p walshadow-bench --bin walshadow-local-bench -- \
  --bench bootstrap --seed-rows 25000000 --row-width 128 \
  --metrics-url http://127.0.0.1:9484/metrics \
  --stop-cmd /path/to/stop-benchmark-shadow.sh \
  --reset-cmd /path/to/reset-benchmark-shadow.sh | tee /tmp/bootstrap.txt
```

Provide hooks for your deployment:

- Stop hook: stop daemon and wait for process exit
- Reset hook: clear benchmark shadow data and completion markers, install
  `BENCH_BOOTSTRAP_CONFIG` in daemon TOML config, then launch daemon against same
  endpoints and return immediately
- Optional prepare hook: create object-store backup after seed and stop

Hooks receive quoted `BENCH_SOURCE_TABLE`, `BENCH_DESTINATION_TABLE`, and
`BENCH_BOOTSTRAP_CONFIG` in environment
Install generated config after conflicting namespace settings, for example:

```sh
printf '%s' "$BENCH_BOOTSTRAP_CONFIG" > /path/to/ch-config.d/benchmark.toml
```

Configure direct or object-store bootstrap source on daemon
`--initial-load-mode` selects per-table loads only
Use `exec` in hooks for subprocesses requiring timeout cancellation
Bootstrap timing includes reset hook and startup, excludes seed, stop, and prepare

## EC2

Install Terraform, AWS CLI, SSH, and Docker or Podman
Set account and profile in ignored `bench/ec2/aws.local.env`:

```sh
cat > bench/ec2/aws.local.env <<'CONFIG'
BENCH_AWS_ACCOUNT=<account-id>
BENCH_AWS_PROFILE=<profile>
CONFIG
aws sso login --profile=<profile>

docker build -f docker/Dockerfile --build-arg PG_MAJOR=17 -t walshadow:local .
cd bench/ec2
./stack.sh up walshadow
./stack.sh bench run walshadow-run
./stack.sh bench initial-load base-copy --initial-load-mode copy --seed-rows 25000000
./stack.sh bench initial-load base-backup --initial-load-mode base-backup --seed-rows 25000000
./stack.sh bench bootstrap fresh-shadow \
  --stop-cmd /opt/bench/stop-shadow.sh --reset-cmd /opt/bench/reset-shadow.sh
./stack.sh bench fetch walshadow-run
```

Run one setup at a time, streaming workloads truncate source table
Choose `up peerdb` for PeerDB or `up pg` for physical PostgreSQL replication
Pass `--dest postgres` to `bench run` for `pg`
Review Terraform plan when swapping setups, swaps can destroy ClickHouse node

EC2 runs execute inside VPC and save logs, provenance, and graphs under
`bench/results/<name>/`, use a fresh name for each run
Provision hooks and dependencies under runner's `/opt/bench`, mounted into
benchmark container, SSH keys are not copied automatically
Metrics endpoint resolves automatically from walshadow node state

Set `BENCH_REUSE_STACK=1` to skip provisioning and driver deployment for repeated runs
Set `BENCH_EXPECTED_REVISION` to full daemon commit ID to check image revision label
Set `DATA_VOLUME` during walshadow deployment to isolate shadow state

```sh
./profile.sh walshadow 60       # capture CPU profile, teardown fetches it
./stack.sh status
./stack.sh down                 # remove streamer, keep source, destination, runner
./stack.sh down --all           # remove all resources when finished
```

## Compare runs

Run from repository root, pass captured logs or EC2 result directories:

```sh
cargo run --release -p walshadow-bench --bin walshadow-bench-plot -- \
  --run bench/results/base-run --label base \
  --run bench/results/optimize-run --label optimize \
  --out bench/results/comparison
```

For local runs, pass `--run /tmp/initial-load.txt`
Outputs include PNG graphs, `summary.csv`, `summary.json`, and `stages.csv`
EC2 generates graphs before fetching, `bench plot` and `bench fetch` need no AWS credentials

Repeat with matching mode, rows, width, source size, hardware, PostgreSQL version,
daemon config, and sampling flags, use same label for repeats of one revision
Remove prior benchmark tables, SQL config rows, and generated TOML fragment between
comparisons, base backup transfers whole cluster
Keep stdout logs and adjacent `provenance.txt` for revision attribution

Stage metrics cover bootstrap, shadow replay, COPY, insert flush, publication,
and settlement, include waits and overlap, so do not sum into wall-time percentages

## Production-shaped drain (ceramic)

`bench/ceramic/` generates a fixed WAL backlog with synthetic data. A status
update leaves an out-of-line JSONB value unchanged, so Walshadow fetches it
from shadow PostgreSQL and may have to wait for replay. The workload also
generates row locks, index changes and full-page images.

```sh
bench/ceramic/prepare.sh <oldest-tree>       # golden snapshot, once
DAEMON_CPUS=0-7 SRC_CPUS=8-15 CH_CPUS=16-23 bench/ceramic/run.sh <tree>
WAL_FEED=archive bench/ceramic/run.sh <tree> # same WAL through a local archive
bench/ceramic/ab.sh <treeA> <treeB> 4         # interleaved, mean ± stdev, B/A
```

Needs PostgreSQL 18 (source and shadow share the major; `PG=17` works),
its server dev package for the pgext build, pgbench, `bc` and a `clickhouse`
binary. `prepare.sh` seeds the source (24 tables, the hot one with six
btree indexes including two partial on status, lz4 nested jsonb two and
one TOAST chunks wide), bootstraps a shadow in `[toast] mode = "shadow"`,
stops the daemon and generates about 5.6M records of WAL behind it. It
keeps the result as a golden snapshot: shadow, out, spill and source WAL on
`DISK` (a real disk), and the source data files, sockets and its own
ClickHouse on `SHM` (tmpfs). `run.sh` restores the snapshot (WAL segments
are hard-linked), starts the daemon pinned and resumes it from the persisted
floor. It then drains to the fixed end LSN and appends one line to
`$DISK/results.tsv`. Archive runs expose completed segments through a local
`file://` archive and require evidence of archive replay.
Ports default to 62432-62441 and 62484.

Bootstrap the golden with the oldest tree under comparison: newer trees
resume its state. Swap `SCHEMA`, `SEED` or `WORKLOAD` for other shapes.
`UPD_RECENT_FRAC` (default 0.5) aims that share of transitions at
recently inserted rows instead of random seeded ones. `UPD_REPEAT`
(default 3.5) sets transitions per insert. `CHECKPOINT_S` sets the FPI
share. Set it to `0` to skip forced checkpoints. Change `WAL_COMPRESSION`
from `lz4` to `off` to increase WAL bytes per row without adding updates.
`UPD_PM` (default 910) and `INLINE_UPD_PM` (default 0) set TOAST-bearing and
narrow updates per 1,000 script runs. Use separate short `DISK` and `SHM` paths
for each snapshot so PostgreSQL's Unix socket paths fit. Pass `BIN` and
`BRIDGE_LIB_DIR` for prebuilt artifacts
(`BIN_A`/`BIN_B` and `BRIDGE_LIB_DIR_A`/`BRIDGE_LIB_DIR_B` for `ab.sh`).

For a two-shape comparison, start with `UPD_PM=90 INLINE_UPD_PM=820` in both
snapshots. Use `CHECKPOINT_S=1 WAL_COMPRESSION=lz4` for smaller WAL volume
and `CHECKPOINT_S=0.05 WAL_COMPRESSION=off` for larger WAL volume. Tune
checkpoint frequency and seed size against `WAL-KiB/row` and
`TOAST-batches/1k-rows`, then reuse each snapshot across builds and feeds.

`drain` measures from daemon launch to ClickHouse acknowledgment of the fixed
end LSN. Whole-run metrics include source WAL MiB/s, emitted rows/s, TOAST
fetch batches/s, WAL KiB per emitted row and TOAST fetch batches per 1,000
emitted rows. `TOAST-ms/batch` includes time waiting for replay and can include
overlapping read-ahead. Use `TOAST_READ_AHEAD=false` on builds that support
it to measure read-ahead's effect on the same snapshot. `ab.sh` accepts
`TOAST_READ_AHEAD_A` and `TOAST_READ_AHEAD_B` for interleaved comparisons.
Compare these metrics before interpreting a higher WAL rate.

Worker metrics cover the window from the first to the last record out:

- `rec/s`, `B/rec`: worker throughput and backlog bytes per record
- `<thread>=%core/ns-per-record`: `tokio` sums the runtime workers, and
  `cores` is the whole daemon
- `wait`: pump seconds per second blocked on the full worker queue
- `f`: TOAST values fetched per record. `c_fetch` is the time per fetch,
  and `fetch s/s` is the share of wall time spent fetching. Bridge
  `fetch_toast` and `replay_lsn` are per-call latencies
- `ack_tail`: time from the last record out to ClickHouse acknowledgment

The worker model is `rec/s = 1 / (c_rec + f * c_fetch)`. `c_rec` is the
per-record cost without fetching, so `1/rec_s - f * c_fetch` estimates it.
A fix that batches or overlaps fetches shows up as a drop in `fetch s/s`
and `c_fetch`. One that cheapens discarded records shows up in `c_rec`.
