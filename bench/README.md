# Replication benchmarks

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
