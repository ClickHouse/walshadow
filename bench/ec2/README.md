# walshadow on EC2

EC2 harness for comparing PG→destination replication engines, all `c8i.2xlarge`
(8 vCPU) in a dedicated VPC (`10.42.0.0/16`, ap-south-1), one AZ so no
cross-AZ latency. A **setup** =
a base (always the source Postgres primary; plus ClickHouse for the CDC
engines) + one **streamer** node that does the replication. The base is shared,
so you swap the streamer while keeping the same source data.

| setup | streamer node | base | destination | notes |
|-------|---------------|------|-------------|-------|
| `walshadow` | `ec2-walshadow` | source-pg + clickhouse | ClickHouse | custom WAL→CH daemon; image built locally |
| `peerdb`    | `ec2-peerdb`    | source-pg + clickhouse | ClickHouse | self-hosted PeerDB / ClickPipes |
| `pg`        | `ec2-pg-standby`| source-pg only         | the standby itself | PG→PG physical streaming replica |

Provisioning is terraform (`terraform/`): VPC, per-node SGs, instances
(cloud-init from each node folder), one shared key pair
(`terraform/walshadow-bench.pem`), and a `state.env` endpoint manifest written
into each node folder for `deploy.sh` / `profile.sh` / the bench to read. The
desired node set (streamer, clickhouse, bench runner) persists in
`terraform/setup.auto.tfvars`, written by `stack.sh`. Post-boot setup stays
shell: node folders keep `cloud-init.yaml` and, where needed, `deploy.sh` /
`pre_down.sh`. Shared scripts sit alongside `stack.sh`: `aws-env.sh` (creds),
`lib.sh` (ssh, state.env, readiness waits) and `profile.sh` (on-CPU capture for
any streamer). `*.pem`, `state.env` and terraform state are gitignored.

Record account and profile in `aws.local.env`, which stays gitignored and is
required by `aws-env.sh`. Account check aborts when credentials resolve to a
different account, then Terraform checks same account through
`allowed_account_ids`:

```bash
echo 'BENCH_AWS_ACCOUNT=<account-id>' > aws.local.env
echo 'BENCH_AWS_PROFILE=<p>' >> aws.local.env
aws sso login --profile=<p>     # renew expired session
```

After the account check, `aws-env.sh` resolves that profile to session keys
(`aws configure export-credentials`) and exports them, so terraform runs on
exactly the creds the check validated — its Go SDK rejects a stale SSO token
that the CLI would still serve from cache.

## stack.sh — the main interface

```bash
cd bench/ec2
./stack.sh up <setup>        # terraform apply (base + streamer), then deploy the streamer
./stack.sh down              # tear down current streamer (base kept)
./stack.sh down --all        # terraform destroy (everything)
./stack.sh bench up|down     # optional in-VPC bench-runner box (+ deploy)
./stack.sh status            # list running project instances
```

`terraform apply` is interactive — review the plan before confirming,
especially on setup swaps (e.g. walshadow→pg destroys the ClickHouse node).
Before a streamer node is destroyed or swapped, `stack.sh` copies any on-CPU
profiles off it and runs its `pre_down.sh` hook. Start a capture with
`./profile.sh <walshadow|peerdb|pg-standby> [secs]` just before a benchmark; it
returns immediately and teardown copies the result into the node folder. Terraform can also be driven
directly: `source aws-env.sh && terraform -chdir=terraform plan`. Knobs like
`instance_type` / `az` / `my_ip` are variables (see `terraform/variables.tf`);
`instance_type` is global — the AZ is picked from its offerings.

**Run one setup at a time** (enforced: `streamer` is a single terraform
variable). All setups share the source primary, and the benchmark `TRUNCATE`s
the source table at startup — so a second setup's run would disturb the first.

## Per-setup

### walshadow
Build the image first (the deploy ships a locally-built image rather than building on the box).
`PG_MAJOR` must match `ec2-source-pg` (`postgres:17`) — the shadow data dir comes
from a BASE_BACKUP of the source, so PG 18 binaries cannot open it; `deploy.sh`
compares the two and refuses to start on a mismatch:
```bash
docker build -f docker/Dockerfile --build-arg PG_MAJOR=17 -t walshadow:local .   # run from repository root
./stack.sh up walshadow                                   # source-pg + clickhouse + daemon
./stack.sh down                                           # daemon only (base kept)
```
Building with podman works: it tags locally-built images `localhost/walshadow:local`,
and `docker load` on the node keeps that prefix, so the deploys resolve whichever
tag actually landed (`remote_image_tag` in `lib.sh`) instead of assuming `$IMAGE`.
`deploy.sh` ships `walshadow:local` (`docker save | ssh | docker load`), writes
`ch-config.toml` (ClickHouse private IP, `flush_timeout_ms`), and runs the daemon.

### peerdb
```bash
./stack.sh up peerdb     # source-pg + clickhouse + PeerDB stack, creates the CDC mirror
./stack.sh down          # PeerDB box only; pre_down.sh drops the source replication slot
```
`deploy.sh` brings up the PeerDB compose stack and (idempotently) ensures the
MinIO bucket + Temporal search attribute, points the S3 staging endpoint at the
box's private IP, then drops+recreates the `demo.users` mirror.

### pg (physical standby)
Base is just the source primary — no ClickHouse:
```bash
./stack.sh up pg     # source-pg + pg-standby; pg_basebackup -R, starts streaming standby
./stack.sh down      # standby only
```
`deploy.sh` runs `pg_basebackup` from the primary into a fresh volume and starts
a read-only hot standby that streams WAL. Re-running takes a fresh base backup.

## Benchmark a setup

`walshadow-ec2-bench` reads endpoints from the relevant `state.env`. `--suite
<name>` runs all four benches into `bench/results/<name>/` (a gitignored dir,
created on demand; an existing name is refused). Run it from the repository
root, where the `--state-dir` and `--results-dir` defaults resolve:
```bash
B="cargo run --release --bin walshadow-ec2-bench --"
# CDC engines (walshadow / peerdb) → ClickHouse:
$B --suite walshadow-run                       # --dest defaults to clickhouse
# physical standby:
$B --suite pg-run --dest postgres              # reads ec2-pg-standby
# Run sustained and interleaved loads for five minutes; interleaved-long always runs 10 30-second rounds
$B --suite walshadow-5min --run-secs 300
# one bench on its own, with its own knobs:
$B --bench interleaved --xact-secs 150
```
Each shape runs as a child process, so one failure does not end the pass: its
output is teed to `<shape>.txt` with a `# FAILED` footer and the suite exits
non-zero listing what failed.

## Notes
- `c8i.2xlarge`s bill while running (~8× a t2.small) — `down` (or `down --all`) when idle.
- SSH/Postgres/ClickHouse are open to the operator IP + VPC CIDR only; Postgres
  uses `trust` auth, so keep 5432 off `0.0.0.0/0`. The operator IP is captured
  at apply time — if yours changes, re-run `terraform apply` (or any `stack.sh`
  verb) to refresh the SG rules.
- Applies converge: re-running `up` re-applies drift and re-runs the deploy; a
  changed `cloud-init.yaml` replaces that instance (fresh data).
