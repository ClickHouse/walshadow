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
`profile.sh` / `pre_down.sh`. Shared script helpers live in `aws-env.sh`
(creds) and `lib.sh` (ssh/state.env). `*.pem`, `state.env` and terraform state
are gitignored.

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
profiles off it and runs its `pre_down.sh` hook. Terraform can also be driven
directly: `source aws-env.sh && terraform -chdir=terraform plan`. Knobs like
`instance_type` / `az` / `my_ip` are variables (see `terraform/variables.tf`);
`instance_type` is global — the AZ is picked from its offerings.

**Run one setup at a time** (enforced: `streamer` is a single terraform
variable). All setups share the source primary, and the benchmark `TRUNCATE`s
the source table at startup — so a second setup's run would disturb the first.

## Per-setup

### walshadow
Build the image first (the deploy ships a locally-built image rather than building on the box):
```bash
docker build -f docker/Dockerfile -t walshadow:local .   # from repo root
./stack.sh up walshadow                                   # source-pg + clickhouse + daemon
./stack.sh down                                           # daemon only (base kept)
```
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

`walshadow-ec2-bench` reads endpoints from the relevant `state.env`. Use
`run_bench_suite.sh <name>` (at the bench crate root) to run all four benches into
`bench/results/<name>/` (a gitignored dir, created on demand):
```bash
# CDC engines (walshadow / peerdb) → ClickHouse:
../run_bench_suite.sh walshadow-run            # DEST defaults to clickhouse
# physical standby:
DEST=postgres ../run_bench_suite.sh pg-run     # reads ec2-pg-standby
```

## Notes
- `c8i.2xlarge`s bill while running (~8× a t2.small) — `down` (or `down --all`) when idle.
- SSH/Postgres/ClickHouse are open to the operator IP + VPC CIDR only; Postgres
  uses `trust` auth, so keep 5432 off `0.0.0.0/0`. The operator IP is captured
  at apply time — if yours changes, re-run `terraform apply` (or any `stack.sh`
  verb) to refresh the SG rules.
- Applies converge: re-running `up` re-applies drift and re-runs the deploy; a
  changed `cloud-init.yaml` replaces that instance (fresh data).
