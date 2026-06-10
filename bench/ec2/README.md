# walshadow on EC2

EC2 harness for comparing PG→destination replication engines, all `c8i.2xlarge`
(8 vCPU) in one VPC (`10.0.0.0/16`, ap-south-1). Override per node with
`INSTANCE_TYPE=…`. A **setup** = a base (always the source
Postgres primary; plus ClickHouse for the CDC engines) + one **streamer** node
that does the replication. The base is shared, so you swap the streamer while
keeping the same source data.

| setup | streamer node | base | destination | notes |
|-------|---------------|------|-------------|-------|
| `walshadow` | `ec2-walshadow` | source-pg + clickhouse | ClickHouse | custom WAL→CH daemon; image built locally |
| `peerdb`    | `ec2-peerdb`    | source-pg + clickhouse | ClickHouse | self-hosted PeerDB / ClickPipes |
| `pg`        | `ec2-pg-standby`| source-pg only         | the standby itself | PG→PG physical streaming replica |

Shared helpers live here: `aws-env.sh` (creds), `lib.sh` (AMI/subnet/keypair/SG),
`stack.sh` (orchestrator). Each node folder also has its own `provision.sh` /
`teardown.sh` / `cloud-init.yaml` (and `deploy.sh` where a node needs post-boot
setup). `*.pem` and `state.env` are gitignored.

## stack.sh — the main interface

```bash
cd bench/ec2
./stack.sh up   <setup>        # provision base + streamer, then deploy the streamer
./stack.sh down <setup>        # tear down just the streamer (base kept)
./stack.sh down <setup> --all  # also tear down the base (source-pg [+ clickhouse])
./stack.sh status              # list running project instances
```

**Run one setup at a time.** All setups share the source primary, and the
benchmark `TRUNCATE`s the source table at startup — so a second setup's run
would disturb the first. To switch engines: `down` the old one, `up` the new.

## Per-setup

### walshadow
Build the image first (the deploy ships a locally-built image rather than building on the box):
```bash
docker build -f docker/Dockerfile -t walshadow:local .   # from repo root
./stack.sh up walshadow                                   # source-pg + clickhouse + daemon
./stack.sh down walshadow                                 # daemon only (base kept)
```
`deploy.sh` ships `walshadow:local` (`docker save | ssh | docker load`), writes
`ch-config.toml` (ClickHouse private IP, `flush_timeout_ms`), and runs the daemon.

### peerdb
```bash
./stack.sh up peerdb     # source-pg + clickhouse + PeerDB stack, creates the CDC mirror
./stack.sh down peerdb   # PeerDB box only; also drops the source replication slot
```
`deploy.sh` brings up the PeerDB compose stack and (idempotently) ensures the
MinIO bucket + Temporal search attribute, points the S3 staging endpoint at the
box's private IP, then drops+recreates the `demo.users` mirror.

### pg (physical standby)
Base is just the source primary — no ClickHouse:
```bash
./stack.sh up pg     # source-pg + pg-standby; pg_basebackup -R, starts streaming standby
./stack.sh down pg   # standby only
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
  uses `trust` auth, so keep 5432 off `0.0.0.0/0`.
- Bringing a setup up is idempotent: `provision.sh` reuses a running instance,
  and `deploy.sh` re-applies config.
