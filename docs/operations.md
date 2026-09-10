# Operate walshadow

Keep daemon state on persistent storage, monitor lag and backfills, and use
pause only for bounded maintenance windows

## Check status

```bash
walshadow-stream ctl status
```

Primary fields:

| Field | Meaning |
|---|---|
| `paused` | source consumption intentionally frozen |
| `rows_synced` | rows sent since process start |
| `backfills_pending` | tables still loading existing rows |
| `lag_bytes`, `lag_seconds` | shadow replay distance from source |
| `source_received` | newest source position received |
| `drain` | newest committed position decoded |
| `emitter_ack` | newest contiguous position acknowledged by ClickHouse |
| `shadow_replay` | newest position applied by managed shadow |
| `source_swap_pending` | requested source endpoint has not completed handoff |
| `crossing_blocked_on` | timeline crossing is parked on named proof |

Healthy steady state has `drain`, `emitter_ack`, and `shadow_replay` moving
toward `source_received`. Short differences are expected while batches flush

## Monitor with Prometheus and Grafana

Set `--metrics-bind` to expose Prometheus text metrics. Docker deployment uses
port 9484 and includes optional provisioned dashboard

```bash
docker compose -f docker/docker-compose.yml \
    -f docker/docker-compose.grafana.yml up -d
```

Watch:

- source receive and shadow apply lag
- ClickHouse acknowledgement backlog
- row and byte throughput
- decoder queue depth
- resident memory and spill usage
- pending backfills
- timeline crossing and endpoint swap failures

Alert on sustained failure to advance, not just process availability. Shadow
replay lag measures catalog progress; ClickHouse acknowledgement backlog measures
destination progress. Either can stall while daemon still answers status requests
Choose thresholds from workload's normal lag and retained-WAL budget

Grafana reads walshadow metrics only. It does not connect to source or
ClickHouse

## Pause and resume

```bash
walshadow-stream ctl pause
walshadow-stream ctl status
walshadow-stream ctl resume
```

Pause stops new WAL consumption while already accepted work can drain. Source
keeps producing WAL, so replication slot or archive usage can grow throughout
pause. Keep maintenance window bounded and verify available storage before
pausing a busy source

Pause persists in config and survives restart

## Reload config

Edit base config or operator-owned fragment, then run:

```bash
walshadow-stream ctl reload
```

SIGHUP performs same reload. Prefer `ctl apply` when several values must change
atomically or validation rollback is useful

## Restart safely

Normal stop and start resumes from persisted manifest. Keep together:

- managed shadow data directory
- filtered WAL directory
- spill directory and manifests
- config and fragment directory

Keep descriptor history, backfill ledgers, and deferred TOAST retirement state
alongside manifest. Do not clean spill directory wholesale between starts
Daemon removes transient transaction spill itself and reconstructs it from WAL

Filtered-WAL cleanup also preserves shadow restartpoint. Do not delete segments
solely because current replay position is newer; restarted shadow may still
need them. Disabling configured retention leaves filtered segments on disk

Do not reuse state against unrelated PostgreSQL cluster. Source system-ID
mismatch fails startup

`--ignore-cursor` and `--start-lsn` intentionally discard normal resume
position. Reserve them for recovery drills or operator-directed rebuilds

## Retain source WAL

Configure a physical replication slot for routine deployments:

```sql
SELECT pg_create_physical_replication_slot('walshadow');
```

Then include slot in source URL or config

```toml
[source]
slot = "walshadow"
```

At startup walshadow creates configured slot when absent and reserves WAL
immediately. `init` reports SQL above so slot can exist before daemon starts

Live source moves never create target slot because a new slot cannot protect
earlier resume position. Pre-create target slot before planned switchover

Without slot, ensure `wal_keep_size` or continuous archive covers worst-case
outage and backlog. Missing source WAL stops replication rather than skipping
data

## ClickHouse interruptions

walshadow retries bounded ClickHouse failures with reconnect and backoff. Once
retry budget expires, daemon exits and relies on process supervisor restart

Restart replays from durable floor, and generated `ReplacingMergeTree` tables
converge duplicate row versions by `_lsn`

An outage can grow source slot retention, local filtered WAL, and spill usage
Monitor all three alongside destination backlog. If required source WAL is lost,
recovery needs retained archive coverage or a fresh baseline

If ClickHouse remains unavailable:

1. Keep source WAL available through slot or archive
2. Restore ClickHouse connectivity
3. Restart walshadow if supervisor stopped retrying
4. Confirm `emitter_ack` advances
5. Confirm lag returns toward zero

## Managed shadow

Keep shadow in recovery. Do not promote it, vacuum it locally, or write into its
catalogs. Source catalog vacuum and maintenance arrive through filtered WAL
Shadow is not a data-bearing source replica

Shadow's replication connection to walshadow requires a live stream for catalog
boundary capture; archive files provide recovery fallback, not an equivalent
startup mode. Keep connection timeout enabled and investigate attachment failure
instead of bypassing it

Walshadow's shadow-facing sender currently trusts its client and lacks TLS/SCRAM
Keep that listener on loopback or an otherwise isolated local deployment
Source PostgreSQL and ClickHouse connection security are separate settings

Avoid long-running diagnostic queries on shadow. Sender ignores hot-standby
feedback, so query conflicts can still be canceled by recovery. Type conversion
pins output settings, but matching extension and timezone-data versions remains
a deployment responsibility

## Common startup failures

| Message | Action |
|---|---|
| PostgreSQL version below 16 | upgrade source or use compatible walshadow release |
| `wal_level` is not `logical` | change setting and restart PostgreSQL |
| no usable row key | add primary key, choose replica identity index, or use `FULL` |
| slot missing | create named physical slot or remove slot setting |
| shadow major mismatch | rebuild image with matching `PG_MAJOR` |
| source system ID mismatch | restore correct state/source pairing or perform explicit rebuild |
| source WAL missing | restore archive coverage or rebuild from fresh baseline |
| unsupported source type change | migrate ClickHouse column and set type override |
