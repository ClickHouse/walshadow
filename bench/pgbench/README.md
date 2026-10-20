# pgbench throughput workload

Small multi-row transactions against a wide, indexed hot table, sized to load
walshadow's serial queueing worker

## Run

Point libpq env at source and run from a host near it:

    export PGHOST=<source-host> PGPORT=5432 PGUSER=postgres PGDATABASE=postgres
    export PGSSLMODE=require PGPASSWORD=<password>

    psql -f bench/pgbench/schema.sql      # first time, or to reset tables and indexes
    bench/pgbench/run.sh 192 900          # clients, seconds, optional jobs (default 32)

`run.sh` builds payload pool on first use, prints replication lag before and
after, and reports per-script transaction splits. Between runs:

    psql -f bench/pgbench/truncate.sql

Every table carries a `bench_` prefix; `schema.sql` and `truncate.sql` touch
only those. Round-trip time to source caps commit rate regardless of client
count, so run in same region as source

## Watch

| metric | meaning |
|---|---|
| `walshadow_queue_records_out_total` | serial worker throughput, the ceiling being measured |
| `walshadow_pump_queue_depth` | out of 131072; near cap means worker is the constraint |
| `walshadow_pump_queue_wait_seconds_total` | rate() is fraction of time pump is blocked |
| `node_disk_io_time_seconds_total` on shadow data device | rate() near 1.0 means measuring disk, not walshadow |

Check disk before believing any result. A 5m `rate()` takes five minutes to
render a ramp; drop to 1m to see one

Source side:

    psql -tAXc "select pg_wal_lsn_diff(pg_current_wal_lsn(), sent_lsn) as unsent,
                       pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn) as replay_lag
                from pg_stat_replication"

Growing `unsent` means pump is not draining the socket: walsender falls behind
primary, and from there it races WAL recycling

## A/B a build

Keep schema, `steps.sql`, and client count fixed; change only the binary. Let
each build reach ~300s uptime before sampling, first two minutes read low.
Interleave runs rather than running back to back: two tight runs agreeing
closely does not generalise

Changing index count or rows per transaction invalidates comparison, records/s
then reflects schema, not code

## Knobs

`steps.sql` `generate_series(0, N)` sets rows per transaction. Each row costs
1 heap record + 1 record per index, so records/s = commits x rows x (1 + indexes)

Index count on `bench_workflow_run_steps` is the main dial and cuts both ways:
more indexes multiply walshadow's record rate but throttle source and multiply
random page writes. At 7 indexes disk saturated (util 1.00, 3,422 write IOPS)
before walshadow's queue could pin. At 3 it stays under 0.10

TOAST is the other dial, and a trap. Chunk records are cheap for the queueing
worker, so a chunk-heavy stream flatters it; chunk writes are random IOPS,
which is what saturates disk. Payload pool exists for runs that want TOAST
(`runs.sql`/`aux.sql` use it); `steps.sql` does not

## Reference numbers

Shadow on 4 vCPU with local NVMe:

| shape | rec/s | MiB/s | queue | pump blocked | commits/s | disk util |
|---|---|---|---|---|---|---|
| 3 indexes, 6 rows | 266,289 | 42.7 | 99,737 (76%) | 0.216 | 11,488 | 0.095 |
| 7 indexes, 3 rows | 354,494 | 66.7 | 123,999 (95%) | 0.165 | 15,423 | 0.79 |

Worker cost scales per record, not per byte: workloads with fatter records
show lower records/s at higher bytes/s
