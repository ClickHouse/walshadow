# walshadow throughput repro

Source: `wsperf-aa8678e1...us-east-2.aws.pg.clickhouse.cloud:5432` db `postgres`
Dest:   `https://ab3s16cqtt.us-east-2.aws.clickhouse.cloud:8443`
Shadow: `pwsc596xmf4j6mh8swvnd6t55f` (m6id.xlarge, 4 vCPU)

## Run it

Copy this directory to the in-region box (the one walshadow runs on) and:

    export PGHOST=wsperf-aa8678e1.pg5jfh11pfst4mhvv5wbsecd8h.c0.us-east-2.aws.pg.clickhouse.cloud
    export PGPORT=5432
    export PGUSER=postgres
    export PGDATABASE=postgres
    export PGSSLMODE=require
    export PGPASSWORD='...'      # ~/Downloads/postgres_credentials.txt

    psql -f schema.sql           # first time, or to reset tables+indexes
    ./run.sh 192 900             # clients, seconds  (3rd arg: jobs, default 32)

`run.sh` builds the payload pool on first use, prints replication lag before and
after, and reports per-script transaction splits. Between runs:

    psql -f truncate.sql

Run in-region. From outside us-east-2 the ~224ms RTT caps pgbench near
2,200 commits/s regardless of client count.

If pgbench reports `database " " does not exist`, something in the environment
set a `PG*` var to whitespace, or a line-continuation backslash has a trailing
space. `run.sh` passes connection flags explicitly so it is immune; a hand-typed
pgbench on one line is the fallback.

## Watch it

Grafana, substituting the shadow id:

    https://grafana.clickhouse-prd.com/d/datagres-walshadow/datagres-walshadow
      ?var-shadow=pwsc596xmf4j6mh8swvnd6t55f&from=now-30m&to=now

Panels lag by ~15-30s of scrape staleness plus the panel's own rate window, so a
5m `rate()` takes five minutes to render a ramp. Drop to 1m to see one.

The four numbers that matter, and what they mean:

| metric | meaning |
|---|---|
| `walshadow_queue_records_out_total` | the serial worker's throughput -- the ceiling being measured |
| `walshadow_pump_queue_depth` | /131072. Near the cap = worker is the constraint |
| `walshadow_pump_queue_wait_seconds_total` | rate() = fraction of time the pump is blocked |
| `node_disk_io_time_seconds_total{device="nvme1n1"}` | rate() near 1.0 = measuring the disk, not walshadow |

Check the last one before believing any result.

Source-side truth, which the dashboard does not show:

    psql -tAXc "select pg_wal_lsn_diff(pg_current_wal_lsn(), sent_lsn) as unsent,
                       pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn) as replay_lag
                from pg_stat_replication"

`unsent` growing means the pump is not draining the socket -- the walsender
stalls behind the primary, and from there it is a race against WAL recycling.

## A/B a build

Same schema, same `steps.sql`, same client count; only the binary changes. Let
each build reach ~300s uptime before sampling -- the first two minutes read low.
Run them interleaved, not back-to-back: two tight runs agreeing closely has
already turned out not to generalise.

Changing the index count or rows-per-transaction between runs invalidates the
comparison -- records/s then reflects the schema, not the code.

## What the knobs do

`steps.sql` `generate_series(0, N)` sets rows per transaction. Each row costs
1 heap record + 1 record per index, so records/s = commits x rows x (1 + indexes).

Index count on `tc_workflow_run_steps` is the main dial, and cuts both ways: more
indexes multiply walshadow's record rate but throttle the source and multiply
random page writes. At 7 indexes the disk saturated (util 1.00, 3,422 write IOPS)
before walshadow's queue could pin. At 3 it stays under 0.10.

TOAST is the other dial, and it was a trap. Chunk records are cheap for the
queueing worker, so a chunk-heavy stream flatters it; chunk *writes* are random
IOPS, which is what saturates the disk. The payload pool exists for runs that
want TOAST (`runs.sql`/`aux.sql` still use it); `steps.sql` no longer does.

## Measured

Production (pwbs0g27sykp6qtt4qq9zsa1k2, 2026-10-15, i8ge.2xlarge):
  112,539 rec/s | 697 B/rec | 78 MB/s | queue pinned 126-130k | pump_blocked 0.78

Repro, 3 indexes + 6 rows, perf build (2026-10-19):
  266,289 rec/s | 42.7 MiB/s | queue 99,737 (76%) | pump_blocked 0.216
  11,488 commits/s | 61,134 rows/s | 1.74 of 4 cores | disk util 0.095

Repro, 7 indexes + 3 rows (earlier, not comparable):
  354,494 rec/s | 66.7 MiB/s | queue 123,999 (95%) | pump_blocked 0.165
  15,423 commits/s | disk util 0.79

The worker is priced per record, not per byte: ~3x production's record rate for
less byte throughput, because production's records were 5x fatter. Matching
production's bytes/record needs volume that isn't TOAST -- most likely full-page
images, which this workload barely generates (small, freshly checkpointed
tables). Untested.
