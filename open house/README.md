# Market Pulse — OpenHouse NYC

A simulated prediction market. Trades commit to PostgreSQL, walshadow replicates
them to ClickHouse, and ClickHouse powers a cross-market "unusual buying" alert.

Two panels run the **identical detector on the identical feature frames**. One
evaluates each frame the moment it arrives; the other evaluates the *same frame*
five seconds later off a monotonic-clock queue. The live alert lands inside a
two-second buying burst; the delayed alert lands after it ended.

```
open house/
  docker-compose.yml   postgres + clickhouse + walshadow + engine
  config/demo.toml     thresholds, rates, cadences  (freeze after rehearsal)
  sql/                 schema, seed, and the three stage-facing queries
  engine/              the backend + the one-page app
  bin/                 setup / observer / shock / load / measure / reset
  results/             measured runs and engine.log
```

## Start it

```bash
cd "open house"
cp env.example env.local          # edit if your endpoints differ

docker compose up -d postgres clickhouse
psql "postgres://postgres:postgres@localhost:5433/postgres" \
     -f sql/pg/01-schema.sql
docker compose up -d walshadow    # first build takes a few minutes
docker compose logs -f walshadow  # wait for: shadow caught up to bootstrap end_lsn

./bin/setup.sh                    # seed, wait for destination, preflight
./bin/observer.sh start           # build + run the engine in tmux
```

Then open **http://localhost:8080**.

`setup.sh` must end in `preflight OK`. It refuses to pass if the destination
sort key would make the 31-second feature window scan history.

## Run the demo

Baselines need 31 s of traffic before a shock is accepted — that is deliberate,
a surge means nothing without a baseline to compare against.

```bash
./bin/shock.sh --market 101 --duration 2 --in 5
```

Prints the accepted run id and **returns immediately**. The countdown runs
server-side, so you switch to the browser before any source write happens. The
countdown is recorded separately and never counted toward alert timing.

Then, for the second beat:

```bash
./bin/load.sh --rate 50000
./bin/shock.sh --market 202 --duration 2 --in 35
```

`load.sh` drops the percentile windows and re-warms baselines, because a p95
measured at the old rate is not a result for the new one. The `--in 35` gives
the fresh market time to build its baseline.

Show the SQL between the two:

```bash
clickhouse-client --host localhost --database openhouse \
    --queries-file sql/market-radar.sql     # the feature calculation, 5 rows
clickhouse-client --host localhost --database openhouse \
    --queries-file sql/lateness.sql         # how late rows landed
```

## Keys on the page

- `D` — evidence drawer (query p50/p99, sample counts, frame pairing, SQL).
  Also reachable at `http://localhost:8080/#details` for a second screen.
- `F` — fullscreen.

## Measure

```bash
./bin/measure.sh baseline-10k --window 60
./bin/monitor.sh              # engineering diagnostic, prints a live line
```

Writes `results/<name>.json` and prints a PASS/FAIL on sub-500 ms p95.

## Reset

```bash
./bin/reset.sh                # truncate + reseed + clear run history; stack stays up
./bin/reset.sh --no-reseed    # faster, leaves the table empty
```

The engine keeps running and traffic resumes immediately. Allow ~30 s for the
reseed to replicate and baselines to re-warm before the next shock.

Full teardown:

```bash
./bin/observer.sh stop
docker compose down -v        # -v also drops the shadow and its state
```

## The five measurements, and what each one means

| Number | Definition |
|---|---|
| Committed trades/s | actual successful commits. The requested rate is shown next to it, never as the result |
| Replication p95 | monotonic stamp taken **before the INSERT** → first ClickHouse query that observes that row, poll overhead included. Headline number, single clock |
| Row lateness | `created_at` (PostgreSQL) → `_arrived_at` (ClickHouse `DEFAULT now64(6)`). Per-row trip time. Spans two clocks, so it corroborates the headline rather than replacing it |
| Feature query | server-side elapsed for the full cross-market collector query |
| Shock-to-alert | first burst **commit** → actual alert emission. Countdown excluded |

Probes ride `market_trades` itself, tagged `scenario_tag='probe'` and filed
under market 100 (outside the traded range), so they take the same path, the
same batches and the same flush as real trades. A dedicated probe table would
measure an easier path and understate the number on stage. The collector
filters the tag out, so probes never reach the detector.

`setup.sh` reports the PostgreSQL-to-engine clock offset and warns above 10 ms,
because row lateness is only meaningful if the two clocks agree.

## Measured locally (2026-09-07, laptop docker, 400 trades/s)

| | |
|---|---|
| Replication p95 (probe) | 83 ms over 1457 probes, 0 timeouts |
| Row lateness p50 / p95 | 31 / 53 ms |
| Feature query p95 | 11 ms across 40 markets |
| Live alert | 155 ms after first burst commit — during burst |
| Delayed alert | 5155 ms — after burst ended, same frame id |

These are laptop numbers at the `local` profile. The 10k and 50k targets are
unvalidated until run on the real stack.

## Gotchas that cost time

- **The source superuser must be `postgres`.** The shadow is a physical clone,
  so it inherits the source's roles, and the entrypoint connects to it as
  `postgres`. Renaming the source superuser breaks the shadow, not the source.
- **The source database must be named `postgres`**, matching the shadow's
  dbname. A mismatch silently drops every change.
- **`PG_MAJOR` must equal the source major** (17 here). The shadow cannot read
  a data dir from a different major.
- Source needs `wal_level=logical` *and* a `pg_hba` line allowing replication
  connections from other hosts — `docker/pg-init.sh` adds it. initdb's default
  only allows replication from localhost.
- The destination sort key is pinned in `docker/walshadow.toml`
  (`order_by = ["event_ts","market_id","id"]`). It only applies at CREATE TABLE
  time; walshadow never rekeys a table ClickHouse already holds. Get it wrong
  and the fix is `sql/ch/02-projection.sql`.
- Don't write lateness as `now64() - created_at`. That measures a row's age,
  dominated by where it falls in the window, not by lateness.
