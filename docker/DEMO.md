# DEMO — walshadow live, in a browser

Source PG → walshadow → ClickHouse, with **pgbench hammering the source**
and **Grafana dashboards** showing throughput, replication lag, and rows
landing in ClickHouse in near-real-time — then an operator evolving the
schema live and watching the column appear downstream.

Two browser surfaces: **Grafana** on :3000 for the time series, and the
**driving UI** on :8088 for clicking the demo — insert rows, update a row,
evolve the schema — and watching rowcount parity and the ClickHouse schema
converge.

Five services from the base stack plus a demo tier:

| service | role |
|---|---|
| `source` | postgres:18, `wal_level=logical`, seeds `demo.users` + pgbench TPC-B schema (`REPLICA IDENTITY FULL`) |
| `walshadow` | daemon: in-container daemon-owned shadow PG + WAL→CH stream, `/metrics` on :9484 |
| `clickhouse` | destination, `demo.*` tables pre-created |
| `pgbench` | hammers `source` with the TPC-B workload |
| `postgres-exporter` | source PG stats (TPS, tuple rates) → Prometheus |
| `prometheus` | scrapes walshadow + postgres-exporter |
| `grafana` | the dashboards — http://localhost:3000 |
| `ui` | the driving UI — http://localhost:8088 · write controls, rowcount parity, live schema |

> The demo tier lives in an overlay file. Everything below uses both
> compose files. Set a shell alias once and reuse it:
>
> ```
> dc="docker compose -f docker/docker-compose.yml -f docker/docker-compose.demo.yml"
> ```

## 1. Bring up the stack

```
git submodule update --init --recursive
$dc up --build -d
```

First build is heavy (Rust release + PGXS shared object); subsequent
`up`s reuse layers. Grafana pulls the `grafana-clickhouse-datasource`
plugin on first boot, so the first `up` needs internet.

> The pgbench schema is seeded by one-shot init scripts that run only
> when the data volumes are empty. If you previously ran the lean base
> stack, drop its volumes first so the demo tables get created:
> `$dc down -v` before the `up` above.

Tunable load (defaults shown), set before `up`:

```
PGBENCH_SCALE=1 PGBENCH_CLIENTS=4 PGBENCH_THREADS=2 $dc up --build -d
```

`PGBENCH_SCALE=1` is ~100k accounts; bump it for a bigger backfill and a
heavier hammer.

## 2. Watch bootstrap land

```
$dc logs -f walshadow
```

Wait for the four bootstrap phase lines, ending with:

```
walshadow::bootstrap: shadow caught up to bootstrap end_lsn
```

The `pgbench` service is gated on walshadow's metrics port, which opens
only *after* bootstrap — so the hammer starts swinging the moment the
backfill is durable. Confirm it's swinging:

```
$dc logs -f pgbench      # progress lines every 5s: tps, latency
```

## 3. Open the dashboards

Browse to **http://localhost:3000** (anonymous admin — no login). It
lands on **walshadow — live CDC pipeline**. Set the top-right refresh to
`2s` / `5s` and the range to `Last 5 minutes`.

Five sections, top to bottom:

1. **pgbench → PostgreSQL · the hammer** — source commit TPS and tuple
   write rate (insert/update/delete) from `postgres-exporter`. This is
   the load going in.
2. **walshadow pipeline** — heap records decoded/s, transactions
   committed/s, and filtered WAL records/s broken out by resource
   manager (Heap / Transaction / Btree / …). Throughput *through*
   walshadow.
3. **replication lag · latency** — shadow apply lag in seconds (the
   headline latency number) and the byte backlog between source, shadow
   PG, and the ClickHouse ack point. Under steady load this hugs zero
   and snaps back after any burst.
4. **buffers & memory** — xact-buffer bytes in memory vs spilled to
   disk, active buffered xacts, aborts, spill evictions.
5. **ClickHouse destination · rows landed** — queried straight from CH:
   cumulative `pgbench_history` rows, the per-second insert rate landing
   in ClickHouse (compare its shape to section 1's source TPS), and a
   live `demo.users` table — the one that grows a column in step 4.

Direct link to the dashboard: http://localhost:3000/d/walshadow-live

Let it run a minute. The CH "rows landed /s" bars should track the
source TPS line with the lag shown in section 3.

## 3b. Drive it from the browser

Browse to **http://localhost:8088**. One page, no login, no psql. It polls
walshadow's `/metrics`, source Postgres, and ClickHouse once a second and
pushes a snapshot to the browser over a WebSocket.

It comes up *before* walshadow finishes bootstrapping — the walshadow tiles
read `—` and the status line says `walshadow bootstrapping — /metrics not
open yet`, while the controls and the parity panel are already live (they
only need source PG and ClickHouse).

Four controls, all against `demo.users` on the source:

| control | what it does |
|---|---|
| **INSERT N ROWS** | one server-side `INSERT … SELECT … FROM generate_series(…)`, chunked at 250k. Watch source rows step up and ClickHouse chase it. |
| **UPDATE RANDOM ROW** | picks an existing id by index range scan and rewrites its email. The row reappears at the top of the rows feed with a higher `_lsn`; the *older* version is still there further down — that's what CDC actually writes. |
| **ADD COLUMN signup_ts** | the schema beat, §4 below in one click. Bounded to 100 rows of `signup_ts = now()`. Disables itself once the column exists. |
| **DROP COLUMN** | `DROP COLUMN IF EXISTS signup_ts`, replicated to ClickHouse too — so the beat is repeatable without a `down -v`. |

Five panels, top to bottom: the controls plus an action log; walshadow
pipeline tiles (lag, throughput, buffering xacts, uptime); rowcount parity
(source vs `count() FINAL WHERE _is_deleted = 0`, with a delta pill); source
and ClickHouse schema side by side; and the newest rows in ClickHouse by
`_lsn`.

Two things worth knowing when you read the numbers:

- **The throughput tiles are whole-pipeline, pgbench included.**
  `walshadow_emitter_rows_total` carries no table label, so "rows → CH /s"
  counts the TPC-B hammer too. The number that responds to *your* click is
  **`demo.users` versions /s** on the parity card, derived by differencing the
  ClickHouse count. For a quiet demo, silence the hammer:
  `$dc stop pgbench` (and `$dc start pgbench` to bring the roar back).
- **"ClickHouse rows · FINAL" and "row versions landed" are different
  numbers, deliberately.** The first dedupes to the winning version per id;
  the second is every version ever landed. Grafana's `demo.users` rowcount
  panel is a bare `count()`, so it matches the second, not the first.

To iterate on the UI itself, edit `docker/ui/app.py` or
`docker/ui/index.html` and `$dc restart ui` — both files are bind-mounted, so
no rebuild. (On a Linux host you can add `--reload` to the uvicorn command;
inotify over Docker Desktop's virtiofs on macOS is unreliable enough that it
isn't the default.)

## 4. Drive a row change, then evolve the schema (live) — from the CLI

Both beats below have a button in the UI at :8088. The CLI form is here for
scripting, and for seeing exactly what the buttons do. One divergence: the
UI's `signup_ts` update is bounded to 100 rows, while the unbounded form
below sweeps the whole table — fine at three rows, a very long pause after
you've clicked **INSERT N ROWS** a few times.

`demo.users` is tiny, pinned in the emitter config to exactly `id` /
`name` / `email`, and untouched by pgbench — so it's the clean stage for
showing CDC and live DDL replication while the hammer roars in the
background.

**Beat 1 — a row change rides the stream.** Update a row on the source
and read it back from ClickHouse:

```
$dc exec source psql -U postgres -d postgres -c \
    "UPDATE demo.users SET email='opifex@merces-digna' WHERE id=1"

$dc exec clickhouse clickhouse-client --query \
    "SELECT id, email, _lsn FROM demo.users FINAL ORDER BY id"
```

Row 1's email updates; its `_lsn` advances — CDC in flight. Pure
demonstration now: walshadow seeds each pinned relation's column baseline
at startup, so Beat 2's schema change replicates even if you skip this
beat. (Pre-seed this row change was load-bearing — a pinned table needed
one DML before an `ALTER` read as *column added* rather than *new table*,
since operator-pinned tables are assumed CH-managed on first sight.)

**Beat 2 — the schema evolves.** Add a column on the source and watch
walshadow replicate the DDL to ClickHouse — no config edit, no restart:

```
$dc exec source psql -U postgres -d postgres \
    -c "ALTER TABLE demo.users ADD COLUMN signup_ts timestamptz" \
    -c "UPDATE demo.users SET signup_ts = now()"

$dc exec clickhouse clickhouse-client --query "DESCRIBE TABLE demo.users"
```

`signup_ts` appears as `Nullable(DateTime64(6, 'UTC'))` — walshadow ran
the `ALTER TABLE … ADD COLUMN` on ClickHouse the instant it decoded the
source DDL, then auto-extended the column mapping so the `UPDATE`'s
values ship too:

```
$dc exec clickhouse clickhouse-client --query \
    "SELECT id, name, signup_ts FROM demo.users FINAL ORDER BY id"
```

The Grafana **demo.users (live schema)** panel (section 5) shows the
same thing without leaving the browser: the new `signup_ts` column pops
into the table on the next refresh.

Want to evolve a *hot* table too? It's already streaming, so its shape
is known — `ALTER TABLE pgbench_accounts ADD COLUMN region text DEFAULT
'eu'` on the source propagates the same way mid-hammer; watch
`DESCRIBE TABLE demo.pgbench_accounts` on CH grow the column.

## 5. Teardown

```
$dc down -v --remove-orphans
```

`-v` drops the named volumes (`source-data`, `clickhouse-data`,
`walshadow-data`); next `up` rebootstraps from scratch.

---

## Appendix — CLI verification (no browser)

The pipeline is fully inspectable from the shell.

Snapshot of a streamed pgbench table:

```
$dc exec clickhouse clickhouse-client --query \
    "SELECT count() FROM demo.pgbench_history"
$dc exec clickhouse clickhouse-client --query \
    "SELECT aid, abalance, _lsn FROM demo.pgbench_accounts FINAL ORDER BY aid LIMIT 5"
```

Shadow PG (in-container standby) replay position:

```
$dc exec walshadow psql -h /var/run/postgresql -U postgres \
    -c "SELECT pg_is_in_recovery(), pg_last_wal_replay_lsn()"
```

Source-side LSN + replication state (note: slotless physical
replication — `pg_replication_slots` stays empty):

```
$dc exec source psql -U postgres -c \
    "SELECT pid, state, sent_lsn, write_lsn, replay_lsn FROM pg_stat_replication"
```

Raw metrics the dashboards are built on:

```
curl -s http://localhost:9484/metrics | grep -E '^walshadow_(decoder_decoded|xacts_committed|shadow_apply_lag)'
```
