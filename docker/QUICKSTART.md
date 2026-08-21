# Connect existing databases

Requires Docker Compose, PostgreSQL 16 or newer, and ClickHouse. Set
`PG_MAJOR` to source PostgreSQL major because shadow is its physical clone

```
git submodule update --init --recursive

export PG_MAJOR=17
export WALSHADOW_SOURCE_URL='postgres://replicator:secret@db.internal:5432/app?sslmode=require'
export WALSHADOW_CH_URL='clickhouse://default:secret@ch.internal:9000/cdc'

docker compose -f docker/docker-compose.yml build
docker compose -f docker/docker-compose.yml run --rm walshadow \
    init --all-tables
docker compose -f docker/docker-compose.yml up -d
docker compose -f docker/docker-compose.yml logs -f walshadow
```

`init` validates both connections, creates destination database, and selects
source tables with row keys. It reports SQL needed when source does not have
`wal_level = logical`, replication permission, or usable row keys

Wait for:

```
walshadow::bootstrap: shadow caught up to bootstrap end_lsn
```

Stop following logs, change a selected source table, then query matching
ClickHouse table:

```
clickhouse-client --host ch.internal --database cdc --query \
    "SELECT * FROM users FINAL ORDER BY id"
```

Destination table includes `_lsn`. `FINAL` returns latest row version

## Browser status

Start optional Prometheus and Grafana layer:

```
docker compose -f docker/docker-compose.yml \
    -f docker/docker-compose.grafana.yml up -d
```

Open http://localhost:3000. Provisioned dashboard shows daemon health,
replication lag, ClickHouse acknowledgement backlog, throughput, queue depth,
memory and spill use, backfills, and timeline or endpoint-swap problems. Set
`WALSHADOW_GRAFANA_PORT` before `up` to use another host port

Grafana reads only walshadow's Prometheus endpoint. It does not connect to
source PostgreSQL or destination ClickHouse

## Select tables

```
docker compose -f docker/docker-compose.yml run --rm walshadow init
```

Interactive mode lists source tables. Select by number or enter `all`.
Non-interactive selection accepts repeated table names:

```
docker compose -f docker/docker-compose.yml run --rm walshadow \
    init --table public users --table public orders
```

`--initial-load copy` is default. Use `--initial-load none` to stream only
changes after selection. Config persists in `walshadow-config` volume

## Teardown

```
docker compose -f docker/docker-compose.yml down -v
```

`-v` removes local shadow and config volumes. It does not modify source or
remove ClickHouse tables

When browser status layer is running, include its Compose file so Prometheus
and Grafana are removed too:

```
docker compose -f docker/docker-compose.yml \
    -f docker/docker-compose.grafana.yml down -v
```
