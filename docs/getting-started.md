# Get started

Connect an existing PostgreSQL database to an existing ClickHouse service with
Docker Compose

## Before starting

Prepare:

- Docker with Compose
- PostgreSQL 16 or newer
- ClickHouse Native endpoint
- PostgreSQL account with `REPLICATION` and read access to selected tables
- Matching PostgreSQL major for walshadow image

Source must use `wal_level = logical` and at least two WAL senders for
concurrent streaming and base backup. Each selected table needs a usable row
key

```sql
ALTER SYSTEM SET wal_level = logical;
ALTER SYSTEM SET max_wal_senders = 8;

ALTER ROLE replicator REPLICATION;

-- Use only when adding a primary key is unsuitable
ALTER TABLE public.events REPLICA IDENTITY FULL;
```

Restart PostgreSQL after changing `wal_level` or `max_wal_senders`

## 1. Set connection URLs

Clone repository with submodules, then export source major and both endpoints

```bash
git submodule update --init --recursive

export PG_MAJOR=17
export WALSHADOW_SOURCE_URL='postgres://replicator:secret@db.internal:5432/app?sslmode=require'
export WALSHADOW_CH_URL='clickhouse://default:secret@ch.internal:9000/cdc'
```

Use `clickhouses://` for ClickHouse Native over TLS. Default secure port is
9440, default plaintext port is 9000

Set `PG_MAJOR` to source PostgreSQL major. Physical bootstrap cannot cross
major versions

## 2. Validate connections and choose tables

Build image, validate both endpoints, create ClickHouse database, and queue
every source table with a usable row key for initial load

```bash
docker compose -f docker/docker-compose.yml build
docker compose -f docker/docker-compose.yml run --rm walshadow \
    init --all-tables
```

`init` reports all source requirements together and prints SQL remedies when
possible. Fix each reported error, then rerun command

`init` writes `[source]`, `[ch]`, and one `[table.*]` block per chosen table.
Those blocks set initial load, not scope. Broad scope stays on because
`replicate_all` defaults to `true`, so unchosen and future user tables
replicate too, from start LSN and without initial load

To replicate only tables you name, add `[stream] replicate_all = false` to
written config, see [Select tables](table-selection.md)

## 3. Start replication

```bash
docker compose -f docker/docker-compose.yml up -d
docker compose -f docker/docker-compose.yml logs -f walshadow
```

First run copies source into managed shadow, loads selected rows into
ClickHouse, then starts continuous replication. Wait for:

```text
walshadow::bootstrap: shadow caught up to bootstrap end_lsn
```

Later starts resume from persisted state without copying every table again

## 4. Verify a change

Insert or update one selected source row, then query matching ClickHouse table

```bash
clickhouse-client --host ch.internal --database cdc \
    --user default --password secret --query \
    "SELECT * FROM users FINAL WHERE _is_deleted = 0 ORDER BY id"
```

`FINAL` resolves versions written during updates or replay. `_is_deleted = 0`
keeps live rows when soft-delete mode retains tombstones

Check daemon status from another terminal:

```bash
docker compose -f docker/docker-compose.yml exec walshadow \
    walshadow-stream ctl status
```

## 5. Add browser monitoring

Start provisioned Prometheus and Grafana services

```bash
docker compose -f docker/docker-compose.yml \
    -f docker/docker-compose.grafana.yml up -d
```

Open <http://localhost:3000> to inspect health, lag, throughput, queues,
memory, backfills, and source-transition state

## Stop or remove deployment

Stop containers while retaining state:

```bash
docker compose -f docker/docker-compose.yml down
```

Remove containers and persistent volumes:

```bash
docker compose -f docker/docker-compose.yml down -v
```

`-v` removes local shadow, resume state, and generated config. It does not
change source PostgreSQL or remove ClickHouse tables
