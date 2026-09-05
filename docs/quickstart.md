# Quickstart

Run seeded PostgreSQL 18, ClickHouse, and walshadow locally with Docker
Compose, then verify a replicated update

Install Git and Docker with Compose before starting

## 1. Clone repository

```bash
# Clone walshadow and required submodules
git clone --recurse-submodules https://github.com/ClickHouse/walshadow.git
cd walshadow
```

## 2. Start local databases and walshadow

```bash
# Build walshadow image for bundled PostgreSQL 18 source
docker compose -f docker/docker-compose.yml \
    -f docker/docker-compose.quickstart.yml build walshadow

# Start databases, validate connections, and select all seeded tables
docker compose -f docker/docker-compose.yml \
    -f docker/docker-compose.quickstart.yml run --rm walshadow \
    init --all-tables

# Start replication in background
docker compose -f docker/docker-compose.yml \
    -f docker/docker-compose.quickstart.yml up -d

# Follow replication until bootstrap completion message appears
docker compose -f docker/docker-compose.yml \
    -f docker/docker-compose.quickstart.yml logs -f walshadow
```

Wait for `shadow caught up to bootstrap end_lsn`, then stop following logs
with `Ctrl-C`

## 3. Replicate an update

```bash
# Update seeded row in PostgreSQL
docker compose -f docker/docker-compose.yml \
    -f docker/docker-compose.quickstart.yml exec postgres \
    psql -U postgres -c \
    "UPDATE public.users SET email='alice@walshadow.dev' WHERE id=1"

# Read current row version from ClickHouse
docker compose -f docker/docker-compose.yml \
    -f docker/docker-compose.quickstart.yml exec clickhouse \
    clickhouse-client --query \
    "SELECT id, name, email FROM walshadow.users FINAL ORDER BY id"
```

Expected result includes updated address:

```text
1	Alice	alice@walshadow.dev
```

## 4. Remove local stack

```bash
# Stop containers and delete quickstart data plus generated walshadow config
docker compose -f docker/docker-compose.yml \
    -f docker/docker-compose.quickstart.yml down -v
```

Continue with [Getting started](getting-started.md) to connect existing
databases or [Configuration](configuration.md) to customize replication
