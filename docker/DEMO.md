# DEMO — walshadow under docker-compose

Three services wire **source PG → walshadow → ClickHouse**. Copy-paste
each block top-to-bottom from the repo root

## 1. Bring up the stack

```
git submodule update --init --recursive
docker compose -f docker/docker-compose.yml up --build -d
```

First build is heavy (Rust release + PGXS shared object); subsequent
`up`s reuse layers

## 2. Watch bootstrap land

```
docker compose -f docker/docker-compose.yml logs -f walshadow
```

Four INFO lines mark the bootstrap phases in order:

```
walshadow::backfill_bootstrap: catalog seed populated
walshadow::bootstrap: bootstrap emitter drained
walshadow::bootstrap: bootstrap landed
walshadow::bootstrap: shadow caught up to bootstrap end_lsn
```

Continue once the last line lands

## 3. Verify the snapshot in ClickHouse

```
docker compose -f docker/docker-compose.yml exec clickhouse \
    clickhouse-client --query "SELECT id,name,email,_op,_lsn FROM demo.users FINAL ORDER BY id"
```

Expect three rows, all sharing the same `_lsn` (`start_lsn` from the
`bootstrap landed` line):

```
1  Opifex      opifex@rerum.novarum       insert  33554472
2  Dominus     dominus@rerum.novarum      insert  33554472
3  Respublica  respublica@rerum.novarum   insert  33554472
```

## 4. Drive a change & verify streaming

`walshadow-stream` parses WAL one 16 MiB segment at a time
(`WalStream::push` → `filter_segment`), so a small UPDATE on a quiet
source sits in the in-flight segment until either the segment fills
or source rolls it. Two ways to force the roll:

- **Demo / low-volume:** `SELECT pg_switch_wal()` after each change.
- **Production / steady throughput:** set `archive_timeout=30s` (or
  similar) on source so PG auto-rotates segments under any
  back-pressure. Documented in `docker-compose.yml` would expose this
  per-source.

Each statement runs in its own psql autocommit txn — `psql -c
"UPDATE…; SELECT pg_switch_wal()"` would wrap **both** in one txn
whose COMMIT would land *after* the SWITCH (in a half-empty next
segment that can't reach the 16 MiB flush threshold), breaking the
recipe. Run them as separate `-c` calls, or pipe via heredoc:

```
docker compose -f docker/docker-compose.yml exec -T source psql -U postgres <<'SQL'
UPDATE demo.users SET email='opifex@merces-digna' WHERE id=1;
SELECT pg_switch_wal();
SQL

sleep 3

docker compose -f docker/docker-compose.yml exec clickhouse \
    clickhouse-client --query "SELECT id,email,_op,_lsn FROM demo.users FINAL ORDER BY id"
```

Row 1's email becomes `opifex@merces-digna`; `_lsn` advances past
bootstrap; `_op` reads `update`

## 5. Inspect the shadow PG (in-container standby)

```
docker compose -f docker/docker-compose.yml exec walshadow \
    psql -h /var/run/postgresql -U postgres \
    -c "SELECT pg_is_in_recovery(), pg_last_wal_replay_lsn()"
```

```
docker compose -f docker/docker-compose.yml exec walshadow \
    psql -h /var/run/postgresql -U postgres \
    -c "SELECT count(*) FROM pg_class WHERE relkind='r'"
```

## 6. Source-side state

```
docker compose -f docker/docker-compose.yml exec source \
    psql -U postgres -c "SELECT pg_current_wal_lsn(), pg_current_wal_insert_lsn()"
```

```
docker compose -f docker/docker-compose.yml exec source psql -U postgres -c \
    "SELECT pid, state, sent_lsn, write_lsn, replay_lsn FROM pg_stat_replication"
```

`pg_replication_slots` stays empty, walshadow uses slotless physical
replication via `START_REPLICATION` with no slot name

```
docker compose -f docker/docker-compose.yml exec source psql -U postgres -c \
    "SELECT * FROM pg_replication_slots"
```

## 7. Metrics

```
curl -s http://localhost:9484/metrics | head -40
```

## 8. Teardown

```
docker compose -f docker/docker-compose.yml down -v --remove-orphans
```

`-v` drops the three named volumes (`source-data`, `clickhouse-data`,
`walshadow-data`); next `up` rebootstraps from scratch
