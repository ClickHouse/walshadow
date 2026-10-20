#!/bin/sh
#   ./run.sh <clients> <seconds> [jobs]
# Connects with libpq env (PGHOST, PGPORT, PGUSER, PGDATABASE, PGSSLMODE, PGPASSWORD)
# Run close to source: round-trip time caps commit rate before client count does
set -eu

CLIENTS="${1:?usage: run.sh <clients> <seconds> [jobs]}"
RUN_SECONDS="${2:?usage: run.sh <clients> <seconds> [jobs]}"
JOBS="${3:-$(( CLIENTS < 32 ? CLIENTS : 32 ))}"

HERE="$(cd "$(dirname "$0")" && pwd)"

if ! psql -tAXqc "select 1 from bench_payload_pool limit 1" >/dev/null 2>&1; then
  echo "building payload pool..."
  psql -v ON_ERROR_STOP=1 -q -f "$HERE/pool.sql"
fi

lag() {
  psql -tAXqc "select coalesce('unsent=' ||
      pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), sent_lsn)::bigint) || ' replay_lag=' ||
      pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn)::bigint), 'no walshadow')
    from pg_stat_replication where application_name like 'walshadow%' limit 1"
}

echo "host=${PGHOST:-local} db=${PGDATABASE:-${PGUSER:-$USER}} clients=$CLIENTS jobs=$JOBS duration=${RUN_SECONDS}s"
echo "before: $(lag)"

pgbench -n -M prepared \
  -f "$HERE/steps.sql@86" -f "$HERE/runs.sql@13" -f "$HERE/aux.sql@1" \
  -c "$CLIENTS" -j "$JOBS" -T "$RUN_SECONDS" -P 10

echo "after:  $(lag)"
psql -tAXqc "select 'steps_rows=' || count(*) from bench_workflow_run_steps"
