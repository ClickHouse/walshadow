#!/usr/bin/env bash
#   ./run.sh <clients> <seconds> [jobs]
# Run IN-REGION. From outside us-east-2 the ~224ms RTT caps throughput well
# below the generator's reach and pgbench becomes the bottleneck.
set -euo pipefail

CLIENTS="${1:?usage: run.sh <clients> <seconds> [jobs]}"
RUN_SECONDS="${2:?usage: run.sh <clients> <seconds> [jobs]}"
JOBS="${3:-$(( CLIENTS < 32 ? CLIENTS : 32 ))}"

HOST_DEFAULT=wsperf-aa8678e1.pg5jfh11pfst4mhvv5wbsecd8h.c0.us-east-2.aws.pg.clickhouse.cloud
# A PG* var set to whitespace is neither unset nor empty, so `:=` keeps it and
# pgbench then asks for a database literally named " ". Trim, then default.
trim() { printf '%s' "${1:-}" | tr -d '[:space:]'; }
PGHOST=$(trim "${PGHOST:-}"); PGHOST=${PGHOST:-$HOST_DEFAULT}
PGPORT=$(trim "${PGPORT:-}"); PGPORT=${PGPORT:-5432}
PGUSER=$(trim "${PGUSER:-}"); PGUSER=${PGUSER:-postgres}
PGDATABASE=$(trim "${PGDATABASE:-}"); PGDATABASE=${PGDATABASE:-postgres}
PGSSLMODE=$(trim "${PGSSLMODE:-}"); PGSSLMODE=${PGSSLMODE:-require}
export PGHOST PGPORT PGUSER PGDATABASE PGSSLMODE
if [[ -z "${PGPASSWORD:-}" ]]; then
  echo "set PGPASSWORD first" >&2; exit 1
fi
CONN=(-h "$PGHOST" -p "$PGPORT" -U "$PGUSER" -d "$PGDATABASE")

HERE="$(cd "$(dirname "$0")" && pwd)"

if ! psql "${CONN[@]}" -tAXqc "select 1 from tc_payload_pool limit 1" >/dev/null 2>&1; then
  echo "building payload pool..."
  psql "${CONN[@]}" -v ON_ERROR_STOP=1 -q -f "$HERE/pool.sql"
fi

lag() {
  psql "${CONN[@]}" -tAXqc "select coalesce('unsent=' ||
      pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), sent_lsn)::bigint) || ' replay_lag=' ||
      pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), replay_lsn)::bigint), 'no walshadow')
    from pg_stat_replication where application_name like 'walshadow%' limit 1"
}

echo "host=$PGHOST db=$PGDATABASE clients=$CLIENTS jobs=$JOBS duration=${RUN_SECONDS}s"
echo "before: $(lag)"

pgbench "${CONN[@]}" -n -M prepared \
  -f "$HERE/steps.sql@86" -f "$HERE/runs.sql@13" -f "$HERE/aux.sql@1" \
  -c "$CLIENTS" -j "$JOBS" -T "$RUN_SECONDS" -P 10

echo "after:  $(lag)"
psql "${CONN[@]}" -tAXqc "select 'steps_rows=' || count(*) from tc_workflow_run_steps"
