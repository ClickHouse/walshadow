#!/usr/bin/env bash
# Demo-only: seed the standard pgbench TPC-B schema so the `pgbench`
# hammer service has tables to pound. No-op unless WALSHADOW_DEMO_PGBENCH
# is set (the lean base stack leaves it unset, keeping this an empty
# init step). Runs inside the postgres image's init phase, so the four
# tables land in source's data dir before walshadow takes its base
# backup — they're present at bootstrap, satisfying preflight's
# "mapped relation exists with REPLICA IDENTITY FULL" gate.

set -euo pipefail

[ -n "${WALSHADOW_DEMO_PGBENCH:-}" ] || exit 0

SCALE="${PGBENCH_SCALE:-1}"

# `-i` drops+recreates pgbench_{accounts,branches,tellers,history} and
# loads scale*100k accounts. Quiet the per-100k progress chatter.
pgbench -i -s "$SCALE" -q -U "$POSTGRES_USER" -d "$POSTGRES_DB"

# walshadow decodes physical WAL; UPDATE/DELETE need the full old-tuple
# image on the wire, which only REPLICA IDENTITY FULL ships. Preflight
# refuses to stream a mapped relation without it.
psql -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB" <<'SQL'
ALTER TABLE pgbench_accounts REPLICA IDENTITY FULL;
ALTER TABLE pgbench_branches REPLICA IDENTITY FULL;
ALTER TABLE pgbench_tellers  REPLICA IDENTITY FULL;
ALTER TABLE pgbench_history  REPLICA IDENTITY FULL;
SQL

echo "walshadow-demo: pgbench schema seeded (scale=$SCALE), REPLICA IDENTITY FULL set"
