#!/usr/bin/env bash
# Demo-only: seed the standard pgbench TPC-B schema so the `pgbench`
# hammer service has tables to pound. No-op unless WALSHADOW_DEMO_PGBENCH
# is set (the lean base stack leaves it unset, keeping this an empty
# init step).
#
# Runs in two modes, and must be safe in both:
#   1. postgres image init phase (mounted at /docker-entrypoint-initdb.d)
#      over the local socket, on a fresh data dir only;
#   2. standalone from the `source-init` one-shot service over TCP, on
#      every `up`. Mode 2 is what makes seeding independent of volume
#      state — initdb.d hooks are skipped entirely on a non-empty PGDATA,
#      so a volume carried over from the lean base stack would otherwise
#      never grow the pgbench tables and walshadow's preflight would
#      reject the demo ch-config's pinned relations.
#
# Either way the tables land before walshadow takes its base backup, so
# they're present at bootstrap and satisfy preflight's "mapped relation
# exists with a row key for deletes" gate.

set -euo pipefail

[ -n "${WALSHADOW_DEMO_PGBENCH:-}" ] || exit 0

SCALE="${PGBENCH_SCALE:-1}"

# psql/pgbench read these; PGHOST stays unset in mode 1 so libpq picks
# the local socket, and is set to `source` by the one-shot service.
export PGUSER="${PGUSER:-${POSTGRES_USER:-postgres}}"
export PGDATABASE="${PGDATABASE:-${POSTGRES_DB:-postgres}}"

# Mode 2 races source's healthcheck; mode 1 already has a live server on
# the socket, so this returns immediately there.
for _ in $(seq 60); do
    pg_isready -q && break
    sleep 1
done

# `pgbench -i` DROPs and recreates the four tables, so it must not run
# against an already-seeded source: mode 2 fires on every `up` and would
# otherwise wipe accumulated pgbench data (and churn walshadow's
# filenode mappings) each restart.
if [ "$(psql -tAX -c \
    "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE c.relkind = 'r' AND n.nspname = 'public'
       AND c.relname IN ('pgbench_accounts','pgbench_branches',
                         'pgbench_tellers','pgbench_history')")" = "4" ]; then
    echo "walshadow-demo: pgbench schema already present, skipping load"
else
    # Loads scale*100k accounts. Quiet the per-100k progress chatter.
    pgbench -i -s "$SCALE" -q
    echo "walshadow-demo: pgbench schema seeded (scale=$SCALE)"
fi

# walshadow decodes physical WAL; UPDATE/DELETE need the full old-tuple
# image on the wire, which only REPLICA IDENTITY FULL ships. Preflight
# refuses to stream a mapped relation without it. Idempotent, so reassert
# unconditionally — cheap, and covers a source seeded by an older image.
psql -v ON_ERROR_STOP=1 <<'SQL'
ALTER TABLE pgbench_accounts REPLICA IDENTITY FULL;
ALTER TABLE pgbench_branches REPLICA IDENTITY FULL;
ALTER TABLE pgbench_tellers  REPLICA IDENTITY FULL;
ALTER TABLE pgbench_history  REPLICA IDENTITY FULL;
SQL

echo "walshadow-demo: REPLICA IDENTITY FULL set on pgbench tables"
