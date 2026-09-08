#!/usr/bin/env bash
#
# psql against the demo's source Postgres. Credentials come from env.local, so
# they stay out of your shell history and out of this file.
#
#   ./connect_postgres.sh                       interactive psql
#   ./connect_postgres.sh -c 'SELECT count(*) FROM market_trades'
#   ./connect_postgres.sh -f sql/samples.sql
#
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
set -a; . ./env.local; set +a
: "${PG_DSN:?PG_DSN missing from env.local}"
exec psql "$PG_DSN" "$@"
