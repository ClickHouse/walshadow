#!/usr/bin/env bash
#
# Offstage. Returns the demo to a clean slate without tearing anything down:
# truncates source and destination, reseeds, and clears the app's run history.
# The engine keeps running and traffic resumes immediately.
#
#   ./reset.sh [--history-rows 1000000] [--no-reseed]
#
. "$(dirname "${BASH_SOURCE[0]}")/_common.sh"
need psql

HISTORY_ROWS=1000000; RESEED=1
while [ $# -gt 0 ]; do
  case "$1" in
    --history-rows) HISTORY_ROWS="${2:?}"; shift 2 ;;
    --no-reseed)    RESEED=0; shift ;;
    -h|--help)      awk 'NR==1{next} /^#/{sub(/^# ?/,""); print; next} {exit}' "$0"; exit 0 ;;
    *) die "unknown argument '$1'" ;;
  esac
done

echo "==> clearing app run history and detector state"
api POST /api/reset >/dev/null 2>&1 || echo "  (engine not reachable — skipping)"

echo "==> truncating source"
psql "$PG_DSN" -v ON_ERROR_STOP=1 -q -c \
  "TRUNCATE public.market_trades, public.shock_events"

echo "==> truncating destination"
ch "TRUNCATE TABLE IF EXISTS market_trades" >/dev/null

if [ "$RESEED" = 1 ]; then
  echo "==> reseeding markets + $HISTORY_ROWS history rows"
  psql "$PG_DSN" -v ON_ERROR_STOP=1 -q -v history_rows="$HISTORY_ROWS" -f "$ROOT/sql/pg/02-seed.sql"
fi

echo
echo "reset complete. A shock is accepted once each market has rebuilt 30 trades of baseline (a few seconds at demo rates)."
