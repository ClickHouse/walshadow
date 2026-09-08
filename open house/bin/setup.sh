#!/usr/bin/env bash
#
# Offstage. Loads schema, seeds markets and history, ensures the destination
# shape, and runs preflight. Safe to re-run.
#
#   ./setup.sh [--history-rows 1000000] [--skip-seed] [--create-dest]
#
. "$(dirname "${BASH_SOURCE[0]}")/_common.sh"
need psql; need python3

HISTORY_ROWS=1000000; SKIP_SEED=0; CREATE_DEST=0
while [ $# -gt 0 ]; do
  case "$1" in
    --history-rows) HISTORY_ROWS="${2:?}"; shift 2 ;;
    --skip-seed)    SKIP_SEED=1; shift ;;
    --create-dest)  CREATE_DEST=1; shift ;;
    -h|--help)      awk 'NR==1{next} /^#/{sub(/^# ?/,""); print; next} {exit}' "$0"; exit 0 ;;
    *) die "unknown argument '$1'" ;;
  esac
done

echo "==> source schema"
psql "$PG_DSN" -v ON_ERROR_STOP=1 -q -f "$ROOT/sql/pg/01-schema.sql"

if [ "$SKIP_SEED" = 0 ]; then
  echo "==> seeding 1000 markets + $HISTORY_ROWS history rows (this takes a minute)"
  psql "$PG_DSN" -v ON_ERROR_STOP=1 -q -v history_rows="$HISTORY_ROWS" -f "$ROOT/sql/pg/02-seed.sql"
fi

echo "==> ensuring destination database exists"
DB=$(python3 -c "import urllib.parse,sys;print(urllib.parse.urlparse(sys.argv[1]).path.strip('/'))" "$CH_URL")
CH_DB_OVERRIDE=default ch "CREATE DATABASE IF NOT EXISTS \`$DB\`" >/dev/null

# The replication pipeline owns the destination tables; it creates them once
# the first change for each table arrives. Wait rather than racing preflight.
echo "==> waiting for the pipeline to create destination tables"
for i in $(seq 1 "${DEST_WAIT_SECS:-180}"); do
  HAVE=$(ch "SELECT count() FROM system.tables WHERE database = currentDatabase() AND name = 'market_trades' FORMAT TSV" 2>/dev/null || echo 0)
  [ "${HAVE:-0}" = "1" ] && break
  printf "\r    %ss  (waiting for market_trades) " "$i"
  sleep 1
done
printf "\r%*s\r" 44 ""

if [ "$CREATE_DEST" = 1 ]; then
  echo "==> creating destination tables by hand (--create-dest)"
  while IFS= read -r stmt; do
    [ -n "${stmt// /}" ] && ch "$stmt" >/dev/null
  done < <(python3 - "$ROOT/sql/ch/01-dest-shape.sql" <<'PY'
import re, sys
text = open(sys.argv[1]).read()
text = re.sub(r'--[^\n]*', '', text)
for stmt in text.split(';'):
    s = ' '.join(stmt.split())
    if s:
        print(s)
PY
)
fi

# ClickHouse-side arrival stamp. The pipeline names its columns explicitly in
# the INSERT, so it never writes this one and the DEFAULT fires on arrival —
# giving per-row trip time (created_at -> _arrived_at) without touching the
# pipeline. Rows that predate this ALTER read as the epoch and are filtered out.
echo "==> ensuring _arrived_at stamp on the destination"
ch "ALTER TABLE market_trades ADD COLUMN IF NOT EXISTS _arrived_at DateTime64(6, 'UTC') DEFAULT now64(6)" >/dev/null 2>&1 \
  || echo "  WARN  could not add _arrived_at — sql/lateness.sql will not work"

echo
echo "==> preflight"
FAIL=0

TRADES=$(ch "SELECT name FROM system.tables WHERE database = currentDatabase() AND name = 'market_trades' FORMAT TSV" || true)
if [ -z "$TRADES" ]; then
  echo "  FAIL  destination table market_trades not found."
  echo "        The pipeline creates it on the first change for that table."
  echo "        Check the replication logs, or re-run with --create-dest to own it here."
  FAIL=1
else
  echo "  ok    destination table market_trades present"
  KEY=$(ch "SELECT sorting_key FROM system.tables WHERE database = currentDatabase() AND name = 'market_trades' FORMAT TSV")
  case "$KEY" in
    event_ts*) echo "  ok    sorting key leads with event_ts ($KEY)" ;;
    *)
      PROJ=$(ch "SELECT count() FROM system.projection_parts WHERE database = currentDatabase() AND table = 'market_trades' AND name = 'by_event_ts' FORMAT TSV" 2>/dev/null || echo 0)
      if [ "${PROJ:-0}" -gt 0 ]; then
        echo "  ok    sorting key is '$KEY' but the by_event_ts projection is materialised"
      else
        echo "  FAIL  sorting key is '$KEY'; the 31s event_ts window will scan history."
        echo "        Rekey to (event_ts, market_id, id) or apply sql/ch/02-projection.sql."
        FAIL=1
      fi
      ;;
  esac
  MISSING=$(ch "SELECT arrayStringConcat(arrayFilter(x -> NOT has(groupArray(name), x), ['id','market_id','taker_side','price_cents','quantity','event_ts','created_at','scenario_tag']), ', ') FROM system.columns WHERE database = currentDatabase() AND table = 'market_trades' FORMAT TSV")
  if [ -n "$MISSING" ]; then
    echo "  FAIL  destination is missing columns: $MISSING"
    FAIL=1
  else
    echo "  ok    all required columns present (including created_at)"
  fi
fi


MARKETS=$(psql "$PG_DSN" -tAc "SELECT count(*) FROM public.markets")
echo "  ok    $MARKETS markets seeded"
# probes deliberately sit outside the traded market range, so exclude them
ORPHANS=$(psql "$PG_DSN" -tAc "SELECT count(*) FROM public.market_trades t LEFT JOIN public.markets m USING (market_id) WHERE m.market_id IS NULL AND t.scenario_tag <> 'probe'")
[ "$ORPHANS" = "0" ] && echo "  ok    no trades reference an unknown market" \
  || { echo "  WARN  $ORPHANS trades reference an unknown market"; }

# created_at spans the source clock and the engine clock. The headline p95 is
# probe-based and single-clock, but the lateness number is only meaningful if
# these two agree, so measure and record the offset instead of assuming it.
SKEW=$(psql "$PG_DSN" -tAc "SELECT round(extract(epoch FROM (now() - timezone('UTC', now())))*0)" >/dev/null 2>&1; \
       python3 - "$PG_DSN" <<'PY'
import subprocess, sys, time, datetime
t0 = time.time()
out = subprocess.run(["psql", sys.argv[1], "-tAc", "SELECT now()"],
                     capture_output=True, text=True).stdout.strip()
t1 = time.time()
try:
    pg = datetime.datetime.fromisoformat(out).timestamp()
    print(f"{(pg - (t0 + t1) / 2) * 1000:.1f}")
except Exception:
    print("unknown")
PY
)
echo "  info  postgres-to-engine clock offset: ${SKEW} ms (round-trip corrected)"
python3 -c "
import sys
s='$SKEW'
if s!='unknown' and abs(float(s))>10:
    print('  WARN  offset above 10 ms — the created_at lateness number will carry that bias')
" || true

echo
[ "$FAIL" = 0 ] && echo "preflight OK" || { echo "preflight FAILED"; exit 1; }
