#!/usr/bin/env bash
#
# Offstage. Holds the current load for a stable window, then freezes the
# measured result to results/<name>.json and prints it.
#
#   ./measure.sh baseline-10k [--window 60]
#
. "$(dirname "${BASH_SOURCE[0]}")/_common.sh"

NAME="${1:?usage: measure.sh <name> [--window secs]}"; shift || true
WINDOW=60
while [ $# -gt 0 ]; do
  case "$1" in
    --window) WINDOW="${2:?}"; shift 2 ;;
    *) die "unknown argument '$1'" ;;
  esac
done

echo "==> settling and sampling for ${WINDOW}s at the current rate"
for i in $(seq "$WINDOW" -5 1); do printf "\r    %3ss remaining " "$i"; sleep 5; done
printf "\r%*s\r" 30 ""

api POST /api/results "$(printf '{"name":"%s"}' "$NAME")" >/dev/null
api GET /api/state | python3 - "$NAME" <<'PY'
import json, sys
s = json.load(sys.stdin)
r, q = s["replication"], s["query"]
print(f"result: {sys.argv[1]}")
print(f"  profile               {s['profile']}")
print(f"  target rate           {s['target_rate']:,}/s")
print(f"  committed trades/s    {s['committed_per_s']:,.0f}   (rows/s {s['rows_per_s']:,.0f}, {s['rows_per_commit']} per commit)")
print(f"  generator errors      {s['gen_errors']}")
print(f"  replication p50/p95/p99  {r['p50']:.1f} / {r['p95']:.1f} / {r['p99']:.1f} ms")
print(f"  replication samples   {r['count']} over {r['span_secs']:.0f}s (window {r['window_secs']}s), {r['timeouts']} timed out")
print(f"  feature query p95     {q['p95']:.1f} ms  (n={q['count']})")
verdict = "PASS" if (r["count"] and r["p95"] < 500) else "FAIL"
print(f"  sub-500ms p95         {verdict}")
if not r["count"]:
    print("  NOTE: no replication samples — nothing was validated")
PY
echo "written to results/$NAME.json"
