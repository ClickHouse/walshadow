#!/usr/bin/env bash
#
# Engineering diagnostic, not a stage command. Prints the measured numbers on a
# loop so you can watch a soak without the browser.
#
#   ./monitor.sh [interval_secs]
#
. "$(dirname "${BASH_SOURCE[0]}")/_common.sh"
INTERVAL="${1:-2}"

read -r -d '' RENDER <<'PY' || true
import json, sys
s = json.load(sys.stdin)
r, c, q = s["replication"], s["source_commit"], s["query"]
run = s.get("current_run") or {}
live, delayed = run.get("live"), run.get("delayed")

def ms(x):
    return "    -  " if x is None else "%7.0f" % x

print("%s  tps %6d/%-6d  repl p50/p95 %s/%s n=%-4d to=%-2d  commit p95 %s  query p95 %s  live %s  +5s %s  %s%s" % (
    s["emitted_wall"][11:23],
    s["committed_per_s"], s["target_rate"],
    ms(r["p50"]), ms(r["p95"]), r["count"], r["timeouts"],
    ms(c["p95"]), ms(q["p95"]),
    ms(live and live["ms_from_burst_start"]),
    ms(delayed and delayed["ms_from_burst_start"]),
    "BURST " if s["burst_active"] else "",
    ("ERR: " + s["collector_error"][:40]) if s.get("collector_error") else "",
))
PY

while true; do
  api GET /api/state 2>/dev/null | python3 -c "$RENDER" || echo "engine unreachable"
  sleep "$INTERVAL"
done
