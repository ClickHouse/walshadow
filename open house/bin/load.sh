#!/usr/bin/env bash
#
# Change the sustained trade rate. Returns immediately.
#
# Percentile windows are dropped on the way through: a p95 measured at the old
# rate is not a result for the new one, and per-market baselines must re-warm
# before another shock is valid.
#
#   ./load.sh --rate 50000
#
. "$(dirname "${BASH_SOURCE[0]}")/_common.sh"

RATE=""
while [ $# -gt 0 ]; do
  case "$1" in
    --rate)    RATE="${2:?}"; shift 2 ;;
    -h|--help) awk 'NR==1{next} /^#/{sub(/^# ?/,""); print; next} {exit}' "$0"; exit 0 ;;
    *) [ -z "$RATE" ] && { RATE="$1"; shift; } || die "unknown argument '$1'" ;;
  esac
done
[ -n "$RATE" ] || die "--rate is required"
need curl

RESP=$(api POST /api/load "$(printf '{"rate":%d}' "$RATE")") || die "engine unreachable at $ENGINE_URL"
python3 - "$RESP" <<'PY'
import json, sys
d = json.loads(sys.argv[1])
if not d.get("accepted"):
    print("REJECTED: " + (d.get("reason") or "unknown")); sys.exit(2)
print(f"ACCEPTED  {d['action']}")
PY
