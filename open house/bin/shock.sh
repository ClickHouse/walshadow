#!/usr/bin/env bash
#
# Arm a buying burst. Prints the accepted run id and returns immediately — the
# countdown runs server-side, so you can switch back to the browser before any
# source write happens. The countdown is never counted toward alert timing.
#
#   --duration  burst length in SECONDS (default 30, fractions allowed).
#               Keep it BELOW the simulated delay or both panels alert together.
#   --in        countdown in seconds before the burst starts (default 5)
#
#   ./shock.sh --market 101 --duration 10 --in 5
#
. "$(dirname "${BASH_SOURCE[0]}")/_common.sh"

MARKET=""; DURATION=30; IN=5
while [ $# -gt 0 ]; do
  case "$1" in
    --market)   MARKET="${2:?}"; shift 2 ;;
    --duration) DURATION="${2:?}"; shift 2 ;;
    --in)       IN="${2:?}"; shift 2 ;;
    -h|--help)  awk 'NR==1{next} /^#/{sub(/^# ?/,""); print; next} {exit}' "$0"; exit 0 ;;
    *) die "unknown argument '$1'" ;;
  esac
done
[ -n "$MARKET" ] || die "--market is required"
need curl

MS=$(python3 -c "print(int(float('$DURATION')*1000))")
BODY=$(printf '{"market_id":%d,"duration_ms":%d,"in_secs":%d}' "$MARKET" "$MS" "$IN")
RESP=$(api POST /api/shock "$BODY") || die "engine unreachable at $ENGINE_URL"

python3 - "$RESP" <<'PY'
import json, sys
d = json.loads(sys.argv[1])
if not d.get("accepted"):
    print("REJECTED: " + (d.get("reason") or "unknown"))
    sys.exit(2)
print(f"ACCEPTED  run {d['run_id']}")
print(f"  market  {d['market_id']}  {d.get('market_name','')}")
print(f"  action  {d['action']}")
print(f"  starts  {d['scheduled_wall']}  (in {d['starts_in_secs']}s)")
if d.get("warning"):
    print()
    print("  WARNING: " + d["warning"])
print()
print("Switch to the browser now. The burst has not started yet.")
PY
