#!/usr/bin/env bash
# Background on-CPU profile (perf over the container, eBPF over walshadow-stream)
# for N seconds. Usage: ./profile.sh [seconds]
set -euo pipefail
cd "$(dirname "$0")"
source ./state.env   # PUBLIC_IP, PEM

DUR="${1:-120}"
SSH=(ssh -i "$PEM" -o StrictHostKeyChecking=accept-new -o ConnectTimeout=15 "ubuntu@$PUBLIC_IP")

echo "starting on-CPU profile of walshadow on $PUBLIC_IP for ${DUR}s (background)…"
"${SSH[@]}" "DUR='$DUR' bash -s" <<'PROF'
set -e
DUR="${DUR:-120}"
TS="$(date +%Y%m%d-%H%M%S)"
OUT=/opt/profile
sudo install -d -o ubuntu "$OUT"
sudo sysctl -w kernel.perf_event_paranoid=-1 kernel.kptr_restrict=0 >/dev/null 2>&1 || true

PIDS=$(sudo docker top walshadow -eo pid --no-headers 2>/dev/null | awk '{print $1}' | paste -sd,)
[ -n "$PIDS" ] || PIDS=$(sudo pgrep -f 'walshadow-stream|postgres' 2>/dev/null | paste -sd,)
[ -n "$PIDS" ] || { echo "no walshadow processes found — is the daemon up?" >&2; exit 1; }
WS=$(sudo pgrep -f 'walshadow-stream' 2>/dev/null | head -1 || true)
[ -n "$WS" ] || WS=$(sudo docker inspect -f '{{.State.Pid}}' walshadow 2>/dev/null || true)
{ [ -n "$WS" ] && sudo test -d "/proc/$WS"; } \
  || { echo "walshadow-stream daemon not running — nothing to profile (is the walshadow container up?)" >&2; exit 1; }
echo "walshadow pids (perf): $PIDS    walshadow-stream pid (eBPF): $WS"

sudo nohup bash -c "
  profile-bpfcc -F 99 -f -p $WS $DUR > $OUT/oncpu-walshadow-$TS.folded 2>$OUT/oncpu-$TS.log &
  # Re-filter to live PIDs: perf -p aborts the record if any one has exited.
  LIVE=\"\"; for x in \$(echo '$PIDS' | tr ',' ' '); do [ -d /proc/\$x ] && LIVE=\"\$LIVE,\$x\"; done; LIVE=\${LIVE#,}
  # DWARF unwinding (musl build, no frame pointers); --no-buildid* avoids
  # finalization that fails on this host.
  perf record -F 99 --call-graph dwarf,65528 -m 64M --no-buildid --no-buildid-cache \
    -p \"\$LIVE\" -o $OUT/perf-$TS.data -- sleep $DUR 2>>$OUT/perf-$TS.log \
    || echo 'perf record failed (see log)' >>$OUT/perf-$TS.log
  wait
  chown -R ubuntu $OUT
" >/dev/null 2>&1 &
echo "capturing ${DUR}s → $OUT/perf-$TS.data + oncpu-walshadow-$TS.folded (background)"
PROF
echo "started — now kick off the benchmark. ../stack.sh down copies the profiles back."
