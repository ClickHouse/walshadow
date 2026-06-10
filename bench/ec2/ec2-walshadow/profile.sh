#!/usr/bin/env bash
# On-CPU profile of walshadow for N seconds (default 120), in the background,
# then return — run just before the benchmark so the capture covers it. Scoped
# to the walshadow container's processes (NOT system-wide):
#   * perf → every process in the `walshadow` container (the walshadow-stream
#            daemon + the auto-spawned shadow Postgres) → /opt/profile/perf-<ts>.data
#   * eBPF (bcc) → the walshadow-stream daemon (the decode/insert engine; bcc
#            profiles one process) → /opt/profile/oncpu-walshadow-<ts>.folded
# teardown.sh copies /opt/profile back to this machine.
#
# Usage: ./profile.sh [seconds]
# Note: tools are installed by cloud-init.
set -euo pipefail
cd "$(dirname "$0")"
source ../aws-env.sh
source ./state.env   # PUBLIC_IP, KEY_NAME

DUR="${1:-120}"
PEM="./${KEY_NAME}.pem"
SSH=(ssh -i "$PEM" -o StrictHostKeyChecking=accept-new -o ConnectTimeout=15 "ubuntu@$PUBLIC_IP")

echo "starting on-CPU profile of walshadow on $PUBLIC_IP for ${DUR}s (background)…"
"${SSH[@]}" "DUR='$DUR' bash -s" <<'PROF'
set -e
DUR="${DUR:-120}"
TS="$(date +%Y%m%d-%H%M%S)"
OUT=/opt/profile
sudo install -d -o ubuntu "$OUT"
sudo sysctl -w kernel.perf_event_paranoid=-1 kernel.kptr_restrict=0 >/dev/null 2>&1 || true

# Host PIDs of every process in the walshadow container (docker top shows host
# PIDs); falls back to pgrep. Covers walshadow-stream + the shadow Postgres.
PIDS=$(sudo docker top walshadow -eo pid --no-headers 2>/dev/null | awk '{print $1}' | paste -sd,)
[ -n "$PIDS" ] || PIDS=$(sudo pgrep -f 'walshadow-stream|postgres' 2>/dev/null | paste -sd,)
[ -n "$PIDS" ] || { echo "no walshadow processes found — is the daemon up?" >&2; exit 1; }
# The walshadow-stream daemon (decode/insert engine) for the single-PID eBPF profiler.
WS=$(sudo pgrep -f 'walshadow-stream' 2>/dev/null | head -1 || true)
[ -n "$WS" ] || WS=$(sudo docker inspect -f '{{.State.Pid}}' walshadow 2>/dev/null || true)
echo "walshadow pids (perf): $PIDS    walshadow-stream pid (eBPF): ${WS:-none}"

sudo nohup bash -c "
  { [ -n \"$WS\" ] && profile-bpfcc -F 99 -f -p $WS $DUR > $OUT/oncpu-walshadow-$TS.folded 2>$OUT/oncpu-$TS.log ; } &
  perf record -F 99 -g -p $PIDS -o $OUT/perf-$TS.data -- sleep $DUR 2>>$OUT/perf-$TS.log \
    || echo 'perf record failed (see log)' >>$OUT/perf-$TS.log
  wait
  chown -R ubuntu $OUT
" >/dev/null 2>&1 &
echo "capturing ${DUR}s → $OUT/perf-$TS.data + oncpu-walshadow-$TS.folded (background)"
PROF
echo "started — now kick off the benchmark. Run ./teardown.sh later to copy the profiles back."
