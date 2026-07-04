#!/usr/bin/env bash
# Start an on-CPU profile of PeerDB for N seconds (default 120), in the
# background, then return — run this just before kicking off the benchmark so
# the capture covers it. Scoped to the PeerDB processes (NOT system-wide):
#   * perf  → every PeerDB compose container's PID (the whole stack's CPU,
#             excluding the Docker daemon / kernel / OS) → /opt/profile/perf-<ts>.data
#   * eBPF (bcc) → the flow-worker (the CDC engine; bcc profiles one process)
#             → /opt/profile/oncpu-flowworker-<ts>.folded
# stack.sh down copies /opt/profile back to this machine.
#
# Usage: ./profile.sh [seconds]
# Note: tools are installed by cloud-init; in-container Go binaries may
# symbolize only partially from the host.
set -euo pipefail
cd "$(dirname "$0")"
source ./state.env   # PUBLIC_IP, PEM

DUR="${1:-120}"
SSH=(ssh -i "$PEM" -o StrictHostKeyChecking=accept-new -o ConnectTimeout=15 "ubuntu@$PUBLIC_IP")

echo "starting on-CPU profile of PeerDB on $PUBLIC_IP for ${DUR}s (background)…"
"${SSH[@]}" "DUR='$DUR' bash -s" <<'PROF'
set -e
DUR="${DUR:-120}"
TS="$(date +%Y%m%d-%H%M%S)"
OUT=/opt/profile
sudo install -d -o ubuntu "$OUT"
sudo sysctl -w kernel.perf_event_paranoid=-1 kernel.kptr_restrict=0 >/dev/null 2>&1 || true

# PIDs of every PeerDB container (excludes the Docker daemon / OS).
PIDS=$(cd /opt/peerdb && sudo docker compose ps -q 2>/dev/null \
        | xargs -r -I{} sudo docker inspect -f '{{.State.Pid}}' {} 2>/dev/null | paste -sd,)
[ -n "$PIDS" ] || { echo "no PeerDB container PIDs found — is the stack up?" >&2; exit 1; }
# flow-worker PID for the single-process eBPF profiler.
FW=$(sudo docker inspect -f '{{.State.Pid}}' "$(cd /opt/peerdb && sudo docker compose ps -q flow-worker 2>/dev/null)" 2>/dev/null || echo "")
echo "PeerDB pids (perf): $PIDS    flow-worker pid (eBPF): ${FW:-none}"

# Detach so it survives this SSH session; chown output back to ubuntu at the end.
sudo nohup bash -c "
  { [ -n \"$FW\" ] && profile-bpfcc -F 99 -f -p $FW $DUR > $OUT/oncpu-flowworker-$TS.folded 2>$OUT/oncpu-$TS.log ; } &
  perf record -F 99 -g -p $PIDS -o $OUT/perf-$TS.data -- sleep $DUR 2>>$OUT/perf-$TS.log \
    || echo 'perf record failed (see log)' >>$OUT/perf-$TS.log
  wait
  chown -R ubuntu $OUT
" >/dev/null 2>&1 &
echo "capturing ${DUR}s → $OUT/perf-$TS.data + oncpu-flowworker-$TS.folded (background)"
PROF
echo "started — now kick off the benchmark. ../stack.sh down copies the profiles back."
