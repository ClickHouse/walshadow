#!/usr/bin/env bash
# On-CPU profile of the STANDBY Postgres for N seconds (default 120), in the
# background, then return — run just before the benchmark so the capture covers
# it. We profile the destination (standby), not the primary: physical
# replication's work is the WAL replay on the standby, whereas the primary's CPU
# is the insert workload (common to every engine). Scoped to the standby's
# Postgres processes (NOT system-wide):
#   * perf → every process in the pg-standby container (postmaster, walreceiver,
#            startup, checkpointer, …) → /opt/profile/perf-<ts>.data
#   * eBPF (bcc) → the startup/recovery process (the WAL-apply worker; bcc
#            profiles one process) → /opt/profile/oncpu-startup-<ts>.folded
# teardown.sh copies /opt/profile back to this machine.
#
# Usage: ./profile.sh [seconds]
set -euo pipefail
cd "$(dirname "$0")"
source ../aws-env.sh
source ./state.env   # PUBLIC_IP, KEY_NAME

DUR="${1:-120}"
PEM="./${KEY_NAME}.pem"
SSH=(ssh -i "$PEM" -o StrictHostKeyChecking=accept-new -o ConnectTimeout=15 "ubuntu@$PUBLIC_IP")

echo "starting on-CPU profile of the standby on $PUBLIC_IP for ${DUR}s (background)…"
"${SSH[@]}" "DUR='$DUR' bash -s" <<'PROF'
set -e
DUR="${DUR:-120}"
TS="$(date +%Y%m%d-%H%M%S)"
OUT=/opt/profile
sudo install -d -o ubuntu "$OUT"
sudo sysctl -w kernel.perf_event_paranoid=-1 kernel.kptr_restrict=0 >/dev/null 2>&1 || true

# The standby is the only Postgres on this box, so grab its processes by name
# (host PIDs — the container shares the host kernel). Robust regardless of the
# container name; falls back to `docker top pg-standby` if pgrep finds nothing.
PIDS=$(sudo pgrep -x postgres 2>/dev/null | paste -sd,)
[ -n "$PIDS" ] || PIDS=$(sudo docker top pg-standby -eo pid --no-headers 2>/dev/null | awk '{print $1}' | paste -sd,)
[ -n "$PIDS" ] || { echo "no postgres processes found on this box — is the standby up?" >&2; exit 1; }
# The startup/recovery process replays the streamed WAL — the apply work.
SU=$(sudo pgrep -f 'postgres: startup' 2>/dev/null | head -1 || true)
[ -n "$SU" ] || SU=$(sudo pgrep -f 'postgres: .*recover' 2>/dev/null | head -1 || true)
echo "standby pids (perf): $PIDS    startup/replay pid (eBPF): ${SU:-none}"

sudo nohup bash -c "
  { [ -n \"$SU\" ] && profile-bpfcc -F 99 -f -p $SU $DUR > $OUT/oncpu-startup-$TS.folded 2>$OUT/oncpu-$TS.log ; } &
  perf record -F 99 -g -p $PIDS -o $OUT/perf-$TS.data -- sleep $DUR 2>>$OUT/perf-$TS.log \
    || echo 'perf record failed (see log)' >>$OUT/perf-$TS.log
  wait
  chown -R ubuntu $OUT
" >/dev/null 2>&1 &
echo "capturing ${DUR}s → $OUT/perf-$TS.data + oncpu-startup-$TS.folded (background)"
PROF
echo "started — now kick off the benchmark (DEST=postgres). Run ./teardown.sh later to copy the profiles back."
