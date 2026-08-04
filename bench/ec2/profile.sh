#!/usr/bin/env bash
# Start an on-CPU profile of a node's replication engine for N seconds (default
# 120) in the background, then return — run this just before kicking off the
# benchmark so the capture covers it. Both captures are scoped to the engine's
# processes, NOT system-wide (no Docker daemon, kernel threads or OS):
#   * perf → every process of the engine       → /opt/profile/perf-<ts>.data
#   * eBPF (bcc, one process) → its apply loop → /opt/profile/oncpu-<label>-<ts>.folded
# ../stack.sh down copies /opt/profile back into the node dir.
#
# Usage: ./profile.sh <walshadow|peerdb|pg-standby> [seconds]
# Note: tools are installed by cloud-init; in-container Go binaries may
# symbolize only partially from the host.
set -euo pipefail
cd "$(dirname "$0")"

NODE="${1:-}"
DUR="${2:-120}"

# Per node: dir, eBPF output label, perf flags, and the commands that discover
# PIDS (perf scope) and TARGET (the single process bcc profiles). The *_ALT
# commands are fallbacks, tried when the first yields nothing.
PIDS_ALT=''
TARGET_ALT=''
case "$NODE" in
  walshadow)
    DIR=ec2-walshadow LABEL=walshadow
    # musl build has no frame pointers, so unwind with DWARF; --no-buildid*
    # skips a finalization step that fails on these hosts
    PERF_FLAGS='--call-graph dwarf,65528 -m 64M --no-buildid --no-buildid-cache'
    PIDS_CMD='sudo docker top walshadow -eo pid --no-headers | tr -d " "'
    PIDS_ALT="sudo pgrep -f 'walshadow-stream|postgres'"
    TARGET_CMD='sudo pgrep -f walshadow-stream'
    TARGET_ALT='sudo docker inspect -f "{{.State.Pid}}" walshadow'
    ;;
  peerdb)
    DIR=ec2-peerdb LABEL=flowworker
    PERF_FLAGS='-g'
    PIDS_CMD='cd /opt/peerdb && sudo docker compose ps -q | xargs -r -I{} sudo docker inspect -f "{{.State.Pid}}" {}'
    # flow-worker is the CDC engine
    TARGET_CMD='cd /opt/peerdb && sudo docker inspect -f "{{.State.Pid}}" "$(sudo docker compose ps -q flow-worker)"'
    ;;
  pg-standby)
    # Profile the destination, not the primary: physical replication's work is
    # WAL replay on the standby, whereas the primary's CPU is the insert
    # workload every engine shares.
    DIR=ec2-pg-standby LABEL=startup
    PERF_FLAGS='-g'
    PIDS_CMD='sudo pgrep -x postgres'
    PIDS_ALT='sudo docker top pg-standby -eo pid --no-headers | tr -d " "'
    # startup process replays the streamed WAL
    TARGET_CMD="sudo pgrep -f 'postgres: startup'"
    TARGET_ALT="sudo pgrep -f 'postgres: .*recover'"
    ;;
  *)
    echo "usage: ./profile.sh <walshadow|peerdb|pg-standby> [seconds]" >&2
    exit 1
    ;;
esac

cd "$DIR"
source ./state.env   # PUBLIC_IP, PEM
source ../lib.sh
node_ssh_setup

echo "starting on-CPU profile of $NODE on $PUBLIC_IP for ${DUR}s (background)…"
"${SSH[@]}" "DUR=$(printf %q "$DUR") LABEL=$(printf %q "$LABEL") \
  PERF_FLAGS=$(printf %q "$PERF_FLAGS") \
  PIDS_CMD=$(printf %q "$PIDS_CMD") PIDS_ALT=$(printf %q "$PIDS_ALT") \
  TARGET_CMD=$(printf %q "$TARGET_CMD") TARGET_ALT=$(printf %q "$TARGET_ALT") \
  bash -s" <<'PROF'
set -e
TS="$(date +%Y%m%d-%H%M%S)"
OUT=/opt/profile
sudo install -d -o ubuntu "$OUT"
sudo sysctl -w kernel.perf_event_paranoid=-1 kernel.kptr_restrict=0 >/dev/null 2>&1 || true

# Echo the first command's pids as a comma list, else the next one's.
first_pids() {
  local c out
  for c in "$@"; do
    [ -n "$c" ] || continue
    out="$(eval "$c" 2>/dev/null | paste -sd,)"
    [ -n "$out" ] && { echo "$out"; return; }
  done
}

PIDS="$(first_pids "$PIDS_CMD" "$PIDS_ALT")"
[ -n "$PIDS" ] || { echo "no $LABEL processes on this box — is the engine up?" >&2; exit 1; }
TARGET="$(first_pids "$TARGET_CMD" "$TARGET_ALT")"
TARGET="${TARGET%%,*}"
echo "$LABEL pids (perf): $PIDS    eBPF target: ${TARGET:-none}"

# Detach so the capture survives this SSH session; hand the output back to ubuntu.
sudo nohup bash -c "
  { [ -n '$TARGET' ] && profile-bpfcc -F 99 -f -p '$TARGET' $DUR > $OUT/oncpu-$LABEL-$TS.folded 2>$OUT/oncpu-$TS.log ; } &
  # Re-filter to live pids: perf -p aborts the record if any one has exited.
  LIVE=\"\"; for x in \$(echo '$PIDS' | tr ',' ' '); do [ -d /proc/\$x ] && LIVE=\"\$LIVE,\$x\"; done; LIVE=\${LIVE#,}
  perf record -F 99 $PERF_FLAGS -p \"\$LIVE\" -o $OUT/perf-$TS.data -- sleep $DUR 2>>$OUT/perf-$TS.log \
    || echo 'perf record failed (see log)' >>$OUT/perf-$TS.log
  wait
  chown -R ubuntu $OUT
" >/dev/null 2>&1 &
echo "capturing ${DUR}s → $OUT/perf-$TS.data + oncpu-$LABEL-$TS.folded (background)"
PROF
echo "started — now kick off the benchmark. ../stack.sh down copies the profiles back."
