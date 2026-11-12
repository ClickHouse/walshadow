#!/usr/bin/env bash
# Restore the golden snapshot, start the daemon pinned, drain the fixed backlog
# and print one summary line. Builds no Rust; builds the tree's pgext against
# PG once (pgext.sh).
#
#   ./run.sh [walshadow-tree]     (default: this checkout)
#
# Knobs:
#   DAEMON_CPUS SRC_CPUS CH_CPUS   taskset cpulists, see common.sh
#   BIN=path      binary (default tree/target/release, then target/fast)
#   XACT_BUFFER_MAX=64M INSERTERS=16 DECODERS=16   see common.sh
#   KEEP=1        leave the run dirs and servers up for inspection
#   TAG=text      free-form label in the results line
set -euo pipefail
. "$(dirname "$0")/common.sh"
TREE=$(cd "${1:-$HERE/../..}" && pwd)
BIN=${BIN:-$TREE/target/release/walshadow-stream}
[[ -x $BIN ]] || BIN=$TREE/target/fast/release/walshadow-stream
[[ -f $DGOLD/meta.env ]] || { echo "no golden snapshot, run prepare.sh" >&2; exit 1; }
. "$DGOLD/meta.env"
[[ $GOLD_PG == "$PG" ]] || { echo "golden is PG $GOLD_PG, run with PG=$GOLD_PG" >&2; exit 1; }
BRIDGE=$("$HERE/pgext.sh" "$TREE" "$PG")

mkdir -p "$DISK"; exec 9>"$DISK/.lock"; flock 9
teardown
[[ ${KEEP:-0} == 1 ]] || trap teardown EXIT
LOAD=$(cut -d' ' -f1 /proc/loadavg)
TS=$(date +%s.%N)
clear_dirs "$DRUN" "$SRUN"
cp -a "$SGOLD" "$SRUN" &
mkdir -p "$DRUN/srcwal"
# Source WAL before the shutdown checkpoint's segment is only read, so
# hardlink it; copy that segment and everything else that gets written
( cd "$DGOLD"; for e in *; do [[ $e == srcwal ]] || cp -a "$e" "$DRUN/" & done
  cd srcwal; find . -mindepth 1 -maxdepth 1 -type d -exec cp -a {} "$DRUN/srcwal/" \;
  ls | grep -E '^[0-9A-F]{24}$' | awk -v e="$END_SEG" '("x" $1) < ("x" e)' | xargs -r ln -t "$DRUN/srcwal/"
  cp -a "$END_SEG" "$DRUN/srcwal/"; wait )
wait
# Flush the restore so its writeback does not overlap the drain
sync -f "$DRUN"
TR=$(date +%s.%N)
ch_start
src_start
END_LSN=$(psrc -c "select pg_current_wal_lsn()")
start_daemon "$BIN" "$BRIDGE" "$DAEMON_CPUS"
TD=$(date +%s.%N)

scrape() { curl -s -m 2 "http://$METRICS/metrics" || true; }
# Drain starts at the first record out; ack tail is the time from the last
# change of records_out to the source seeing replay at END_LSN
while :; do
  alive "$DAEMON" || { tail -20 "$DRUN/daemon.log"; exit 1; }
  scrape >"$DRUN/m0.txt"
  R0=$(awk '$1 == "walshadow_queue_records_out_total" {print $2}' "$DRUN/m0.txt")
  [[ -n $R0 && $R0 != 0 ]] && break
  sleep 0.05
done
T0=$(date +%s.%N)
threadcpu "$DAEMON" >"$DRUN/cpu0.txt"
ROUT=$R0; TOUT=$T0; SPILL_PEAK=0; cp "$DRUN/m0.txt" "$DRUN/m1.txt"
while :; do
  sleep 0.1
  scrape >"$DRUN/m.txt"
  read -r R S < <(awk '$1 == "walshadow_queue_records_out_total" {r = $2}
    $1 == "walshadow_spill_bytes_active" {s = $2} END {print r == "" ? "-" : r, s + 0}' "$DRUN/m.txt")
  [[ $R != - && $R != "$ROUT" ]] && { ROUT=$R; TOUT=$(date +%s.%N); cp "$DRUN/m.txt" "$DRUN/m1.txt"; threadcpu "$DAEMON" >"$DRUN/cpu1.txt"; }
  (( S > SPILL_PEAK )) && SPILL_PEAK=$S
  BEHIND=$(psrc -c "select pg_wal_lsn_diff('$END_LSN', max(replay_lsn))::bigint from pg_stat_replication")
  [[ -n $BEHIND && $BEHIND -le 0 ]] && break
  alive "$DAEMON" || { tail -20 "$DRUN/daemon.log"; exit 1; }
done
T1=$(date +%s.%N)
# Live shadow processes only; exited backends are not counted
PM=$(head -1 "$DRUN/shadow-data/postmaster.pid")
SHADOW_TICKS=$(for p in $PM $(pgrep -P "$PM"); do awk '{print $14+$15}' /proc/$p/stat 2>/dev/null; done | awk '{s+=$1} END{print s+0}')

N=$((ROUT - R0))
DT=$(bc -l <<<"$T1 - $T0"); DTR=$(bc -l <<<"$TOUT - $T0")
TICK=$(getconf CLK_TCK)
# Per-thread %core and ns/rec over the record window; tokio workers summed
THREADS=$(join -a 2 -e 0 -o 0,1.2,2.2 "$DRUN/cpu0.txt" "$DRUN/cpu1.txt" | awk -v dt="$DTR" -v n="$N" -v hz="$TICK" '
  { d = $3 - $2; all += d; if ($1 ~ /^tokio/) { tk += d; next } if (d > 0.02 * dt * hz) printf "%s=%.0f%%/%.0fns ", $1, 100 * d / hz / dt, d / hz * 1e9 / n }
  END { printf "tokio=%.0f%%/%.0fns cores=%.2f", 100 * tk / hz / dt, tk / hz * 1e9 / n, all / hz / dt }')
# Counter deltas over the record window; key is the full series name with labels
delta() { awk -v k="$1" 'FNR == 1 {f++} $1 == k {v[f] = $2} END {printf "%.6f", v[2] - v[1]}' "$DRUN/m0.txt" "$DRUN/m1.txt"; }
per_s() { bc -l <<<"$(delta "$1") / $DTR"; }
us_per() { bc -l <<<"n = $(delta "$2"); if (n > 0) 1000000 * $(delta "$1") / n else 0"; }
FETCHES=$(delta walshadow_toast_values_fetched_total)
LINE=$(printf 'drain %.1fs %.1f MiB/s %.0f rec/s %.0f B/rec | %s | wait %.2f ack_tail %.2fs | f %.4f fetch/rec, c_fetch %.0fus, fetch %.2f s/s, bridge fetch_toast %.0fus replay_lsn %.0fus, chunks %.0f/s | spill %s ev peak %.0f MiB | shadow %.2f cores | startup %.1fs restore %.1fs load %s' \
  "$DT" "$(bc -l <<<"$BACKLOG * $N / $RECORDS / 1048576 / $DTR")" "$(bc -l <<<"$N / $DTR")" "$(bc -l <<<"$BACKLOG / $RECORDS")" \
  "$THREADS" "$(per_s walshadow_pump_queue_wait_seconds_total)" "$(bc -l <<<"$T1 - $TOUT")" \
  "$(bc -l <<<"$FETCHES / $N")" "$(us_per walshadow_toast_value_fetch_seconds_total walshadow_toast_values_fetched_total)" \
  "$(per_s walshadow_toast_value_fetch_seconds_total)" \
  "$(us_per "walshadow_bridge_request_seconds_total{database=\"postgres\",op=\"fetch_toast\"}" "walshadow_bridge_requests_total{database=\"postgres\",op=\"fetch_toast\"}")" \
  "$(us_per "walshadow_bridge_request_seconds_total{database=\"postgres\",op=\"replay_lsn\"}" "walshadow_bridge_requests_total{database=\"postgres\",op=\"replay_lsn\"}")" \
  "$(per_s walshadow_decoder_toast_chunks_total)" \
  "$(awk '$1 == "walshadow_spill_evictions_total" {print $2}' "$DRUN/m1.txt")" "$(bc -l <<<"$SPILL_PEAK / 1048576")" \
  "$(bc -l <<<"$SHADOW_TICKS / $TICK / ($T1 - $TD)")" "$(bc -l <<<"$T0 - $TD")" "$(bc -l <<<"$TR - $TS")" "$LOAD")
REV=$(git -C "$TREE" rev-parse --short HEAD)$(git -C "$TREE" diff --quiet || echo +dirty)
KNOBS="cpus=${DAEMON_CPUS:-all} xbuf=${XACT_BUFFER_MAX:-default} ins=${INSERTERS:-16} dec=${DECODERS:-16}"
echo "$REV $KNOBS $LINE"
(( N < RECORDS * 98 / 100 )) && echo "warning: $N records out, golden has $RECORDS" >&2
printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\n' "$(date -Is)" "$(hostname)" "$REV" "$BIN" "$KNOBS" "golden $BOOT_REV $GOLDEN ${TAG:-}" "$LINE" >>"$DISK/results.tsv"
