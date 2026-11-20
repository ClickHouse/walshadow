#!/usr/bin/env bash
# ab.sh <treeA> <treeB> [runs] [metric...]: interleave run.sh on two trees
# with alternating pair order. Build nothing and print mean ± stdev and B/A.
# Metrics are summary-line fields; raw lines land in $DISK/ab-last.txt.
# BIN_A/B and BRIDGE_LIB_DIR_A/B override build artifacts for each side.
# TOAST_READ_AHEAD_A/B overrides the TOAST setting for each side.
set -euo pipefail
A=${1:?usage: ab.sh <treeA> <treeB> [runs] [metric...]}; B=${2:?}; N=${3:-4}; shift 3 2>/dev/null || shift $#
METRICS=${*:-drain mib_s rows_s toast_batches_s rec_s wait cores c_fetch f}
HERE=$(cd "$(dirname "$0")" && pwd)
OUT=${DISK:-/var/tmp/walshadow-ceramic}/ab-last.txt; mkdir -p "$(dirname "$OUT")"
# AB_STATS=1 reprints the stats of the last ab-last.txt without running
if [[ -n ${AB_STATS:-} ]]; then N=0; else : >"$OUT"; fi
for ((i = 0; i < N; i++)); do
  order=(A B)
  if ((i % 2)); then order=(B A); fi
  for s in "${order[@]}"; do
    T=$A; BINV=${BIN_A:-}; BRIDGE=${BRIDGE_LIB_DIR_A:-}
    READ_AHEAD=${TOAST_READ_AHEAD_A:-${TOAST_READ_AHEAD:-}}
    if [[ $s == B ]]; then
      T=$B; BINV=${BIN_B:-}; BRIDGE=${BRIDGE_LIB_DIR_B:-}
      READ_AHEAD=${TOAST_READ_AHEAD_B:-${TOAST_READ_AHEAD:-}}
    fi
    if ! output=$(BIN=$BINV BRIDGE_LIB_DIR=$BRIDGE TOAST_READ_AHEAD=$READ_AHEAD "$HERE/run.sh" "$T" 2>&1); then
      printf '%s\n' "$output" >&2
      exit 1
    fi
    L=$(grep " cpus=" <<<"$output")
    echo "$s $L" | tee -a "$OUT"
  done
done
# Pull fields out of the summary line by their labels
awk -v want="$METRICS" '
  { s = $1
    for (i = 2; i <= NF; i++) {
      if ($i == "drain") v["drain"] = $(i + 1) + 0
      if ($i == "MiB/s") v["mib_s"] = $(i - 1)
      if ($i == "rows/s") v["rows_s"] = $(i - 1)
      if ($i == "TOAST-batches/s") v["toast_batches_s"] = $(i - 1)
      if ($i == "rec/s") v["rec_s"] = $(i - 1)
      if ($i == "wait") v["wait"] = $(i + 1)
      if ($i ~ /^cores=/) { split($i, cc, "="); v["cores"] = cc[2] }
      if ($i == "fetch/rec,") v["f"] = $(i - 1)
      if ($i == "c_fetch") { x = $(i + 1); sub(/us,/, "", x); v["c_fetch"] = x }
    }
    for (k in v) { n[s, k]++; sum[s, k] += v[k]; sq[s, k] += v[k] * v[k] }
    delete v }
  function fmt(x) { return x >= 100 ? sprintf("%.0f", x) : sprintf("%.3g", x) }
  END {
    m = split(want, keys, " ")
    for (j = 1; j <= m; j++) {
      k = keys[j]; line = sprintf("%-8s", k)
      for (si = 1; si <= 2; si++) {
        s = si == 1 ? "A" : "B"; c = n[s, k]
        mu[s] = c ? sum[s, k] / c : 0; sd = c > 1 ? sqrt((sq[s, k] - c * mu[s] ^ 2) / (c - 1)) : 0
        line = line sprintf("  %s %s ± %s", s, fmt(mu[s]), fmt(sd))
      }
      printf "%s  B/A %.3f\n", line, mu["A"] ? mu["B"] / mu["A"] : 0
    }
  }' "$OUT"

