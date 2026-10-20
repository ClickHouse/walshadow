#!/usr/bin/env bash
# ab.sh <treeA> <treeB> [runs] [metric...]: interleave run.sh on two trees
# (A B A B ...), builds nothing, and print mean ± stdev per side plus B/A.
# Metrics are summary-line fields: rec_s (default), drain, wait, cores,
# c_fetch, f; raw lines land in $DISK/ab-last.txt. Env knobs (DAEMON_CPUS,
# BIN_A, BIN_B, XACT_BUFFER_MAX...) pass through.
set -euo pipefail
A=${1:?usage: ab.sh <treeA> <treeB> [runs] [metric...]}; B=${2:?}; N=${3:-4}; shift 3 2>/dev/null || shift $#
METRICS=${*:-rec_s drain wait cores c_fetch f}
HERE=$(cd "$(dirname "$0")" && pwd)
OUT=${DISK:-/var/tmp/walshadow-ceramic}/ab-last.txt; mkdir -p "$(dirname "$OUT")"
# AB_STATS=1 reprints the stats of the last ab-last.txt without running
[[ -n ${AB_STATS:-} ]] && N=0 || : >"$OUT"
for i in $(seq "$N"); do
  for s in A B; do
    T=$A; BINV=${BIN_A:-}; [[ $s == B ]] && { T=$B; BINV=${BIN_B:-}; }
    L=$(BIN=$BINV "$HERE/run.sh" "$T" 2>&1 | grep " cpus=" | tail -1)
    echo "$s $L" | tee -a "$OUT"
  done
done
# Pull fields out of the summary line by their labels
awk -v want="$METRICS" '
  { s = $1
    for (i = 2; i <= NF; i++) {
      if ($i == "drain") v["drain"] = $(i + 1) + 0
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

