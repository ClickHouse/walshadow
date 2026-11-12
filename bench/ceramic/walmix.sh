#!/usr/bin/env bash
# walmix.sh <waldump --stats=record output>: share of records and bytes by rmgr,
# bytes/rec, FPI share of bytes and the top record types
OUT=$(awk '
  $1 ~ /\// { gsub(/\([^)]*\)/, ""); split($1, a, "/"); g = a[1]; sub(/2$/, "", g)
    n[g] += $2; b[g] += $5; f[g] += $4; N += $2; B += $5; F += $4; t[$1] = $5; tn[$1] = $2 }
  END {
    printf "records %d, %.0f B/rec, FPI %.1f%% of bytes\n", N, B / N, 100 * F / B
    for (g in n) printf "  %-12s %5.1f%% recs %5.1f%% bytes %6.0f B/rec %5.1f%% FPI\n", g, 100 * n[g] / N, 100 * b[g] / B, b[g] / n[g], b[g] ? 100 * f[g] / b[g] : 0
    for (k in t) printf "  type %-24s %5.1f%% recs %5.1f%% bytes\n", k, 100 * tn[k] / N, 100 * t[k] / B
  }' "$1")
head -1 <<<"$OUT"; grep -v ' type ' <<<"$OUT" | tail -n +2 | sort -k2 -rn; grep ' type ' <<<"$OUT" | sort -k3 -rn | head -10
