#!/usr/bin/env bash
# Run the four walshadow replication-latency benchmarks against the EC2
# deployment and store each run's output under benchmark_results/<name>,
# where <name> is the required first argument (errors if it already exists):
#
#   single             — single-row commit→visible latency distribution
#   sustained          — fixed insert rate, latency under load
#   interleaved        — 2 concurrent long transactions, 1 round each
#   interleaved-long   — 2 concurrent long transactions, multiple rounds
#
# Uses target/release/walshadow-ec2-bench (public IPs from bench/ec2/*/state.env).
# Pick the destination with DEST:
#   DEST=clickhouse (default) — walshadow / peerdb pipelines (reads ec2-clickhouse)
#   DEST=postgres             — PG→PG physical standby (reads ec2-pg-standby)
# Override any bench's flags via the *_ARGS env vars below, the target with
# NETWORK=private, or skip the rebuild with SKIP_BUILD=1.
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
REPO_ROOT="$(cd "$HERE/.." && pwd)"
BIN="${BIN:-$REPO_ROOT/target/release/walshadow-ec2-bench}"
STATE_DIR="${STATE_DIR:-$REPO_ROOT/bench/ec2}"
# Runs land in a gitignored results dir (created on demand). Override with
# RESULTS_DIR; on the remote bench box it resolves to /opt/bench/results.
RESULTS_DIR="${RESULTS_DIR:-$REPO_ROOT/bench/results}"
NETWORK="${NETWORK:-public}"
DEST="${DEST:-clickhouse}"
COMMON=(--dest "$DEST" --network "$NETWORK" --state-dir "$STATE_DIR")

# Results folder name is the required first argument; refuse to clobber.
NAME="${1:-}"
[ -n "$NAME" ] || { echo "usage: $(basename "$0") <results-folder-name>" >&2; exit 1; }
OUT="$RESULTS_DIR/$NAME"
[ -e "$OUT" ] && { echo "error: $OUT already exists — choose a different name" >&2; exit 1; }

# Per-bench flags — override via env, e.g. SUSTAINED_ARGS="--bench sustained --rate 1000".
# Durations are kept modest for a quick pass; bump --xact-secs / --duration-secs for longer runs.
SINGLE_ARGS="${SINGLE_ARGS:---bench single-row --iterations 100 --warmup 10}"
SUSTAINED_ARGS="${SUSTAINED_ARGS:---bench sustained --rate 30000 --duration-secs 20 --concurrency 90}"
INTERLEAVED_ARGS="${INTERLEAVED_ARGS:---bench interleaved --xact-threads 90 --rounds 1 --xact-secs 30}"
LONG_THROUGHPUT="${LONG_THROUGHPUT:---bench interleaved --xact-threads 1 --rounds 10 --xact-secs 30}"

if [ "${SKIP_BUILD:-0}" != "1" ]; then
  echo "building walshadow-ec2-bench…"
  (cd "$REPO_ROOT" && cargo build --release -p walshadow-bench --bin walshadow-ec2-bench)
fi
# Accept BIN as either a path or a command on PATH (e.g. the ec2-bench wrapper).
command -v "$BIN" >/dev/null 2>&1 || { echo "binary not found: $BIN (build it, or set BIN to a valid command/path with SKIP_BUILD=1)" >&2; exit 1; }

mkdir -p "$OUT"
echo "results → $OUT   (dest=$DEST, network=$NETWORK)"

run() {
  local name="$1"; shift            # remaining args = bench flags
  local file="$OUT/$name.txt"
  echo
  echo "===== $name ====="
  {
    echo "# walshadow-ec2-bench $* ${COMMON[*]}"
    echo "# started: $(date -Is)"
    echo
  } >"$file"
  set +e
  "$BIN" "$@" "${COMMON[@]}" 2>&1 | tee -a "$file"
  local status=${PIPESTATUS[0]}
  set -e
  if [ "$status" -eq 0 ]; then
    echo "# ok: $(date -Is)" >>"$file"
  else
    echo "# FAILED (exit $status): $(date -Is)" >>"$file"
    echo "  ⚠ $name FAILED (exit $status) — see $file"
  fi
}

# Word-splitting on the *_ARGS strings is intentional (they're flag lists).
run single           $SINGLE_ARGS
run sustained        $SUSTAINED_ARGS
run interleaved      $INTERLEAVED_ARGS
run interleaved-long $LONG_THROUGHPUT

echo
echo "all done → $OUT"
ls -1 "$OUT"
