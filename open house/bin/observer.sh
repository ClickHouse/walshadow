#!/usr/bin/env bash
#
# Offstage. Builds and starts the engine in a persistent tmux session, then
# waits until it is actually serving.
#
#   ./observer.sh [start|stop|restart|status|logs]
#
. "$(dirname "${BASH_SOURCE[0]}")/_common.sh"

SESSION="${OPENHOUSE_TMUX:-openhouse}"
LOG="$ROOT/results/engine.log"
CMD="${1:-start}"

start() {
  need cargo
  echo "==> building engine (release)"
  (cd "$ROOT/engine" && cargo build --release)

  if tmux has-session -t "$SESSION" 2>/dev/null; then
    echo "==> session '$SESSION' already running; restarting the engine pane"
    tmux send-keys -t "$SESSION:engine" C-c
    sleep 1
  else
    tmux new-session -d -s "$SESSION" -n engine
    tmux new-window -t "$SESSION" -n stage -c "$ROOT/bin"
  fi

  tmux send-keys -t "$SESSION:engine" \
    "cd '$ROOT' && set -a && . ./env.local && set +a && OPENHOUSE_ROOT='$ROOT' STATIC_DIR='$ROOT/engine/static' RESULTS_DIR='$ROOT/results' ./engine/target/release/market-engine 2>&1 | tee '$LOG'" C-m

  printf "==> waiting for %s " "$ENGINE_URL"
  for _ in $(seq 1 60); do
    if curl -sf "$ENGINE_URL/api/state" >/dev/null 2>&1; then
      echo; echo "engine up: $ENGINE_URL"
      echo "attach with: tmux attach -t $SESSION"
      return 0
    fi
    printf .; sleep 1
  done
  echo; die "engine did not come up — check $LOG"
}

case "$CMD" in
  start)   start ;;
  restart) tmux kill-session -t "$SESSION" 2>/dev/null || true; start ;;
  stop)    tmux kill-session -t "$SESSION" 2>/dev/null && echo stopped || echo "not running" ;;
  status)  curl -sf "$ENGINE_URL/api/state" >/dev/null && echo "up: $ENGINE_URL" || echo "down" ;;
  logs)    tail -f "$LOG" ;;
  *)       die "usage: observer.sh [start|stop|restart|status|logs]" ;;
esac
