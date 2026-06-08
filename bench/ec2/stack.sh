#!/usr/bin/env bash
# Bring a replication "setup" up or down on EC2.
#
# A setup = a base (always the source Postgres, plus ClickHouse for the CDC
# pipelines) plus ONE "streamer" node that does the replication. The base is
# shared, so you can swap the streamer while keeping the same source data —
# handy for comparing engines under benchmark_results/.
#
# Setups (streamer node dir in parentheses):
#   walshadow — custom WAL→ClickHouse daemon       (ec2-walshadow)
#   peerdb    — self-hosted PeerDB / ClickPipes     (ec2-peerdb)
#   pg        — PG→PG physical streaming standby     (ec2-pg-standby; base = source only)
#
# Usage:
#   ./stack.sh up   <setup>         provision base + streamer, then deploy streamer
#   ./stack.sh down <setup>         tear down just the streamer (shared base kept)
#   ./stack.sh down <setup> --all   also tear down the shared base (source-pg + CH)
#   ./stack.sh status               list running project instances
set -euo pipefail
cd "$(dirname "$0")"
source ./aws-env.sh

# Per-setup base nodes (everything except the streamer), brought up before the
# streamer so its deploy can read their private IPs. The CDC setups land in
# ClickHouse; the pg physical-replication setup needs only the source.
base_for() {
  case "$1" in
    pg) echo "ec2-source-pg" ;;
    *)  echo "ec2-source-pg ec2-clickhouse" ;;
  esac
}

# Setup name → streamer node directory. Add the third setup here.
streamer_for() {
  case "$1" in
    walshadow) echo ec2-walshadow ;;
    peerdb)    echo ec2-peerdb ;;
    pg)        echo ec2-pg-standby ;;
    *)         echo "" ;;
  esac
}
KNOWN_SETUPS="walshadow peerdb pg"

# Print the leading comment block (after the shebang, up to the first non-comment line).
usage() { awk 'NR==1{next} /^#/{sub(/^# ?/,""); print; next} {exit}' "$0"; }

# Run <script> inside node <dir> (in a subshell so cwd is restored).
run_node() {
  local dir="$1" script="$2"
  [ -x "$dir/$script" ] || { echo "missing: $dir/$script" >&2; return 1; }
  echo "── $dir/$script ──"
  ( cd "$dir" && "./$script" )
}

resolve_streamer() {
  local setup="$1" streamer
  streamer="$(streamer_for "$setup")"
  [ -n "$streamer" ] || { echo "unknown setup '$setup' (known: $KNOWN_SETUPS)" >&2; exit 1; }
  echo "$streamer"
}

up() {
  local setup="$1" streamer
  streamer="$(resolve_streamer "$setup")"
  [ -d "$streamer" ] || { echo "setup '$setup' not implemented yet — $streamer/ does not exist" >&2; exit 1; }

  local base; base="$(base_for "$setup")"
  echo "▲ bringing up '$setup' (base: $base, streamer: $streamer)"
  # 1) base — provision.sh is idempotent, reuses any running instance.
  for n in $base; do run_node "$n" provision.sh; done
  # 2) the streamer instance.
  run_node "$streamer" provision.sh
  # 3) deploy onto the streamer if it ships an image / config (needs the base
  #    private IPs, hence after the base is up).
  if [ -x "$streamer/deploy.sh" ]; then run_node "$streamer" deploy.sh; fi
  echo "✅ '$setup' up"
}

down() {
  local setup="$1" all="${2:-}" streamer
  streamer="$(resolve_streamer "$setup")"

  if [ -d "$streamer" ]; then
    run_node "$streamer" teardown.sh
  else
    echo "($streamer/ absent — nothing to tear down for the streamer)"
  fi

  if [ "$all" = "--all" ]; then
    for n in $(base_for "$setup"); do run_node "$n" teardown.sh; done
    echo "✅ '$setup' down, including base"
  else
    echo "✅ '$setup' streamer down; shared base left running (pass --all to remove it too)"
  fi
}

status() {
  aws ec2 describe-instances \
    --filters "Name=tag:Name,Values=walshadow-*" \
              "Name=instance-state-name,Values=pending,running,stopping,stopped" \
    --query 'Reservations[].Instances[].{Name:Tags[?Key==`Name`]|[0].Value,Id:InstanceId,Type:InstanceType,AZ:Placement.AvailabilityZone,PublicIP:PublicIpAddress,State:State.Name}' \
    --output table
}

cmd="${1:-}"; shift || true
case "$cmd" in
  up)     [ $# -ge 1 ] || { usage; exit 1; }; up "$1" ;;
  down)   [ $# -ge 1 ] || { usage; exit 1; }; down "$1" "${2:-}" ;;
  status) status ;;
  *)      usage; exit 1 ;;
esac
