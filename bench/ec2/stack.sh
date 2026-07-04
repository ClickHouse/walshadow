#!/usr/bin/env bash
# Bring a replication "setup" up or down on EC2.
#
# A setup = a base (always the source Postgres, plus ClickHouse for the CDC
# pipelines) plus ONE "streamer" node that does the replication. The base is
# shared, so you can swap the streamer while keeping the same source data —
# handy for comparing engines under benchmark_results/.
#
# Setups (streamer node dir in parentheses):
#   walshadow — custom WAL→ClickHouse daemon        (ec2-walshadow)
#   peerdb    — self-hosted PeerDB / ClickPipes     (ec2-peerdb)
#   pg        — PG→PG physical streaming standby    (ec2-pg-standby; base = source only)
#
# Provisioning is terraform (terraform/); the desired node set persists in
# terraform/setup.auto.tfvars, so `terraform -chdir=terraform plan` always
# reflects the current setup. This script only sequences: pre-destroy hooks
# (profile copy-off, engine cleanup) → terraform apply → the streamer's
# deploy.sh. Apply is interactive — review the plan, especially on swaps.
#
# Usage:
#   ./stack.sh up <setup>       provision base + streamer, then deploy streamer
#   ./stack.sh down             tear down current streamer (shared base kept)
#   ./stack.sh down --all       tear down everything (terraform destroy)
#   ./stack.sh bench up|down    optional in-VPC bench-runner box (+ deploy)
#   ./stack.sh status           list running project instances
set -euo pipefail
cd "$(dirname "$0")"
source ./aws-env.sh

TF_DIR=terraform
TFVARS="$TF_DIR/setup.auto.tfvars"
KNOWN_SETUPS="walshadow peerdb pg"

# Setup name → streamer node directory. Add new setups here + in nodes.tf.
streamer_dir() {
  case "$1" in
    walshadow) echo ec2-walshadow ;;
    peerdb)    echo ec2-peerdb ;;
    pg)        echo ec2-pg-standby ;;
    *)         echo "" ;;
  esac
}

# Print the leading comment block (after the shebang, up to the first non-comment line).
usage() { awk 'NR==1{next} /^#/{sub(/^# ?/,""); print; next} {exit}' "$0"; }

tf() {
  [ -d "$TF_DIR/.terraform" ] || terraform -chdir="$TF_DIR" init
  terraform -chdir="$TF_DIR" "$@"
}

# Read a var back from setup.auto.tfvars; $2 = default when absent.
tfvar() {
  local v
  v="$(grep -E "^$1 " "$TFVARS" 2>/dev/null | cut -d= -f2- | tr -d ' "')" || true
  echo "${v:-$2}"
}

# Apply the desired node set ($1=streamer $2=clickhouse $3=bench_runner),
# persisting to setup.auto.tfvars only on success — a rejected or failed apply
# leaves the recorded setup matching real infra (-var overrides the old file).
apply_setup() {
  tf apply -var "streamer=$1" -var "clickhouse=$2" -var "bench_runner=$3"
  printf 'streamer     = "%s"\nclickhouse   = %s\nbench_runner = %s\n' "$1" "$2" "$3" > "$TFVARS"
}

# Before the current streamer node is destroyed: copy any on-CPU profiles off
# the box, then run its pre_down.sh hook (engine-specific source cleanup).
pre_down() {
  local dir
  dir="$(streamer_dir "$(tfvar streamer none)")"
  if [ -n "$dir" ] && [ -f "$dir/state.env" ]; then
    ( cd "$dir" && source ./state.env && source ../lib.sh && copy_remote_profiles )
    if [ -x "$dir/pre_down.sh" ]; then ( cd "$dir" && ./pre_down.sh ); fi
  fi
}

up() {
  local setup="$1" dir ch=true
  dir="$(streamer_dir "$setup")"
  [ -n "$dir" ] || { echo "unknown setup '$setup' (known: $KNOWN_SETUPS)" >&2; exit 1; }
  if [ "$(tfvar streamer none)" != "$setup" ]; then pre_down; fi
  [ "$setup" = pg ] && ch=false
  echo "▲ bringing up '$setup' (streamer: $dir)"
  apply_setup "$setup" "$ch" "$(tfvar bench_runner false)"
  if [ -x "$dir/deploy.sh" ]; then ( cd "$dir" && ./deploy.sh ); fi
  echo "✅ '$setup' up"
}

down() {
  pre_down
  if [ "${1:-}" = "--all" ]; then
    tf destroy
    rm -f "$TFVARS"
    echo "✅ everything down"
  else
    apply_setup none "$(tfvar clickhouse true)" "$(tfvar bench_runner false)"
    echo "✅ streamer down; shared base left running (pass --all to remove it too)"
  fi
}

bench() {
  case "${1:-}" in
    up)
      apply_setup "$(tfvar streamer none)" "$(tfvar clickhouse true)" true
      ( cd ec2-bench && ./deploy.sh )
      ;;
    down)
      apply_setup "$(tfvar streamer none)" "$(tfvar clickhouse true)" false
      ;;
    *) usage; exit 1 ;;
  esac
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
  down)   down "${1:-}" ;;
  bench)  bench "${1:-}" ;;
  status) status ;;
  *)      usage; exit 1 ;;
esac
