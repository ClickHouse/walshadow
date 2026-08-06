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
# The bench driver runs on its own in-VPC node, brought up with every setup.
# `bench run` does the whole pass there and copies results back.
#
# Usage:
#   ./stack.sh up <setup>       provision base + streamer + bench runner, then deploy
#   ./stack.sh down             tear down current streamer (shared base kept)
#   ./stack.sh down --all       tear down everything (terraform destroy)
#   ./stack.sh bench run <name> [flags]   run the suite on the runner → bench/results/<name>
#   ./stack.sh bench fetch <name>         re-copy a run's results off the runner
#   ./stack.sh bench up|down    add/remove the bench-runner box on its own
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
  apply_setup "$setup" "$ch" true
  # Base source-PG post-boot setup (runtime-config overlay + replicate-all seed);
  # idempotent, runs before the streamer's deploy so the daemon seeds from it.
  if [ -x ec2-source-pg/deploy.sh ]; then ( cd ec2-source-pg && ./deploy.sh ); fi
  if [ -x "$dir/deploy.sh" ]; then ( cd "$dir" && ./deploy.sh ); fi
  ( cd ec2-bench && ./deploy.sh )
  echo "✅ '$setup' up — benchmark it with: ./stack.sh bench run <name>"
}

down() {
  local all=false a
  for a in "$@"; do [ "$a" = "--all" ] && all=true; done
  pre_down
  if [ "$all" = true ]; then
    tf destroy
    rm -f "$TFVARS"
    echo "✅ everything down"
  else
    apply_setup none "$(tfvar clickhouse true)" "$(tfvar bench_runner true)"
    echo "✅ streamer down; shared base left running (pass --all to remove it too)"
  fi
}

bench() {
  local verb="${1:-}"
  shift || true
  case "$verb" in
    up)
      apply_setup "$(tfvar streamer none)" "$(tfvar clickhouse true)" true
      ( cd ec2-bench && ./deploy.sh )
      ;;
    down)
      apply_setup "$(tfvar streamer none)" "$(tfvar clickhouse true)" false
      ;;
    run)   bench_run "$@" ;;
    fetch) bench_fetch "${1:-}" ;;
    *) usage; exit 1 ;;
  esac
}

# Local landing zone for fetched results (bench/results), matching
# walshadow-ec2-bench's own --results-dir default.
results_root() { echo "$(cd .. && pwd)/results"; }

# Run the standard suite on the in-VPC runner, then copy the results back.
# $1 = run name, rest are extra walshadow-ec2-bench flags (--run-secs, --dest, …).
bench_run() {
  local name="${1:-}" rc=0
  shift || true
  [ -n "$name" ] || { echo "usage: ./stack.sh bench run <name> [bench flags]" >&2; exit 1; }
  [ -e "$(results_root)/$name" ] && { echo "$(results_root)/$name exists — choose another name" >&2; exit 1; }
  [ "$(tfvar streamer none)" != none ] || { echo "no streamer up — ./stack.sh up <setup> first" >&2; exit 1; }

  apply_setup "$(tfvar streamer none)" "$(tfvar clickhouse true)" true
  ( cd ec2-bench && ./deploy.sh )

  # The suite tees each shape into its own file on the box as it goes, so a
  # dropped SSH session loses the tail, not the results already written; `bench
  # fetch <name>` collects whatever landed.
  (
    cd ec2-bench && source ./state.env && source ../lib.sh && node_ssh_setup
    echo "▶ suite '$name' on the runner ($PUBLIC_IP), against the VPC-internal endpoints"
    "${SSH[@]}" "walshadow-ec2-bench --suite '$name' $*"
  ) || rc=$?
  bench_fetch "$name" || true
  [ "$rc" -eq 0 ] || { echo "suite failed (exit $rc)" >&2; exit "$rc"; }
}

# Copy /opt/bench/results/<name> off the runner into bench/results/<name>, with
# a provenance file recording what produced it.
bench_fetch() {
  local name="${1:-}" out
  [ -n "$name" ] || { echo "usage: ./stack.sh bench fetch <name>" >&2; exit 1; }
  out="$(results_root)/$name"
  (
    cd ec2-bench && source ./state.env && source ../lib.sh && node_ssh_setup
    mkdir -p "$(dirname "$out")"
    "${SCP[@]}" -r "ubuntu@$PUBLIC_IP:/opt/bench/results/$name" "$(dirname "$out")/" \
      || { echo "nothing to copy for '$name'" >&2; exit 1; }
  )
  write_provenance "$name" "$out"
  echo "✅ results → $out"
}

# What a reader needs to compare this run against another: which setup, on what
# hardware, from which tree.
write_provenance() {
  local name="$1" out="$2" root head
  root="$(cd ../.. && pwd)"
  if head="$(git -C "$root" rev-parse --short HEAD 2>/dev/null)"; then
    git -C "$root" diff --quiet 2>/dev/null || head="$head-dirty"
  else
    head=unknown
  fi
  {
    echo "run:           $name"
    echo "fetched:       $(date -u +%Y-%m-%dT%H:%M:%SZ)"
    echo "setup:         $(tfvar streamer none)"
    echo "instance_type: $(tf output -raw instance_type 2>/dev/null || echo unknown)"
    echo "az:            $(tf output -raw az 2>/dev/null || echo unknown)"
    echo "driver:        in-VPC bench runner (ec2-bench)"
    echo "repo:          $head"
  } > "$out/provenance.txt"
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
  down)   down "$@" ;;
  bench)  bench "$@" ;;
  status) status ;;
  *)      usage; exit 1 ;;
esac
