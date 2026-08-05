#!/usr/bin/env bash
# Shared helpers for the per-node deploy.sh and the shared profile.sh.
# Provisioning is terraform (terraform/, driven by stack.sh), which writes each
# node's ./state.env (PUBLIC_IP, PRIVATE_IP, PEM, ...). Source state.env BEFORE
# lib.sh; helpers run from the node dir.

# Echo KEY=value's value from a state.env-style file. $1=path, $2=key.
read_state_var() { grep -E "^$2=" "$1" 2>/dev/null | tail -1 | cut -d= -f2-; }

# Echo a sibling node's IP, failing when terraform has not written it yet.
# $1=node dir, $2=state.env key.
require_state_ip() {
  local ip
  ip="$(read_state_var "../$1/state.env" "$2")"
  [ -n "$ip" ] || { echo "$2 unknown in ../$1/state.env — provision $1 first, or pass the host explicitly" >&2; return 1; }
  echo "$ip"
}

# Absolute repository root, from a node dir.
repo_root() { (cd ../../.. && pwd); }

# deploy.sh preamble helper: set SSH/SCP arrays from the sourced state.env
# (PEM, PUBLIC_IP). Populates globals SSH, SCP. Keepalives hold the session open
# through a bench run's quiet stretches, which outlast NAT idle timeouts.
node_ssh_setup() {
  : "${PEM:?state.env must set PEM}"
  SSH=(ssh -i "$PEM" -o StrictHostKeyChecking=accept-new -o ConnectTimeout=15 \
    -o ServerAliveInterval=30 -o ServerAliveCountMax=10 "ubuntu@$PUBLIC_IP")
  SCP=(scp -i "$PEM" -o StrictHostKeyChecking=accept-new)
}

# Run a remote command until it succeeds. Fails after the whole window, so a
# node that never comes up says so instead of surfacing as a confusing error
# from whatever step ran next.
# $1=attempts $2=delay-secs $3=label $4...=remote command
retry_remote() {
  local attempts="$1" delay="$2" label="$3" i
  shift 3
  echo "waiting for $label…"
  for ((i = 0; i < attempts; i++)); do
    "${SSH[@]}" "$@" 2>/dev/null && return 0
    sleep "$delay"
  done
  echo "$label not ready after $((attempts * delay))s" >&2
  return 1
}

# Print matching remote image tag, including Podman's localhost prefix
remote_image_tag() {
  local ref
  for ref in "$1" "localhost/$1"; do
    if "${SSH[@]}" "sudo docker image inspect '$ref' >/dev/null 2>&1"; then
      echo "$ref"
      return
    fi
  done
}

# Block until the node answers SSH and cloud-init has finished (SSH must be set
# up). Everything a deploy needs on the box comes from cloud-init, so this is
# the readiness gate for all of them.
wait_cloud_init() {
  retry_remote 30 10 "SSH on $PUBLIC_IP" true
  echo "waiting for cloud-init…"
  "${SSH[@]}" 'sudo cloud-init status --wait' || { echo "cloud-init did not finish cleanly" >&2; return 1; }
}

# Copy on-CPU profiles (from ../profile.sh) off the box into ./profiles/<ts>/
# BEFORE the node is destroyed — stack.sh runs this for the outgoing streamer.
copy_remote_profiles() {
  [ -n "${PUBLIC_IP:-}" ] && [ -f "${PEM:-}" ] || return 0
  node_ssh_setup
  "${SSH[@]}" 'ls /opt/profile/* >/dev/null 2>&1' || return 0
  local dest
  dest="./profiles/$(date +%Y%m%d-%H%M%S)"
  mkdir -p "$dest"
  echo "copying /opt/profile → $dest …"
  "${SCP[@]}" "ubuntu@$PUBLIC_IP:/opt/profile/*" "$dest/" 2>/dev/null \
    && echo "  copied: $(ls -1 "$dest" 2>/dev/null | tr '\n' ' ')" \
    || echo "  (nothing copied — capture may still be running; re-run down after it finishes)"
}
