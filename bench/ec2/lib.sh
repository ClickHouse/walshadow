#!/usr/bin/env bash
# Shared helpers for the per-node deploy.sh, stack.sh and profile.sh.
# Provisioning is terraform (terraform/, driven by stack.sh), which writes each
# node's ./state.env (PUBLIC_IP, PRIVATE_IP, PEM, ...). Source lib.sh from the
# node dir, then call node_init to load state.env and set up SSH.

: "${LOG_TAG:=$(basename "$PWD")}"

# Progress goes to stderr, so command substitution keeps capturing only values.
log() { printf '[%s %s] %s\n' "$LOG_TAG" "$(date -u +%H:%M:%S)" "$*" >&2; }

# Echo KEY=value's value from a state.env-style file. $1=path, $2=key.
read_state_var() {
  awk -v prefix="$2=" '
    index($0, prefix) == 1 { value = substr($0, length(prefix) + 1) }
    END { print value }
  ' "$1" 2>/dev/null || true
}

# Echo a sibling node's IP, failing when terraform has not written it yet.
# $1=node dir, $2=state.env key.
require_state_ip() {
  local ip
  ip="$(read_state_var "../$1/state.env" "$2")"
  [ -n "$ip" ] || { echo "$2 unknown in ../$1/state.env — provision $1 first, or pass the host explicitly" >&2; return 1; }
  echo "$ip"
}

# Echo an endpoint host: $1 wins when set (caller's override), else the sibling
# node's state.env. $2=node dir, $3=state.env key.
node_ip() {
  if [ -n "$1" ]; then
    echo "$1"
  else
    require_state_ip "$2" "$3"
  fi
}

# Absolute repository root, from a node dir.
repo_root() { (cd ../../.. && pwd); }

# Set SSH/SCP arrays from the sourced state.env (PEM, PUBLIC_IP). Populates
# globals SSH, SCP. Keepalives hold the session open through a bench run's quiet
# stretches, which outlast NAT idle timeouts.
node_ssh_setup() {
  : "${PEM:?state.env must set PEM}"
  SSH=(ssh -i "$PEM" -o StrictHostKeyChecking=accept-new -o ConnectTimeout=15 \
    -o ServerAliveInterval=30 -o ServerAliveCountMax=10 "ubuntu@$PUBLIC_IP")
  SCP=(scp -i "$PEM" -o StrictHostKeyChecking=accept-new)
}

# Preamble for anything running against one node, from that node's dir: load
# terraform's state.env, tag logs with the node, set up SSH.
node_init() {
  # shellcheck source=/dev/null
  source ./state.env
  LOG_TAG="$(basename "$PWD")"
  node_ssh_setup
}

# Run a function in a sibling node dir with that node's state.env and SSH
# loaded, leaving the caller's own SSH target untouched.
# $1=node dir, $2...=function + args
with_node() {
  local dir="$1"
  shift
  ( cd "$dir" && node_init && "$@" )
}

# Run stdin as a script on the node, with the given arguments as "$@". Feed it a
# quoted heredoc (<<'EOS') so the body expands on the box, not here — that keeps
# remote scripts free of escaped $ and nested quoting.
remote_sh() {
  local args=''
  if [ $# -gt 0 ]; then args="$(printf '%q ' "$@")"; fi
  "${SSH[@]}" "bash -s -- $args"
}

# Run a remote command until it succeeds. Fails after the whole window, so a
# node that never comes up says so instead of surfacing as a confusing error
# from whatever step ran next.
# $1=attempts $2=delay-secs $3=label $4...=remote command
retry_remote() {
  local attempts="$1" delay="$2" label="$3" i
  shift 3
  log "waiting for $label"
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

# Copy a locally-built image to the node, echoing the remote tag.
# $1=image, $2=force (1 resends even when the tag is already on the box).
ship_image() {
  local image="$1" force="${2:-1}" tag
  tag="$(remote_image_tag "$image")"
  if [ "$force" != 1 ] && [ -n "$tag" ]; then
    log "image $tag already on host (FORCE=1 to resend)"
  else
    log "shipping $image (docker save | ssh | docker load)"
    docker save "$image" | gzip | "${SSH[@]}" 'gunzip | sudo docker load'
    tag="$(remote_image_tag "$image")"
    [ -n "$tag" ] || { echo "$image missing on host after load" >&2; return 1; }
  fi
  echo "$tag"
}

# Block until the node answers SSH and cloud-init has finished (SSH must be set
# up). Everything a deploy needs on the box comes from cloud-init, so this is
# the readiness gate for all of them.
wait_cloud_init() {
  retry_remote 30 10 "SSH on $PUBLIC_IP" true
  log "waiting for cloud-init"
  "${SSH[@]}" 'sudo cloud-init status --wait' || { echo "cloud-init did not finish cleanly" >&2; return 1; }
}

# Copy on-CPU profiles (from ../profile.sh) off the box into ./profiles/<ts>/
# BEFORE the node is destroyed — stack.sh runs this for the outgoing streamer.
copy_remote_profiles() {
  [ -n "${PUBLIC_IP:-}" ] && [ -f "${PEM:-}" ] || return 0
  "${SSH[@]}" 'ls /opt/profile/* >/dev/null 2>&1' || return 0
  local dest
  dest="./profiles/$(date +%Y%m%d-%H%M%S)"
  mkdir -p "$dest"
  log "copying /opt/profile → $dest"
  if "${SCP[@]}" "ubuntu@$PUBLIC_IP:/opt/profile/*" "$dest/" 2>/dev/null; then
    log "copied: $(find "$dest" -maxdepth 1 -type f -printf '%f ')"
  else
    log "nothing copied — capture may still be running; re-run down after it finishes"
  fi
}
