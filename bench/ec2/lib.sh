#!/usr/bin/env bash
# Shared helpers for the per-node deploy.sh / profile.sh scripts. Provisioning
# is terraform (terraform/, driven by stack.sh), which writes each node's
# ./state.env (PUBLIC_IP, PRIVATE_IP, PEM, ...). Source state.env BEFORE
# lib.sh; helpers run from the node dir.

# Echo KEY=value's value from a state.env-style file. $1=path, $2=key.
read_state_var() { grep -E "^$2=" "$1" 2>/dev/null | tail -1 | cut -d= -f2-; }

# deploy.sh preamble helper: set SSH/SCP arrays from the sourced state.env
# (PEM, PUBLIC_IP). Populates globals SSH, SCP.
node_ssh_setup() {
  : "${PEM:?state.env must set PEM}"
  SSH=(ssh -i "$PEM" -o StrictHostKeyChecking=accept-new -o ConnectTimeout=15 "ubuntu@$PUBLIC_IP")
  SCP=(scp -i "$PEM" -o StrictHostKeyChecking=accept-new)
}

# Block until cloud-init has finished on the node (SSH must be set up).
wait_cloud_init() {
  echo "waiting for SSH + cloud-init…"
  "${SSH[@]}" 'sudo cloud-init status --wait' || { echo "cloud-init did not finish cleanly" >&2; return 1; }
}

# Copy on-CPU profiles (from ./profile.sh) off the box into ./profiles/<ts>/
# BEFORE the node is destroyed — stack.sh runs this for the outgoing streamer.
copy_remote_profiles() {
  [ -n "${PUBLIC_IP:-}" ] && [ -f "${PEM:-}" ] || return 0
  local ssh_p=(ssh -i "$PEM" -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10 "ubuntu@$PUBLIC_IP")
  "${ssh_p[@]}" 'ls /opt/profile/* >/dev/null 2>&1' || return 0
  local dest="./profiles/$(date +%Y%m%d-%H%M%S)"
  mkdir -p "$dest"
  echo "copying /opt/profile → $dest …"
  scp -i "$PEM" -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10 \
    "ubuntu@$PUBLIC_IP:/opt/profile/*" "$dest/" 2>/dev/null \
    && echo "  copied: $(ls -1 "$dest" 2>/dev/null | tr '\n' ' ')" \
    || echo "  (nothing copied — capture may still be running; re-run down after it finishes)"
}
