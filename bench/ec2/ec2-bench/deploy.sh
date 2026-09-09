#!/usr/bin/env bash
# Build the bench image, ship it + the current state.env files to the runner
# box, and install a `walshadow-ec2-bench` wrapper that runs the bench in a
# host-network container, with the on-box paths already filled in. Benches then
# run IN the VPC (private IPs, no WAN round trip in the numbers).
#
# `../stack.sh bench run <name>` drives this end to end. By hand, on the box:
#   walshadow-ec2-bench --bench single-row
#   walshadow-ec2-bench --suite myrun          # all four shapes
set -euo pipefail
cd "$(dirname "$0")"
# shellcheck source-path=SCRIPTDIR
source ../lib.sh
node_init

IMAGE="${IMAGE:-walshadow-bench:local}"

log "building $IMAGE (from docker/Dockerfile.bench)"
docker build -f "$(repo_root)/docker/Dockerfile.bench" -t "$IMAGE" "$(repo_root)"

wait_cloud_init

# Always resend: the image carries the bench binary just built above.
REMOTE_IMAGE="$(ship_image "$IMAGE")"

# Ship the sibling state.env files so --network private can resolve endpoints.
log "shipping endpoint state.env files"
# List every level explicitly so /opt/bench and /opt/bench/ec2 are also
# ubuntu-owned (install -d only reliably applies -o to the leaf dirs) — the scp
# below writes as ubuntu, and results stay readable without sudo.
"${SSH[@]}" 'sudo install -d -o ubuntu /opt/bench /opt/bench/results /opt/bench/ec2 /opt/bench/ec2/ec2-source-pg /opt/bench/ec2/ec2-clickhouse /opt/bench/ec2/ec2-pg-standby /opt/bench/ec2/ec2-walshadow'
"${SSH[@]}" "sudo docker image inspect '$REMOTE_IMAGE' --format '{{.Id}}' > /opt/bench/driver-image.txt"
for n in ec2-source-pg ec2-clickhouse ec2-pg-standby ec2-walshadow; do
  if [ -f "../$n/state.env" ]; then
    "${SCP[@]}" "../$n/state.env" "ubuntu@$PUBLIC_IP:/opt/bench/ec2/$n/state.env"
    log "  $n"
  fi
done

# Install a wrapper: `walshadow-ec2-bench …` → runs the image with host
# networking and /opt/bench mounted at the same path, carrying the on-box
# state.env and results locations. Later flags override these (clap keeps the
# last occurrence), so `--network public` or another dir still works.
log "installing walshadow-ec2-bench wrapper"
remote_sh "$REMOTE_IMAGE" <<'EOS'
image="$1"
cat >/tmp/walshadow-ec2-bench <<WRAP
#!/bin/sh
exec sudo docker run --rm --user "\$(id -u):\$(id -g)" --network host -v /opt/bench:/opt/bench $image \\
  --state-dir /opt/bench/ec2 --results-dir /opt/bench/results "\$@"
WRAP
sudo install -m 0755 /tmp/walshadow-ec2-bench /usr/local/bin/walshadow-ec2-bench
EOS

echo
echo "=== ready ==="
echo "usual path:  ../stack.sh bench run <name>   # runs here, results land in bench/results/<name>"
echo "by hand:     ssh -i $PEM ubuntu@$PUBLIC_IP"
echo "  walshadow-ec2-bench --bench single-row"
echo "  walshadow-ec2-bench --suite myrun         # all four shapes"
echo "  # for the pg standby:  --dest postgres …"
