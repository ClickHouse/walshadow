#!/usr/bin/env bash
# Build the bench image, ship it + the current state.env files to the runner
# box, and install a `walshadow-ec2-bench` wrapper that runs the bench in a
# host-network container. Then benches run IN the VPC (private IPs, no WAN RTT).
#
# After this: ssh to the box and run, e.g.
#   walshadow-ec2-bench --network private --dest clickhouse --bench single-row --state-dir /opt/bench/ec2
#   # or the whole four-bench suite:
#   walshadow-ec2-bench --network private --dest clickhouse --suite myrun \
#     --state-dir /opt/bench/ec2 --results-dir /opt/bench/results
set -euo pipefail
cd "$(dirname "$0")"
source ./state.env   # PUBLIC_IP, PEM, ...
source ../lib.sh

IMAGE="${IMAGE:-walshadow-bench:local}"
REPO_ROOT="$(repo_root)"
node_ssh_setup

echo "building $IMAGE (from docker/Dockerfile.bench)…"
docker build -f "$REPO_ROOT/docker/Dockerfile.bench" -t "$IMAGE" "$REPO_ROOT"

wait_cloud_init

REMOTE_IMAGE="$(remote_image_tag "$IMAGE")"
if [ "${FORCE:-0}" != "1" ] && [ -n "$REMOTE_IMAGE" ]; then
  echo "image $REMOTE_IMAGE already on host (FORCE=1 to resend)"
else
  echo "shipping $IMAGE (docker save | ssh | docker load)…"
  docker save "$IMAGE" | gzip | "${SSH[@]}" 'gunzip | sudo docker load'
  REMOTE_IMAGE="$(remote_image_tag "$IMAGE")"
  [ -n "$REMOTE_IMAGE" ] || { echo "$IMAGE missing on host after load" >&2; exit 1; }
fi

# Ship the sibling state.env files so --network private can resolve endpoints.
echo "shipping endpoint state.env files…"
# List every level explicitly so /opt/bench and /opt/bench/ec2 are also
# ubuntu-owned (install -d only reliably applies -o to the leaf dirs) — the scp
# below writes as ubuntu, and results stay readable without sudo.
"${SSH[@]}" 'sudo install -d -o ubuntu /opt/bench /opt/bench/results /opt/bench/ec2 /opt/bench/ec2/ec2-source-pg /opt/bench/ec2/ec2-clickhouse /opt/bench/ec2/ec2-pg-standby'
for n in ec2-source-pg ec2-clickhouse ec2-pg-standby; do
  if [ -f "../$n/state.env" ]; then
    "${SCP[@]}" "../$n/state.env" "ubuntu@$PUBLIC_IP:/opt/bench/ec2/$n/state.env"
    echo "  $n"
  fi
done

# Install a wrapper: `walshadow-ec2-bench …` → runs the image with host
# networking and /opt/bench mounted at the same path, so --state-dir reads the
# shipped state.env files and --suite writes results back to the host.
echo "installing walshadow-ec2-bench wrapper…"
"${SSH[@]}" "cat | sudo tee /usr/local/bin/walshadow-ec2-bench >/dev/null && sudo chmod +x /usr/local/bin/walshadow-ec2-bench" <<WRAP
#!/usr/bin/env bash
exec sudo docker run --rm --network host -v /opt/bench:/opt/bench $REMOTE_IMAGE "\$@"
WRAP

echo
echo "=== ready ==="
echo "ssh -i $PEM ubuntu@$PUBLIC_IP"
echo "then, in-VPC (private IPs):"
echo "  walshadow-ec2-bench --network private --dest clickhouse --bench single-row --state-dir /opt/bench/ec2"
echo "  # all four into /opt/bench/results/<name>:"
echo "  walshadow-ec2-bench --network private --dest clickhouse --suite myrun --state-dir /opt/bench/ec2 --results-dir /opt/bench/results"
echo "  # for the pg standby:  --dest postgres …"
