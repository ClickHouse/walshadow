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
# networking and /opt/bench mounted at the same path, carrying the on-box
# state.env and results locations. Later flags override these (clap keeps the
# last occurrence), so `--network public` or another dir still works.
echo "installing walshadow-ec2-bench wrapper…"
"${SSH[@]}" "cat | sudo tee /usr/local/bin/walshadow-ec2-bench >/dev/null && sudo chmod +x /usr/local/bin/walshadow-ec2-bench" <<WRAP
#!/usr/bin/env bash
exec sudo docker run --rm --network host -v /opt/bench:/opt/bench $REMOTE_IMAGE \\
  --state-dir /opt/bench/ec2 --results-dir /opt/bench/results "\$@"
WRAP

echo
echo "=== ready ==="
echo "usual path:  ../stack.sh bench run <name>   # runs here, results land in bench/results/<name>"
echo "by hand:     ssh -i $PEM ubuntu@$PUBLIC_IP"
echo "  walshadow-ec2-bench --bench single-row"
echo "  walshadow-ec2-bench --suite myrun         # all four shapes"
echo "  # for the pg standby:  --dest postgres …"
