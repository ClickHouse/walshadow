#!/usr/bin/env bash
# Build the bench image, ship it + the current state.env files to the runner
# box, and install a `walshadow-ec2-bench` wrapper that runs the bench in a
# host-network container. Then benches run IN the VPC (private IPs, no WAN RTT).
#
# After this: ssh to the box and run, e.g.
#   walshadow-ec2-bench --network private --dest clickhouse --bench single-row --state-dir /opt/bench/ec2
#   # or all four via the shipped runner:
#   BIN=walshadow-ec2-bench STATE_DIR=/opt/bench/ec2 NETWORK=private DEST=clickhouse \
#     SKIP_BUILD=1 /opt/bench/run_bench_suite.sh myrun
set -euo pipefail
cd "$(dirname "$0")"
source ../aws-env.sh
source ./state.env   # PUBLIC_IP, KEY_NAME, ...
source ../lib.sh

IMAGE="${IMAGE:-walshadow-bench:local}"
REPO_ROOT="$(cd ../../.. && pwd)"
node_ssh_setup

echo "building $IMAGE (from docker/Dockerfile.bench)…"
docker build -f "$REPO_ROOT/docker/Dockerfile.bench" -t "$IMAGE" "$REPO_ROOT"

wait_cloud_init

if [ "${FORCE:-0}" != "1" ] && "${SSH[@]}" "sudo docker image inspect $IMAGE >/dev/null 2>&1"; then
  echo "image $IMAGE already on host (FORCE=1 to resend)"
else
  echo "shipping $IMAGE (docker save | ssh | docker load)…"
  docker save "$IMAGE" | gzip | "${SSH[@]}" 'gunzip | sudo docker load'
fi

# Ship the sibling state.env files so --network private can resolve endpoints.
echo "shipping endpoint state.env files…"
# List every level explicitly so /opt/bench and /opt/bench/ec2 are also
# ubuntu-owned (install -d only reliably applies -o to the leaf dirs) — needed
# so we can scp run_bench_suite.sh there and the runner can write results.
"${SSH[@]}" 'sudo install -d -o ubuntu /opt/bench /opt/bench/ec2 /opt/bench/ec2/ec2-source-pg /opt/bench/ec2/ec2-clickhouse /opt/bench/ec2/ec2-pg-standby'
for n in ec2-source-pg ec2-clickhouse ec2-pg-standby; do
  if [ -f "../$n/state.env" ]; then
    "${SCP[@]}" "../$n/state.env" "ubuntu@$PUBLIC_IP:/opt/bench/ec2/$n/state.env"
    echo "  $n"
  fi
done

# Install a wrapper: `walshadow-ec2-bench …` → runs the image with host
# networking and /opt/bench/ec2 mounted at the same path (so --state-dir works).
echo "installing walshadow-ec2-bench wrapper…"
"${SSH[@]}" "cat | sudo tee /usr/local/bin/walshadow-ec2-bench >/dev/null && sudo chmod +x /usr/local/bin/walshadow-ec2-bench" <<WRAP
#!/usr/bin/env bash
exec sudo docker run --rm --network host -v /opt/bench/ec2:/opt/bench/ec2 $IMAGE "\$@"
WRAP

# Ship run_bench_suite.sh for the all-four pass (BIN/STATE_DIR overridable).
"${SCP[@]}" "$REPO_ROOT/bench/run_bench_suite.sh" "ubuntu@$PUBLIC_IP:/opt/bench/run_bench_suite.sh"
"${SSH[@]}" 'chmod +x /opt/bench/run_bench_suite.sh'

echo
echo "=== ready ==="
echo "ssh -i $PEM ubuntu@$PUBLIC_IP"
echo "then, in-VPC (private IPs):"
echo "  walshadow-ec2-bench --network private --dest clickhouse --bench single-row --state-dir /opt/bench/ec2"
echo "  # all four into /opt/bench/results/<name>:"
echo "  BIN=walshadow-ec2-bench STATE_DIR=/opt/bench/ec2 NETWORK=private DEST=clickhouse SKIP_BUILD=1 /opt/bench/run_bench_suite.sh myrun"
echo "  # for the pg standby:  DEST=postgres …"
