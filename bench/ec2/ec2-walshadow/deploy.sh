#!/usr/bin/env bash
# Ships the locally-built walshadow image to the daemon host, writes a
# ch-config.toml pointed at the ClickHouse private IP, and (re)starts the
# container streaming from the source PG private IP into ClickHouse.
#
# Prereqs: ./provision.sh has run (state.env exists), the source-pg and
# clickhouse nodes are up, and `walshadow:local` exists locally
#   (docker build -f docker/Dockerfile -t walshadow:local <repo-root>).
#
# Endpoint IPs are read from the sibling state.env files; override with
#   SOURCE_PRIVATE_IP=... CH_PRIVATE_IP=... ./deploy.sh
#
# FLUSH_TIMEOUT_MS sets the CH emitter's [ch] flush_timeout_ms: rows are held
# at most this long before the INSERT block is flushed, batching multiple
# xacts into one MergeTree part instead of one-part-per-xact. 0 = legacy
# emit-per-commit (lowest latency, most parts).
set -euo pipefail
cd "$(dirname "$0")"
source ../aws-env.sh
source ./state.env   # PUBLIC_IP, KEY_NAME, ...
source ../lib.sh

IMAGE="${IMAGE:-walshadow:local}"
FLUSH_TIMEOUT_MS="${FLUSH_TIMEOUT_MS:-50}"
node_ssh_setup

SRC_PRIV="${SOURCE_PRIVATE_IP:-$(read_state_var ../ec2-source-pg/state.env SOURCE_PRIVATE_IP)}"
CH_PRIV="${CH_PRIVATE_IP:-$(read_state_var ../ec2-clickhouse/state.env PRIVATE_IP)}"
[ -n "$SRC_PRIV" ] || { echo "source PG private IP unknown (provision ec2-source-pg first)" >&2; exit 1; }
[ -n "$CH_PRIV" ]  || { echo "clickhouse private IP unknown (provision ec2-clickhouse first)" >&2; exit 1; }
echo "source PG: $SRC_PRIV:5432   clickhouse: $CH_PRIV:9000"

echo "waiting for Docker on the host (cloud-init may still be running)..."
for i in $(seq 1 30); do
  "${SSH[@]}" 'command -v docker >/dev/null && sudo docker info >/dev/null 2>&1' 2>/dev/null && break
  sleep 10
done

# Ship the image unless it's already present on the host (use FORCE=1 to resend).
if [ "${FORCE:-0}" != "1" ] && "${SSH[@]}" "sudo docker image inspect $IMAGE >/dev/null 2>&1"; then
  echo "image $IMAGE already on host (FORCE=1 to resend)"
else
  echo "shipping $IMAGE (docker save | ssh | docker load)..."
  docker save "$IMAGE" | gzip | "${SSH[@]}" 'gunzip | sudo docker load'
fi

# ch-config.toml: the repo config with the CH host swapped to the private IP.
echo "writing ch-config.toml (ch host=$CH_PRIV, flush_timeout_ms=$FLUSH_TIMEOUT_MS) and starting container..."
"${SSH[@]}" "sudo install -d /opt/walshadow && sudo tee /opt/walshadow/ch-config.toml >/dev/null" <<EOF
[ch]
host = "$CH_PRIV"
port = 9000
database = "demo"
user = "default"
password = ""
compression = "lz4"
flush_timeout_ms = $FLUSH_TIMEOUT_MS

[table."demo.users"]
target = "demo.users"
columns = [
    { attnum = 1, target = "id",    type = "UInt64" },
    { attnum = 2, target = "name",  type = "String" },
    { attnum = 3, target = "email", type = "String" },
]
EOF

"${SSH[@]}" "sudo docker rm -f walshadow >/dev/null 2>&1 || true; sudo docker run -d --name walshadow --restart unless-stopped \
  -e RUST_LOG='warn,walshadow=info' \
  -e WALSHADOW_SOURCE_HOST='$SRC_PRIV' \
  -e WALSHADOW_SOURCE_PORT=5432 \
  -v /opt/walshadow/ch-config.toml:/etc/walshadow/ch-config.toml:ro \
  -v walshadow-data:/var/lib/walshadow \
  -p 9484:9484 \
  $IMAGE >/dev/null && echo started"

echo
echo "=== deployed ==="
echo "metrics:  curl http://$PUBLIC_IP:9484/metrics"
echo "logs:     ssh -i $PEM ubuntu@$PUBLIC_IP 'sudo docker logs -f walshadow'"
