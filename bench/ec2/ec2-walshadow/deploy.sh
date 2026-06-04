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
FLUSH_TIMEOUT_MS="${FLUSH_TIMEOUT_MS:-1000}"
JAEGER_IMAGE="${JAEGER_IMAGE:-jaegertracing/all-in-one:1.57}"
JAEGER_MAX_TRACES="${JAEGER_MAX_TRACES:-50000}"
JAEGER_MEMORY="${JAEGER_MEMORY:-1g}"
TRACE_SAMPLE_RATIO="${TRACE_SAMPLE_RATIO:-0.01}"
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

# Memory-bound Jaeger (in-memory storage capped at JAEGER_MAX_TRACES, RAM at
# JAEGER_MEMORY) on walshadow-net so the daemon ships spans to it by name.
"${SSH[@]}" "sudo docker network inspect walshadow-net >/dev/null 2>&1 || sudo docker network create walshadow-net"
"${SSH[@]}" "sudo docker rm -f jaeger >/dev/null 2>&1 || true; sudo docker run -d --name jaeger --restart unless-stopped \
  --network walshadow-net \
  --memory '$JAEGER_MEMORY' \
  -e COLLECTOR_OTLP_ENABLED=true \
  -e SPAN_STORAGE_TYPE=memory \
  -e MEMORY_MAX_TRACES='$JAEGER_MAX_TRACES' \
  -p 16686:16686 -p 4317:4317 \
  $JAEGER_IMAGE >/dev/null && echo 'jaeger started'"

"${SSH[@]}" "sudo docker rm -f walshadow >/dev/null 2>&1 || true; sudo docker run -d --name walshadow --restart unless-stopped \
  --network walshadow-net \
  -e RUST_LOG='warn,walshadow=info' \
  -e WALSHADOW_SOURCE_HOST='$SRC_PRIV' \
  -e WALSHADOW_SOURCE_PORT=5432 \
  -e OTEL_EXPORTER_OTLP_ENDPOINT='http://jaeger:4317' \
  -v /opt/walshadow/ch-config.toml:/etc/walshadow/ch-config.toml:ro \
  -v walshadow-data:/var/lib/walshadow \
  -p 9484:9484 \
  $IMAGE --trace-sample-ratio '$TRACE_SAMPLE_RATIO' >/dev/null && echo started"

# Grafana + Prometheus: only uploaded/recreated when FORCE_METRICS=1.
if [ "${FORCE_METRICS:-0}" = "1" ]; then
  GRAFANA_IMAGE="${GRAFANA_IMAGE:-grafana/grafana:13.0.2}"
  PROM_IMAGE="${PROM_IMAGE:-prom/prometheus:v3.12.0}"
  REPO_ROOT="$(cd ../../.. && pwd)"

  "${SSH[@]}" "sudo docker network inspect walshadow-net >/dev/null 2>&1 || sudo docker network create walshadow-net"
  "${SSH[@]}" "sudo docker network connect walshadow-net walshadow 2>/dev/null || true"

  tar -C "$REPO_ROOT/docker" -czf - grafana prometheus \
    | "${SSH[@]}" "sudo install -d /opt/walshadow/obs && sudo tar -C /opt/walshadow/obs -xzf -"
  # compose hostname 'clickhouse' -> private IP
  "${SSH[@]}" "sudo grep -rl clickhouse /opt/walshadow/obs 2>/dev/null | sudo xargs -r sed -i 's/clickhouse:/$CH_PRIV:/g; s#//clickhouse#//$CH_PRIV#g'"

  "${SSH[@]}" "sudo docker rm -f prometheus >/dev/null 2>&1 || true; sudo docker run -d --name prometheus --restart unless-stopped \
    --network walshadow-net \
    -v /opt/walshadow/obs/prometheus/prometheus.yml:/etc/prometheus/prometheus.yml:ro \
    -p 9090:9090 \
    $PROM_IMAGE >/dev/null && echo 'prometheus started'"

  "${SSH[@]}" "sudo docker rm -f grafana >/dev/null 2>&1 || true; sudo docker run -d --name grafana --restart unless-stopped \
    --network walshadow-net \
    -e GF_AUTH_ANONYMOUS_ENABLED=true -e GF_AUTH_ANONYMOUS_ORG_ROLE=Admin \
    -e GF_INSTALL_PLUGINS=grafana-clickhouse-datasource \
    -v /opt/walshadow/obs/grafana/provisioning:/etc/grafana/provisioning:ro \
    -v /opt/walshadow/obs/grafana/dashboards:/var/lib/grafana/dashboards:ro \
    -p 3000:3000 \
    $GRAFANA_IMAGE >/dev/null && echo 'grafana started'"
fi

echo
echo "=== deployed ==="
echo "metrics:  curl http://$PUBLIC_IP:9484/metrics"
echo "traces:   http://$PUBLIC_IP:16686  (jaeger)"
echo "logs:     ssh -i $PEM ubuntu@$PUBLIC_IP 'sudo docker logs -f walshadow'"
