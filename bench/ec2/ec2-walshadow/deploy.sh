#!/usr/bin/env bash
# Ships the locally-built walshadow image to the daemon host, writes a
# ch-config.toml pointed at the ClickHouse private IP, and (re)starts the
# container streaming from the source PG private IP into ClickHouse.
#
# Require stack.sh setup and local `walshadow:local` image,
# built for source PostgreSQL major version:
#   docker build -f docker/Dockerfile --build-arg PG_MAJOR=17 -t walshadow:local <repo-root>
# Reject incompatible image versions below
#
# Endpoint IPs are read from the sibling state.env files; override with
#   SOURCE_PRIVATE_IP=... CH_HOST=... ./deploy.sh
#
# ClickHouse target defaults to the in-VPC ec2-clickhouse node (native 9000,
# no auth/TLS). Point it at ClickHouse Cloud instead with:
#   CH_HOST=xxx.clickhouse.cloud CH_PORT=9440 CH_SECURE=true \
#   CH_USER=default CH_PASSWORD=... CH_DATABASE=default ./deploy.sh
# (CH_DATABASE must already exist on the target — walshadow creates tables,
# not databases.)
#
# FLUSH_TIMEOUT_MS sets the CH emitter's [ch] flush_timeout_ms: rows are held
# at most this long before the INSERT block is flushed, batching multiple
# xacts into one MergeTree part instead of one-part-per-xact. 0 = legacy
# emit-per-commit (lowest latency, most parts).
set -euo pipefail
cd "$(dirname "$0")"
source ./state.env   # PUBLIC_IP, PEM, ...
source ../lib.sh

IMAGE="${IMAGE:-walshadow:local}"
FLUSH_TIMEOUT_MS="${FLUSH_TIMEOUT_MS:-1000}"
# Source-PG schema housing the config_* overlay tables (ec2-source-pg/deploy.sh
# installs + seeds them). Empty string disables the overlay (TOML-only scope).
RUNTIME_CONFIG_SCHEMA="${RUNTIME_CONFIG_SCHEMA:-walshadow}"
CH_PORT="${CH_PORT:-9000}"
CH_SECURE="${CH_SECURE:-false}"
CH_USER="${CH_USER:-default}"
CH_PASSWORD="${CH_PASSWORD:-}"
CH_DATABASE="${CH_DATABASE:-demo}"
JAEGER_IMAGE="${JAEGER_IMAGE:-jaegertracing/all-in-one:1.57}"
JAEGER_MAX_TRACES="${JAEGER_MAX_TRACES:-50000}"
JAEGER_MEMORY="${JAEGER_MEMORY:-1g}"
TRACE_SAMPLE_RATIO="${TRACE_SAMPLE_RATIO:-0.01}"
XACT_BUFFER_MAX="${XACT_BUFFER_MAX:-1073741824}"
node_ssh_setup

SRC_PRIV="${SOURCE_PRIVATE_IP:-$(require_state_ip ec2-source-pg SOURCE_PRIVATE_IP)}"
# CH host: explicit CH_HOST (e.g. Cloud endpoint) wins, else legacy
# CH_PRIVATE_IP, else the in-VPC ec2-clickhouse node.
CH_HOST="${CH_HOST:-${CH_PRIVATE_IP:-$(require_state_ip ec2-clickhouse PRIVATE_IP)}}"
echo "source PG: $SRC_PRIV:5432   clickhouse: $CH_HOST:$CH_PORT (secure=$CH_SECURE, db=$CH_DATABASE)"

wait_cloud_init

# Ship the image unless it's already present on the host (use FORCE=1 to resend).
REMOTE_IMAGE="$(remote_image_tag "$IMAGE")"
if [ "${FORCE:-0}" != "1" ] && [ -n "$REMOTE_IMAGE" ]; then
  echo "image $REMOTE_IMAGE already on host (FORCE=1 to resend)"
else
  echo "shipping $IMAGE (docker save | ssh | docker load)..."
  docker save "$IMAGE" | gzip | "${SSH[@]}" 'gunzip | sudo docker load'
  REMOTE_IMAGE="$(remote_image_tag "$IMAGE")"
  [ -n "$REMOTE_IMAGE" ] || { echo "$IMAGE missing on host after load" >&2; exit 1; }
fi

# Backup data requires matching PostgreSQL major versions
IMG_MAJOR="$("${SSH[@]}" "sudo docker run --rm --entrypoint postgres $REMOTE_IMAGE -V" | grep -oE '[0-9]+' | head -1)"
SRC_VERSION_NUM="$("${SSH[@]}" "sudo docker run --rm --entrypoint psql $REMOTE_IMAGE -h '$SRC_PRIV' -U postgres -tAc 'SHOW server_version_num'" | tr -dc '0-9')"
if [ -z "$IMG_MAJOR" ] || [ -z "$SRC_VERSION_NUM" ]; then
  echo "could not read PG majors (image $REMOTE_IMAGE, source $SRC_PRIV)" >&2
  exit 1
fi
SRC_MAJOR=$((SRC_VERSION_NUM / 10000))
if [ "$IMG_MAJOR" != "$SRC_MAJOR" ]; then
  echo "PG major mismatch: image is PG $IMG_MAJOR, source is PG $SRC_MAJOR" >&2
  echo "  rebuild: docker build -f docker/Dockerfile --build-arg PG_MAJOR=$SRC_MAJOR -t $IMAGE <repo-root>" >&2
  exit 1
fi

# ch-config.toml: the repo config with the CH host swapped to the private IP.
# Table scope is overlay-driven (walshadow.config_* on the source, seeded by
# ec2-source-pg/deploy.sh) — no hardcoded [table.*] blocks, so the daemon
# replicates whatever the overlay opts in.
echo "writing ch-config.toml (ch host=$CH_HOST:$CH_PORT secure=$CH_SECURE, flush_timeout_ms=$FLUSH_TIMEOUT_MS, runtime_config_schema='$RUNTIME_CONFIG_SCHEMA') and starting container..."
"${SSH[@]}" "sudo install -d /opt/walshadow && sudo tee /opt/walshadow/ch-config.toml >/dev/null" <<EOF
[ch]
host = "$CH_HOST"
port = $CH_PORT
database = "$CH_DATABASE"
user = "$CH_USER"
password = "$CH_PASSWORD"
secure = $CH_SECURE
compression = "lz4"
flush_timeout_ms = $FLUSH_TIMEOUT_MS

[runtime_config]
schema = "$RUNTIME_CONFIG_SCHEMA"
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
  -e WALSHADOW_XACT_BUFFER_MAX='$XACT_BUFFER_MAX' \
  -e OTEL_EXPORTER_OTLP_ENDPOINT='http://jaeger:4317' \
  -v /opt/walshadow/ch-config.toml:/etc/walshadow/ch-config.toml:ro \
  -v walshadow-data:/var/lib/walshadow \
  -p 9484:9484 \
  $REMOTE_IMAGE --trace-sample-ratio '$TRACE_SAMPLE_RATIO' >/dev/null && echo started"

# Grafana + Prometheus: only uploaded/recreated when FORCE_METRICS=1.
if [ "${FORCE_METRICS:-0}" = "1" ]; then
  GRAFANA_IMAGE="${GRAFANA_IMAGE:-grafana/grafana:13.0.2}"
  PROM_IMAGE="${PROM_IMAGE:-prom/prometheus:v3.12.0}"

  "${SSH[@]}" "sudo docker network inspect walshadow-net >/dev/null 2>&1 || sudo docker network create walshadow-net"
  "${SSH[@]}" "sudo docker network connect walshadow-net walshadow 2>/dev/null || true"

  tar -C "$(repo_root)/docker" -czf - grafana prometheus \
    | "${SSH[@]}" "sudo install -d /opt/walshadow/obs && sudo tar -C /opt/walshadow/obs -xzf -"
  # compose hostname 'clickhouse' -> private IP
  "${SSH[@]}" "sudo grep -rl clickhouse /opt/walshadow/obs 2>/dev/null | sudo xargs -r sed -i 's/clickhouse:/$CH_HOST:/g; s#//clickhouse#//$CH_HOST#g'"

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
