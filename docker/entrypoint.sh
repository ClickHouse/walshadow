#!/usr/bin/env bash
# walshadow demo entrypoint. Wipes any stale shadow data dir so
# --bootstrap-mode=direct sees an empty target, then execs the daemon
# pointed at the docker-compose source / clickhouse hostnames.

set -euo pipefail

SHADOW_DATA="${WALSHADOW_SHADOW_DATA:-/var/lib/walshadow/shadow-data}"
OUT_DIR="${WALSHADOW_OUT_DIR:-/var/lib/walshadow/out}"
SPILL_DIR="${WALSHADOW_SPILL_DIR:-/var/lib/walshadow/spill}"
SOCKET_DIR="${WALSHADOW_SHADOW_SOCKET_DIR:-/var/run/postgresql}"

# Direct bootstrap refuses to land into a non-empty data dir, mirror
# initdb's contract. Drop the volume contents on every (re)start so
# `docker compose up --force-recreate` rebootstraps cleanly.
if [ -d "$SHADOW_DATA" ]; then
    find "$SHADOW_DATA" -mindepth 1 -delete
else
    mkdir -p "$SHADOW_DATA"
fi
# Shadow PG refuses to start on anything other than 0700/0750. Named
# volume mount drops Dockerfile-time perms, so reassert on each boot.
chmod 700 "$SHADOW_DATA"
mkdir -p "$OUT_DIR" "$SPILL_DIR" "$SOCKET_DIR"

exec walshadow-stream \
    --host "${WALSHADOW_SOURCE_HOST:-source}" \
    --port "${WALSHADOW_SOURCE_PORT:-5432}" \
    --user "${WALSHADOW_SOURCE_USER:-postgres}" \
    --dbname "${WALSHADOW_SOURCE_DB:-postgres}" \
    --sslmode disable \
    --out-dir "$OUT_DIR" \
    --spill-dir "$SPILL_DIR" \
    --shadow-socket-dir "$SOCKET_DIR" \
    --shadow-port "${WALSHADOW_SHADOW_PORT:-5432}" \
    --shadow-user postgres \
    --shadow-dbname postgres \
    --bootstrap-mode direct \
    --bootstrap-shadow-data-dir "$SHADOW_DATA" \
    --bootstrap-autospawn-shadow \
    --walsender-bind 127.0.0.1:5433 \
    --ch-config "${WALSHADOW_CH_CONFIG:-/etc/walshadow/ch-config.toml}" \
    --metrics-bind 0.0.0.0:9484 \
    --status-interval "${WALSHADOW_STATUS_INTERVAL:-5}" \
    "$@"
