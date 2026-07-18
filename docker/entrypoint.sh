#!/usr/bin/env bash
# walshadow demo entrypoint. Creates state directories, then execs daemon
# against docker-compose source and ClickHouse services

set -euo pipefail

SHADOW_DATA="${WALSHADOW_SHADOW_DATA:-/var/lib/walshadow/shadow-data}"
OUT_DIR="${WALSHADOW_OUT_DIR:-/var/lib/walshadow/out}"
SPILL_DIR="${WALSHADOW_SPILL_DIR:-/var/lib/walshadow/spill}"
SOCKET_DIR="${WALSHADOW_SHADOW_SOCKET_DIR:-/var/run/postgresql}"

mkdir -p "$SHADOW_DATA"
# Shadow PG refuses to start on anything other than 0700/0750. Named
# volume mount drops Dockerfile-time perms, so reassert on each boot.
chmod 700 "$SHADOW_DATA"
mkdir -p "$OUT_DIR" "$SPILL_DIR" "$SOCKET_DIR"

# Pool sizes default here for the local stack, but the EC2 deploy.sh forwards
# explicit --decoder-pool-size/--inserter-pool-size via "$@"; clap rejects a
# flag passed twice, so only inject our defaults when the caller didn't.
POOL_ARGS=()
case " $* " in
    *" --decoder-pool-size "*) ;;
    *) POOL_ARGS+=(--decoder-pool-size "${WALSHADOW_DECODER_POOL:-1}") ;;
esac
case " $* " in
    *" --inserter-pool-size "*) ;;
    *) POOL_ARGS+=(--inserter-pool-size "${WALSHADOW_INSERTER_POOL:-4}") ;;
esac
case " $* " in
    *" --xact-buffer-max "*) ;;
    *) POOL_ARGS+=(--xact-buffer-max "${WALSHADOW_XACT_BUFFER_MAX:-1073741824}") ;;
esac

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
    --walsender-bind 127.0.0.1:5433 \
    --ch-config "${WALSHADOW_CH_CONFIG:-/etc/walshadow/ch-config.toml}" \
    --metrics-bind 0.0.0.0:9484 \
    --status-interval "${WALSHADOW_STATUS_INTERVAL:-5}" \
    "${POOL_ARGS[@]}" \
    "$@"
