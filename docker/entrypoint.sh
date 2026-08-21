#!/usr/bin/env bash
# walshadow container entrypoint. Creates state directories, then execs the
# daemon against the configured source and ClickHouse.
#
# Connection settings come from WALSHADOW_SOURCE_URL / WALSHADOW_CH_URL (the
# daemon reads both from the environment), from a config mounted at
# WALSHADOW_CH_CONFIG, or from the discrete WALSHADOW_SOURCE_* variables.
#
# `init` and `ctl` run as themselves, so `docker compose run --rm walshadow
# init` and `docker compose exec walshadow walshadow-stream ctl status` both
# work without the daemon's flags.

set -euo pipefail

case "${1:-}" in
    init | ctl)
        exec walshadow-stream "$@"
        ;;
esac

SHADOW_DATA="${WALSHADOW_SHADOW_DATA:-/var/lib/walshadow/shadow-data}"
OUT_DIR="${WALSHADOW_OUT_DIR:-/var/lib/walshadow/out}"
SPILL_DIR="${WALSHADOW_SPILL_DIR:-/var/lib/walshadow/spill}"
SOCKET_DIR="${WALSHADOW_SHADOW_SOCKET_DIR:-/var/run/postgresql}"

mkdir -p "$SHADOW_DATA"
# Shadow PG refuses to start on anything other than 0700/0750. Named
# volume mount drops Dockerfile-time perms, so reassert on each boot.
chmod 700 "$SHADOW_DATA"
mkdir -p "$OUT_DIR" "$SPILL_DIR" "$SOCKET_DIR"

# conf.d drop-in dir the control API writes fragments into (base is ro).
CH_CONFIG="${WALSHADOW_CH_CONFIG:-/etc/walshadow/ch-config.toml}"
mkdir -p "${CH_CONFIG%.toml}.d"

# Discrete source flags for callers predating WALSHADOW_SOURCE_URL
# (bench/ec2/ec2-walshadow/deploy.sh). Skipped once a URL is set, which the
# daemon reads for itself.
SOURCE_ARGS=()
if [ -z "${WALSHADOW_SOURCE_URL:-}" ]; then
    if [ -z "${WALSHADOW_SOURCE_HOST:-}" ] && [ ! -f "$CH_CONFIG" ]; then
        echo "walshadow: no source configured. Set WALSHADOW_SOURCE_URL," >&2
        echo "  eg postgres://user:password@host:5432/dbname, or mount a" >&2
        echo "  config at $CH_CONFIG (write one with \`init\`)." >&2
        exit 64
    fi
    if [ -n "${WALSHADOW_SOURCE_HOST:-}" ]; then
        SOURCE_ARGS+=(
            --host "$WALSHADOW_SOURCE_HOST"
            --port "${WALSHADOW_SOURCE_PORT:-5432}"
            --user "${WALSHADOW_SOURCE_USER:-postgres}"
            --dbname "${WALSHADOW_SOURCE_DB:-postgres}"
            --sslmode "${WALSHADOW_SOURCE_SSLMODE:-disable}"
        )
    fi
fi

# Pool sizes fall through to the binary's compiled defaults unless overridden
# via env. clap rejects a flag passed twice, so only inject when the caller
# (e.g. EC2 deploy.sh via "$@") didn't already pass it.
POOL_ARGS=()
if [ -n "${WALSHADOW_DECODER_POOL:-}" ]; then
    case " $* " in
        *" --decoder-pool-size "*) ;;
        *) POOL_ARGS+=(--decoder-pool-size "$WALSHADOW_DECODER_POOL") ;;
    esac
fi
if [ -n "${WALSHADOW_INSERTER_POOL:-}" ]; then
    case " $* " in
        *" --inserter-pool-size "*) ;;
        *) POOL_ARGS+=(--inserter-pool-size "$WALSHADOW_INSERTER_POOL") ;;
    esac
fi
case " $* " in
    *" --xact-buffer-max "*) ;;
    *) POOL_ARGS+=(--xact-buffer-max "${WALSHADOW_XACT_BUFFER_MAX:-1073741824}") ;;
esac

exec walshadow-stream \
    "${SOURCE_ARGS[@]}" \
    --out-dir "$OUT_DIR" \
    --spill-dir "$SPILL_DIR" \
    --shadow-socket-dir "$SOCKET_DIR" \
    --shadow-port "${WALSHADOW_SHADOW_PORT:-5432}" \
    --shadow-user postgres \
    --shadow-dbname postgres \
    --bootstrap-mode direct \
    --bootstrap-shadow-data-dir "$SHADOW_DATA" \
    --walsender-bind 127.0.0.1:5433 \
    --ch-config "$CH_CONFIG" \
    --metrics-bind 0.0.0.0:9484 \
    --control-socket "${WALSHADOW_CONTROL_SOCKET:-/var/run/walshadow/control.sock}" \
    --status-interval "${WALSHADOW_STATUS_INTERVAL:-5}" \
    "${POOL_ARGS[@]}" \
    "$@"
