#!/usr/bin/env bash
# Capture a WAL segment containing an XLOG_SWITCH record for the
# XLOG_SWITCH fixture.
#
# WALSHADOW_PG_IMAGE / WALSHADOW_USE_LOCAL behave as in
# fixtures/wal/filter/capture.sh. Default: postgres:16 in docker.
# walshadow rejects PG <= 14 captures (FPI bit layout).
#
# Output: segments/000000010000000000000002.gz
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
out="$here/segments"
mkdir -p "$out"

work="$(mktemp -d)"
PG_IMAGE="${WALSHADOW_PG_IMAGE:-postgres:16}"
CID=""
trap '[[ -n "$CID" ]] && docker rm -f "$CID" >/dev/null 2>&1 || true; rm -rf "$work"' EXIT

if [[ "${WALSHADOW_USE_LOCAL:-0}" == "1" ]]; then
    echo "WALSHADOW_USE_LOCAL=1: using local postgres" >&2
    local_major=$(postgres -V | awk '{print $3}' | cut -d. -f1)
    if [[ -z "$local_major" || "$local_major" -lt 15 ]]; then
        echo "local postgres major '$local_major' < 15; walshadow rejects PG <= 14 captures" >&2
        exit 1
    fi
    PGDATA="$work/data"
    SOCKDIR="$work/sock"
    mkdir -p "$SOCKDIR"
    PORT="${WALSHADOW_PG_PORT:-55435}"
    initdb -D "$PGDATA" -U postgres --no-instructions --auth=trust --encoding=UTF8 --locale=C >/dev/null
    cat >>"$PGDATA/postgresql.conf" <<EOF
wal_level = logical
max_wal_senders = 4
wal_keep_size = 128MB
fsync = on
full_page_writes = off
wal_compression = off
autovacuum = off
shared_buffers = 32MB
unix_socket_directories = '$SOCKDIR'
listen_addresses = ''
port = $PORT
EOF
    pg_ctl -D "$PGDATA" -l "$work/log" -o "-c full_page_writes=off -c wal_compression=off" start -w >/dev/null
    psql -h "$SOCKDIR" -p "$PORT" -U postgres -d postgres -v ON_ERROR_STOP=1 -f "$here/workload.sql" >/dev/null
    pg_ctl -D "$PGDATA" stop -m fast -w >/dev/null
    seg="$PGDATA/pg_wal/000000010000000000000002"
else
    echo "using docker image $PG_IMAGE" >&2
    CID=$(docker run -d \
        -e POSTGRES_PASSWORD=pg \
        -e POSTGRES_DB=postgres \
        "$PG_IMAGE" \
        -c wal_level=logical \
        -c max_wal_senders=4 \
        -c full_page_writes=off \
        -c wal_compression=off \
        -c autovacuum=off \
        -c shared_buffers=32MB)
    for _ in $(seq 1 60); do
        if docker exec "$CID" pg_isready -U postgres -d postgres >/dev/null 2>&1; then
            break
        fi
        sleep 0.5
    done
    docker cp "$here/workload.sql" "$CID:/tmp/workload.sql"
    docker exec -u postgres "$CID" psql -d postgres -v ON_ERROR_STOP=1 -f /tmp/workload.sql >/dev/null
    pgdata=$(docker exec -u postgres "$CID" psql -d postgres -tAX -c "SHOW data_directory")
    docker cp "$CID:$pgdata/pg_wal/000000010000000000000002" "$work/segment"
    seg="$work/segment"
fi

[[ -f "$seg" ]] || { echo "no segment at $seg" >&2; exit 1; }

python3 - "$seg" "$out/000000010000000000000002" <<'PY'
import sys
src, dst = sys.argv[1], sys.argv[2]
with open(src, 'rb') as f:
    data = f.read()
PAGE = 8192
n_pages = len(data) // PAGE
last_nonzero = 0
for i in range(n_pages):
    page = data[i*PAGE:(i+1)*PAGE]
    if any(page):
        last_nonzero = i
keep = (last_nonzero + 2) * PAGE
keep = min(keep, len(data))
with open(dst, 'wb') as f:
    f.write(data[:keep])
print(f"kept {keep // PAGE} pages ({keep} bytes) of {n_pages} ({len(data)} bytes)", file=sys.stderr)
PY

gzip -f -9 "$out/000000010000000000000002"
echo "wrote $out/000000010000000000000002.gz"
