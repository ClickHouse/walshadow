#!/usr/bin/env bash
# Capture a WAL segment for the Phase 0 classifier fixture.
#
# Uses docker postgres:14 by default. wal-rs walparser (the parser
# walshadow depends on) misreads the PG 15+ BKPIMAGE_APPLY flag as
# BKPIMAGE_IS_COMPRESSED and can't decode any record that contains an
# FPI from PG ≥ 15. Phase 1 of walshadow will either teach wal-rs to be
# version-aware or ship an in-tree image-header reader. Until then,
# captures must come from PG ≤ 14.
#
# Override the image with WALSHADOW_PG_IMAGE=postgres:13 / 14 / etc.
# Set WALSHADOW_USE_LOCAL=1 to bypass docker — only works if local
# `postgres` is PG ≤ 14 (the in-PATH binary on the dev box is PG 18,
# so the docker path is the default).
#
# Output: segments/000000010000000000000001.gz
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
out="$here/segments"
mkdir -p "$out"

work="$(mktemp -d)"
PG_IMAGE="${WALSHADOW_PG_IMAGE:-postgres:14}"
CID=""
trap '[[ -n "$CID" ]] && docker rm -f "$CID" >/dev/null 2>&1 || true; rm -rf "$work"' EXIT

if [[ "${WALSHADOW_USE_LOCAL:-0}" == "1" ]]; then
    echo "WALSHADOW_USE_LOCAL=1: using local postgres" >&2
    PGDATA="$work/data"
    SOCKDIR="$work/sock"
    mkdir -p "$SOCKDIR"
    PORT="${WALSHADOW_PG_PORT:-55432}"
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
    seg="$PGDATA/pg_wal/000000010000000000000001"
else
    echo "using docker image $PG_IMAGE" >&2
    # No --rm: postmaster shutdown races docker cp otherwise; trap does cleanup
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
    # Wait for readiness.
    for _ in $(seq 1 60); do
        if docker exec "$CID" pg_isready -U postgres -d postgres >/dev/null 2>&1; then
            break
        fi
        sleep 0.5
    done
    docker cp "$here/workload.sql" "$CID:/tmp/workload.sql"
    docker exec -u postgres "$CID" psql -d postgres -v ON_ERROR_STOP=1 -f /tmp/workload.sql >/dev/null
    # PG ≥ 18 image moved PGDATA from /var/lib/postgresql/data to a
    # version-pathed dir; query SHOW data_directory and grab from there
    pgdata=$(docker exec -u postgres "$CID" psql -d postgres -tAX -c "SHOW data_directory")
    # Grab the segment while the container is still alive. Workload
    # ends in CHECKPOINT, so segment 0..1 is fully flushed
    docker cp "$CID:$pgdata/pg_wal/000000010000000000000001" "$work/segment"
    seg="$work/segment"
fi

[[ -f "$seg" ]] || { echo "no segment at $seg" >&2; exit 1; }

# Truncate trailing zero 8 KiB pages: find first run of >=2 zero pages
# from EOF and snip there. Keeps a few zero pages of padding so the
# parser sees a clean end-of-data marker without surprises.
python3 - "$seg" "$out/000000010000000000000001" <<'PY'
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
keep = (last_nonzero + 2) * PAGE  # one trailing zero page as EOF marker
keep = min(keep, len(data))
with open(dst, 'wb') as f:
    f.write(data[:keep])
print(f"kept {keep // PAGE} pages ({keep} bytes) of {n_pages} ({len(data)} bytes)", file=sys.stderr)
PY

gzip -f -9 "$out/000000010000000000000001"
echo "wrote $out/000000010000000000000001.gz"
