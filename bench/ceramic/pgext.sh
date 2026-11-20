#!/usr/bin/env bash
# pgext.sh <tree> [pg-major]: build the tree's pgext against that Postgres once,
# into <tree>/target/ceramic-pgext/<rev>-pg<major>, and print the directory
set -euo pipefail
TREE=$(cd "$1" && pwd); PG=${2:-18}
D=$TREE/target/ceramic-pgext/$(git -C "$TREE" rev-parse --short HEAD)-pg$PG
if [[ ! -f $D/walshadow.so ]]; then
  mkdir -p "$(dirname "$D")"; cp -a "$TREE/pgext" "$D"
  make -C "$D" clean >/dev/null 2>&1 || true
  make -C "$D" PG_CONFIG="${PGBIN:-/usr/lib/postgresql/$PG/bin}/pg_config" >"$D.log" 2>&1 || { tail "$D.log" >&2; exit 1; }
fi
echo "$D"
