# shared by every script in bin/; sourced, not executed
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/.." && pwd)"

if [ -f "$ROOT/env.local" ]; then
  set -a; . "$ROOT/env.local"; set +a
elif [ -f "$ROOT/env.example" ]; then
  echo "env.local not found — falling back to env.example defaults" >&2
  set -a; . "$ROOT/env.example"; set +a
fi

: "${PG_DSN:?PG_DSN not set (copy env.example to env.local)}"
: "${CH_URL:?CH_URL not set (copy env.example to env.local)}"
: "${ENGINE_URL:=http://localhost:8080}"

die() { echo "error: $*" >&2; exit 1; }

need() { command -v "$1" >/dev/null 2>&1 || die "$1 is required but not installed"; }

# ClickHouse HTTP. Splits user/password/database out of CH_URL so callers just
# pass SQL on stdin or as $1.
ch() {
  python3 - "$CH_URL" "$@" <<'PY'
import sys, urllib.parse, urllib.request
url = urllib.parse.urlparse(sys.argv[1])
sql = sys.argv[2] if len(sys.argv) > 2 else sys.stdin.read()
import os
db = os.environ.get('CH_DB_OVERRIDE') or url.path.strip('/') or 'default'
base = f"{url.scheme}://{url.hostname}:{url.port or 8123}/?database={urllib.parse.quote(db)}"
req = urllib.request.Request(base, data=sql.encode())
req.add_header('X-ClickHouse-User', url.username or 'default')
req.add_header('X-ClickHouse-Key', url.password or '')
try:
    sys.stdout.write(urllib.request.urlopen(req, timeout=60).read().decode())
except urllib.error.HTTPError as e:
    sys.stderr.write(e.read().decode())
    sys.exit(1)
PY
}

api() {
  local method="$1" path="$2" body="${3:-}"
  if [ -n "$body" ]; then
    curl -sS -X "$method" -H 'content-type: application/json' -d "$body" "$ENGINE_URL$path"
  else
    curl -sS -X "$method" "$ENGINE_URL$path"
  fi
}

# Pretty-print a JSON field without requiring jq.
jget() { python3 -c 'import json,sys;d=json.load(sys.stdin);print(d.get(sys.argv[1],""))' "$1"; }
