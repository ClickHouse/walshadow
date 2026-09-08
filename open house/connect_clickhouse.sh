#!/usr/bin/env bash
#
# clickhouse-client against the demo's destination. Credentials come from
# env.local. CH_URL is the HTTP endpoint the app uses; the native client wants
# the secure native port instead, so the host is reused and the port swapped.
#
#   ./connect_clickhouse.sh                     interactive client
#   ./connect_clickhouse.sh -q 'SELECT count() FROM market_trades'
#   ./connect_clickhouse.sh --queries-file sql/samples-ch.sql
#
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"
set -a; . ./env.local; set +a
: "${CH_URL:?CH_URL missing from env.local}"

eval "$(python3 - "$CH_URL" <<'PY'
import shlex, sys, urllib.parse
u = urllib.parse.urlparse(sys.argv[1])
db = u.path.strip("/") or "default"
for k, v in (("CH_HOST", u.hostname), ("CH_USER", u.username or "default"),
             ("CH_PASS", urllib.parse.unquote(u.password or "")), ("CH_DB", db)):
    print("%s=%s" % (k, shlex.quote(v)))
PY
)"

exec clickhouse-client \
  --host "$CH_HOST" --port "${CH_NATIVE_PORT:-9440}" --secure \
  --user "$CH_USER" --password "$CH_PASS" --database "$CH_DB" "$@"
