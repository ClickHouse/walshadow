#!/usr/bin/env bash
# Bring up the PeerDB stack on the host and create the source-PG → ClickHouse
# CDC mirror. Endpoint private IPs are read from the sibling state.env files
# (override with SOURCE_PRIVATE_IP=... CH_PRIVATE_IP=...).
#
# Set CONFIGURE_MIRROR=0 to only bring up the stack and configure peers/mirror
# yourself via the UI (http://<public-ip>:3000) or psql on :9900.
#
# NOTE: the CREATE PEER / CREATE MIRROR DDL below is best-effort against
# PeerDB's stable ClickHouse support and may need tweaks for your PeerDB
# version (peer option names, staging). It's run non-fatally so the stack still
# comes up if the DDL needs adjusting — validate it on first run, or use the UI.
# PeerDB stages ClickHouse CDC through the MinIO bundled in its compose file.
set -euo pipefail
cd "$(dirname "$0")"
source ./state.env   # PUBLIC_IP, PEM, ...
source ../lib.sh

node_ssh_setup

SRC_PRIV="${SOURCE_PRIVATE_IP:-$(read_state_var ../ec2-source-pg/state.env SOURCE_PRIVATE_IP)}"
CH_PRIV="${CH_PRIVATE_IP:-$(read_state_var ../ec2-clickhouse/state.env PRIVATE_IP)}"
[ -n "$SRC_PRIV" ] || { echo "source PG private IP unknown (provision ec2-source-pg first)" >&2; exit 1; }
[ -n "$CH_PRIV" ]  || { echo "clickhouse private IP unknown (provision ec2-clickhouse first)" >&2; exit 1; }
echo "source PG: $SRC_PRIV:5432   clickhouse: $CH_PRIV:9000"

echo "waiting for SSH on the host…"
ssh_ok=0
for i in $(seq 1 30); do "${SSH[@]}" true 2>/dev/null && { ssh_ok=1; break; }; sleep 10; done
[ "$ssh_ok" = 1 ] || { echo "host not reachable over SSH after ~300s" >&2; exit 1; }

# Block until cloud-init has actually finished (Docker install + PeerDB clone),
# rather than racing it. --wait returns non-zero if cloud-init errored.
echo "waiting for cloud-init to finish (Docker install + PeerDB clone)…"
"${SSH[@]}" 'sudo cloud-init status --wait' || { echo "cloud-init did not complete cleanly on the host" >&2; exit 1; }
"${SSH[@]}" 'command -v docker >/dev/null && [ -d /opt/peerdb ]' \
  || { echo "host missing docker or /opt/peerdb after cloud-init" >&2; exit 1; }

# PeerDB stages CDC as avro into its bundled MinIO and has ClickHouse load it
# via the s3() function. The quickstart compose hardcodes that staging endpoint
# to http://host.docker.internal:9001 — only reachable from THIS box. Our
# ClickHouse runs on a separate host, so rewrite it to this box's private IP
# (MinIO is published on :9001; terraform opens 9001 to the VPC). Idempotent.
echo "pointing PeerDB S3 staging endpoint at http://$PRIVATE_IP:9001 (for the external ClickHouse)…"
"${SSH[@]}" "sudo sed -i -E 's|(PEERDB_CLICKHOUSE_AWS_CREDENTIALS_AWS_ENDPOINT_URL_S3:[[:space:]]*).*|\\1http://$PRIVATE_IP:9001|' /opt/peerdb/docker-compose.yml"

echo "bringing up the PeerDB stack (docker compose up -d; first run pulls several GB)…"
"${SSH[@]}" 'cd /opt/peerdb && sudo docker compose up -d 2>&1 | tail -8'

# psql helper: a throwaway client container on the host network talks to the
# PeerDB SQL server (Postgres wire) on :9900.
PSQL='sudo docker run --rm -i --network host postgres:17-alpine psql "host=127.0.0.1 port=9900 user=peerdb password=peerdb dbname=peerdb sslmode=disable"'

echo "waiting for the PeerDB SQL server on :9900…"
sql_ok=0
for i in $(seq 1 40); do
  "${SSH[@]}" "$PSQL -tAc 'select 1'" 2>/dev/null | grep -q 1 && { sql_ok=1; break; }
  sleep 10
done
[ "$sql_ok" = 1 ] || { echo "PeerDB SQL server not reachable on :9900 after ~400s" >&2; exit 1; }
echo "peerdb-server up"

# The PeerDB quickstart runs one-shot inits that race their dependencies on a
# fresh/slow boot and can silently fail: (a) MinIO bucket creation — without it
# CREATE PEER fails S3 validation (NoSuchBucket); (b) Temporal search-attribute
# registration — without MirrorName, CREATE MIRROR can't start its workflow.
# Re-assert both, idempotently, before configuring anything.
echo "ensuring MinIO staging bucket 'peerdbbucket' exists…"
"${SSH[@]}" "cd /opt/peerdb && sudo docker compose exec -T minio sh -c 'mc alias set m http://localhost:9000 _peerdb_minioadmin _peerdb_minioadmin >/dev/null 2>&1; mc mb -p m/peerdbbucket' 2>&1 | tail -1" || true
echo "ensuring Temporal search attribute 'MirrorName' is registered…"
for i in $(seq 1 6); do
  "${SSH[@]}" "cd /opt/peerdb && sudo docker compose exec -T temporal-admin-tools temporal operator search-attribute create --namespace default --name MirrorName --type Keyword --address temporal:7233" 2>/dev/null && break
  sleep 10
done

if [ "${CONFIGURE_MIRROR:-1}" = "1" ]; then
  # Drop an existing mirror first so re-running deploy.sh actually re-applies
  # config changes — CREATE MIRROR errors if the mirror already exists, so
  # without this a re-deploy silently keeps the old config. DROP MIRROR also
  # removes the source slot + publication. Non-fatal (no-op on first run).
  echo "dropping existing mirror (if any) so re-deploy re-applies config…"
  "${SSH[@]}" "$PSQL -c 'DROP MIRROR IF EXISTS demo_users;'" 2>&1 | tail -1 || true

  # PeerDB OWNS its destination table — the normalized table carries _peerdb_*
  # metadata columns, so it can't reuse the walshadow-shaped demo.users the
  # ClickHouse init pre-creates. Drop it (over CH's HTTP port, reachable across
  # the VPC) so PeerDB recreates it with the right schema. The mapping below
  # also targets the table `users` in the peer's `demo` database — NOT the
  # literal name `demo.users`, which would create a dotted-name table.
  echo "dropping any pre-existing demo.users on ClickHouse so PeerDB owns it…"
  "${SSH[@]}" "curl -s 'http://$CH_PRIV:8123/' --data-binary 'DROP TABLE IF EXISTS demo.users' && echo '  dropped'"

  echo "creating peers + mirror (non-fatal; see NOTE)…"
  "${SSH[@]}" "$PSQL" <<SQL || echo "  ⚠ peer/mirror DDL returned non-zero — adjust for your PeerDB version or use the UI"
-- Source Postgres peer (demo source; trust auth accepts any password).
CREATE PEER source_pg FROM POSTGRES WITH (
  host = '$SRC_PRIV', port = '5432', user = 'postgres', password = 'postgres', database = 'postgres'
);
-- ClickHouse target peer (native protocol on 9000, no TLS).
CREATE PEER ch_target FROM CLICKHOUSE WITH (
  host = '$CH_PRIV', port = '9000', user = 'default', password = '', database = 'demo', disable_tls = true
);
-- CDC mirror: source demo.users -> ClickHouse table "users" in db "demo".
-- Destination is the bare table name (it lands in the peer's demo database);
-- writing demo.users here would create a literal dotted-name table instead.
-- idle_timeout_seconds = lowest the field allows (integer seconds; 0 means
-- "use default", which is slower). Each sync still has an OCF->S3->INSERT->
-- normalize floor, so end-to-end is a couple of seconds, not sub-second.
CREATE MIRROR demo_users FROM source_pg TO ch_target
WITH TABLE MAPPING (demo.users:users)
WITH (do_initial_copy = true, idle_timeout_seconds = 1);
SQL
else
  echo "CONFIGURE_MIRROR=0 — skipping peer/mirror creation (use the UI :3000 or psql :9900)"
fi

echo
echo "=== deployed ==="
echo "PeerDB UI:  http://$PUBLIC_IP:3000"
echo "PeerDB SQL: psql 'host=$PUBLIC_IP port=9900 user=peerdb password=peerdb dbname=peerdb'"
echo "logs:       ssh -i $PEM ubuntu@$PUBLIC_IP 'cd /opt/peerdb && sudo docker compose logs -f'"
echo "profile:    ./profile.sh [secs]   # start an on-CPU capture before the bench; teardown copies it back"
