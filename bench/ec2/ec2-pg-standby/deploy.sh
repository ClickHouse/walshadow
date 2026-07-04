#!/usr/bin/env bash
# Make this host a physical streaming standby of the source primary:
# pg_basebackup -R from the primary into a fresh data volume (writes
# standby.signal + primary_conninfo), then start postgres — it detects
# standby.signal and streams WAL. hot_standby is on, so the bench can read it
# on :5432. The primary already allows replication (pg_hba "host replication
# all all trust", max_wal_senders=8).
#
# Source private IP is read from ../ec2-source-pg/state.env (override with
# SOURCE_PRIVATE_IP=...). Re-running takes a fresh base backup.
set -euo pipefail
cd "$(dirname "$0")"
source ./state.env   # PUBLIC_IP, PEM, ...
source ../lib.sh

# MUST match the primary's major version for physical replication.
PG_IMAGE="${PG_IMAGE:-postgres:17-bookworm}"
node_ssh_setup

SRC_PRIV="${SOURCE_PRIVATE_IP:-$(read_state_var ../ec2-source-pg/state.env SOURCE_PRIVATE_IP)}"
[ -n "$SRC_PRIV" ] || { echo "source PG private IP unknown (provision ec2-source-pg first)" >&2; exit 1; }
echo "primary: $SRC_PRIV:5432   standby image: $PG_IMAGE"

echo "waiting for SSH + cloud-init…"
ssh_ok=0
for i in $(seq 1 30); do "${SSH[@]}" true 2>/dev/null && { ssh_ok=1; break; }; sleep 10; done
[ "$ssh_ok" = 1 ] || { echo "host not reachable over SSH after ~300s" >&2; exit 1; }
"${SSH[@]}" 'sudo cloud-init status --wait' || { echo "cloud-init did not finish cleanly" >&2; exit 1; }

# Fresh base backup each deploy: stop any old standby, recreate the data volume,
# then pg_basebackup -R as the postgres user (so the data dir ownership is right
# for the server). -X stream ships WAL during the backup; -c fast = fast checkpoint.
echo "taking base backup from primary (this can take a moment)…"
"${SSH[@]}" "set -e
  sudo docker rm -f pg-standby >/dev/null 2>&1 || true
  sudo docker volume rm standby-data >/dev/null 2>&1 || true
  sudo docker volume create standby-data >/dev/null
  sudo docker run --rm --user postgres -v standby-data:/var/lib/postgresql/data $PG_IMAGE \
    pg_basebackup -h $SRC_PRIV -p 5432 -U postgres -D /var/lib/postgresql/data -R -X stream -c fast -P"

# Start the standby. listen_addresses='*' is passed explicitly (the primary set
# it on its command line, so it isn't in the copied postgresql.conf) so the
# bench can reach the standby; hot_standby defaults on for read queries.
echo "starting streaming standby (read-only, :5432)…"
"${SSH[@]}" "sudo docker run -d --name pg-standby --restart unless-stopped \
  -p 5432:5432 \
  -v standby-data:/var/lib/postgresql/data \
  $PG_IMAGE postgres -c listen_addresses='*' >/dev/null && echo started"

echo
echo "=== deployed ==="
echo "is-standby: psql -h $PUBLIC_IP -U postgres -d postgres -tAc 'SELECT pg_is_in_recovery()'   # expect t"
echo "lag (on primary): psql -h <source> -U postgres -c 'SELECT application_name,state,sync_state FROM pg_stat_replication'"
echo "bench:      walshadow-ec2-bench --dest postgres --bench single-row"
