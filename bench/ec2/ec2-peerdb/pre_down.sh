#!/usr/bin/env bash
# stack.sh pre-destroy hook: drop the CDC replication slot + publication
# PeerDB left on the SOURCE (now inactive — this consumer box is going away)
# so the next `up peerdb` starts clean and the source stops retaining WAL.
set -euo pipefail
cd "$(dirname "$0")"
LOG_TAG=peerdb-pre-down
# shellcheck source-path=SCRIPTDIR
source ../lib.sh

drop_source_slot() {
  log "dropping PeerDB slot/publication on source ($PUBLIC_IP)"
  # shellcheck disable=SC2119
  remote_sh <<'EOS'
sudo docker exec source psql -U postgres \
  -c "SELECT pg_drop_replication_slot('peerflow_slot_demo_users') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='peerflow_slot_demo_users' AND NOT active)" \
  -c "DROP PUBLICATION IF EXISTS peerflow_pub_demo_users"
EOS
}

if [ -f ../ec2-source-pg/state.env ]; then
  with_node ../ec2-source-pg drop_source_slot \
    || log "source slot/publication cleanup skipped"
fi
