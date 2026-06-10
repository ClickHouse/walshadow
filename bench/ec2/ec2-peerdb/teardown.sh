#!/usr/bin/env bash
# Tear down PeerDB: copy profiles off, then drop the CDC replication slot +
# publication it left on the SOURCE (now inactive — the consumer box is going
# away) so the next `up peerdb` starts clean and the source stops retaining WAL.
set -euo pipefail
cd "$(dirname "$0")"
source ../aws-env.sh
source ../lib.sh
node_pre_teardown() {
  copy_remote_profiles
  local src_pub src_key=../ec2-source-pg/walshadow-source-pg.pem
  src_pub="$(read_state_var ../ec2-source-pg/state.env PUBLIC_IP)"
  if [ -n "$src_pub" ] && [ -f "$src_key" ]; then
    echo "dropping PeerDB slot/publication on source ($src_pub)…"
    ssh -i "$src_key" -o StrictHostKeyChecking=accept-new -o ConnectTimeout=10 ubuntu@"$src_pub" \
      "sudo docker exec source psql -U postgres \
         -c \"SELECT pg_drop_replication_slot('peerflow_slot_demo_users') WHERE EXISTS (SELECT 1 FROM pg_replication_slots WHERE slot_name='peerflow_slot_demo_users' AND NOT active)\" \
         -c \"DROP PUBLICATION IF EXISTS peerflow_pub_demo_users\"" 2>&1 \
      || echo "  (source slot/publication cleanup skipped)"
  fi
}
teardown_node
