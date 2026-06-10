#!/usr/bin/env bash
# Tear down this node, first pulling any on-CPU profiles off the box.
set -euo pipefail
cd "$(dirname "$0")"
source ../aws-env.sh
source ../lib.sh
node_pre_teardown() { copy_remote_profiles; }
teardown_node
