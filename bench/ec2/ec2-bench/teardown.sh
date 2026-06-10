#!/usr/bin/env bash
# Tear down this node (terminate + delete SG). Shared flow: lib.sh:teardown_node.
set -euo pipefail
cd "$(dirname "$0")"
source ../aws-env.sh
source ../lib.sh
teardown_node
