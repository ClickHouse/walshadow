#!/usr/bin/env bash
# Provision this node. Config lives in ./node.env; the shared launch flow is
# lib.sh:provision_node (key pair, SG + ingress, run-instances, write state.env).
set -euo pipefail
cd "$(dirname "$0")"
source ../aws-env.sh
source ../lib.sh
source ./node.env
provision_node
