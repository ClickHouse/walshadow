#!/usr/bin/env bash
# Permit replication connections from Compose network

set -euo pipefail

echo "host replication all all scram-sha-256" >> "$PGDATA/pg_hba.conf"
