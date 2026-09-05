#!/bin/sh
# Permit replication connections from Compose network

set -eu

echo "host replication all all scram-sha-256" >> "$PGDATA/pg_hba.conf"
