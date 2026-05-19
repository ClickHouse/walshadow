#!/usr/bin/env bash
# postgres image's POSTGRES_HOST_AUTH_METHOD only writes a `host all all`
# rule, replication needs its own line. Init scripts run before the
# server restarts into normal mode, so this takes effect on restart.

set -euo pipefail
echo "host replication all all trust" >> "$PGDATA/pg_hba.conf"
