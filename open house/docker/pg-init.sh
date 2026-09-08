#!/usr/bin/env bash
# Runs once at initdb. walshadow bootstraps its shadow by taking a physical
# base backup and then streaming WAL, both of which are replication
# connections — and initdb's default pg_hba only allows those from localhost.
set -eu
{
  echo "host replication all all trust"
  echo "host all         all all trust"
} >> "$PGDATA/pg_hba.conf"
