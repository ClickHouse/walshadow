#!/usr/bin/env bash
# Post-boot source-PG setup: install the walshadow runtime-config overlay and
# opt every existing table into replication by default.
#
# Runs once the `source` container is accepting connections (stack.sh calls it
# on every `up`, before the streamer's deploy.sh; idempotent via ON CONFLICT).
#
#   1. Pipe sql/runtime_config_install.sql into the container's psql, creating
#      walshadow.{config_global,config_namespace,config_table,config_column}
#      with REPLICA IDENTITY FULL.
#   2. Seed the overlay to "replicate all tables by default":
#        config_namespace(auto_create=true)      — future tables auto-mirror
#        config_table(replicate=true, initial_load='copy') — existing tables
#      target_database is left NULL so every namespace derives the global
#      [ch] database (which is guaranteed to exist); set REPLICA IDENTITY FULL
#      on every user table (DELETE/UPDATE carry keys).
#   3. pg_switch_wal() so a live daemon picks the rows up off the WAL segment.
#
# Pair with ec2-walshadow/deploy.sh run with RUNTIME_CONFIG_SCHEMA=walshadow.
set -euo pipefail
cd "$(dirname "$0")"
source ./state.env   # PUBLIC_IP, PEM, ...
source ../lib.sh

REPO_ROOT="$(cd ../../.. && pwd)"
INSTALL_SQL="$REPO_ROOT/sql/runtime_config_install.sql"
[ -f "$INSTALL_SQL" ] || { echo "missing $INSTALL_SQL" >&2; exit 1; }
node_ssh_setup

PSQL=(sudo docker exec -i source psql -v ON_ERROR_STOP=1 -U postgres -d postgres)

echo "waiting for the source Postgres container to accept connections..."
for i in $(seq 1 30); do
  "${SSH[@]}" 'command -v docker >/dev/null && sudo docker exec source pg_isready -U postgres >/dev/null 2>&1' 2>/dev/null && break
  sleep 10
done

echo "installing walshadow.config_* overlay from sql/runtime_config_install.sql..."
"${SSH[@]}" "${PSQL[*]}" < "$INSTALL_SQL"

echo "seeding overlay to replicate all tables by default..."
"${SSH[@]}" "${PSQL[*]}" <<'SQL'
INSERT INTO walshadow.config_namespace (namespace, target_database, auto_create)
SELECT nspname, NULL, true
  FROM pg_namespace
 WHERE nspname NOT IN ('walshadow', 'pg_catalog', 'information_schema', 'pg_toast')
   AND nspname NOT LIKE 'pg_temp%'
   AND nspname NOT LIKE 'pg_toast_temp%'
ON CONFLICT (namespace) DO UPDATE
   SET target_database = EXCLUDED.target_database,
       auto_create     = EXCLUDED.auto_create;

INSERT INTO walshadow.config_table (namespace, relname, replicate, initial_load)
SELECT n.nspname, c.relname, true, 'copy'
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
 WHERE c.relkind = 'r'
   AND n.nspname NOT IN ('walshadow', 'pg_catalog', 'information_schema', 'pg_toast')
   AND n.nspname NOT LIKE 'pg_temp%'
   AND n.nspname NOT LIKE 'pg_toast_temp%'
ON CONFLICT (namespace, relname) DO UPDATE
   SET replicate    = EXCLUDED.replicate,
       initial_load = EXCLUDED.initial_load;

DO $$
DECLARE r record;
BEGIN
  FOR r IN
    SELECT n.nspname, c.relname,
           EXISTS (SELECT 1 FROM pg_index i WHERE i.indrelid = c.oid AND i.indisprimary) AS has_pk
      FROM pg_class c
      JOIN pg_namespace n ON n.oid = c.relnamespace
     WHERE c.relkind = 'r'
       AND n.nspname NOT IN ('walshadow', 'pg_catalog', 'information_schema', 'pg_toast')
       AND n.nspname NOT LIKE 'pg_temp%'
       AND n.nspname NOT LIKE 'pg_toast_temp%'
  LOOP
    IF r.has_pk THEN
      EXECUTE format('ALTER TABLE %I.%I REPLICA IDENTITY DEFAULT', r.nspname, r.relname);
    ELSE
      EXECUTE format('ALTER TABLE %I.%I REPLICA IDENTITY FULL', r.nspname, r.relname);
    END IF;
  END LOOP;
END $$;

SELECT pg_switch_wal();
SQL

echo "✅ source overlay installed; tables opted in:"
"${SSH[@]}" "${PSQL[*]}" <<'SQL'
SELECT namespace, relname, replicate, initial_load FROM walshadow.config_table ORDER BY namespace, relname;
SQL
