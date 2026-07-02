-- walshadow runtime config overlay: source-PG-side install script.
--
-- Run by operator. walshadow NEVER writes these tables
-- (single-writer = source PG, single-reader = daemon); it reads them at boot
-- via SELECT and tracks live changes off the WAL stream.
--
-- Schema must match TOML `[runtime_config] schema`. Override default with:
-- psql -v walshadow_schema=myschema -f runtime_config_install.sql
--
-- Columns are scoped to the knobs walshadow resolves today; schema grows
-- additively (a NULL column means "daemon default applies"), so a newer daemon
-- reading an older install still works.

\if :{?walshadow_schema}
\else
  \set walshadow_schema walshadow
\endif

CREATE SCHEMA IF NOT EXISTS :"walshadow_schema";

-- Global emitter knobs. Single row (id = 1). NULL = daemon default / TOML.
CREATE TABLE IF NOT EXISTS :"walshadow_schema".config_global (
    id                  smallint PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    row_budget          bigint,
    byte_budget         bigint,
    flush_timeout_ms    bigint,
    compression         text,        -- none / lz4 / zstd
    retry_max_attempts  integer,
    drop_table_strategy text         -- retain / drop / warn
);

-- Per-namespace defaults, keyed on source PG schema name.
CREATE TABLE IF NOT EXISTS :"walshadow_schema".config_namespace (
    namespace           text PRIMARY KEY,
    target_database     text,
    auto_create         boolean,
    drop_table_strategy text         -- overrides config_global for this namespace
);

-- Per-relation destination mapping, keyed on qualified name (text, not
-- relfilenode: rfn is unknown at row-insert time for forward-declared tables).
CREATE TABLE IF NOT EXISTS :"walshadow_schema".config_table (
    namespace    text NOT NULL,
    relname      text NOT NULL,
    target       text,               -- "<database>.<table>" on ClickHouse
    replicate    boolean,            -- inclusion switch: true opt-in, false
                                     -- opt-out, NULL leaves scope unchanged
                                     -- (target override only, as before)
    initial_load text,               -- one-time backfill mode for pre-opt-in
                                     -- rows: 'copy' | 'base_backup' |
                                     -- 'object_store'; NULL streams from the
                                     -- opt-in LSN with no backfill
    PRIMARY KEY (namespace, relname)
);

-- Per-column type override, keyed on (relation, source column name).
CREATE TABLE IF NOT EXISTS :"walshadow_schema".config_column (
    namespace   text NOT NULL,
    relname     text NOT NULL,
    attname     text NOT NULL,
    target_type text,                -- ClickHouse type expression
    PRIMARY KEY (namespace, relname, attname)
);

-- Additive upgrade: columns introduced after the initial config_table shape.
-- CREATE TABLE IF NOT EXISTS above no-ops on an existing install, so an
-- upgrading deployment re-runs this to gain the columns the newer daemon reads.
ALTER TABLE :"walshadow_schema".config_table ADD COLUMN IF NOT EXISTS replicate    boolean;
ALTER TABLE :"walshadow_schema".config_table ADD COLUMN IF NOT EXISTS initial_load text;

-- REPLICA IDENTITY FULL logs the complete old-row image on UPDATE/DELETE, so a
-- DELETE always carries the key columns the decoder reads (namespace/relname/
-- attname), independent of each table's primary-key shape, and with no
-- dependency on prior in-daemon state. At walshadow's wal_level=logical floor PG
-- logs the new tuple whole (prefix/suffix compression is off for logically-
-- logged relations, heapam.c heap_update), so INSERT/UPDATE already carry every
-- column; FULL governs only the old image. Config-table write volume is
-- operator-scale and rows tiny, so the extra WAL is negligible.
ALTER TABLE :"walshadow_schema".config_global    REPLICA IDENTITY FULL;
ALTER TABLE :"walshadow_schema".config_namespace REPLICA IDENTITY FULL;
ALTER TABLE :"walshadow_schema".config_table     REPLICA IDENTITY FULL;
ALTER TABLE :"walshadow_schema".config_column    REPLICA IDENTITY FULL;
