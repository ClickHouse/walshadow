-- walshadow_oracle 0.1.
\echo Use "CREATE EXTENSION walshadow_oracle" to load this file. \quit

CREATE FUNCTION walshadow_decode_disk(typoid oid, raw bytea)
RETURNS text
AS 'MODULE_PATHNAME', 'walshadow_decode_disk'
LANGUAGE C STRICT IMMUTABLE;

COMMENT ON FUNCTION walshadow_decode_disk(oid, bytea) IS
  'Decode an on-disk Datum body via typoutput; used by walshadow''s Phase 9 oracle.';
