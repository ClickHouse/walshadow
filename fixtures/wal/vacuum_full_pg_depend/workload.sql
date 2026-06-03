-- VACUUM FULL pg_depend (non-mapped catalog) fixture.
--
-- Goal: produce a WAL segment containing pg_class UPDATE records that
-- PG prefix-compresses past the OID column. Decoder must signal
-- `pg_class_writes_oid_in_prefix` and must NOT tick
-- `pg_class_writes_undecoded`.
--
-- Layout follows fixtures/wal/filter/capture.sh: phase A builds enough
-- catalog state then pg_switch_wal's into segment 2; phase B runs the
-- target operation. Capturing segment 2 isolates the VACUUM FULL WAL
-- from bootstrap noise.

-- Phase A: pad pg_depend with enough rows that a VACUUM FULL actually
-- has WAL to emit. A fresh initdb pg_depend already has ~7k rows; we
-- just need rotation, not bulk, so a single CREATE TABLE round is
-- enough to give the rewrite some pg_depend churn.
CREATE TABLE IF NOT EXISTS t1 (id int primary key, payload text);
CREATE TABLE IF NOT EXISTS t2 (id int primary key, payload text);
CREATE INDEX IF NOT EXISTS t1_payload_idx ON t1 (payload);

CHECKPOINT;
SELECT pg_switch_wal();
CHECKPOINT;

-- Phase B: lands in segment 2. VACUUM FULL on a non-mapped catalog
-- updates pg_class.relfilenode for pg_depend (and pg_depend's indexes).
-- pg_class cols 1..7 stay byte-identical so PG sets
-- XLH_UPDATE_PREFIX_FROM_OLD with prefixlen ≈ 88.
VACUUM FULL pg_depend;
VACUUM FULL pg_namespace;
VACUUM FULL pg_constraint;

CHECKPOINT;
