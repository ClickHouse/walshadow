-- walshadow demo destination table. Synthetic columns (_lsn, _xid,
-- _commit_ts, _is_deleted) match the emitter's TablePlan; their types
-- are fixed by walshadow::ch_emitter::TablePlan::build.

CREATE DATABASE IF NOT EXISTS demo;

CREATE TABLE IF NOT EXISTS demo.users (
    id          UInt64,
    name        String,
    email       String,
    _lsn        UInt64,
    _xid        UInt32,
    _commit_ts  DateTime64(6, 'UTC'),
    _is_deleted Bool
)
ENGINE = ReplacingMergeTree(_lsn, _is_deleted)
ORDER BY id;
