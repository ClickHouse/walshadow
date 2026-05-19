-- walshadow demo destination table. Synthetic columns (_lsn, _xid,
-- _op, _commit_ts) match the emitter's TablePlan; their types are
-- fixed by walshadow::ch_emitter::TablePlan::build.

CREATE DATABASE IF NOT EXISTS demo;

CREATE TABLE IF NOT EXISTS demo.users (
    id          UInt64,
    name        String,
    email       String,
    _lsn        UInt64,
    _xid        UInt32,
    _op         Enum8('insert' = 1, 'update' = 2, 'delete' = 3),
    _commit_ts  DateTime64(6, 'UTC')
)
ENGINE = ReplacingMergeTree(_lsn)
ORDER BY id;
