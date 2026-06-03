#!/usr/bin/env bash
# Demo-only: pre-create the four pgbench destination tables on CH so the
# walshadow emitter's pinned mappings (ch-config.demo.toml) have targets
# to INSERT into. No-op unless WALSHADOW_DEMO_PGBENCH is set. Column
# order + synthetic _lsn/_xid/_op/_commit_ts trailer mirror the emitter's
# TablePlan; engine is ReplacingMergeTree(_lsn) so a row's newest LSN
# wins on FINAL. Shapes match tests/pgbench_acceptance.rs.

set -euo pipefail

[ -n "${WALSHADOW_DEMO_PGBENCH:-}" ] || exit 0

clickhouse-client -n --query "
CREATE DATABASE IF NOT EXISTS demo;

CREATE TABLE IF NOT EXISTS demo.pgbench_accounts (
    aid Int32, bid Int32, abalance Int32, filler String,
    _lsn UInt64, _xid UInt32,
    _op Enum8('insert' = 1, 'update' = 2, 'delete' = 3),
    _commit_ts DateTime64(6, 'UTC')
) ENGINE = ReplacingMergeTree(_lsn) ORDER BY aid;

CREATE TABLE IF NOT EXISTS demo.pgbench_branches (
    bid Int32, bbalance Int32, filler Nullable(String),
    _lsn UInt64, _xid UInt32,
    _op Enum8('insert' = 1, 'update' = 2, 'delete' = 3),
    _commit_ts DateTime64(6, 'UTC')
) ENGINE = ReplacingMergeTree(_lsn) ORDER BY bid;

CREATE TABLE IF NOT EXISTS demo.pgbench_tellers (
    tid Int32, bid Int32, tbalance Int32, filler Nullable(String),
    _lsn UInt64, _xid UInt32,
    _op Enum8('insert' = 1, 'update' = 2, 'delete' = 3),
    _commit_ts DateTime64(6, 'UTC')
) ENGINE = ReplacingMergeTree(_lsn) ORDER BY tid;

CREATE TABLE IF NOT EXISTS demo.pgbench_history (
    tid Int32, bid Int32, aid Int32, delta Int32,
    mtime DateTime64(6), filler Nullable(String),
    _lsn UInt64, _xid UInt32,
    _op Enum8('insert' = 1, 'update' = 2, 'delete' = 3),
    _commit_ts DateTime64(6, 'UTC')
) ENGINE = ReplacingMergeTree(_lsn) ORDER BY (tid, mtime, aid);
"

echo "walshadow-demo: pgbench destination tables created on ClickHouse"
