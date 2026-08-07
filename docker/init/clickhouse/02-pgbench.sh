#!/usr/bin/env bash
# Demo-only: pre-create the four pgbench destination tables on CH so the
# walshadow emitter's pinned mappings (ch-config.demo.toml) have targets
# to INSERT into. No-op unless WALSHADOW_DEMO_PGBENCH is set. Column
# order + synthetic _lsn/_xid/_commit_ts/_is_deleted trailer mirror the
# emitter's TablePlan; engine ReplacingMergeTree(_lsn, _is_deleted) drops
# deletes on FINAL. Shapes match tests/pgbench_acceptance.rs.
#
# Like the source-side counterpart, runs both as a clickhouse image init
# hook (fresh volume only, local server) and standalone from the `ch-init`
# one-shot service over TCP on every `up` — CLICKHOUSE_HOST selects which.
# Every statement is CREATE IF NOT EXISTS, so re-running is a no-op.

set -euo pipefail

[ -n "${WALSHADOW_DEMO_PGBENCH:-}" ] || exit 0

CH_HOST="${CLICKHOUSE_HOST:-localhost}"

for _ in $(seq 60); do
    clickhouse-client --host "$CH_HOST" --query "SELECT 1" >/dev/null 2>&1 && break
    sleep 1
done

clickhouse-client --host "$CH_HOST" -n --query "
CREATE DATABASE IF NOT EXISTS demo;

CREATE TABLE IF NOT EXISTS demo.pgbench_accounts (
    aid Int32, bid Int32, abalance Int32, filler String,
    _lsn UInt64, _xid UInt32,
    _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool
) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY aid;

CREATE TABLE IF NOT EXISTS demo.pgbench_branches (
    bid Int32, bbalance Int32, filler Nullable(String),
    _lsn UInt64, _xid UInt32,
    _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool
) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY bid;

CREATE TABLE IF NOT EXISTS demo.pgbench_tellers (
    tid Int32, bid Int32, tbalance Int32, filler Nullable(String),
    _lsn UInt64, _xid UInt32,
    _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool
) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY tid;

CREATE TABLE IF NOT EXISTS demo.pgbench_history (
    tid Int32, bid Int32, aid Int32, delta Int32,
    mtime DateTime64(6), filler Nullable(String),
    _lsn UInt64, _xid UInt32,
    _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool
) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY (tid, mtime, aid);
"

echo "walshadow-demo: pgbench destination tables created on ClickHouse"
