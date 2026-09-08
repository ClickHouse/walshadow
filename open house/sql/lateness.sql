-- How late did source rows land in ClickHouse?
--
-- created_at is stamped by PostgreSQL (clock_timestamp()) when the row is
-- inserted. _arrived_at is stamped by ClickHouse (DEFAULT now64(6)) when the
-- row is inserted here — the replication pipeline names its columns explicitly
-- in the INSERT, so it never writes this one and the default always fires.
-- The difference is the per-row trip time.
--
-- This spans two machines' clocks; bin/setup.sh measures and reports the
-- offset. The headline replication p95 on the page is probe-based and
-- single-clock. This corroborates that number, it does not replace it.
--
-- Do NOT write this as now64() - created_at. That measures a row's age, which
-- is dominated by where it happens to fall in the window, not by lateness.

SELECT
    count()                                                            AS rows,
    round(avg(dateDiff('millisecond', created_at, _arrived_at)))       AS avg_ms,
    round(quantile(0.50)(dateDiff('millisecond', created_at, _arrived_at))) AS p50_ms,
    round(quantile(0.95)(dateDiff('millisecond', created_at, _arrived_at))) AS p95_ms,
    round(max(dateDiff('millisecond', created_at, _arrived_at)))       AS max_ms
FROM market_trades
WHERE created_at >= now64(3) - INTERVAL 5 SECOND
  AND _arrived_at > toDateTime64(0, 6)
  AND scenario_tag != 'probe'
