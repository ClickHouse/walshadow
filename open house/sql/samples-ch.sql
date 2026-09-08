-- The ClickHouse side of sql/samples.sql — the same trades, plus what
-- replication added: _arrived_at (stamped here on arrival) and _lsn (its
-- position in the Postgres WAL).
--
-- trip_ms = created_at (stamped by Postgres on insert) -> _arrived_at.
-- That is the per-row journey, for that exact row.

SELECT id,
       market_id,
       taker_side,
       price_cents,
       quantity,
       substring(toString(event_ts),    12) AS pg_event_ts,
       substring(toString(created_at),  12) AS pg_created_at,
       substring(toString(_arrived_at), 12) AS ch_arrived_at,
       dateDiff('millisecond', created_at, _arrived_at) AS trip_ms,
       _lsn
FROM market_trades FINAL
WHERE _is_deleted = 0 AND scenario_tag != 'probe'
ORDER BY id DESC
LIMIT 5
