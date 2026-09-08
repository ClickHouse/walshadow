-- Sample rows, same trades on both sides. Run the two blocks side by side to
-- show a row landing in Postgres and the identical row in ClickHouse.
--
--   psql "$PG_DSN" -f sql/samples.sql
--   clickhouse-client --database openhouse --queries-file sql/samples-ch.sql
--
-- The pair to point at is created_at (stamped by Postgres on insert) and
-- _arrived_at (stamped by ClickHouse on arrival). The gap between them is the
-- trip time for that individual row.

SELECT id, market_id, taker_side, price_cents, quantity,
       to_char(event_ts   AT TIME ZONE 'UTC', 'HH24:MI:SS.MS') AS event_ts,
       to_char(created_at AT TIME ZONE 'UTC', 'HH24:MI:SS.MS') AS created_at,
       scenario_tag
FROM public.market_trades
WHERE scenario_tag <> 'probe'
ORDER BY id DESC
LIMIT 5;
