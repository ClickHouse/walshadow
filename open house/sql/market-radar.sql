-- Stage version: the same feature calculation the app runs, capped to a few
-- readable rows and columns. The engine runs sql/collector.sql, which evaluates
-- the full market set.

WITH
    now64(6, 'UTC') AS t,
    flows AS (
        SELECT
            market_id,
            argMax(price_cents, tuple(event_ts, id)) AS last_price_cents,
            sumIf(toInt64(price_cents) * toInt64(quantity),
                  taker_side = 'BUY'  AND event_ts >= t - INTERVAL 1 SECOND) AS buy_1s,
            sumIf(toInt64(price_cents) * toInt64(quantity),
                  taker_side = 'SELL' AND event_ts >= t - INTERVAL 1 SECOND) AS sell_1s,
            sumIf(toInt64(price_cents) * toInt64(quantity),
                  taker_side = 'BUY'  AND event_ts <  t - INTERVAL 1 SECOND) / 30.0 AS normal_buy_per_s
        FROM market_trades
        WHERE scenario_tag != 'probe'
          AND event_ts >= t - INTERVAL 31 SECOND
          AND event_ts <= t
        GROUP BY market_id
    )
SELECT
    market_id                                            AS market,
    last_price_cents                                     AS price,
    round(buy_1s / greatest(toFloat64(normal_buy_per_s), 1.0), 1)   AS buy_multiple,
    round((buy_1s - sell_1s) / greatest(toFloat64(buy_1s + sell_1s), 1.0), 2) AS imbalance
FROM flows
ORDER BY buy_multiple DESC
LIMIT 5
