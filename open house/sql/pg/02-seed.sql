TRUNCATE public.markets;
TRUNCATE public.market_trades;
TRUNCATE public.shock_events;

INSERT INTO public.markets (market_id, name, slug, active)
SELECT
    101 + g,
    (ARRAY[
        'Will the S&P 500 close green?',
        'Will the Fed cut rates this meeting?',
        'Will CPI print above forecast?',
        'Will BTC close above 100k?',
        'Will the jobs report beat consensus?',
        'Will oil close above $80?',
        'Will the 10Y yield fall this week?',
        'Will gold set a new high?',
        'Will the Nasdaq outperform the Dow?',
        'Will the dollar index close lower?'
    ])[1 + (g % 10)]
      || CASE WHEN g < 10 THEN '' ELSE ' (' || to_char(current_date + (g / 10), 'Mon DD') || ')' END,
    'mkt-' || (101 + g),
    true
FROM generate_series(0, 999) AS g;

-- seeded history stays strictly older than the 31s live feature window so it
-- can never contaminate a detector baseline
INSERT INTO public.market_trades
    (market_id, trader_id, taker_side, price_cents, quantity, event_ts, created_at, scenario_tag)
SELECT
    101 + (i % 1000),
    1 + (i % 5000),
    CASE WHEN random() < 0.5 THEN 'BUY' ELSE 'SELL' END,
    GREATEST(1, LEAST(99, 42 + (i % 17) - 8)),
    1 + (i % 25),
    ts,
    ts,
    'seed'
FROM generate_series(1, :history_rows) AS i,
     LATERAL (SELECT now() - make_interval(secs => 120 + 3480 * random())) AS s(ts);

ANALYZE public.markets;
ANALYZE public.market_trades;
