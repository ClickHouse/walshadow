-- Fallback when the destination table already exists with a different sort key
-- and cannot be rekeyed. Adding the projection is cheap; materialising it over
-- existing history is not, so run this well before a rehearsal.

ALTER TABLE market_trades
    ADD PROJECTION IF NOT EXISTS by_event_ts
    (SELECT * ORDER BY (event_ts, market_id, id));

ALTER TABLE market_trades MATERIALIZE PROJECTION by_event_ts;
