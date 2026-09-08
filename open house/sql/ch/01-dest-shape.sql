-- Destination shape the app requires.
--
-- In the cloud run the replication pipeline owns this table; bin/setup.sh only
-- verifies it. Locally (docker + dev-relay) this file creates it.
--
-- The ordering is the part that matters: the feature query filters a 31-second
-- event_ts window across ~1000 markets, so event_ts must lead the sort key for
-- granule pruning. id is included so the ReplacingMergeTree key stays unique
-- per row; the table is append-only, so no key column ever changes and dedup
-- on replay remains correct.

CREATE TABLE IF NOT EXISTS market_trades
(
    id           Int64,
    market_id    Int32,
    trader_id    Int32,
    taker_side   LowCardinality(String),
    price_cents  Int32,
    quantity     Int32,
    event_ts     DateTime64(6, 'UTC'),
    created_at   DateTime64(6, 'UTC'),
    scenario_tag LowCardinality(String),
    -- stamped by ClickHouse on arrival; the pipeline names its columns
    -- explicitly so it never writes this one. created_at -> _arrived_at is the
    -- per-row trip time (see sql/lateness.sql).
    _arrived_at  DateTime64(6, 'UTC') DEFAULT now64(6)
)
ENGINE = ReplacingMergeTree
ORDER BY (event_ts, market_id, id);

