CREATE TABLE IF NOT EXISTS public.markets (
    market_id  integer PRIMARY KEY,
    name       text    NOT NULL,
    slug       text    NOT NULL UNIQUE,
    active     boolean NOT NULL DEFAULT true
);

CREATE TABLE IF NOT EXISTS public.market_trades (
    id           bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    market_id    integer     NOT NULL,
    trader_id    integer     NOT NULL,
    taker_side   text        NOT NULL CHECK (taker_side IN ('BUY', 'SELL')),
    price_cents  integer     NOT NULL CHECK (price_cents BETWEEN 1 AND 99),
    quantity     integer     NOT NULL CHECK (quantity > 0),
    event_ts     timestamptz NOT NULL,
    created_at   timestamptz NOT NULL DEFAULT clock_timestamp(),
    scenario_tag text        NOT NULL DEFAULT 'live'
);

CREATE INDEX IF NOT EXISTS market_trades_market_event_idx
    ON public.market_trades (market_id, event_ts);

CREATE INDEX IF NOT EXISTS market_trades_created_at_idx
    ON public.market_trades (created_at);

CREATE TABLE IF NOT EXISTS public.shock_events (
    run_id          uuid PRIMARY KEY,
    market_id       integer     NOT NULL,
    duration_ms     integer     NOT NULL,
    accepted_at     timestamptz NOT NULL DEFAULT clock_timestamp(),
    scheduled_for   timestamptz NOT NULL,
    first_commit_at timestamptz,
    last_commit_at  timestamptz,
    rows_written    bigint      NOT NULL DEFAULT 0,
    status          text        NOT NULL DEFAULT 'armed'
);

