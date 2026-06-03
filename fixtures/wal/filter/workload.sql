-- Steady-state OLTP workload for the filter fixture.
-- Two phases separated by `pg_switch_wal()` so the captured segment
-- contains only DML traffic (bootstrap DDL ends up in segment 1; the
-- target capture is segment 2). Acceptance §1: catalog fraction well
-- under 1% on a non-DDL workload.

-- Phase A: bootstrap. Catalog-heavy. Lands in segment 1.
CREATE TABLE IF NOT EXISTS accounts (
    aid     bigint PRIMARY KEY,
    bid     int,
    balance bigint NOT NULL DEFAULT 0,
    filler  text
);
CREATE INDEX IF NOT EXISTS accounts_bid_idx ON accounts (bid);

CREATE TABLE IF NOT EXISTS branches (
    bid     int PRIMARY KEY,
    bbalance bigint NOT NULL DEFAULT 0
);

INSERT INTO branches SELECT g, 0 FROM generate_series(1, 10) g
    ON CONFLICT DO NOTHING;
INSERT INTO accounts
SELECT g, ((g - 1) % 10) + 1, 0, repeat('x', 32)
FROM generate_series(1, 1000) g
    ON CONFLICT DO NOTHING;

CHECKPOINT;
SELECT pg_switch_wal();
CHECKPOINT;

-- Phase B: steady-state OLTP. Lands in segment 2. No DDL inside.
DO $$
BEGIN
    FOR i IN 1..10000 LOOP
        UPDATE accounts
            SET balance = balance + 1, filler = repeat('y', 8 + (i % 16))
            WHERE aid = 1 + (i % 1000);
    END LOOP;
END$$;

DO $$
BEGIN
    FOR i IN 1..500 LOOP
        INSERT INTO accounts (aid, bid, balance, filler)
            VALUES (10000 + i, ((i - 1) % 10) + 1, 0, repeat('z', 24));
    END LOOP;
END$$;

DO $$
BEGIN
    FOR i IN 1..200 LOOP
        DELETE FROM accounts WHERE aid = 10000 + i;
    END LOOP;
END$$;

CHECKPOINT;
