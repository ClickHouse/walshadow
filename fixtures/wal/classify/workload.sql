-- Mixed DDL + DML workload for the classifier fixture.
-- Goal: produce WAL with a representative spread across rmgrs & both
-- catalog and user relfilenodes. Reproducible: deterministic SQL, no
-- random clocks beyond the LSN/CRC churn already in WAL.

CREATE TABLE accounts (
    aid     bigint PRIMARY KEY,
    bid     int,
    balance bigint NOT NULL DEFAULT 0,
    filler  text
);
CREATE INDEX accounts_bid_idx ON accounts (bid);

CREATE TABLE branches (
    bid     int PRIMARY KEY,
    bbalance bigint NOT NULL DEFAULT 0
);

INSERT INTO branches SELECT g, 0 FROM generate_series(1, 10) g;
INSERT INTO accounts
SELECT g, ((g - 1) % 10) + 1, 0, repeat('x', 16)
FROM generate_series(1, 500) g;

-- A round of DDL touches several catalog tables: pg_class, pg_attribute,
-- pg_attrdef, pg_constraint, pg_index, pg_depend, pg_type.
ALTER TABLE accounts ADD COLUMN tag text;
ALTER TABLE accounts ADD CONSTRAINT accounts_balance_nonneg CHECK (balance >= 0);
ALTER TABLE accounts ALTER COLUMN filler SET DEFAULT 'pad';

-- A small DML batch after DDL to exercise heap WAL on the new shape.
UPDATE accounts SET tag = 'a' WHERE aid % 7 = 0;
UPDATE accounts SET balance = balance + 1 WHERE aid <= 100;
DELETE FROM accounts WHERE aid > 450;

-- Every index access method and a sequence, with a subtransaction whose
-- records carry their toplevel xid
CREATE TABLE shapes (id serial PRIMARY KEY, p point, tags int[]);
CREATE INDEX ON shapes USING hash (id);
CREATE INDEX ON shapes USING gin (tags);
CREATE INDEX ON shapes USING gist (p);
CREATE INDEX ON shapes USING spgist (p);
CREATE INDEX ON shapes USING brin (id);
BEGIN;
INSERT INTO shapes (p, tags)
SELECT point(g, g), ARRAY[g, g + 1] FROM generate_series(1, 200) g;
SAVEPOINT s;
INSERT INTO shapes (p, tags)
SELECT point(g, -g), ARRAY[g] FROM generate_series(1, 200) g;
RELEASE s;
COMMIT;

-- Force a checkpoint so the WAL segment we capture has the full record
-- mix landed (avoids waiting for the bgwriter on shutdown).
CHECKPOINT;
