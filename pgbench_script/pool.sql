-- Payload pool, built once. 1888 bytes with EXTERNAL storage: out-of-line and
-- uncompressed, so bytes / 1996 gives chunk count directly. md5 hex halves
-- under pglz, so a compressible payload makes the chunk rate unpredictable.
DROP TABLE IF EXISTS tc_payload_pool CASCADE;
CREATE TABLE tc_payload_pool (id int PRIMARY KEY, body text NOT NULL);
INSERT INTO tc_payload_pool (id, body)
SELECT g, (SELECT string_agg(md5(random()::text), '') FROM generate_series(1, 59))
FROM generate_series(0, 63) g;
ALTER TABLE tc_payload_pool REPLICA IDENTITY FULL;
ALTER TABLE tc_payload_pool ALTER COLUMN body SET STORAGE EXTERNAL;
SELECT pg_size_pretty(avg(pg_column_size(body))::bigint) AS avg_payload,
       count(*) AS pool_rows FROM tc_payload_pool;
