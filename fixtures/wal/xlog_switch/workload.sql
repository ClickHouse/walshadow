-- xlog_switch fixture: produce a WAL segment containing an XLOG_SWITCH
-- record (rmgr RM_XLOG_ID, info 0x40). pg_switch_wal() emits one each
-- time it forces a segment cut; XLOG_SWITCH must pass through the
-- filter byte-identically because shadow PG's recovery state machine
-- depends on its presence at the segment tail.
--
-- Layout mirrors fixtures/wal/filter/capture.sh: phase A pads segment 1
-- and switches into segment 2 (the first XLOG_SWITCH lands in segment 1
-- and is captured incidentally). Phase B runs a brief workload then
-- switches again; the second XLOG_SWITCH lands inside segment 2, which
-- is the one captured.

-- Phase A: bootstrap noise + first switch.
CREATE TABLE IF NOT EXISTS t (id int primary key, payload text);
INSERT INTO t SELECT g, repeat('x', 80) FROM generate_series(1, 200) g;
CHECKPOINT;
SELECT pg_switch_wal();
CHECKPOINT;

-- Phase B: lands in segment 2. The second pg_switch_wal() emits an
-- XLOG_SWITCH record at the end of segment 2's used pages; the parser
-- treats the remainder as zero-padding.
INSERT INTO t SELECT g + 1000, repeat('y', 80) FROM generate_series(1, 50) g;
SELECT pg_switch_wal();
CHECKPOINT;
