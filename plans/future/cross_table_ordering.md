# cross_table_ordering — preserve interleaved writes across tables in one xact

Gap #6 in the overview. End-state correct; mid-drain only

## Lead

Xact `T` writes:

```
LSN 100  INSERT INTO t1 ...
LSN 110  INSERT INTO t2 ...
LSN 120  UPDATE t1 ...
LSN 130  INSERT INTO t2 ...
COMMIT
```

CH today sees `(t1@100, t1@120)` then `(t2@110, t2@130)`. Per-table
encoder batches inside `BufferingDecoderSink`'s xact buffer collapse
the interleave: `XactBuffer::commit` walks tables in iteration
order, drains each table's full per-xact batch via the emitter,
then moves to the next

## Today

* End-state consistent. `_lsn UInt64` synthetic column +
  `ReplacingMergeTree(_lsn)` dedup means any reader doing a full
  scan post-commit sees source's final state. Walshadow's WAL
  semantics promise end-state, not real-time order
* Mid-drain readers see partial state. A consumer running
  `SELECT * FROM t1 JOIN t2` against CH while xact `T` is
  mid-drain may see `t1@120` rows but no `t2@110` rows — a state
  source PG never held
* `_xid UInt32` carries the source xact id, so a consumer building
  per-xact snapshots can defer reading any tables touching xact `T`
  until all expected tables show that xid. Workaround, not fix

## Sketch — k-way merge per-xact across tables

Subxact rollback already k-way-merges per-subxid buffers in
`source_lsn` ASC for the commit path. Same shape, lifted from "across subxids" to "across tables":

* `XactBuffer` holds `tables: BTreeMap<RelOid, TableBuffer>` already
  (per-xact, per-relation row batches)
* `commit` today calls `emitter.drain_xact()` which iterates the map
  per-table. Replace with one ordered merge across all tables'
  `(source_lsn, row)` sequences
* Emitter routes each merged row to its destination table encoder
  in source-LSN order; per-table encoders flush more frequently
  (one row per encoder call worst case)

Cost: emitter loses the "one batched INSERT per (xact, table)" win.
CH INSERT throughput drops as per-row routing replaces per-batch
routing. Mitigation: micro-batch within the merge by grouping
consecutive rows hitting the same destination, flush on table
switch. Typical workload has runs of same-table writes, so the
micro-batch coalesces back toward the current shape

## Why deferred

* CH semantics question, not walshadow correctness. Mid-drain
  readers querying a CDC sink during xact replay are racing the
  replication pipeline; the same race exists against logical
  decoders, dual-writes, change-data-capture more broadly
* End-state agreement holds via `_lsn` dedup. The ordering invariant
  walshadow promises today
* Per-row routing cost is real; consumer that demands strict
  ordering should also be the one quantifying the throughput trade
* No consumer surfaced demand. Workaround (defer-read by `_xid`)
  exists for any consumer that needs per-xact snapshots

Reconsider when first consumer specifies a per-xact-snapshot SLA
that the deferred-read workaround can't satisfy
