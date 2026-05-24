# sequence_state — reconstruct source `last_value` for CH consumers

Gap #5 in the overview. Not blocking; no consumer asks today

## Lead

Tables with `serial` / `bigserial` PKs replicate correctly. Assigned
`int4` / `int8` column rides heap WAL just like any other user-column
write, so `_lsn`-deduped CH rows match source row-for-row

What's missing is `pg_class.relkind = 'S'` *sequence* state itself.
`nextval()` allocates blocks of 32 values from `pg_sequence` and
journals advances through `RM_SEQ_ID` records. Filter drops
`RM_SEQ_ID` ([`src/filter.rs`](../src/filter.rs) keep-set excludes
`RM_SEQ_ID`); shadow PG never sees sequence advances, decoder never
emits to CH. Downstream readers cannot ask "what was source's
`last_value` for `public.users_id_seq` at LSN X"

## Why this is graceful today

* No consumer needs it. CDC pipelines key on table rows, not sequence
  cursors
* Source sequences advance independently of replicated state; a CH
  consumer that needs "next id" would query source, not walshadow
* Sequence WAL volume is dwarfed by heap WAL even on insert-heavy
  workloads; ignoring it cleanly is cheap

## Sketch — CH-side synthetic `_sequence_value`

Option A: per-relation virtual column

Emit one row per `RM_SEQ_ID` advance to a CH table per sequence:

```
CREATE TABLE _walshadow_sequence_state (
  schema     LowCardinality(String),
  name       LowCardinality(String),
  last_value Int64,
  is_called  UInt8,
  _lsn       UInt64,
  _commit_ts DateTime64(6)
) ENGINE = ReplacingMergeTree(_lsn)
ORDER BY (schema, name)
```

Decoder path:

* Filter passes `RM_SEQ_ID` through to decoder (catalog-keep already
  covers `pg_sequence`'s heap)
* New decoder module mirrors `heap_decoder` but for the seq xlog
  record. `xl_seq_rec` carries `(node, t_data)` where `t_data` is
  a HeapTupleHeader-shaped image of `pg_sequence`'s tuple
* Resolve `(dbNode, relNode)` to sequence name via shadow's
  `pg_class` (already cached by `ShadowCatalog`)
* Emit `(schema, name, last_value, is_called)` against the
  synthetic CH table

Option B: pass-through via existing relation-routed emitter

Treat `pg_sequence` like any other heap. Drops the synthetic-table
machinery, but couples CH schema to PG's internal sequence
representation. Backslide on type-stability across PG majors

Option A is the cleaner shape. Sequence advances are coarse (one WAL
record per 32-value cache fill), volume low. Synthetic column scheme
matches the `_lsn` / `_xid` / `_op` / `_commit_ts` pattern already in
[`src/type_bridge.rs`](../../src/type_bridge.rs)

## Why deferred

* No downstream consumer surfaced demand
* Workaround is trivial: consumer queries source directly for
  sequence cursor when needed
* `RESTART_SEQS` flag on TRUNCATE ignored for the same reason —
  surfaces under the same lift when this lands

Reconsider when the first consumer asks. Estimated lift ~300 LOC
walshadow + one CH table per sequence
