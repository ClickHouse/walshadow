# 03 — `TRUNCATE` propagation

Closes [PLAN.md §"Known correctness gaps" #3](../PLAN.md#known-correctness-gaps).
[`main_data.rs:15-19`](../../src/main_data.rs) recognises
`XLOG_HEAP_TRUNCATE` for filter-keep, but the decoder doesn't route
it onward. Source `TRUNCATE t` leaves CH's `t` populated with stale
rows. CH's `ReplacingMergeTree` dedup can't model "row no longer
exists" without a delete marker

## Why

`TRUNCATE` is unique among heap WAL records: it carries no tuple
payload, just the list of relids affected (`xl_heap_truncate`'s
`relids[]` is an array of pg_class OIDs, not relfilenodes — the
relmap path resolves OIDs at replay time on the standby). The
decoder needs to walk that OID array, resolve each through the
catalog cache, and emit a delete-all event per relation

## Surface

New `HeapOp::Truncate` variant on
[`HeapOp`](../../src/heap_decoder.rs). Carries no tuple payload —
`new = None`, `old = None`. The xid + commit_lsn carrier is enough
for the emitter to issue the CH-side truncation

Decoder: when `record.header.resource_manager_id == RmId::Heap` and
`info_op == XLOG_HEAP_TRUNCATE` (`0x30`), walk `main_data` per PG's
`xl_heap_truncate`:

```c
typedef struct xl_heap_truncate {
    Oid     dbId;
    uint32  nrelids;
    uint8   flags;          // XLH_TRUNCATE_CASCADE / RESTART_SEQS
    Oid     relids[FLEXIBLE_ARRAY_MEMBER];
} xl_heap_truncate;
```

Emit one `DecodedHeap { op: Truncate, rfn: <rfn for relid>, ... }`
per relid. Descriptor lookup goes through
[`ShadowCatalog::relation_at`](../../src/shadow_catalog.rs) by OID
(new accessor `relation_at_oid(oid, db_id, lsn)` — symmetric to the
existing `relation_at(rfn, lsn)` but keyed on pg_class OID instead
of relfilenode, since the WAL record carries OIDs)

[`ch_emitter::route`](../../src/ch_emitter.rs) gains a `Truncate`
branch. Issues `TRUNCATE TABLE <dest>` to CH at xact-end drain time
(must execute after any preceding writes in the same xact drain
batch — CH's `TRUNCATE` is synchronous). The truncate is buffered
in the xact buffer alongside heap writes; drain order is unchanged

The CASCADE flag is informational on the source side — PG resolves
cascade at the SQL layer and emits one `xl_heap_truncate` per
affected relid. Walshadow processes the OIDs as given; nothing
extra to do for cascade

## Tests

Unit ([`main_data.rs`](../../src/main_data.rs) test module):
- `xl_heap_truncate` main_data parse — extract dbId, nrelids, flags,
  relids
- `decode_heap_record` emits N tuples for N relids

Integration (`tests/phase14_truncate.rs`):
- INSERT 100 rows
- `TRUNCATE t`
- INSERT 50 more rows
- Drain, then CH `SELECT count(*) FROM t FINAL` == 50, post-truncate
  row IDs match the source's post-truncate set

## Size

~180 LOC product + ~150 LOC test

## Risks

- **TRUNCATE on multiple relations in one xact** (cascade or
  explicit `TRUNCATE a, b, c`). PG emits one `xl_heap_truncate` per
  relid; xact buffer commits them as one atomic batch. Verify
  the emitter's per-relation drain order doesn't matter for CH-side
  `TRUNCATE TABLE` (it's per-table, no interaction)
- **TRUNCATE before any WAL-driven INSERT.** The relation's
  descriptor may not be cached when the TRUNCATE arrives. The
  shadow-catalog gate (`wait_for_replay(at_lsn)`) ensures the
  descriptor is fetchable at the TRUNCATE's commit LSN — no
  special-case needed
- **Per-table strategy knob.** v1 emits a single `TRUNCATE TABLE
  <dest>` per relation; no operator escape hatch (e.g. "retain CH
  rows despite source TRUNCATE"). If a downstream consumer asks
  for it, add a per-table `[table.<name>] truncate_strategy =
  "passthrough" | "ignore"` in a v1.1 commit. Defer until measured
- **`RESTART_SEQS` flag.** Sequence state is out-of-scope per
  [PLAN.md gaps #5](../PLAN.md#known-correctness-gaps); walshadow
  ignores the flag. Document as an open item for the sequence-state
  follow-up
