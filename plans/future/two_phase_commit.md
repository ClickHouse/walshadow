# two-phase commit

`XLOG_XACT_PREPARE` records are ignored today. `PREPARE TRANSACTION
'gxid'` followed by `COMMIT PREPARED 'gxid'` across a daemon restart
can lose prepared writes: xact buffer keys on `xid` and
flushes on `XLOG_XACT_COMMIT`; a `PREPARE` record stashes the xact
in PG's `pg_prepared_xacts` and the COMMIT arrives much later
(potentially across restarts) as `XLOG_XACT_COMMIT_PREPARED`. The
buffer doesn't survive the gap, so the prepared writes never reach
the emitter. No code today

## Sketch

Separate buffer keyed on `gxid` (global xid string from
`pg_prepared_xacts.gid`) parallel to the existing xid-keyed buffer.
Lifecycle:

- `XLOG_XACT_PREPARE` arrives → decoder routes the buffered tuples
  for that xid into the gxid-keyed buffer, evicts the xid entry,
  emits a cursor checkpoint that records `(gxid, lsn, …)`. Shadow
  PG already replays the PREPARE into its own
  `pg_prepared_xacts` since catalog xacts are part of catalog
  replay — confirm with a fixture
- `XLOG_XACT_COMMIT_PREPARED` arrives → drop the prepare from the
  gxid buffer, flush its tuples through the normal emitter drain,
  cursor checkpoint advances past the COMMIT
- `XLOG_XACT_ROLLBACK_PREPARED` arrives → drop the gxid buffer
  entry without emitting

Shadow PG holds the catalog side of the prepared xact in
`pg_prepared_xacts`, matching source. Decoder's gxid buffer holds
the user-data side. Symmetry between the two surfaces keeps the
mental model close to PG's own (catalog xact lives in
`pg_prepared_xacts`; data xact lives in walshadow's buffer; both
keyed on gxid)

## Cursor handling

Cursor schema bump required. Today the cursor records
`(last_committed_lsn, last_drained_xid, …)`; prepared xacts surviving
restart need explicit representation:

```
prepared_xacts: Vec<{
    gxid:        String,
    prepare_lsn: Lsn,
    tuple_spill: PathBuf,  // serialized buffer contents
}>
```

`tuple_spill` because a long-lived prepared xact's tuple set can
exceed in-memory budget; spill to disk under the existing spill
area, keyed by gxid. On daemon restart, cursor restore reads
prepared_xacts list, rehydrates each gxid's buffer from spill,
resumes from `last_committed_lsn`. The COMMIT PREPARED arriving
post-restart drains the rehydrated buffer normally

Spill path is the operationally heavy piece. Prepared xacts can sit
in `pg_prepared_xacts` for hours or days (XA workloads, ops
maintenance windows); the spill must survive at least as long as
PG's own prepared-xact retention or the daemon will drop writes the
operator expected to land

## Why deferred

Major work. Touches:

- `decoder` — new record dispatch for PREPARE / COMMIT PREPARED /
  ROLLBACK PREPARED, gxid-keyed buffer routing
- `xact_buffer` — second buffer keyed on gxid, eviction protocol
  between the xid-keyed buffer and the gxid-keyed one
- `cursor` — schema bump for prepared_xacts list, spill-aware
  restore path
- `emitter` — no shape change but flush sequencing against COMMIT
  PREPARED ordering needs an integration test matrix
- Spill — extends spill area with a new keying scheme
- Tests — restart-across-PREPARE fixtures, ROLLBACK PREPARED
  fixtures, long-lived prepared xact stress

Warrants standalone scope. Bundling risks underestimating the cursor
+ spill work; wire-format dispatch is the small piece, durability +
restart story is the larger one. Defer until either (a) a deployment
depends on XA / 2PC workloads,
or (b) production telemetry shows non-zero `XLOG_XACT_PREPARE`
record counts that walshadow is silently dropping

Until then, walshadow's behaviour on PREPARE is "filter passes the
record through (since the catalog rfns it touches are catalog
classification), shadow replays catalog state correctly, decoder
ignores it, COMMIT PREPARED arrives later and the user-data side of
the xact never materialises in CH". Detect via a metric:
`walshadow_xlog_xact_prepare_records_total`. Operator alerts on
non-zero, knows to escalate the work

## Dependencies

- **Xact buffer extension.** Second buffer keyed on gxid; eviction
  protocol moving entries from the xid-keyed buffer at PREPARE time;
  spill integration for long-lived entries. Touches the hot path so
  the buffer abstraction needs to admit two keying schemes without
  per-tuple branch overhead. Probably a generic
  `XactBuffer<K>` with `K = Xid` for the live path and `K = Gxid`
  for the prepared path
- **Cursor schema bump.** `prepared_xacts: Vec<{gxid, prepare_lsn,
  tuple_spill}>` added to cursor record. Existing cursors must
  upgrade cleanly (missing field defaults to empty list, no
  prepared xacts in flight). Forward-compat for cursor consumers
  (cursor restore path) — confirm cursor version is bumped and old
  daemons refuse new cursors with a clear error
- **Spill area extension.** Spill keyed by xid extends to
  also key by gxid. Per-gxid spill files survive daemon restart;
  cleaned up only on COMMIT PREPARED / ROLLBACK PREPARED or on
  explicit operator command (`walshadow-stream prepared list /
  drop`). Operator surface for stranded prepared xacts (PG's own
  `ROLLBACK PREPARED 'gxid'` doesn't reach walshadow if shadow has
  already dropped the catalog entry; need a manual escape hatch)

## Open question

Prepared xacts whose source PG entry vanishes (e.g. operator runs
`ROLLBACK PREPARED 'gxid'` on a different replica during failover,
the rollback record never reaches walshadow). Walshadow's gxid
buffer would hold tuples for a xact that PG has already discarded.
Cleanup via timeout (max prepared-xact age) plus operator command
(`walshadow-stream prepared drop <gxid>`). Don't auto-drop on
arbitrary timeout — XA workloads legitimately hold prepares for
hours; default timeout would need to be configurable (default: 0 =
never auto-drop, operator drives cleanup)

## Acceptance

- Source-side `BEGIN; INSERT INTO public.t VALUES (...); PREPARE
  TRANSACTION 'tx1'; COMMIT PREPARED 'tx1';` results in the row
  reaching CH. Confirm via integration test
- Same sequence with a daemon restart between PREPARE and COMMIT
  PREPARED still results in the row reaching CH (cursor + spill
  restore path)
- `ROLLBACK PREPARED 'tx2'` after PREPARE → row never reaches CH,
  buffer entry cleaned up, no leaked spill files
- `walshadow_xlog_xact_prepare_records_total` metric ticks per
  PREPARE; `walshadow_xact_commit_prepared_total` /
  `walshadow_xact_rollback_prepared_total` tick on resolution.
  Operator can monitor unresolved prepared xact count
- Cursor schema upgrade path tested: pre-2PC cursor restores cleanly
  into post-2PC daemon (empty prepared_xacts list); post-2PC cursor
  refused by pre-2PC daemon with clear error
