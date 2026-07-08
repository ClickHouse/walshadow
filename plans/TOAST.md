# TOAST support — pg_toast chunks stored off the WAL window

Externally-toasted column values are reconstructable in every path, including
values toasted *before* the replication window. Chunks land in a pluggable
store of record (`ToastResolver` / `ChunkStore`, `src/toast.rs`), selected by
`[toast] mode`, so reassembly does not depend on a value's chunks coinciding
with the referring tuple in WAL. In-xact WAL reassembly is the fast path — see
[xact.md](xact.md).

## Stores and reassembly

- **Stores.** `disabled` (default; NULL/default-fill on miss, counted
  `toast_values_filled_default`, never an error), `disk` (`DiskChunkStore`,
  append-only file per value, miss is a hard error), `clickhouse`
  (`ClickHouseChunkStore`, chunks as rows in a `pg_toast_<relid>`
  `ReplacingMergeTree(_lsn)` table, minimal `chunk_id`/`chunk_seq`/`chunk_data`/
  `_lsn` form, `ORDER BY (chunk_id, chunk_seq)`). All `src/toast.rs`.
- **WAL path.** Same-xact values reassemble inline from the buffered chunk map
  (`reassemble`, `src/xact_buffer.rs`), the fast path; chunks also `put`
  to the store for future re-emit. A `MissingToastChunk` miss (pre-window
  re-emit) falls back to `fetch_into` + `try_reassemble`.
- **Bootstrap.** Page walk decodes `pg_toast_*` tuples into chunks instead of
  counting-and-dropping; the drain defers any main-table tuple carrying a mapped
  `ExternalToast` (`Deferred`), `put`s all chunks durable, then resolves via
  `resolve_or_fill_toast` (`src/pipeline/bootstrap.rs`). One miss→fetch codepath
  covers bootstrap and pre-window alike (option (b), not the two-pass (a)).
- **Decode shape (R2).** Value reassembled before the main-table INSERT, stored
  inline `Bytea`/`Text`; `encode_value` (`src/ch_emitter.rs`) needs no
  toast-specific handling. Tier 3
  detoast routing: `detoasted_value` runs reassembled bytes back through
  `varlena_to_value` (`src/heap_decoder.rs`), so a detoasted jsonb/array/numeric
  resolves like an inline one (`PgPending` → oracle).
- **Compression.** `chunk_data` holds PG's compressed bytes; the reassembler
  decompresses at ingest from the pointer it already holds, via the shared
  `decompress_varlena` (`src/heap_decoder.rs`, pglz/lz4).
- **Convergence.** Toast tables are `ReplacingMergeTree(_lsn)`; chunk rows are
  immutable per `va_valueid`, so re-shipped chunks are byte-identical and `_lsn`
  dedup is purely a dedup, never a value change
  ([[project_walshadow_eventual_consistency]]).

## Chunk GC — source anti-join sweep

PG drops superseded chunks in the same xact as the superseding main-table op
(`heap_toast_delete`, PG `src/backend/access/heap/heaptoast.c`), but the toast
rel's replica identity is `nothing`: the delete WAL carries a TID, no
`chunk_id` — unappliable to the `(chunk_id, chunk_seq)` store (same blind spot
as system catalogs, [[feedback_pg_version_wal_skew]]). The decoder drops it
under `toast_chunk_deletes`; dead values accumulate. Correctness never depends
on GC — chunks are immutable per `va_valueid`, dedup keeps live values right,
the leak is storage-only. GC's risk is the inverse: deleting a chunk a future
fetch needs.

Sweep (`src/toast_gc.rs`, `[toast] gc_interval_secs`, 0 = disabled default):
per store relid, anti-join store valueids against the live source toast rel
over a sidecar SQL session. Bounds precede the scans: read
`pre = pg_current_wal_lsn()`, then snapshot `xmax` in a separate statement
(a statement's snapshot precedes its target-list eval, so a combined read
anchors `xmax` before `pre` and a xact can take xid ≥ `xmax` yet commit
≤ `pre`; ordered, any commit ≤ `pre` holds a xid below `xmax`), then wait
`pg_snapshot_xmin ≥ xmax`. PG orders commit WAL-record → flush → sync-rep
wait → `ProcArrayEndTransaction` (PG `src/backend/access/transam/xact.c`),
so walsender ships a commit — and its chunks land with `_lsn ≤ pre` — while
snapshots still miss it, unboundedly long under sync-rep wait; the barrier
makes every commit ≤ `pre` scan-visible, and later commits carry
`_lsn > pre`. Absence at a scan's statement snapshot then proves death,
never a not-yet-visible insert. Live sets stream into roaring bitmaps and
the store scan anti-joins against them inline — CH aggregates in order over
the `(chunk_id, chunk_seq)` sort key (no whole-table hash agg) with `HAVING
max(_lsn) <= pre`, disk filters during the dir walk — so a sweep holds
compressed bitmaps plus dead ids, never the stored set. `S =
pg_current_wal_lsn()` read **after** the scans gates deletion on
`emitter_ack ≥ S`. Fetches serve replay re-decode (starts at ack) and fresh
WAL keeping a pre-window `va_valueid`; a dead value's every referencing
record has LSN < `L_dead` ≤ statement snapshot ≤ `S`, so ack past `S` means
no fetch can want it. A dropped source rel rides the same argument (empty
live set, drop LSN ≤ `S`) — orphaned store tables collect fully. Ack is a
commit watermark, so an idle stream never reaches an `S` read after its last
commit; both waits are time-bounded, expiry abandons the round, and the
stateless next sweep recomputes. Deletion is idempotent.

Valueid reuse is the wrinkle: `GetNewOidWithIndex` checks uniqueness only
against the live toast index, so a dead valueid's OID can be re-allocated and
re-put mid-sweep. The `pre` horizon guards twice — `HAVING max(_lsn) <= pre`
drops re-puts at candidate scan, and the delete re-checks: lightweight
`DELETE … WHERE _lsn <= pre AND chunk_id IN (…)` on a dedicated connection
(tombstone rows rejected: `ReplacingMergeTree` reclaims them only on merge,
and parts holding dead-forever values may never merge — a second leak). A
pre-delete `countDistinct` over the same predicate keeps the deleted count
exact: nothing else deletes, interleaved puts land past `pre`. Disk frames
carry no LSN: skip when file mtime ≥ sweep start, unlink-vs-append
serialized in-store — why the sweep borrows the pipeline's store instance
rather than opening a second one. Source unreachable ⇒ sweep skips and
counts (`toast_gc_skipped_source_unreachable`), never errors.

Rejected: WAL-driven tombstones (decode toast deletes by TID, persist a
TID→chunk map beside the store) would serve archive replay with no live
source, but collects only deletes observed while the store is enabled —
pre-enable history leaks forever. The sweep catches every class.

## Scope limits

- **R1 query-time-JOIN mode.** Out of scope. Would be per-table opt-in: store
  the `ToastPointer` in the main column and reassemble via a CH JOIN on
  `chunk_id = va_valueid` instead of inline at ingest. Wins dedup + defers
  reassembly cost off ingest, costs a CH-side concat + PGLZ path (materialized
  view / UDF / client-side) and a pointer column carrying `va_extinfo` +
  `va_rawsize`. R2 inline is the default.
- **Bounded-memory streaming reassembly.** A multi-MB value is thousands of
  chunks. `fetch` streams the SELECT block-by-block (no unbounded buffered
  result read), but the reassembled value is still fully materialised in memory
  (the `BTreeMap` supplement, then `try_reassemble`'s concat) — R2-inherent,
  same as inline `reassemble`. Streaming reassembly of huge values is out of
  scope.
- **Torn-fetch distinction.** `fetch` is one SELECT, its result taken as final,
  no retry. No in-flight-vs-truncated distinction (which would compare
  `va_rawsize` to summed chunk length and retry the in-flight case). Benign
  while a completed `put` makes a value's chunks atomically visible (single-node
  CH, synchronous INSERT ack, chunks immutable per `va_valueid`); reopen if a
  partial or racing `put` can surface a torn set.

## Rejected alternatives

- **Inline reassembly only.** Correct for same-xact WAL, wrong for bootstrap
  (errors at the emitter) and pre-window values (`MissingToastChunk`). This is
  behavior with no `[toast]` store configured.
- **NULL / raw-marker fallback as the resolution.** Lossy: the WAL re-emit of
  the referring tuple does not carry the chunks (PG reuses the old
  `va_valueid`), so the value never resolves. Kept only as the explicit,
  surfaced `disabled`-mode fill, never silent loss.
- **pg_toast in the shadow PG catalog.** Would promote the catalog shadow to a
  full data replica, reintroducing the cross-seg missing-page PANIC class the
  NOOP rewrite exists to avoid ([[reference_walshadow_cross_seg_records]]) and
  coupling every detoast to a replay-LSN wait + the catalog mutex. The disk/CH
  stores are append-only, walshadow-owned, lifecycle-independent.
