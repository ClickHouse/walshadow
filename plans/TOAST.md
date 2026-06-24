# TOAST support — pg_toast chunks stored off the WAL window

Externally-toasted column values are reconstructable in every path, including
values toasted *before* the replication window. Chunks land in a pluggable
store of record (`ToastResolver` / `ChunkStore`, `src/toast.rs`), selected by
`[toast] mode`, so reassembly no longer depends on a value's chunks coinciding
with the referring tuple in WAL. In-xact WAL reassembly is the fast path — see
[xact.md](xact.md).

## Shipped

- **Stores.** `disabled` (default; NULL/default-fill on miss, counted
  `toast_values_filled_default`, never an error), `disk` (`DiskChunkStore`,
  append-only file per value, miss is a hard error), `clickhouse`
  (`ClickHouseChunkStore`, chunks as rows in a `pg_toast_<relid>`
  `ReplacingMergeTree(_lsn)` table, minimal `chunk_id`/`chunk_seq`/`chunk_data`/
  `_lsn` form, `ORDER BY (chunk_id, chunk_seq)`). All `src/toast.rs`.
- **WAL path.** Same-xact values reassemble inline from the buffered chunk map
  (`reassemble`, `src/xact_buffer.rs`), unchanged fast path; chunks also `put`
  to the store for future re-emit. A `MissingToastChunk` miss (pre-window
  re-emit) falls back to `fetch_into` + `try_reassemble`.
- **Bootstrap.** Page walk decodes `pg_toast_*` tuples into chunks instead of
  counting-and-dropping; the drain defers any main-table tuple carrying a mapped
  `ExternalToast` (`Deferred`), `put`s all chunks durable, then resolves via
  `resolve_or_fill_toast` (`src/pipeline/bootstrap.rs`). One miss→fetch codepath
  covers bootstrap and pre-window alike (option (b), not the two-pass (a)).
- **Decode shape (R2).** Value reassembled before the main-table INSERT, stored
  inline `Bytea`/`Text`; `encode_value` (`src/ch_emitter.rs`) unchanged. Tier 3
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

## Deferred

- **R1 query-time-JOIN mode.** Per-table opt-in: store the `ToastPointer` in the
  main column and reassemble via a CH JOIN on `chunk_id = va_valueid` instead of
  inline at ingest. Wins dedup + defers reassembly cost off ingest, costs a
  CH-side concat + PGLZ path (materialized view / UDF / client-side) and a
  pointer column carrying `va_extinfo` + `va_rawsize`. Behind demand; R2 inline
  stays the default.
- **Chunk GC / vacuum reclaim.** PG drops superseded chunks when a value is
  deleted or updated to a new `va_valueid`. The shipped CH schema has no `_op`
  column and the toast relation's replica identity is `nothing` (delete WAL
  carries no key — same blind spot as system catalogs,
  [[feedback_pg_version_wal_skew]]), so a delete marker has nowhere to land.
  Dead chunk rows leak; dedup keeps the live `va_valueid`'s chunks correct.
- **Bounded-memory streaming reassembly.** A multi-MB value is thousands of
  chunks. `fetch` streams the SELECT block-by-block (no unbounded buffered
  result read), but the reassembled value is still fully materialised in memory
  (the `BTreeMap` supplement, then `try_reassemble`'s concat) — R2-inherent,
  same as inline `reassemble`. Streaming reassembly of huge values unaddressed.
- **Torn-fetch distinction.** `fetch` is one SELECT, its result taken as final,
  no retry. The planned in-flight-vs-truncated distinction (compare `va_rawsize`
  to summed chunk length, retry the in-flight case) is not implemented. Benign
  while a completed `put` makes a value's chunks atomically visible (single-node
  CH, synchronous INSERT ack, chunks immutable per `va_valueid`); reopen if a
  partial or racing `put` can surface a torn set.

## Rejected alternatives

- **Inline reassembly only.** Correct for same-xact WAL, wrong for bootstrap
  (errors at the emitter) and pre-window values (`MissingToastChunk`). The
  pre-`[toast]`-store status quo.
- **NULL / raw-marker fallback as the resolution.** Lossy: the WAL re-emit of
  the referring tuple does not carry the chunks (PG reuses the old
  `va_valueid`), so the value never resolves. Kept only as the explicit,
  surfaced `disabled`-mode fill, never silent loss.
- **pg_toast in the shadow PG catalog.** Would promote the catalog shadow to a
  full data replica, reintroducing the cross-seg missing-page PANIC class the
  NOOP rewrite exists to avoid ([[reference_walshadow_cross_seg_records]]) and
  coupling every detoast to a replay-LSN wait + the catalog mutex. The disk/CH
  stores are append-only, walshadow-owned, lifecycle-independent.
