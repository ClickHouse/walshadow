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
  append-only file per value, framed `[seq][len][lsn][body]` under a magic
  header — headerless pre-LSN files read as `lsn = 0` and upgrade on the
  next append — miss is a hard error), `clickhouse` (`ClickHouseChunkStore`,
  chunks as rows in a `pg_toast_<relid>` `ReplacingMergeTree(_lsn)` table,
  minimal `chunk_id`/`chunk_seq`/`chunk_data`/`_lsn` form,
  `ORDER BY (chunk_id, _lsn, chunk_seq)`). All `src/toast.rs`.
- **Generations.** `GetNewOidWithIndex` checks a fresh `va_valueid` only
  against the live toast index, so a dead-and-vacuumed id can be
  re-allocated: one `(toast_relid, chunk_id)` may hold several generations,
  written whole under one commit LSN each (one xact writes every chunk of a
  value; bootstrap stamps a uniform walk LSN). `fetch` returns max-LSN group
  no later than referring record, so lagging decode cannot read a future
  generation and a shorter regeneration never yields new seq 0 + stale
  suffix. Generation LSN in ClickHouse sorting key keeps older generations
  available until ack-gated GC. Defense in depth: `try_reassemble` validates
  concatenated length against the pointer's stored size, as PG's
  `toast_fetch_datum` does (`src/backend/access/common/detoast.c`) — a
  mismatch is a loud error, never a silent chimera.
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

## Chunk GC — TID death tracking

PG drops superseded chunks in the same xact as the superseding main-table op
(`heap_toast_delete`, PG `src/backend/access/heap/heaptoast.c`), but the toast
rel's replica identity is `nothing`: the delete WAL carries a TID, no
`chunk_id` — unappliable directly to the `(chunk_id, chunk_seq)` store (same
blind spot as system catalogs, [[feedback_pg_version_wal_skew]]). Correctness
never depends on GC — generations are immutable, fetch keeps the newest, the
leak is storage-only. GC's risk is the inverse: deleting a chunk a future
fetch needs.

The tracker (`src/toast_tid.rs`) keeps the TID→`chunk_id` bridge: every
observed chunk INSERT births a map entry (blkno from block ref 0, offnum from
`xl_heap_insert`; page-walk tuples carry their on-page TID), every toast
DELETE (offnum in `xl_heap_delete`) resolves against it at commit into a
`(chunk_id, death commit LSN)`. Deletes buffer through the xact spill like
chunks, so aborts discard them and the drain merge preserves WAL order —
an xact can toast a value and delete it (INSERT then UPDATE of the same
row), so a death may target a same-commit birth. Per-chunk (not per-value)
mapping lets each sibling delete resolve against its own entry —
`toast_deaths_unresolved` then signals genuine leaks, not sibling noise —
at one map entry per stored chunk (~2KB of value bytes each). Each mapping
retains its birth LSN so stale replay cannot replace or delete a newer
occupant. A birth landing on an occupied TID replaces the mapping and counts
an unresolved leak, never a death: relation rewrites can reuse numeric TIDs
while the prior value remains live.

State is an append-only journal beside the store (`[toast] tid_journal`,
defaulting into `disk_dir`; explicit for `clickhouse` mode), fsynced per
applied commit before the commit's rows dispatch — `emitter_ack` never
covers a commit whose events aren't durable, and replay from ack re-applies
idempotently (re-births and re-deaths are no-ops). Rebuilt at startup, torn
tail truncated, compacted to live map + pending deaths after GC when it
outgrows them.

The sweep (`src/toast_gc.rs`, `[toast] gc_interval_secs`, 0 = disabled
default) needs no source PG session: it applies every pending death with
`death_lsn ≤ emitter_ack` as a bounded delete — rows of that value with
`lsn ≤ death_lsn` (`DELETE … WHERE chunk_id = V AND _lsn <= L` lightweight
deletes on CH, tombstone rows rejected: RMT reclaims them only on merge and
parts holding dead-forever values may never merge; frame-filtering rewrite
on disk, serialized in-store against `put` — why the sweep borrows the
pipeline's store instance). Fetches serve replay re-decode (starts at ack)
and fresh WAL keeping a pre-window `va_valueid`; every record referencing
the dead generation precedes its death record, so `ack ≥ death_lsn` means
no fetch can want rows under the bound, and a reused-OID rebirth commits
past it and survives — the death LSN is an exact generation boundary, so
even a currently-live id's dead generations collect. Deaths whose ack
hasn't caught up stay pending; completions journal only after the store
delete succeeds, deletes are idempotent, and a pre-delete `countDistinct`
over the same predicate keeps the deleted count exact (nothing else
deletes; interleaved puts land past the bound).

Untracked classes leak, storage-only, counted `toast_deaths_unresolved`:
chunks predating the journal (greenfield-walk tuples dead before WAL
coverage — the backup walk's visibility gate never ships those), toast rels
rewritten by `VACUUM FULL`/`CLUSTER` (the new heap arrives as `log_newpage`
FPIs, no tuple-level inserts, so the map stales for that rel), and
TRUNCATE / DROP of the owning table (relfilenode swap, no per-tuple
deletes; `xl_heap_truncate` lists only logically-logged rels, never toast).

Rejected: source anti-join sweep (per relid, anti-join store valueids
against the live source toast rel under a `pg_current_wal_lsn()` horizon +
`pg_snapshot_xmin` visibility barrier). Catches every leak class including
the ones above, but requires a sidecar SQL session with `pg_toast` schema
read (superuser or `pg_read_all_data` — replication privileges don't
grant it), cannot collect a reused id's dead generations (a live id is
excluded whole, so a shorter regeneration's stale suffix never collects),
and its LSN-less disk guard reduced to an mtime-vs-wall-clock comparison
that coarse mtime granularity or clock steps can defeat into deleting a
live re-put. Worth revisiting only as an explicit repair mode for the
untracked leak classes.

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
- **Torn-fetch distinction.** `fetch` is one SELECT, its result taken as
  final, no retry. `try_reassemble`'s stored-size check turns a torn set
  into a loud error rather than distinguishing in-flight (retryable) from
  truncated. Benign while a completed `put` makes a generation's chunks
  atomically visible (single-node CH, synchronous INSERT ack, generations
  written whole under one LSN); reopen if a partial or racing `put` can
  surface a torn set.

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
