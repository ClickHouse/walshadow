# Full TOAST support, pg_toast chunks stored on ClickHouse

Make externally-toasted column values reconstructable in every path,
including values toasted *before* the replication window. Central choice:
replicate `pg_toast_<relid>` relations to ClickHouse as their own
chunk-storage tables, mirroring PG's on-disk model, so a toasted value can
be rebuilt from CH-resident chunks instead of requiring the chunk records
to coincide in WAL with the referring tuple.

## Problem statement

Detoast today works only for the WAL path and only within a single xact.

* `decode_varlena` (`src/heap_decoder.rs`, `first == 0x01`, `tag == 18`,
  VARTAG_ONDISK) turns an on-disk pointer into
  `ColumnValue::ExternalToast(ToastPointer { va_rawsize, va_extinfo,
  va_valueid, va_toastrelid })`. It carries metadata only, no bytes.
* Reassembly is the xact buffer's job. The `XactRecordSink` heap path
  (`src/xact_buffer.rs`) recognises a `pg_toast` heap record by
  `rel.kind == 't'`, repacks its `(chunk_id oid, chunk_seq int4,
  chunk_data bytea)` INSERT into a `ToastChunk` via
  `toast_chunk_from_decoded`, and buffers it under the *same xid* as every
  other entry. At COMMIT, `commit` / `drain_committed` fold chunks into
  `chunks: HashMap<(u32,u32), BTreeMap<u32, Vec<u8>>>` keyed by
  `(toast_relid, value_id)` then `chunk_seq` (see `accumulate`), and
  `detoast_heap` → `detoast_tuple` → `reassemble` rebuilds each
  `ExternalToast` into `Bytea` / `Text`.
* This only works because PG's `toast_save_datum`
  (`src/backend/access/common/toast_internals.c`) writes the
  chunk INSERTs in the *same* xact as the referring tuple, so every chunk a
  value needs is already buffered when the heap drains. The module header
  in `xact_buffer.rs` states this explicitly ("Why bundle TOAST chunks into
  the same buffer as heap tuples").

Two gaps fall out:

* **Bootstrap has no chunk map.** Page walk classifies `pg_toast_*` files
  via `CatalogMap::is_toast` / `toast_filenodes` (`src/backup_page_walk.rs`)
  but `flush_full_pages` does `if is_toast { count; continue }` — pages are
  counted, never decoded into chunks. So a page-walked main-table tuple with
  an externally-stored column produces a bare `ExternalToast`, the bootstrap
  drain never runs `detoast_heap`, and the encoder rejects it:
  `encode_value` (`src/ch_emitter.rs`) returns
  `EmitterError::UnsupportedValue { kind: "unresolved TOAST pointer (xact
  buffer should have reassembled)" }`. This is the
  [[pipeline_backpressure_and_scaling]] "Blockers, refined" item 2 — bootstrap
  *fails*, it does not ship. Latent only because test fixtures keep values
  inline (< ~2 KiB, below `TOAST_TUPLE_THRESHOLD`).
* **Pre-window values can't be rebuilt from WAL alone.** A row whose value
  was toasted before `start_lsn` re-emits its referring tuple on any later
  UPDATE, but PG does *not* re-write the toast chunks unless the toasted
  value itself changed (`toast_save_datum` reuses the old `va_valueid`).
  So the chunk records never appear in the replication window, the same-xact
  bundling invariant is violated, and `reassemble` returns
  `MissingToastChunk`. WAL-only detoast is structurally blind to chunks
  written in the past.

## Chunk-store backend (findings, 2026-06)

The reassembler needs a *store of record* for chunks it didn't see in the
current xact (bootstrap baseline, pre-window re-emits). That store is now a
pluggable backend behind `ToastResolver` / the `ChunkStore` trait
(`src/toast.rs`), selected by `[toast] mode`:

* `disabled` (default) — no store; an unresolved pointer NULL/default-fills
  at the emitter and is counted (`toast_values_filled_default`), never an
  error. `fill_on_miss` is true only here. This is the "explicit, surfaced"
  form the NULL-fallback alternative below allows, *not* silent loss.
* `disk` — local persistent `DiskChunkStore`: one append-only file per value
  at `<dir>/<relid>/<value>.chunks`, framed `[seq u32][len u32][body]`,
  torn-trailing-record tolerant. Dir is persistent, distinct from the wiped
  `--spill-dir`. A miss here is a hard error, not a fill — opt-in durable
  storage means an absent chunk is data loss, surfaced loud.
* `clickhouse` — the mirror-to-CH design in the rest of this doc, shipped
  as `ClickHouseChunkStore` (`src/toast.rs`). `put` writes chunk rows to a
  `pg_toast_<relid>` `ReplacingMergeTree(_lsn)` table, `fetch` is a CH
  `SELECT`; same control flow as `disk` (the store is the only swap).

`disk` shipped first. It decouples the backstop from the CH ingest path and
from shadow-PG lifecycle, and already exercises the full bootstrap flow end
to end: the page walk reinterprets `pg_toast_*` tuples into chunks
(`chunk_from_columns`), persists them, and the drain *defers* any main-table
tuple carrying a mapped `ExternalToast` until the walk completes (its chunks
may live in a later-walked toast file), then fetches them back and
reassembles via `try_reassemble` (`src/pipeline/bootstrap.rs`). That is a
disk-backed realisation of incremental-delivery steps 2–3 below; the CH
backend swaps the storage target, not the control flow. (The bootstrap
module's "explicit fail-fast at the producer" header predates this and is
stale — the drain now resolves or fills, it no longer fails fast.)

**Why not store pg_toast in the shadow PG catalog.** Tempting, since the
shadow PG already replays WAL: query it (or let PG detoast natively) and
reassembly, decompression, and vacuum come for free. Rejected. The shadow
today is a *catalog* shadow, not a *data* shadow — user-heap (and thus
`pg_toast`) records are NOOP-rewritten so shadow PG does not PANIC on missing
pages ([[reference_walshadow_cross_seg_records]]), and the catalog cache
resolves relations via `pg_relation_filenode` + `pg_attribute`/`pg_type`
without holding table bytes (`src/shadow_catalog.rs`). Making `pg_toast`
queryable there means promoting shadow PG to a full data replica, which:
reintroduces the exact missing-page / cross-seg PANIC class the NOOP rewrite
exists to avoid; couples every detoast to a `pg_last_wal_replay_lsn` wait +
the catalog mutex (the relation-resolution bottleneck); and exposes reads to
shadow vacuum reclaiming a value still needed for a pre-window re-emit. The
disk and CH backends are append-only, walshadow-owned, and
lifecycle-independent — they trade owning detoast correctness (compression,
header variants) and chunk GC for not depending on a heavyweight replica.
Decompression-for-free is moot anyway: `try_reassemble` already handles
PGLZ/LZ4 from raw chunks, so disk and CH both get correct detoast without PG.

## Proposed approach: mirror pg_toast to ClickHouse

Treat each `pg_toast_<relid>` relation as a first-class replicated table.
Its chunks land on CH under the same `ReplacingMergeTree(_lsn)` model the
main tables use (`src/ch_ddl.rs::render_create_table`). A toasted value is
then reconstructable from CH-resident chunks regardless of which xact, or
which bootstrap pass, produced them.

This removes the same-xact coupling as a *correctness* requirement. Inline
reassembly can stay as a fast path, but it stops being the only path.

### CH-side toast table schema

Mirror the on-disk PG shape, plus walshadow's convergence columns:

```text
CREATE TABLE `<db>`.`pg_toast_<relid>` (
  `chunk_id`   UInt32,        -- PG oid, == va_valueid
  `chunk_seq`  UInt32,        -- 0-based, dense
  `chunk_data` String,        -- raw bytea body (see Compression)
  `_lsn`       UInt64,
  `_xid`       UInt32,
  `_op`        Enum8('insert'=1,'update'=2,'delete'=3),
  `_commit_ts` DateTime64(6,'UTC')
) ENGINE = ReplacingMergeTree(`_lsn`)
ORDER BY (`chunk_id`, `chunk_seq`)
```

`ORDER BY (chunk_id, chunk_seq)` so a value's chunks are contiguous and a
range read rebuilds it in order. This differs from the main-table default
(`(_lsn)` or PK columns); the toast table has a natural composite key, so
`render_create_table`'s PK-or-`_lsn` fallback needs a toast-aware branch
(`rel.kind == 't'` → key on the first two attrs). Chunk records are
INSERT-only in normal operation, so `_op` is almost always `insert`; the
column stays for uniformity and for vacuum-driven deletes (see Risks).

*Shipped (R2, `ClickHouseChunkStore`):* the created table is the minimal
chunk-source form — `chunk_id`, `chunk_seq`, `chunk_data`, `_lsn` only. Under
R2 it is a reassembler source, not a query-time JOIN target, so the `_xid` /
`_op` / `_commit_ts` columns above are omitted, and `create_sql` keys
`ORDER BY (chunk_id, chunk_seq)` directly rather than via
`render_create_table`. Dropping `_op` removes any landing spot for the
vacuum-driven delete marker below — chunk GC stays fully deferred (see Risks).

`chunk_id` is the OID (`va_valueid`). Key the CH table on it, not on
`toast_relid` — `toast_relid` is fixed per table, it identifies *which* CH
toast table, not a row within it. This matches the existing WAL key
`(toast_relid, value_id)`: `toast_relid` selects the table, `value_id`
selects the row group.

### Pointer-and-join vs reassemble-at-ingest

Two ways the main-table column can carry a toasted value.

**Option R1 — store pointer, reassemble at query time (JOIN on CH).**
The main-table column stores the `ToastPointer` (or just `va_valueid` +
`va_toastrelid` + compression flags), and the toast bytes live only in the
mirrored CH table. A reader reconstructs via a JOIN keyed on
`chunk_id = va_valueid`, ordered by `chunk_seq`, concatenated, optionally
decompressed.

* Pro: no chunk-availability dependency at ingest. Main-table INSERT never
  blocks on chunks. Bootstrap ships the pointer immediately, chunks stream
  independently. Storage is deduplicated when many rows share a value
  (rare in PG, but free here).
* Con: every reader must JOIN + reassemble + decompress. CH has no native
  "concat ordered chunks then PGLZ-decompress" function, so this needs a
  materialized view, a UDF, or client-side reassembly. The main table no
  longer holds the column's logical value — a regression for the
  "queryable mirror" promise. Pointer columns are meaningless without the
  walshadow-specific reassembly convention.

**Option R2 — reassemble at ingest, store inline value (status quo shape).**
walshadow rebuilds the value before the main-table INSERT and stores the
decoded `Bytea` / `Text` inline, exactly as `detoast_tuple` does today. The
CH toast table is then a *source of chunks for the reassembler*, not a
read-time JOIN target.

* Pro: main table stays a faithful mirror — column holds the real value,
  no reader-side machinery. Matches existing emitter (`Bytea` / `Text`
  arms in `encode_value`) with zero schema change to main tables.
* Con: reassembly needs every chunk present at ingest. Same-xact WAL values
  already satisfy this (inline path). Pre-window / bootstrap values do not —
  the reassembler must read chunks back from the CH toast table (a CH SELECT
  during ingest), or from a side cache populated as toast pages stream.

**Recommendation: R2, with the CH toast table as the chunk store of record.**
R2 preserves the mirror semantics walshadow already ships and keeps
`encode_value` unchanged. The CH toast table exists so the reassembler has
somewhere to find chunks it didn't see in the current xact. Concretely:

* same-xact WAL value: reassemble inline from the buffered `chunks` map
  (unchanged fast path), *and* ship the chunk rows to the CH toast table so
  the value is rebuildable later if the main row re-emits;
* pre-window / bootstrap value: `reassemble` misses the in-memory map, so
  fall back to a chunk fetch keyed on `(toast_relid, value_id)` against the
  CH toast table (or a local chunk cache hydrated during bootstrap).

R1 stays the documented escape hatch for operators who want
dedup / want to defer reassembly cost off the ingest path; it is a
per-table mode, not the default. Keep both in the design; ship R2 first.

### Bootstrap: tap pg_toast filenodes into the pipeline

Page walk already routes `pg_toast_*` tar entries through `PageWalkSink`
and short-circuits them (`is_toast` branch in `flush_full_pages`). Replace
the short-circuit with a real walk:

* the toast relation's `RelDescriptor` is a 3-column heap
  (`chunk_id oid`, `chunk_seq int4`, `chunk_data bytea`), so `PageWalker`
  can decode its tuples through the *same* `decode_block_data` path the main
  tables use — no new decoder. Seed the toast descriptors into `CatalogMap`
  (the seed already inserts them, that's how `is_toast` knows them; today it
  files them under `toast_filenodes` and drops the descriptor for walk
  purposes — keep the descriptor walkable too).
* emit one `BackfillTuple` per chunk into the same drain. The drain maps it
  to the mirrored CH toast table (a `TableMapping` for `pg_toast_<relid>`),
  same as any other table. Reuse `toast_chunk_from_decoded`'s shape check to
  validate the 3-column layout before shipping.
* wire this through the shared tail per [[pipeline_backpressure_and_scaling]]
  "Base backup through the same pipeline": chunks are just more rows, one
  `op=Insert` at `_lsn = start_lsn`, no aborts, no barriers. Per-rfn seqs
  cover toast filenodes for free.

This turns the currently-observed-but-dropped toast pages into CH rows.
Combined with R2's CH-toast-table fallback, a page-walked main-table tuple
with an external column becomes rebuildable: its chunks were shipped from
the same backup pass.

Ordering note: a main-table tuple's chunks may arrive after the tuple in
the backup stream (different filenode, walked later). Under R2 the
main-table INSERT would then need its chunks not-yet-shipped. Two outs:
(a) two-pass — walk all `pg_toast_*` filenodes first, then main heaps, so
chunks precede their referrers; or (b) defer reassembly for any
bootstrap tuple whose chunks aren't yet local and resolve it from the CH
toast table after bootstrap drain completes. *Shipped: (b).* The drain
collects referrers as `Deferred`, `put`s all chunks durable, then resolves
them via `fetch_into` + `try_reassemble` (`resolve_or_fill_toast`,
`src/pipeline/bootstrap.rs`). (a) was the original preference (simpler,
bounded) but (b) reuses the WAL miss→fetch path verbatim, so one resolution
codepath covers bootstrap and pre-window alike.

### WAL path: keep inline, add CH chunk shipping

Minimal change to the hot path:

* keep `toast_chunk_from_decoded` + `on_toast_chunk` + the in-xact `chunks`
  map + inline `detoast_heap` exactly as-is for same-xact values (no
  CH round-trip on the common path);
* additionally route each `ToastChunk` to the mirrored CH toast table so
  the chunk is durable for future re-emits. This is a second sink for the
  chunk, not a replacement;
* when `reassemble` hits `MissingToastChunk` (pre-window value, chunk not in
  this xact), fall back to the CH-toast-table fetch instead of erroring.
  This is the one new failure-to-success conversion.

Do not unify the WAL path onto pure CH-stored chunks. The inline path
avoids a CH SELECT per toasted value and is correct for the dominant
same-xact case; the CH store is the durability + pre-window backstop.

### Compression

PG stores toast chunks as the *compressed* representation when the value
was compressed; `va_extinfo` top 2 bits carry the method
(`VARATT_EXTERNAL_GET_COMPRESS_METHOD`, PG `src/include/varatt.h`),
the low 30 bits (`VARLENA_EXTSIZE_MASK`) the external size.
`reassemble` (`src/xact_buffer.rs`) already concatenates chunks then, if
`(va_extinfo & !VARLENA_EXTSIZE_MASK) != 0`, decompresses via
`pglz::decompress_into` (`TOAST_COMPRESSION_PGLZ = 0`) or
`lz4_flex::decompress` (`TOAST_COMPRESSION_LZ4 = 1`), sized by
`va_rawsize - VARHDRSZ`.

Decide what `chunk_data` on CH holds:

* **store compressed bytes (as PG wrote them).** Smaller on CH; faithful to
  on-disk. But the compression *method* and `va_rawsize` live on the
  referring tuple's pointer, not in the toast table, so a reader rebuilding
  from CH chunks alone can't decompress without that side metadata. Under
  R2 the reassembler holds the pointer at ingest, so it has both — fine.
  Under R1 (query-time JOIN) the main-table pointer column must also carry
  `va_extinfo` + `va_rawsize`, else CH can't decompress.
* **store decompressed bytes.** Larger, but `chunk_data` is then the literal
  value bytes and a reader needs no decompress step. Loses the chunk-size
  fidelity (re-chunking is arbitrary) but walshadow never re-feeds PG from
  these.

Recommendation: under R2, **store compressed** (reassembler decompresses at
ingest using the pointer it already holds, reusing `reassemble`'s codepath
unchanged). Under R1, **store decompressed** so the JOIN doesn't need a CH
PGLZ implementation. The chosen R2 default therefore reuses `reassemble`
verbatim — the only new code is the chunk-fetch fallback that re-fills the
`chunks` map from CH before calling it.

### Convergence semantics

Toast tables are `ReplacingMergeTree(_lsn)` like every other table, so the
[[project_walshadow_eventual_consistency]] promise holds: end-state agreement
via `_lsn` dedup, no mid-drain ordering guarantee. A chunk re-shipped at a
higher `_lsn` (bootstrap baseline then WAL re-emit) collapses to the latest,
matching the main-table baseline-then-tail story in `plans/bootstrap.md`.
Chunk rows are immutable in PG (a value's chunks are written once under a
fresh `va_valueid`), so duplicate chunk rows are byte-identical and `_lsn`
dedup is purely a dedup, never a value change.

## Alternatives considered

* **Inline reassembly only (status quo).** Correct for same-xact WAL,
  wrong for bootstrap (errors at the emitter) and pre-window values
  (`MissingToastChunk`). This is what ships today; it is not full support.
* **NULL / raw-marker fallback.** Emit NULL or a `<toast:valueid>` sentinel
  for unresolved pointers, converge later via WAL re-emit. Lossy: the WAL
  re-emit of the *referring* tuple does not carry the chunks either (PG
  reuses the old `va_valueid`), so the value never resolves. Only correct
  if the toasted column is itself rewritten. Reject as a silent data-loss
  path; acceptable only as an explicit, surfaced fail-fast for an
  unsupported config.
* **Full pg_toast mirroring (this plan).** Chunks become first-class CH
  rows; reassembly has a durable source independent of the WAL window.
  Highest fidelity, costs one CH table per toast relation and a
  chunk-fetch fallback.
* **Local disk chunk store.** Same control flow, store of record is a local
  append-only file instead of CH (shipped first). See findings above.
* **pg_toast in the shadow PG catalog.** Rejected — requires a full data
  replica and reintroduces the cross-seg PANIC class. See findings above.

## Open questions / risks

* **Chunk ordering / atomicity across xacts.** Same-xact bundling
  guaranteed completeness; CH-stored chunks do not. *Confirmed 2026-06:*
  `try_reassemble` (`src/xact_buffer.rs`) treats any gap in the dense
  `chunk_seq` 0..N run as `Ok(None)` — a plain miss — so an incomplete fetched
  set NULL-fills in `disabled` mode or errors `MissingToastChunk` with an
  active store. The planned in-flight-vs-truncated distinction (compare
  `va_rawsize` to summed chunk length, retry the former) is *not* implemented:
  `fetch` is one SELECT, its result taken as final, no retry. Benign while a
  completed `put` makes a value's chunks atomically visible (single-node CH,
  synchronous INSERT ack, chunks immutable per `va_valueid`); reopen if a
  partial or racing put can surface a torn set.
* **toast relid ↔ owning rel mapping.** `va_toastrelid` is the toast
  relation's OID; `toast_chunk_from_decoded` already keys on `rel.oid` (not
  `rel_node`) precisely because the pointer carries the OID, and notes the
  OID/relfilenode divergence after `VACUUM FULL` / `CLUSTER`. The CH
  toast-table name must be stable across such ops — key the CH table on the
  toast OID, and remap relfilenode→OID via the catalog as today.
* **Vacuum / chunk deletion.** PG deletes toast chunks when the referring
  value is deleted or updated to a new `va_valueid`. Those deletes are
  `heap_delete` on the toast relation; under `_lsn` / RMT they would need an
  `_op=delete` chunk row (replica identity on the toast table is `nothing`
  by default, so the delete WAL carries no key — same blind spot as system
  catalogs, [[feedback_pg_version_wal_skew]]). The shipped CH schema has no
  `_op` column at all (R2 minimal form), so a delete marker has nowhere to
  land regardless. V1 leaks superseded chunk rows on CH (dedup keeps the live
  `va_valueid`'s chunks correct; dead chunks are unreferenced garbage).
  Reclaim is a later concern.
* **Very large values.** A multi-MB value is thousands of chunks
  (`TOAST_MAX_CHUNK_SIZE` ≈ 1996 bytes, `EXTERN_TUPLES_PER_PAGE = 4`,
  PG `src/include/access/heaptoast.h`). *Confirmed 2026-06:* the CH
  fetch streams the SELECT block-by-block (`recv_event` → `read_chunk_block`,
  `src/toast.rs`), so there is no single unbounded buffered read of the result
  set. The reassembled value is still fully materialised in memory (the
  `BTreeMap` supplement, then `try_reassemble`'s concat) — R2-inherent, same as
  inline `reassemble` today, not specific to the fetch. Bounded-memory
  streaming reassembly of huge values stays unaddressed.
* **Type reconstruction.** *Done 2026-06:* `detoasted_value`
  (`src/xact_buffer.rs`) now delegates to `varlena_to_value`
  (`src/heap_decoder.rs`, taking a bare `type_oid`), the same dispatch the
  inline decoder runs on the header-stripped body. A detoasted value resolves
  identically to an inline one — numeric/inet/json typed directly, Tier 3
  (jsonb, arrays, ranges, tsvector, …) carried as `PgPending` for the oracle
  at emit — instead of the old `Unsupported` for everything past bytea/text.

## Incremental delivery

Smallest-first, each step independently shippable, tied to the
[[pipeline_backpressure_and_scaling]] TOAST blocker.

**Status (2026-06):** the `disk` backend (see findings above) already lands
disk-backed equivalents of steps 2–3 — page-walk tap, deferred resolve,
`try_reassemble` fallback — plus a counted NULL-fill in `disabled` mode that
supersedes step 1's hard fail-fast. The `clickhouse` backend
(`ClickHouseChunkStore`) now reuses that same control flow, so steps 2–4 are
done for CH too: bootstrap chunks `put` to CH toast tables, the bootstrap +
WAL reassemblers `fetch` them back on a miss, and same-xact WAL chunks `put`
through `reorder`'s `put_map` for durable re-emit. Step 5's Tier 3 detoast
routing is also done (`detoasted_value` → `varlena_to_value`); only R1
query-time-JOIN mode stays deferred.

1. **Fail-fast cleanly.** Make the bootstrap `ExternalToast` path surface a
   documented, counted error instead of a bare `UnsupportedValue` deep in
   `encode_value`. Detect at decode/route time, name the relation + value
   id, bump a stat. Turns a latent emitter reject into an operator-visible
   "TOAST not yet supported for bootstrap of table X". No data path change.
2. **Tap toast chunks to CH (bootstrap).** Walk `pg_toast_*` pages instead of
   counting them (reuse `decode_block_data` + `toast_chunk_from_decoded`),
   create the mirrored CH toast table (minimal `create_sql`, see schema note),
   ship chunk rows through the shared tail. Ordering: defer referrers, resolve
   after all chunks durable (option (b), not the two-pass (a) originally
   sketched). At this point chunks are *on CH* but main-table reassembly still
   doesn't consult them.
3. **Reassemble from CH chunks.** Add the `MissingToastChunk` → CH-fetch
   fallback in the R2 reassembler (hydrate the `chunks` map from the CH
   toast table, then call `reassemble` unchanged). Bootstrap and pre-window
   WAL values now resolve. This resolves the
   [[pipeline_backpressure_and_scaling]] blocker fully.
4. **WAL chunk durability.** Route same-xact `ToastChunk`s to the CH toast
   table too (second sink), so a future re-emit of a pre-window referrer
   finds its chunks. Without this, step 3's fallback only covers
   bootstrap-baseline values.
5. **Tier 3 detoast routing (done) + (optional) R1 query-time-JOIN mode.**
   Tier 3 routing shipped: `detoasted_value` runs reassembled bytes back
   through `varlena_to_value`, so a detoasted jsonb/array/numeric resolves
   like an inline one (`PgPending` → oracle). R1 per-table opt-in pointer
   storage stays deferred behind demand.
