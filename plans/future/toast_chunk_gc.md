# toast chunk GC

PG drops superseded chunks when a value is deleted or updated to a new
`va_valueid` (`heap_toast_delete`, PG `src/backend/access/heap/heaptoast.c`
→ `toast_delete_datum`, PG `src/backend/access/common/toast_internals.c`),
in the same xact as the superseding main-table op. The toast relation's
replica identity is `nothing`, so its `XLOG_HEAP_DELETE` carries
`(xmax, offnum, infobits, flags)` plus a block ref — a TID, no `chunk_id`.
The store is keyed `(chunk_id, chunk_seq)`, so the delete cannot be applied
directly; dead chunk rows leak ([../TOAST.md](../TOAST.md) scope limit).
Decoder side: `toast_chunk_from_decoded` (`src/xact_buffer.rs`) accepts
`Insert` only, so toast deletes tick `toast_chunks_malformed` — a miscount
to fix in any variant (route non-insert toast ops to an explicit drop with
their own counter)

Correctness never depends on GC: chunks are immutable per `va_valueid` and
dedup keeps live values right; the leak is storage-only. GC's risk is the
inverse — deleting a chunk a future fetch needs. The design reduces to
proving when deletion is safe

## Safety invariant

Fetches serve two consumers: replay re-decode from the cursor (= emitter
ack LSN) after restart, and fresh WAL whose referring tuple keeps a
pre-window `va_valueid` (unchanged toast column across UPDATE). For a dead
value V whose last reference was deleted at `L_dead`, every WAL record
referencing V has LSN < `L_dead`. Once `emitter_ack_lsn ≥ L_dead`, no fetch
can legitimately want V: replay starts at ack, and WAL past `L_dead` cannot
re-reference V. Every deletion below is gated on
`ack ≥ (upper bound of L_dead)`

Valueid reuse is the one wrinkle: `GetNewOidWithIndex` (PG
`src/backend/catalog/catalog.c`) checks uniqueness only against the live
toast index, so a dead valueid's OID can be re-allocated. A re-put under a
reused OID must survive GC of the dead generation; per-chunk `_lsn`
distinguishes generations

## Design — source anti-join sweep

Mark-and-sweep with the source toast table as liveness ground truth. Per
toast relid present in the store (CH mode: `pg_toast_*` tables in the dest
db; disk mode: subdirs of the store root):

1. resolve source relname by OID (`SELECT relname FROM pg_class WHERE oid =
   $relid`); empty ⇒ toast rel dropped ⇒ live set empty
2. `SELECT DISTINCT chunk_id FROM pg_toast.<relname>` — index-only over the
   toast PK index
3. `SELECT pg_current_wal_lsn()` **after** the scan completes → `S`
4. candidates = store valueids (with `max(_lsn)` per value) − live set,
   skipping candidates whose `max(_lsn) > S` (reused OID, already re-put)
5. wait `emitter_ack_lsn ≥ S`, then delete

Reading `S` after the scan is load-bearing: the statement snapshot is taken
at scan start, so absence at the snapshot bounds `L_dead ≤ snapshot LSN ≤
S`, and the ack gate then covers `L_dead`. The dropped-table case rides the
same argument (drop at `L_drop ≤ S`, referring inserts < `L_drop`), so
orphaned store tables collect fully

Sweep is stateless: `(candidates, S)` live in memory; a crash discards
them and the next sweep recomputes. Deletion is idempotent

### Deletion mechanics

- **CH.** Lightweight `DELETE FROM pg_toast_<relid> WHERE chunk_id IN (…)
  AND _lsn <= {S}`. The `_lsn` predicate re-excludes reused-OID re-puts
  landing between candidate computation and apply, with no coordination
  against the put path; concurrent INSERTs land in parts the mutation
  doesn't cover and the predicate excludes them anyway. Not tombstone rows:
  `ReplacingMergeTree` reclaims tombstones only on merge, parts holding
  dead-forever values may never merge again — tombstones are a second leak
- **Disk.** Unlink `<dir>/<relid>/<value_id>.chunks`. Frames carry no LSN,
  so the predicate trick is unavailable: stat before unlink, skip when
  mtime ≥ sweep scan start (re-put appended; value live again, next sweep
  re-evaluates). The unlink-vs-append race needs per-value serialization
  inside the store — `ChunkStore` grows `delete(toast_relid, value_id)` so
  each impl owns its guard
- **`disabled` mode.** No store, nothing to collect; sweep refuses to arm

### Scheduling and config

`[toast] gc_interval` (0 = disabled, the default). Own task off the hot
path. Source access rides the source-PG connection
([runtime_config_from_pg.md](runtime_config_from_pg.md)); degraded mode
(source unreachable) skips the sweep and ticks a counter — never an error.
Cadence is purely a storage-reclaim knob; correctness is identical at any
frequency

Metrics: `toast_gc_sweeps_total`, `toast_gc_values_deleted`,
`toast_gc_skipped_source_unreachable`

## Alternative — WAL-driven tombstones

For deployments replaying archived WAL with no live source at sweep time,
liveness must come from the WAL itself:

- decoder accepts `Delete` on `kind == 't'`, capturing the TID (block ref
  blockno + `xl_heap_delete.offnum`, PG
  `src/include/access/heapam_xlog.h`); chunk inserts capture their TID
  likewise
- xact buffer carries `ToastChunkDelete` per xid beside puts; abort
  discards, commit drain emits
- TID → `(chunk_id, chunk_seq)` map persisted in the store: CH gains
  `_block UInt32` / `_offnum UInt16` plus a projection or side table
  ordered `(_block, _offnum)` (the main `ORDER BY (chunk_id, chunk_seq)`
  makes TID lookup a scan); disk gains a sidecar index. Post-vacuum
  line-pointer reuse resolves by WAL-order upsert
- apply gated on `ack ≥ delete commit LSN` — same invariant: a re-decoded
  referring insert has LSN > ack, and its chunk delete sits in a later
  xact, so it is not yet applied
- pending queue is in-memory only; replay re-derives it and deletion is
  idempotent

Not preferred: touches decoder, xact buffer, and both store schemas, and
collects only deletes observed while the store is enabled — store-disabled
gaps and pre-enable history leak forever. The sweep catches every class.
Both gates compose; hybrid is possible if both deployment shapes matter

## Why deferred

The leak is storage-only and workload-dependent: only churned toasted
values (UPDATE rewriting the value, DELETE of referring rows) accumulate;
append-mostly toast workloads leak ~nothing. Escalate when store size
visibly outgrows the source toast relation. Sweep transport depends on the
source-PG connection ([runtime_config_from_pg.md](runtime_config_from_pg.md))

## Acceptance

- UPDATE rewrites a toasted value to a new valueid; sweep with `ack ≥ S`
  removes the old valueid's chunks; subsequent referring rows reassemble
- DELETE of the referring row; sweep removes the value's chunks
- daemon restart between chunk death and sweep: replay re-emits the
  referring tuple and reassembly succeeds (candidates apply only after
  `ack ≥ S`)
- reused valueid: value deleted, OID re-allocated and re-put before apply;
  `_lsn <= S` (CH) / mtime guard (disk) keeps the new generation intact
- owning table dropped on source: the orphaned store table collects fully
- source unreachable: sweep skips, `toast_gc_skipped_source_unreachable`
  ticks, no error
- toast deletes no longer tick `toast_chunks_malformed` (dedicated counter)
