# backup-sourced per-table initial load

High-fidelity `initial_load` modes for per-table opt-in
([runtime_config_from_pg.md](runtime_config_from_pg.md) §Per-table opt-in):
source pre-opt-in rows from a base backup instead of a live
`COPY (SELECT …)`. Reuses greenfield bootstrap plumbing
([../bootstrap.md](../bootstrap.md)) — `BackupSource` impls, `PageWalkSink`,
`pipeline::bootstrap::drain` — with a per-rel filter over the tables being
added.

`config_table.initial_load` (text) selects the mode:

| value | source | WAL leg |
|---|---|---|
| `'copy'` | live `COPY (SELECT …)` at `_lsn = S` (`src/copy_backfill.rs`) | live stream from `S` |
| `'base_backup'` | fresh `BASE_BACKUP` over a second replication conn (`DirectSource`) | live stream from `S` |
| `'object_store'` | existing wal-g base backup from bucket (`ObjectStoreSource`) | archive-WAL gap replay `B_redo → S`, then live stream |
| NULL | none | live stream from `S` |

## Why beyond COPY

- **One decoder, not two.** The page walk feeds on-disk tuples through the
  same heap decoder the WAL hot path uses (`decode_on_page_tuple` →
  `decode_block_data`) — zero codec drift by construction. COPY carries a
  parallel `typsend`-wire codec plus `::text` casts for out-of-matrix types;
  [../bootstrap.md](../bootstrap.md) §Why not 2C rejected exactly that shape
  for greenfield. Backup modes bring opt-in backfill back under the 2A
  doctrine
- **Source load.** COPY holds a statement snapshot and burns the query path of
  the node walshadow streams from, for the table's full scan. `BASE_BACKUP` is
  bulk-file I/O, boundable via the protocol's `MAX_RATE`; `object_store` costs
  source PG nothing — bucket bandwidth only
- **Node coupling.** COPY must run against the streamed node (the `P ≥ S`
  snapshot argument). `object_store` needs no source SQL for the data leg
- **Deleted-row hygiene.** COPY sees an MVCC snapshot. The page walk sees raw
  pages, so backup modes add a visibility gate (below) instead of inheriting
  greenfield's emit-every-`LP_NORMAL` stance

## Shared mechanics

- **Filter set = the tables being added.** All rels with a pending
  backup-mode ledger entry at pass start; an opt-in burst coalesces into one
  backup pass. Page-walk `CatalogMap` seeds only those descriptors plus their
  `pg_toast_<oid>` rels. `PageWalkSink` Taps main-fork filenodes in the set
  (`_fsm`/`_vm` forks skip; `.N` segment suffixes route via `parse_base_path`);
  every other file Skips
- **Nothing lands on disk.** No `DiskLanderSink`, shadow untouched — backup
  bytes stream through the walker and drop. Exception: `pg_xact/` Taps into
  memory (256 KiB segments) for the visibility gate
- **Visibility gate.** Emit a tuple only when backup-era clog says
  `xmin` committed and `xmax` absent/aborted; infomask hint bits
  (`HEAP_XMIN_COMMITTED`, `HEAP_XMAX_INVALID`, PG
  `src/include/access/htup_details.h`) short-circuit most lookups. Skipped
  in-flight tuples are re-delivered by the mode's WAL leg (their commits are
  `> B`); skipped aborted tuples were never real. Dead-but-unvacuumed tuples
  stop resurrecting — the gate is what makes these modes higher-fidelity than
  greenfield's walk, not just cheaper than COPY
- **Same tail as COPY.** `BackfillTuple` → `pipeline::bootstrap::drain` →
  dedicated insert tail (own CH connection); the live pipeline never blocks on
  a backfill. Regime A holds: a failed backfill stays pending in the ledger,
  never poisons the pump
- **Ledger.** `backfills.json` entries gain `mode`; boot re-runs the recorded
  mode at the recorded `S`. Dedup keeps re-runs idempotent. `note_opt_out`
  semantics unchanged: entry drops, an in-flight walk drains against the
  shared routing map and its rows skip once the mapping is gone

## `_lsn` tagging invariant

Walked rows must lose to every WAL-delivered mutation the backup state does
not already reflect. Rule: **tag walked rows with the LSN where continuous
WAL coverage of the rel begins**, never later.

- `'base_backup'`: backup starts at `B ≥ S`, live WAL covers `(S, ∞)` → tag
  `_lsn = S`. A delete committed in `(S, B)` may leave a dead tuple that the
  visibility gate misses only if clog hasn't caught up in the tar; its
  `_is_deleted` row at `commit_lsn > S` must outrank the walked copy. Tagging
  `B` would resurrect it
- `'object_store'`: backup predates the opt-in; the gap `(B_redo, S]` is
  covered by archive replay, not the live stream → tag `_lsn = B_redo` (the
  backup's checkpoint redo LSN, from the sentinel). Replayed rows carry real
  commit LSNs `> B_redo` and win. Tagging `S` would let stale walked rows beat
  fresher gap-replay rows

## base_backup mode

Issue `BASE_BACKUP` on a second replication connection once the opt-in
applies. The protocol is cluster-scoped (PG `src/backend/backup/basebackup.c`
walks the whole datadir; no per-rel filter exists), so the tar streams the
entire cluster past the daemon and the filter discards all but the target
filenodes — bandwidth is cluster-sized even for one table. Coalescing
concurrent opt-ins amortises the pass; `MAX_RATE` bounds source I/O.

No replay leg: any xact the tar catches mid-write commits after `S`, so the
live stream re-delivers it at its real commit LSN. Torn pages lose only
tuples whose writers are post-`S` — strictly safer than greenfield's
no-FPI-replay caveat, where nothing replays `[start_lsn, end_lsn]`.

## object_store mode

1. **Resolve backup.** `fetch_sentinel` on the wal-g bucket → latest full
   backup, `B_redo`/`B_end` LSNs, timeline. Delta chains reject
   (`increment_from` set) with the same operator-actionable error as
   bootstrap: streaming walk has no disk-resident base to overlay
2. **Gap catalog pre-scan.** Fetch archive WAL `B_redo → S` (wal-rus
   `pg::wal::fetch` + `prefetch`) and scan records-only for catalog writes
   touching filtered rels (`pg_class`/`pg_attribute` heap writes, rewrite /
   truncate — the classifier already isolates catalog rmgrs). Any hit aborts
   the backfill before a row is emitted: the walk would decode with the wrong
   shape, and a rewrite in the gap means the backup's filenode isn't the
   current rfn at all. Error names the remedies — fresher backup, or `'copy'`
3. **Filtered walk.** Tar parts stream through `PageWalkSink` with the filter
   set; rows tag `_lsn = B_redo`
4. **Gap replay.** Walk the fetched segments through the existing
   record-decode path, keep heap records whose rfn is in the filter set,
   buffer per-xid, emit at real commit LSNs through the backfill tail.
   Commits `> S` drop — the live stream owns them (dedup absorbs overlap
   regardless). Timeline switch inside the gap aborts; the pending entry
   re-runs against a newer backup

Convergence: walk EOF + gap replay reaching `S`. As with COPY, completion is
observability, not correctness — nothing gates on it.

## Observability

- `config_backfills_pending` gauge stays the umbrella count; per-mode label
  (`mode="copy|base_backup|object_store"`) splits it
- Per-pass INFO summary mirrors bootstrap's: pages walked, tuples emitted,
  tuples gated (visibility), gap records replayed, `B_redo`/`S` bounds

## Anti-goals

- **No shadow involvement.** Backup bytes never land in a data dir; the modes
  add no second shadow or scratch cluster
- **Bucket/creds stay TOML.** Object-store settings ride the same TOML surface
  bootstrap uses, never the overlay — credentials in a source-PG table is the
  wrong trust direction
- **No server-side filtering wishes.** Do not fork the BASE_BACKUP protocol;
  cluster-sized bandwidth is the documented cost of `'base_backup'`, and
  `'object_store'` is the answer when that cost bites

## Open questions

- **Multixact xmax.** `HEAP_XMAX_IS_MULTI` needs `pg_multixact` to resolve
  whether the updater committed; v1 emits conservatively (matches greenfield)
  and counts, since the WAL leg re-delivers the committed update anyway
- **Coalesce window.** How long a backup-mode opt-in waits for siblings before
  the pass fires (amortise the cluster-sized tar) — fixed debounce vs
  operator signal (`flush_backfills`)
- **Gap length guard.** A months-old backup makes `B_redo → S` replay dominate
  cost; past some segment count `'copy'` is strictly cheaper. Warn-with-metric
  threshold, or hard cap with operator override
- **clog torn-tail during base_backup.** Commits landing while the tar streams
  may miss the copied `pg_xact` (read as in-progress → gated → live WAL
  re-delivers). Confirm the gate never reads *ahead* of the copied clog for
  pre-`S` xids; pre-`S` commits are durable in clog before `BASE_BACKUP`
  starts, so exposure should be nil
- **Backup encryption/compression matrix.** wal-rus `compression` covers
  wal-g's codecs; encrypted buckets defer to storage-layer config

## Acceptance drills

- **base_backup opt-in.** Non-empty `app.orders`; insert
  `config_table (replicate=true, initial_load='base_backup')`. Daemon streams
  WAL from `S`, second replication conn pulls the backup, only `app.orders`
  filenodes walk, rows land at `_lsn = S`. Mutations committed mid-backup
  reflect the WAL copy, not the walked copy
- **Dead tuple stays dead.** Delete a row, opt in via `'base_backup'` before
  autovacuum prunes. Walked page still carries the tuple; visibility gate
  drops it; CH never resurrects
- **object_store opt-in.** wal-g bucket with a full backup + continuous
  archive. Opt in with `'object_store'`; zero source SQL/replication load
  beyond the live slot; walked rows at `_lsn = B_redo`, gap replay bridges to
  `S`, post-`S` inserts stream live. Row mutated in the gap ends at its gap
  commit LSN value
- **Catalog skew aborts.** `ALTER TABLE app.orders ADD COLUMN` between backup
  and opt-in. Pre-scan detects the `pg_attribute` write, backfill aborts with
  the remedy error, ledger entry stays pending, live stream unaffected
- **Coalesce.** Two `'base_backup'` opt-ins in one xact → one backup pass,
  both rels' filenodes walk in it
- **Unknown mode.** `initial_load='snapshot'` warns
  (`unknown initial_load mode`), scope change still applies, streaming from
  `S` only
