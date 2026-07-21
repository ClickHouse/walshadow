# backup-sourced per-table initial load

High-fidelity `initial_load` modes for per-table opt-in
([future/runtime_config_from_pg.md](future/runtime_config_from_pg.md)
§Per-table opt-in): source pre-opt-in rows from a base backup instead of a
live `COPY (SELECT …)`. Reuses greenfield bootstrap plumbing
([bootstrap.md](bootstrap.md)) — `BackupSource` impls, `PageWalkSink`,
`pipeline::bootstrap::drain` — with a per-rel filter over the tables being
added. Orchestrated by `src/backup_backfill.rs`; dispatch + coalescing live
on the backfiller (`src/copy_backfill.rs`); visibility gate in
`src/visibility.rs`.

`config_table.initial_load` (text) selects the mode; TOML
`[table.*] initial_load` is the equivalent surface for pinned mappings
(applies at boot with `S` = the WAL resume LSN):

| value | source | WAL leg |
|---|---|---|
| `'none'` \| NULL | none | live stream from `S` |
| `'copy'` | live `COPY (SELECT …)` at `_lsn = S` (`src/copy_backfill.rs`) | live stream from `S` |
| `'base_backup'` | fresh `BASE_BACKUP` over a second replication conn (`DirectSource`) | live stream from `S` |
| `'object_store'` | latest wal-g full backup from bucket (`ObjectStoreSource`) | archive-WAL gap replay `B_redo → S`, then live stream |

## Why beyond COPY

- **One decoder, not two.** The page walk feeds on-disk tuples through the
  same heap decoder the WAL hot path uses (`decode_on_page_tuple` →
  `decode_block_data`) — zero codec drift by construction. COPY carries a
  parallel `typsend`-wire codec plus `::text` casts for out-of-matrix types;
  [bootstrap.md](bootstrap.md) §Why not 2C rejected exactly that shape
  for greenfield. Backup modes bring opt-in backfill back onto the 2A path
- **Source load.** COPY holds a statement snapshot and full-scans the table
  through the query path of the node walshadow streams from. `BASE_BACKUP` is
  bulk-file I/O, boundable via the protocol's `MAX_RATE`; `object_store` costs
  source PG nothing — bucket bandwidth only
- **Node coupling.** COPY must run against the streamed node (the `P ≥ S`
  snapshot argument). `object_store` needs no source SQL for the data leg
- **Dead tuples.** COPY sees an MVCC snapshot. The page walk sees raw
  pages, so backup modes add a visibility gate (below) where greenfield's
  walk emits every `LP_NORMAL` tuple

## Shared mechanics

- **Filter set = the tables being added.** Backup-mode opt-ins queue on the
  backfiller and wait a fixed coalesce window (`BACKUP_COALESCE_WINDOW`, 1s)
  for siblings, so an opt-in burst — several rows in one xact, or a boot
  seed — coalesces into one cluster-sized pass per mode. Page-walk
  `CatalogMap` seeds only those descriptors plus their `pg_toast_<oid>` rels
  (`ShadowCatalog::toast_descriptor_for`). `PageWalkSink` Taps main-fork
  filenodes in the set (`_fsm`/`_vm` forks skip; `.N` segment suffixes route
  via `parse_base_path`); every other file Skips
- **Nothing lands on disk.** No `DiskLanderSink`, shadow untouched — backup
  bytes stream through the walker and drop. Exceptions: `pg_xact/` and
  `pg_multixact/{offsets,members}` Tap into memory (256 KiB segments,
  `PgXactAccum` / `PgMultiXactAccum`) for the visibility gate, and
  object_store's gap segments land under `{spill_dir}/backup_backfill/`
- **Visibility gate** (`src/visibility.rs`). Emit a tuple only when
  backup-era `pg_xact` says `xmin` committed and `xmax` absent/aborted; infomask
  hint bits (`HEAP_XMIN_COMMITTED`, `HEAP_XMAX_INVALID`, PG
  `src/include/access/htup_details.h`) short-circuit most lookups. Unhinted
  tuples defer until walk EOF through a `DeferredSpool` (in-memory prefix,
  disk past it — [xact.md](xact.md) Spill backend) —
  same posture as the drain's deferred-TOAST spool. Deferrals resolve only
  after a successful walk (`pg_xact` complete only then): a failed source
  drops the sink too, and partial `pg_xact` reads committed deleters as
  in-progress, emitting dead tuples reruns can't remove.
  Skipped in-flight tuples are re-delivered by the mode's WAL leg; skipped
  aborted tuples were never real. Dead-but-unvacuumed tuples stop
  resurrecting — the gate is what makes these modes higher-fidelity than
  greenfield's walk, not just cheaper than COPY. `HEAP_XMAX_IS_MULTI`
  resolves through backup `pg_multixact` (`PgMultiXactAccum::updater`): the
  update/delete member's xid runs through the same pg_xact view, so a
  pre-coverage committed updater (UPDATE/DELETE alongside lockers, dead
  tuple still on-page) gates instead of resurrecting. Bytes the backup never
  copied prove the multi postdates the copy — copies happen past redo, so
  the WAL leg re-delivers that update and the old version emits safely
  (counted, `multixact_emitted`). A multi the snapshot can't bound
  (truncated below collected range, garbage reads) aborts the pass, error
  naming the remedies — emitting risks resurrection, skipping risks losing a
  live row whose updater aborted
- **PgXact patch.** For object_store, commit/abort records harvested from the
  gap pre-scan (`PgXactPatch`, top xid + subxids) overlay backup `pg_xact`
  before deferred tuples resolve. This covers xacts in flight across
  `B_redo` whose commits land inside the gap: their pre-`B_redo` rows exist
  only in backup pages that backup-era `pg_xact` calls in-progress, and the gap
  replay re-delivers only records `≥ B_redo` — without the patch those rows
  would be lost
- **Same tail as COPY.** `BackfillTuple` → gate → `pipeline::bootstrap::drain`
  → dedicated insert tail (own CH connection); the live pipeline never blocks
  on a backfill. Rows route via a per-pass snapshot of the mapping pointed at
  staging tables — the destination is untouched until publish (§Staging
  swap). The pass resolver shares the pipeline's memory budget
  (`PassContext`, [emitter.md](emitter.md) Memory budget). Regime A
  ([config.md](config.md) §Failure containment) holds: a failed pass
  leaves every entry pending in the ledger, never poisons the pump
- **Ledger.** `backfills.toml` entries carry `mode` (absent ⇒ `copy`) and the
  staging-swap phase (`swapped` + staging uuid, §Staging swap); boot re-runs
  the recorded mode at the recorded `S` — or resumes the swap tail — the
  config row's current mode applies only to a fresh entry. Dedup keeps
  re-runs idempotent. `note_opt_out`: entry drops, a queued request withdraws
  before its pass fires; an in-flight walk keeps loading its staging table
  (the pass routes via its own snapshot) and the publish discards an unmapped
  rel's staging instead of swapping

## `_lsn` tagging invariant

Walked rows must lose to every WAL-delivered mutation the backup state does
not already reflect. Rule: **tag walked rows with the LSN where continuous
WAL coverage of the rel begins**, never later. Tags are per-rel
(`PageWalkSink::with_lsn_overrides`); a rel's toast rows tag with their
parent.

- `'base_backup'`: backup starts at `B ≥ S`, live WAL covers `(S, ∞)` → tag
  `_lsn = S`. A delete committed in `(S, B)` may leave a dead tuple that the
  visibility gate misses only if `pg_xact` hasn't caught up in the tar; its
  `_is_deleted` row at `commit_lsn > S` must outrank the walked copy. Tagging
  `B` would resurrect it
- `'object_store'`: the backup normally predates the opt-in; the gap
  `(B_redo, S]` is covered by archive replay, not the live stream → tag
  `_lsn = min(B_redo, S)` (`B_redo` = the backup's start LSN, from the
  sentinel). Replayed rows carry real commit LSNs `> B_redo` and win; tagging
  `S` would let stale walked rows beat fresher gap-replay rows. A backup
  *newer* than a rel's opt-in needs no replay for it and its rows tag `S`

## base_backup mode

Issue `BASE_BACKUP` on a second replication connection once the opt-in
applies (`wal: false` — WAL rides the live stream). The protocol is
cluster-scoped (PG `src/backend/backup/basebackup.c` walks the whole datadir;
no per-rel filter exists), so the tar streams the entire cluster past the
daemon and the filter discards all but the target filenodes — bandwidth is
cluster-sized even for one table. Coalescing concurrent opt-ins amortizes the
pass.

No replay leg: any xact the tar catches mid-write commits after `S`, so the
live stream re-delivers it at its real commit LSN (pre-`S` heap records were
buffered inclusion-agnostically by the pump). Pre-`S` commits are durable in
`pg_xact` before `BASE_BACKUP` starts, so the gate never reads ahead of copied
`pg_xact` for pre-`S` xids. Torn pages lose only tuples whose writers are
post-`S` — strictly safer than greenfield's no-FPI-replay caveat, where
nothing replays `[start_lsn, end_lsn]`.

## object_store mode

1. **Resolve backup.** `resolve_name(LATEST)` + `fetch_sentinel` on the wal-g
   bucket (`WALG_*` env, same surface as bootstrap) → `B_redo`/`B_end` LSNs,
   timeline. Delta chains reject (`increment_from` set) with the same
   operator-actionable error as bootstrap: the streaming walk has no
   disk-resident base to overlay
2. **Gap fetch + pre-scan.** Fetch archive WAL `B_redo → max(S)` (wal-rus
   `pg::wal::fetch`) into scratch and sweep records-only: harvest the pg_xact
   patch, and abort on catalog skew touching filtered rels — a `pg_attribute`
   write on a filtered rel, a `pg_class` write changing a filtered rel's
   filenode, a catalog write whose row oid is undecodable (`OidInPrefix`, so
   possibly a filtered rel), an `RM_RELMAP` update (mapped-catalog rewrite),
   or an `XLOG_HEAP_TRUNCATE` naming a filtered oid. Any hit aborts before a
   row is emitted: the walk would decode with the wrong shape, and a rewrite
   in the gap means the backup's filenode isn't the current rfn at all. Error
   names the remedies — fresher backup, or `'copy'`. A timeline switch or
   archive gap surfaces as a fetch failure with the same remedies
3. **Filtered walk.** Tar parts stream through `PageWalkSink` with the filter
   set; rows tag `_lsn = min(B_redo, S)` per rel; the gate resolves deferred
   tuples against backup pg_xact + patch at successful walk EOF
4. **Gap replay.** The fetched segments drive the shared decode path
   (`BufferingDecoderSink` + `ReplaySink` over `drain_committed` +
   `into_walk`, so subxacts, TOAST reassembly and update/delete decode
   match the hot path) over records whose block-0 rfn is in the filter
   set; committed rows ship at real commit LSNs through the same insert
   tail, continuing the walk's seq space. Commits `≤ B_redo` drop (the
   walked copy carries them); commits `> S` drop per rel — the live
   stream owns them (dedup absorbs overlap regardless)

Convergence: walk EOF + gap replay reaching `S`. As with COPY, completion is
observability, not correctness — nothing gates on it. No gap-length guard: a
months-old backup makes `B_redo → S` replay dominate cost, where `'copy'` is
strictly cheaper — the pass runs regardless and the segment count is logged.

## Staging swap

Backup-mode passes never insert into the destination
(`src/backfill_staging.rs`). Rows land in a per-rel staging table
(`<table>__wsstg`, same database, `CREATE TABLE .. AS` structure clone,
rebuilt from scratch per attempt); a successful pass publishes per rel:

1. **Gate.** Destination and staging schemas must still fingerprint equal
   (`system.columns` name+type by position) — mid-pass DDL means the loaded
   copy has the pre-DDL shape; discard staging, entry stays pending, next
   boot re-loads. A rel unmapped at publish (opt-out mid-pass) discards the
   same way
2. **Exchange.** Persist `swapped` + the staging table's uuid in the ledger,
   *then* `EXCHANGE TABLES`. Order is load-bearing: post-swap the staging
   name holds the only copy of the live-window rows, and a pending-looking
   entry would re-run the pass and rebuild staging over them. EXCHANGE never
   blindly resends — an ambiguous timeout may have applied and a resend
   would swap back; the recorded uuid disambiguates at recovery
3. **Copy-back.** Live rows delivered during the pass resolved the
   destination name pre-swap, so they sit in the swapped-out storage:
   `INSERT INTO real (cols) SELECT cols FROM staging WHERE _lsn > S`. The
   filter is what keeps a re-opt-in purge effective — only live-window rows
   carry `_lsn > S`; prior-life rows stay purged. The column list is the two
   tables' intersection (destination order) so DDL applied after the swap
   can't wedge it. Runs one `insert_timeout` after the exchange: an INSERT
   that resolved the name pre-swap finishes into the old storage within one
   attempt cap, later attempts re-resolve to the swapped-in table
4. **Drop + done.** Staging (now the pre-swap storage) drops; ledger marks
   done

Why: a failed pass flushes nothing visible, so no retry has to tombstone what
an earlier attempt leaked. Without staging, a failed `'object_store'` attempt
retried against a newer `LATEST` leaves phantoms: a row deleted between the
two backups is on neither the retry's pages nor inside its replay window, so
its stale copy survives every rerun. Staging also lets a re-opt-in purge
stale rows wholesale (mutations during the opted-out window were never
delivered) and gives readers an atomic cutover instead of a growing table.

Boot recovery for a `swapped` entry resumes the tail instead of re-loading;
the staging name's uuid tells the phase apart — unchanged ⇒ exchange never
applied (re-gate schemas, exchange); changed ⇒ exchange applied (copy-back +
drop); missing ⇒ only the done mark is owed. A failed pass joins its whole
tail before surfacing, so no detached inserter can final-flush into a staging
table a retry has already rebuilt.

Caveats: `EXCHANGE TABLES` needs an Atomic/Replicated database engine (the
modern default); `CREATE TABLE .. AS` clones engine args, so a `Replicated*`
destination with a hard-coded ZooKeeper path (no `{uuid}` macro) collides;
materialized views on the destination never fire for backfill rows (staging
inserts don't trigger them) and fire twice for copy-back rows.

## Observability

- `config_backfills_pending` gauge keeps counting all modes; per-mode
  labelled series (`mode="copy|base_backup|object_store"`) split it
- Per-pass INFO summary: rows walked / gated / deferred, multixact emits,
  gap segments + records replayed, pg_xact segments + patch size, `B_redo`

## Anti-goals

- **No shadow involvement.** Backup bytes never land in a data dir; the modes
  add no second shadow or scratch cluster
- **Bucket/creds stay out of the overlay.** Object-store settings ride the
  same `WALG_*` env surface bootstrap uses, never the source-PG config
  tables — credentials in a source-PG table is the wrong trust direction
- **No server-side filtering.** Do not fork the BASE_BACKUP protocol;
  cluster-sized bandwidth is the documented cost of `'base_backup'`, and
  `'object_store'` is the answer when that cost bites

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
- **Failed pass leaves destination untouched.** Kill the source mid-walk;
  destination carries only live rows, the partial load sits in
  `<table>__wsstg`, ledger stays pending. Retry rebuilds staging and
  publishes exactly once
- **Swap crash recovery.** Crash between `EXCHANGE` and copy-back; boot
  resumes from `swapped`: uuid mismatch ⇒ exchange already applied, copy-back
  delivers the live-window rows (`_lsn > S`), staging drops, entry marks done
