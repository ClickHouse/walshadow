# walshadow glossary

Terms of art used across [plans/](INDEX.md) component docs. Each entry
gives working meaning plus doc that owns the mechanism; read the doc for
detail. PG / CH vocabulary appears only where walshadow bends it.
Alphabetical, leading `_` ignored

**2A** — initial-load shape: walk on-disk backup pages through same heap
decoder as WAL hot path, zero codec drift. Sole initial-load path; 2C
(CH-side parallel COPY against an exported snapshot) rejected to avoid a
second per-OID codec ([bootstrap.md](bootstrap.md))

**ack collector** — pipeline stage counting rows placed (decode-routed)
vs acked (insert-drained) per seq; publishes contiguous-done watermark
that becomes `emitter_ack_lsn` ([emitter.md](emitter.md))

**auto_create** — namespace flag letting DDL applicator
`CREATE TABLE IF NOT EXISTS` on first sight of a relation and derive its
mapping; per-table config still wins ([emitter.md](emitter.md))

**B_redo / B_end** — backup start (redo) and end LSNs from backup
sentinel. `object_store` backfill tags walked rows
`_lsn = min(B_redo, S)`, bridges gap `(B_redo, S]` via archive replay
([add_table.md](add_table.md))

**backfill** — per-table initial load triggered by opt-in, mode chosen
by `initial_load`. Dispatch + coalescing live in the backfiller
(`src/copy_backfill.rs`), backup modes in `src/backup_backfill.rs`
([add_table.md](add_table.md))

**BackfillTuple** — `{rfn, xid, source_lsn, columns}` unit page walk
ships over bounded mpsc to bootstrap drain; channel cap backpressures
tar pump against drain rate ([bootstrap.md](bootstrap.md))

**BackupSource / BackupSink** — traits bounding backup plumbing: source
(`Direct` BASE_BACKUP or `ObjectStore` wal-g bucket) pumps every backup
file through sink, returns `(StartInfo, EndInfo)` LSN pair; sink decides
per-file `FileAction` ([bootstrap.md](bootstrap.md))

**barrier / barrier fence** — synchronous ordering region around
DDL / TRUNCATE / config apply: wait placed, `FlushAll`, wait durable,
then apply. Global, acceptable because barriers run at DDL rate
([emitter.md](emitter.md))

**baseline ledger** — see prev_known

**bootstrap** — greenfield initial attach: stream source `BASE_BACKUP`
once through `MultiplexSink`, catalog files land on shadow data dir
while user-heap pages Tap through page walk into shared insert tail;
handoff to WAL pump at `end_lsn` ([bootstrap.md](bootstrap.md))

**BufferingDecoderSink** — decoder-side record sink: gates on catalog
replay, decodes heap records into XactBuffer, intercepts TRUNCATE and
config writes. Shared by hot path and gap replay
([source.md](source.md), [decoder.md](decoder.md))

**catalog gate** — see wait_for_replay

**catalog-only constraint** — user-heap bytes pass through daemon during
bootstrap but never settle on shadow's data dir; keeps shadow MiB-scale
by construction ([bootstrap.md](bootstrap.md))

**catalog replay** — shadow PG applying filtered catalog WAL, so
`pg_catalog` tracks source DDL and relfilenode rewrites with zero
operator coordination ([overview.md](overview.md))

**catalog seed** — bootstrap phase building CatalogMap: REPEATABLE READ
SELECTs over `pg_class`/`pg_attribute`/`pg_type`/`pg_index` for
`oid >= 16384`, snapshot so concurrent DDL doesn't tear
([bootstrap.md](bootstrap.md))

**catalog set / catalog whitelist** — `(db_node, rel_node)` filenodes
filter treats as catalog: bootstrapped from
`oid < FirstNormalObjectId`, tracked live by CatalogTracker
([filter.md](filter.md))

**catalog skew** — gap-WAL catalog write touching a backfilled rel
(pg_attribute write, filenode-changing pg_class write, RELMAP update,
TRUNCATE); aborts backup backfill before any row emits, walk would
decode with wrong shape ([add_table.md](add_table.md))

**CatalogMap** — snapshot `filenode → descriptor` map from catalog seed;
bootstrap drain resolves against it directly, no replay gate
([bootstrap.md](bootstrap.md))

**CatalogTracker** — live catalog-filenode set plus per-db pg_class
filenode map, fed by RELMAP records and pg_class heap writes; survives
VACUUM FULL / CLUSTER / REINDEX rotations ([filter.md](filter.md))

**classify** — per-record bucketing into Special / Catalog / User /
Empty driving filter decision. Special rmgrs pass verbatim; User
upgrades to Keep when tracker holds the block ref
([filter.md](filter.md))

**coalescing** — burst of backup-mode opt-ins collapses into one
cluster-sized pass per mode inside fixed 1 s window
(`BACKUP_COALESCE_WINDOW`) ([add_table.md](add_table.md))

**ColumnValue** — decoded per-column value enum: Tier 1 fixed-width
variants, Tier 2 varlena (`Bytea`/`Text`/`Json`), `ExternalToast`,
`PgPending`, `Null`, `Unsupported` ([decoder.md](decoder.md))

**config precedence** — three-layer merge, highest wins:
CLI > PG row > TOML; snapshot rebuilt whole per republish so it never
tears ([config.md](config.md))

**config\_\* tables** — four DBA-written tables on source PG
(`config_global`, `config_namespace`, `config_table`, `config_column`)
carrying overlay config; installed by `sql/runtime_config_install.sql`,
REPLICA IDENTITY FULL, text-keyed so forward declaration works
([config.md](config.md))

**ConfigEvent** — typed event interpreted from an intercepted
config-table write; rides `DrainEntry::Config`, applies at row's commit
LSN under barrier fence ([config.md](config.md))

**ConfigResolver** — layered resolver merging TOML base, PG overlay, CLI
into `ResolvedConfig` snapshots on a watch channel; SIGHUP re-reads
TOML, malformed values reject at merge leaving prior value in effect
([config.md](config.md))

**contiguous-done watermark** — highest seq with `placed == acked` and
every predecessor done; its `commit_lsn` is the durable frontier slot
feedback advances on ([emitter.md](emitter.md))

**convergence** — backfill completion: walk EOF plus gap replay reaching
S. Observability only, nothing gates on it
([add_table.md](add_table.md))

**cursor / `cursor.bin`** — 64-byte durable file in spill dir persisting
six LSNs + CRC across `kill -9`, written by atomic rename;
`emitter_ack_lsn` is the load-bearing resume LSN ([ops.md](ops.md))

**DdlApplicator** — applies SchemaEvents to CH in source-LSN order
inside the barrier, own connection, no retry (error trips Fatal); also
runs TRUNCATE and gated DROP per DropTableStrategy
([emitter.md](emitter.md))

**decode pool** — pipeline decode workers ×M: detoast, catalog resolve,
mapping lookup, PgPending resolve, route rows to batcher, report
`Placed` after xact's last row ([emitter.md](emitter.md))

**DecodedHeap / DecodedTuple** — per-logged-tuple decode product.
Columns indexed attnum-1 to catalog length; `Some(Null)` explicit NULL,
`None` absent-from-WAL, `partial` flags elision/truncation
([decoder.md](decoder.md))

**denylist** — volatile system dirs (`pg_replslot`, `pg_stat_tmp`, …)
whose backup file contents skip while dir entries land empty
([bootstrap.md](bootstrap.md))

**DiskLanderSink** — bootstrap sink landing catalog and system files
under shadow's data dir; user-heap files route to Skip or Tap
([bootstrap.md](bootstrap.md))

**drain** — commit-time flush of an xact's buffered heaps, reassembled
TOAST, and ordered events; `drain_committed` yields `DrainedXact`
consumed by reorder coordinator ([xact.md](xact.md))

**drain_lsn** — cursor LSN advancing before `on_xact_end` ack;
`drain_lsn > emitter_ack_lsn` gap is how an observer failure surfaces
([xact.md](xact.md))

**DrainEntry** — drain item kinds: Tuple / Catalog / Config.
Catalog-before-tuple tie-break at equal `(xid, source_lsn)` so ALTER
lands on CH before dependent INSERT encodes
([xact.md](xact.md), [config.md](config.md))

**emitter_ack_lsn** — contiguous-done commit LSN: every xact at/below is
durable on CH. Advertised as standby `apply_lsn`, bounds source slot
recycling, resume point after restart
([emitter.md](emitter.md), [ops.md](ops.md))

**FileAction (Keep / Skip / Tap)** — per-backup-file sink decision: land
body under data dir, drain unread, or stream body through `chunk()`
with nothing landing (page walk, pg_xact accumulation)
([bootstrap.md](bootstrap.md))

**filter** — per-record keep/drop engine on WalStream: kept records pass
(catalog-only blocks retained, CRC32C recomputed), drops NOOP-rewrite in
place. Invariant `filtered_lsn == source_lsn`, byte offsets preserved so
no LSN translation exists downstream ([filter.md](filter.md))

**filter contract** — rmgr-level keep table: heap when record touches
catalog set, btree when catalog index, xact / clog / multixact /
standby / relmap / smgr / dbase / tblspc wholesale, everything else
drops ([overview.md](overview.md))

**FirstNormalObjectId** — 16384; oid / filenode threshold below which a
relation is a system catalog ([filter.md](filter.md))

**FlushAll** — batcher message sealing every table, dropping encoders,
bumping schema_epoch; barrier and shutdown path
([emitter.md](emitter.md))

**forward declaration** — `config_table` row whose relation doesn't
exist yet; parked keyed on qualified name, materialized when matching
CREATE TABLE arrives ([config.md](config.md))

**FPI** — full-page image on a WAL block ref; `restore_block_image`
rebuilds 8 KiB page per compression method (none/pglz/lz4/zstd). Serves
page walk and TOAST re-read, never the tuple-bytes path
([decoder.md](decoder.md))

**gap replay** — `object_store` step fetching archive WAL
`B_redo → S` into scratch, replaying committed rows at real commit LSNs
through shared decode path ([add_table.md](add_table.md))

**generation counter** — ShadowCatalog cache invalidation: single u64
bumped on any pg_class write. Coarse-fires by design, over-invalidates
but cannot under-invalidate ([shadow.md](shadow.md))

**greenfield** — fresh attach, no cursor on disk; bootstrap scenario and
resume fallback when cursor read fails
([bootstrap.md](bootstrap.md), [ops.md](ops.md))

**heap decoder** — in-tree WAL heap-tuple decoder
(`src/heap_decoder.rs`); page walk reshapes on-disk tuples into same
`xl_heap_header` shape so one decoder serves both paths
([decoder.md](decoder.md))

**HeapOp** — decoded operation: Insert / Update / HotUpdate / Delete /
Truncate ([decoder.md](decoder.md))

**initial_load** — per-table pre-opt-in row source: `none`, `copy`
(live COPY at `_lsn = S`), `base_backup` (fresh BASE_BACKUP),
`object_store` (wal-g bucket + gap replay)
([add_table.md](add_table.md), [config.md](config.md))

**InsertBatcher** — single hub task owning one TableEncoder per dest
table; rows from all decoders merge into one part per flush window,
seals into `InsertBatch` (owned column slabs + per-seq row counts) for
inserter pool ([emitter.md](emitter.md))

**inserter pool** — ×N CH connections pulling sealed batches off shared
mpmc queue; ack fires only after send drains to `EndOfStream`, retries
resend the still-owned batch and `_lsn` dedup absorbs duplicates
([emitter.md](emitter.md))

**install probe** — `config_global` read at seed: schema named in TOML
but not installed refuses boot, keeping overlay opt-in explicit
([config.md](config.md))

**invalidation_epoch** — shared `AtomicU64` bumped on relmap / pg_class
writes and shape-changing config events; ShadowCatalog loads it per
lookup, `pg_class_delete_epoch` is the narrower DROP-only sibling
throttling `sweep_dropped` ([filter.md](filter.md),
[shadow.md](shadow.md))

**keep-fraction** — share of records filter keeps: ~0.04% steady OLTP,
8%+ in DDL-heavy windows ([filter.md](filter.md))

**ledger (`backfills.json`)** — durable record of backfills with mode
and S; boot re-runs recorded mode at recorded S, `_lsn` dedup keeps
re-runs idempotent ([add_table.md](add_table.md))

**_lsn** — synthetic UInt64 commit-record LSN column;
`ReplacingMergeTree(_lsn)` dedup key. Every overlap story (restart
replay, backfill, retry, bootstrap) collapses through it
([emitter.md](emitter.md))

**_lsn tagging invariant** — walked backfill rows tag with LSN where
continuous WAL coverage of the rel begins, never later, so they lose to
every WAL-delivered mutation the walked state doesn't reflect
([add_table.md](add_table.md))

**manifest** — per-segment sidecar indexing record
`{offset, len, rmid, info, kind}` plus filter stats
([filter.md](filter.md))

**mapped catalogs** — pg_class, pg_attribute, pg_type, pg_proc and
shared catalogs; filenode rotations ride `XLOG_RELMAP_UPDATE` rather
than pg_class heap writes ([filter.md](filter.md),
[decoder.md](decoder.md))

**MappingHandle** — live `Arc<RwLock<HashMap<String, TableMapping>>>`
decode pool consults per row; refresher / SIGHUP swap it whole, cached
encoders rebuild at next barrier ([emitter.md](emitter.md),
[config.md](config.md))

**MultiplexSink** — bootstrap sink composing DiskLanderSink with a Tap
sink (PageWalkSink), per-file dispatch over one backup pass
([bootstrap.md](bootstrap.md))

**Native** — ClickHouse Native wire format (column blocks); emitter's
target format, rebuilt by inserter over batch's owned slabs
([emitter.md](emitter.md))

**NOOP rewrite** — dropped record's bytes mutated in place into
`XLOG_NOOP` of identical `xl_tot_len` with recomputed CRC32C, so
`xl_prev` chain stays valid and shadow recovery never sees a gap
([filter.md](filter.md), [overview.md](overview.md))

**opt-in** — two related switches: `[runtime_config] schema` enables
whole overlay subsystem; `config_table.replicate` opts one table into
replication, triggering backfill per `initial_load`
([config.md](config.md), [add_table.md](add_table.md))

**oracle** — differential decode oracle: shadow re-decodes on-disk bytes
via `walshadow_decode_disk` (same `typoutput` PG would call), diffed
against local decode. `--validate N` samples 1-in-N; mismatch is a
watchdog signal, row still ships ([oracle.md](oracle.md))

**ordered_events** — DrainedXact's catalog/config event positions
interleaved with heaps; pipeline walks them as barrier segments
([xact.md](xact.md))

**overlay** — PG-row config layer: DBA-written `config_*` rows on source
PG, replicated through WAL, applied at each row's commit LSN
([config.md](config.md))

**PageWalkSink** — Tap sink walking user-heap backup pages 8 KiB at a
time, decoding `LP_NORMAL` slots through shared heap decoder, emitting
BackfillTuples with per-rel `_lsn` overrides
([bootstrap.md](bootstrap.md))

**parity check** — end-state comparison (count + sum + md5 of aggregated
rows) between source and CH proving replication fidelity
([ops.md](ops.md))

**PgPending** — ColumnValue fallback `{type_oid, raw}` for types without
in-tree codec (jsonb, ranges, arrays, tsvector, vendor types); resolved
at emit via oracle bridge, raw bytes pass through when extension absent
([decoder.md](decoder.md), [oracle.md](oracle.md))

**pgext** — walshadow PG extension (PGXS, shadow-only) exposing
`walshadow_decode_disk(oid, bytea) -> text`
([oracle.md](oracle.md))

**PgXactAccum / PgXactPatch** — backup-era `pg_xact` accumulated from
backup files, patched with commit/abort records harvested from gap-WAL
pre-scan; backs the visibility gate ([add_table.md](add_table.md))

**pipeline** — parallel decode+insert tail: reorder → decode ×M →
batcher → inserter ×N → ack watermark, in `src/pipeline/`; stands up
only with `--ch-config` ([emitter.md](emitter.md))

**placed** — decoder's per-seq report that all a xact's rows are routed
to batcher; seq is done at `placed == acked`
([emitter.md](emitter.md))

**poisoned** — WalStream error state: subsequent pushes short-circuit,
recovery is fresh stream + upstream connection at durable
`dispatched_lsn` ([source.md](source.md))

**pre-scan** — records-only sweep of fetched gap WAL harvesting
PgXactPatch and aborting on catalog skew ([add_table.md](add_table.md))

**preflight** — boot validators (PG ≥ 16, major match, `wal_level`,
usable replica identity, slot existence) aggregated into one report so
multiple findings surface at once; `--skip-preflight` for drills
([ops.md](ops.md))

**prev_known / baseline ledger** — last source shape CH and source
agreed on, per oid; decides SchemaEvent `Added` vs `Changed`.
`seed_baseline` warms it for pinned rels at boot so a cold cache never
mis-branches post-boot ALTER ([shadow.md](shadow.md),
[emitter.md](emitter.md))

**QueueingRecordSink** — unbounded mpsc decoupling pump from decoder's
replay wait; `soft_cap` yields past threshold, hard bound deliberately
absent (would couple wire pacing to decode) ([source.md](source.md))

**read-time defaults** — writer's natts below catalog count (pre-ALTER
physical tuple) fills from `RelAttr.missing_text`, PG-text form of
`attmissingval` planted by fast-path `ADD COLUMN ... DEFAULT k`
([decoder.md](decoder.md))

**rebind / rebuild** — shadow dispositions after source failover: keep
catalog and replay from new primary, or reinit for diverged clusters
([overview.md](overview.md))

**RecordSink / RecordBytesSink** — fan-out traits everything hangs off:
`on_record` parsed path, `on_wire_chunk` byte path, one walk of bytes
serves both ([source.md](source.md))

**Regime A (failure containment)** — with WAL pump alive, malformed
config value rejects at merge and prior value stays; never crashes,
pauses, or abandons other keys ([config.md](config.md))

**RelDescriptor / RelAttr** — per-relation / per-column catalog product
from ShadowCatalog; dropped columns retained so decoder can walk null
bitmap ([shadow.md](shadow.md))

**ReplacingMergeTree(_lsn)** — CH dest engine: max-`_lsn` version per
key wins on FINAL, end state order-independent. Substrate for
eventual-consistency promise ([emitter.md](emitter.md))

**ReplIdent** — resolved replica identity: `Default{pk_attnums}`,
`Nothing`, `Full`, `UsingIndex{..}`; preflight rejects `Nothing` and
keyless `Default` ([shadow.md](shadow.md))

**reorder coordinator** — single-threaded commit-order boundary matching
`RM_XACT_ID` records; assigns seqs, dispatches decode jobs, owns
barriers ([emitter.md](emitter.md))

**restore_command fallback** — shadow's archive channel
(`cp <out_dir>/%f %p`) at segment cadence when walsender wire drops or a
slow connection is cut off ([shadow.md](shadow.md))

**retention** — sweeper trimming filtered segments whose end LSN falls
below `replay_lsn - retention_bytes`; `--retention-bytes 0` disables
([ops.md](ops.md))

**rfn** — RelFileLocator `(db_node, rel_node)`; relation identity in WAL
block refs. Unique only within a database, hence foreign-DB skip
([decoder.md](decoder.md), [shadow.md](shadow.md))

**rfn flip** — bootstrap drain boundary between one relfilenode's rows
and the next; each flip closes one synthetic ack seq (bootstrap has no
commit boundaries) ([bootstrap.md](bootstrap.md))

**row_budget / byte_budget / flush_timeout** — batch seal triggers
(defaults 65536 rows / 1 MiB / 100 ms floor); live-reloadable knobs read
by batcher per seal decision ([emitter.md](emitter.md),
[config.md](config.md))

**S** — WAL resume LSN for an opt-in: config row's commit LSN live, WAL
resume LSN on boot re-run. `copy` and `base_backup` rows tag
`_lsn = S`; "apply everything from S, discard nothing"
([add_table.md](add_table.md), [config.md](config.md))

**schema_epoch** — counter bumped per FlushAll; table plans and inserter
type caches key on it so post-DDL rows rebuild against new descriptors
([emitter.md](emitter.md))

**SchemaEvent / SchemaDiff** — catalog-change events
(`Added` / `Changed{diff}` / `Dropped`) from ShadowCatalog subscription,
riding xact buffer keyed `(xid, source_lsn)`. Diff carries added /
dropped / renamed columns; type changes reject to operator
([shadow.md](shadow.md), [emitter.md](emitter.md))

**seal** — closing an open batch into a finished INSERT unit on budget,
deadline, or barrier ([emitter.md](emitter.md))

**seed_from_source** — one-shot attach-time query of source for every
catalog `(oid, filenode)` pair, closing the rotated-before-attach hole
in tracker state ([filter.md](filter.md))

**seq** — dense commit-order sequence per drained xact (aborts and empty
commits get rows=0 seqs); unit of ack accounting
([emitter.md](emitter.md))

**shadow PG** — co-located schema-only Postgres standby replaying
filtered catalog WAL; live catalog oracle for every decoder lookup,
never hosts user-heap data, never written locally (local write would
diverge offset-exact pages and PANIC on replay)
([overview.md](overview.md), [shadow.md](shadow.md))

**ShadowCatalog** — async libpq cache over shadow's unix socket:
replay-gated `relation_at`, generation-checked descriptor cache,
SchemaEvent channel, auto-reconnect with transient retry
([shadow.md](shadow.md))

**shadow-zero carve-out** — `shadow_replay_lsn == 0` treated as "no
constraint from shadow" so standby `apply_lsn` uses `emitter_ack_lsn`
alone; otherwise source slot freezes at 0 ([ops.md](ops.md))

**shared catalogs** — `global/` catalogs (`pg_database`, `pg_authid`,
`pg_tablespace`, `pg_shdepend`) with `dbNode = 0`; kept unconditionally,
shadow won't start without them ([overview.md](overview.md))

**slot** — physical replication slot on source, optional: present pins
`pg_wal/` until `apply_lsn` advances, absent relies on `wal_keep_size`
window. Advance keys on `apply_lsn = min(shadow_replay, emitter_ack)`
([source.md](source.md), [ops.md](ops.md))

**soft_delete** — config keeping `_is_deleted` out of engine args so
delete tombstones survive FINAL instead of collapsing
([emitter.md](emitter.md))

**spill** — append-only per-xid disk overflow at
`{spill_dir}/xid-<xid>-<first_lsn>.bin` past 64 MiB budget,
largest-first eviction; cleared on boot per resume contract
([xact.md](xact.md))

**standby-status triple** — `{write, flush, apply}` sent on replication
socket: `source_received_lsn` / `filter_durable_lsn` /
`min(shadow_replay, emitter_ack)`, each clamped to per-field monotonic
high-water ([ops.md](ops.md), [source.md](source.md))

**status line** — per-tick log, single choke point in `bin/stream.rs`;
diverging `shadow_apply` vs `dispatched` signals shadow lag
([ops.md](ops.md))

**SubxactTracker** — parent/children maps fed by
`XLOG_XACT_ASSIGNMENT`; a hint only, authoritative subxact list arrives
inline on commit/abort record ([xact.md](xact.md))

**synthetic columns** — four trailing columns on every dest table:
`_lsn` UInt64, `_xid` UInt32, `_commit_ts` DateTime64(6,'UTC'),
`_is_deleted` Bool ([emitter.md](emitter.md))

**tail** — reusable batcher + inserter pool + ack collector unit; WAL
pipeline and bootstrap drain feed the identical tail, `tail.finish`
seals partials and waits all seqs durable
([emitter.md](emitter.md), [bootstrap.md](bootstrap.md))

**Tier 1 / 2 / 3** — decoder type matrix: fixed-width (`type_len > 0`)
decoded in place, varlena (`type_len == -1`) walked in-tree, hot Tier 3
types (numeric, inet, cidr, interval, json) via `codecs.rs`, everything
else routed `PgPending` to oracle ([decoder.md](decoder.md))

**TOAST chunk store** — `[toast] mode` selecting store of record for
chunks so reassembly doesn't depend on WAL adjacency: `disabled`
(NULL/default-fill on miss, counted), `disk`, `clickhouse` (chunk rows
in `pg_toast_<relid>` ReplacingMergeTree table)
([TOAST.md](TOAST.md))

**ToastPointer / ExternalToast** — on-disk external varlena pointer
(`va_toastrelid`, `va_valueid`, `va_extinfo`, `va_rawsize`) surfacing as
`ColumnValue::ExternalToast`; resolved via xact-buffer reassembly or
chunk-store fetch ([decoder.md](decoder.md), [TOAST.md](TOAST.md))

**two-phase gap** — `XLOG_XACT_PREPARE` ignored; PREPARE ↔
COMMIT PREPARED across daemon restart loses prepared writes since
buffer state is process-local and spill clears on boot
([xact.md](xact.md))

**type_bridge** — PG-OID → CH type mapper: `pk_member` strips
`Nullable(_)` (CH refuses Nullable in ORDER BY), constrained
`numeric(p,s)` maps `Decimal(p,s)` else String
([emitter.md](emitter.md))

**visibility gate** — backup-page tuple filter (`src/visibility.rs`):
emit only when backup-era pg_xact says xmin committed and xmax
absent/aborted, infomask hint bits short-circuit; what makes backup
modes higher-fidelity than greenfield's raw walk
([add_table.md](add_table.md))

**wait_for_replay / catalog gate** — ShadowCatalog gate blocking until
`pg_last_wal_replay_lsn() >= commit_lsn`; enforces ordering invariant
shadow replay LSN ≥ decoder read LSN so decoder reads post-DDL catalog
([shadow.md](shadow.md), [overview.md](overview.md))

**wal-rus** — WAL parsing / replication crate walshadow builds on:
replication client and server halves, record parser, BASE_BACKUP and
object-store fetch primitives

**walsender server** — walshadow's server side of physical replication
protocol, making walshadow shadow's primary; filtered WAL flows
record-by-record so shadow replay advances at ms cadence.
Trust-over-loopback only; slow connections cut past send-queue
threshold, shadow catches up via restore_command
([source.md](source.md), [shadow.md](shadow.md))

**WalStream** — page-cadence ingest: `push(lsn, bytes)` drives stateful
walker (cross-page stitching, 16 MiB segment buffer), fires per-record
dispatch through sink fan-out ([source.md](source.md))

**XactBuffer** — per-xid hold-and-flush of decoded heaps + TOAST chunks
until commit/abort, spilling past budget; PG `ReorderBuffer` analogue
minus snapshot building (catalog state lives in shadow)
([xact.md](xact.md))

**xact / xid** — transaction / transaction id, PG shorthand used
throughout; top-level xid owns buffering, subxacts fold in at
commit-record authority ([xact.md](xact.md))

## future/ vocabulary

Coinages from [future/](future/INDEX.md) proposals referenced across
docs:

**degraded mode** — resolver freezes overlay at last-known state and
falls back to TOML+CLI when WAL pump can't keep config fresh past
staleness threshold
([future/runtime_config_from_pg.md](future/runtime_config_from_pg.md))

**pinned DDL baseline invariant** — cache warmth must never decide which
CH SQL runs; schema-event outcome must be pure function of config +
baseline
([future/pinned_ddl_baseline.md](future/pinned_ddl_baseline.md))

**signal channel** — out-of-band imperatives (`flush_now`,
`pause_emitter`, `ignore-transaction`, …) carried in
`pg_logical_emit_message` WAL records, gated on `admin_database`
([future/runtime_config_from_pg.md](future/runtime_config_from_pg.md))

**sync-commit witness** — walshadow as RPO=0 quorum acker in
`synchronous_standby_names ANY 1 (walshadow, fullpg)`; relay mode ships
surviving WAL to lagging standby across the failover bridge
([future/sync_commit_witness.md](future/sync_commit_witness.md))

**temporal catalog** — bounded `(oid, valid_from_lsn, valid_to_lsn,
descriptor)` schema-history store; earns its keep only with a second
consumer
([future/pinned_ddl_baseline.md](future/pinned_ddl_baseline.md))

**wire/record split** — proposed pump backpressure fix: wire delivery to
shadow runs ahead paced by apply, record dispatch blocks on bounded
queue
([future/pipeline_backpressure_and_scaling.md](future/pipeline_backpressure_and_scaling.md))
