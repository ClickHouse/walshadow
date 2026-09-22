# Current limits

Review these limits before production use

## PostgreSQL

- PostgreSQL 16, 17, 18, and 19, daemon rejects unaudited majors
- shadow PostgreSQL major must match source major
- one process replicates every database its config names
  ([several databases](multi-database.md)); adding or removing one needs a
  restart, and existing rows load from a backup only for the database
  `[source] dbname` names
- `wal_level = logical` required
- every replicated table needs usable replica identity
- prepared transactions are not supported for production use; commit and abort
  records are handled, but full restart and bootstrap cases still need validation
- sequence state is not replicated, values already stored in table rows still replicate
- non-default tablespaces are unsafe for bootstrap and managed-shadow lifecycle
- unplanned primary promotion is not supported

Unsupported behavior is not uniformly rejected at startup. Review source schema
and workload limits before attaching

## ClickHouse

- destination uses ClickHouse Native protocol
- source column type changes require manual ClickHouse migration
- `CREATE TABLE` omits a fast default only shadow PostgreSQL can render (raw
  arrays); the table it creates is empty, and `ADD COLUMN` resolves the
  default through the oracle
- `time` mapping requires ClickHouse `Time64` support
- same-named tables from different PostgreSQL schemas or databases need
  explicit destination mapping. Config parsing rejects conflicts between
  explicit entries; `replicate_all` names destinations after source tables, so
  the second table to claim one destination is refused with an error and left
  unreplicated until it names its own `target_database` or `target_table`
- `base_backup` and `object_store` table loads publish with staging-table swap, database must support `EXCHANGE TABLES`
- backup rows inserted into staging do not fire destination materialized views, live rows copied back after swap can fire twice

## Types shadow PostgreSQL converts

Values outside walshadow's own codec set (arrays, `hstore`, enums, ranges,
domains, extension types) are converted by shadow PostgreSQL, one request per
insert batch

- a value shadow PostgreSQL cannot convert stops the batch and names the
  column and row, rather than writing a substituted value
- a multidimensional array does not fit the default one-layer `Array(...)`
  mapping; map the column to a matching nested `Array(Array(...))` instead
- greenfield bootstrap runs before the shadow exists, so it starts a throwaway
  PostgreSQL instance from source schema to convert them, which requires `pg_dump`
  Schema objects for extensions unavailable on daemon host are omitted

## Ordering and consistency

- committed end state converges by source row key and `_lsn`
- updates from different tables inside one PostgreSQL transaction may become visible in ClickHouse at different moments
- restart can resend acknowledged-nearby rows, generated table engine deduplicates them during merge or `FINAL`
- destination queries without `FINAL` can observe multiple row versions until background merge
- bounded ClickHouse retry exhaustion stops daemon, supervisor restart continues from persisted floor

## Initial loads

- transactions open past greenfield handoff resume from their first buffered
  record, using source or archived WAL; missing history stops replication.
  Rows written before replay starts wait in `<table>__wspending` until commit
  or abort determines visibility
- retain shadow transaction history across restarts; missing `pg_xact` status
  leaves pending rows unpublished. Boot logs unresolved xids and reports
  `walshadow_pending_undecidable_xids` alongside `walshadow_pending_outstanding_xids`
- backup rows with mapped external TOAST values render from walked chunk mirrors
  without source SQL scans. They spool to bootstrap scratch until every walk
  lane flushed its chunks and the backup-window leg replayed; inline values and
  unmapped external columns need no wait
- restored TOAST page images in backup-window WAL mirror every chunk they
  carry, repairing pages the backup copied mid-write. They date from the page's
  own version, so walked chunks never outrank them
- an external value the mirror cannot reassemble stops the load rather than
  substituting one; rerun against a fresher backup, or load the table with
  `initial_load = 'copy'`
- a multixact xmax the backup's `pg_multixact` cannot bound stops the load, with
  the same remedies
- walked rows stream through bounded channels into configured inserter pool;
  user table data never lands in shadow catalog. Unknown visibility spills to
  bootstrap scratch files until transaction logs arrive
- `--bootstrap-max-rate-kib` caps direct backup transfer at configured rate;
  window WAL stays unthrottled
- `--bootstrap-wind-down-secs` controls live window wait for transactions open
  across handoff (default 5, zero skips waiting). Timeout resumes pump below
  `end_lsn` at oldest buffered record; increasing wait reduces these rewinds
- DDL during greenfield bootstrap is unsupported; affected relations may be
  skipped or fail the load
- `copy`, selected by default by `init`, scans selected table through PostgreSQL
  SQL path, source account must be able to read every row
  Rows hidden by row security are skipped without an error
- `copy` in `clickhouse` value mode also copies the table's TOAST heap, needing
  `pg_read_all_data` at source; a live selection holds streaming at the
  selection point until it finishes. COPY falling back from a backup load
  copies it later, so updates between can store NULL for unchanged large values
- `base_backup` transfers cluster-sized backup even for one table
- `object_store` requires full wal-g backup and continuous archived WAL, including archived
  timeline history, to selection point
- promotion between object-store backup and selection point follows the branch the
  stream proved, which gap replay requires archived history to match; reject backups
  extending beyond ancestor fork point
- old object-store backup with intervening catalog changes can be rejected, use newer backup or `copy`
- an interrupted `object_store` load allows up to three attempts using same backup
  to avoid duplicate rows. If checkpoint and temporary data files survive, retry skips
  extraction. Otherwise, it discards partial data and extracts again. Failed cleanup
  counts as an attempt and keeps chosen backup. Manual intervention is required after
  three attempts, if saved recovery state is unreadable or names no backup, or if load
  uses another mode
- `initial_load = "none"` never reconstructs rows which existed before selection and receive no later change

## Large values

Default `clickhouse` value mode uses persistent TOAST chunk mirrors. Missing
required mirror tables stop replication, see
[large toasted values](destination-tables.md#large-toasted-values)

Reused TOAST value IDs can leave ambiguous generations in backup chunk mirrors
when hint bits do not prove older chunks dead. Baseline rows and later
unchanged-pointer updates both resolve from those mirrors. Chunk lookup orders
by `(ver, blkno, offnum)` for determinism, not generation correctness: a newer
generation at a lower TID can lose when versions tie

### Shadow value mode

`[toast] mode = "shadow"` (see
[configuration](configuration.md#value-mode)) serves both a greenfield
bootstrap and live CDC. It has three important limitations:

- **No reclamation fence.** walshadow does not delay prune, vacuum,
  `TRUNCATE`, `DROP TABLE`, or rewrite records during shadow replay. Shadow
  applies them as soon as WAL arrives, while rows wait for ClickHouse. If
  shadow removes a value before ClickHouse writes its row, the value becomes
  NULL and increments `toast_values_filled_superseded`. Restart from a
  position below durable processing progress has the same risk
- **Shadow stores required data, not only catalog state.** Losing shadow does
  not lose a ClickHouse chunk mirror, but it does lose values stored by this
  mode. Recovery requires a fresh bootstrap
- **No migration path.** Modes store different history. Switching an
  existing deployment requires a fresh bootstrap

Shadow reads current generation directly, avoiding mirror's mixed-generation
ambiguity described above. A reused value ID can still return a newer value.
PostgreSQL reissues an ID only after all its chunks are gone, so replacement
chunks postdate referring record. Reads compare chunk `xmin` with highest
transaction ID assigned when that record was written. Newer chunks increment
`toast_values_filled_generation` instead of returning replacement bytes,
whether complete or partial. Sampling at 1 MiB WAL intervals and frozen `xmin`
values can hide reuse, so passing this check does not prove a value is original

## Not an HA system

walshadow consumes PostgreSQL failover decisions, it does not make them. It
does not provide leader election, old-primary fencing, synchronous durability,
DNS movement, or promotion orchestration

Use [planned switchover protocol](failover.md) and retain independent source
backups
