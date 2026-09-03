# Current limits

Review these limits before production use

## PostgreSQL

- PostgreSQL 16, 17, and 18, daemon rejects unaudited majors
- shadow PostgreSQL major must match source major
- one source database per walshadow process
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
- `time` mapping requires ClickHouse `Time64` support
- same-named tables from different PostgreSQL schemas need explicit destination mapping
- `base_backup` and `object_store` table loads publish with staging-table swap, database must support `EXCHANGE TABLES`
- backup rows inserted into staging do not fire destination materialized views, live rows copied back after swap can fire twice

## Types shadow PostgreSQL converts

Values outside walshadow's own codec set (`jsonb`, arrays, `hstore`, enums,
ranges, domains, extension types) are converted by shadow PostgreSQL, one
request per insert batch

- a value shadow PostgreSQL cannot convert stops the batch and names the
  column and row, rather than writing a substituted value
- a multidimensional array does not fit the default one-layer `Array(...)`
  mapping; map the column to a matching nested `Array(Array(...))` instead
- greenfield bootstrap runs before the shadow exists, so it starts a throwaway
  PostgreSQL from the source schema to convert them; that needs `pg_dump` and
  the source's extensions installable on the daemon host, else bootstrap stops

## Ordering and consistency

- committed end state converges by source row key and `_lsn`
- updates from different tables inside one PostgreSQL transaction may become visible in ClickHouse at different moments
- restart can resend acknowledged-nearby rows, generated table engine deduplicates them during merge or `FINAL`
- destination queries without `FINAL` can observe multiple row versions until background merge
- bounded ClickHouse retry exhaustion stops daemon, supervisor restart continues from persisted floor

## Initial loads

- transactions open past greenfield handoff resume from their first buffered
  record, using source or archived WAL; missing history stops replication
  instead of skipping it. Earlier records can still leave missing inserts or
  stale deletes
- mapped external TOAST values trigger streaming `COPY` reads of at most 256
  physical row locations (CTIDs) per query, alongside backup page walk; inline
  values and unmapped external columns stay on page walk. Tables with external
  values in every row still require reading every such row from source
- unresolved multixacts trigger CTID `COPY` repair after backup and window
  replay; repair reads remain serial
- repair output streams through bounded channels into configured inserter pool;
  user table data never lands in shadow catalog. Unknown visibility can still
  spill to bootstrap scratch files until transaction logs arrive
- `--bootstrap-max-rate-kib` caps direct backup transfer and COPY repair output
  separately, each at configured rate; concurrent rates can add together.
  COPY cap counts binary field bytes, not source disk reads; WAL stays unthrottled
- `--bootstrap-wind-down-secs` controls live window wait for transactions open
  across handoff (default 5, zero skips waiting). Timeout resumes pump below
  `end_lsn` at oldest buffered record; increasing wait reduces these rewinds
- repair reads each physical relation with `ONLY` and rejects row-security
  filtering; source account must be able to read every row of repaired relations
- repair locks each relation through descriptor validation and every COPY of
  that batch, rejecting dropped, rewritten, or reshaped relations between batches
- DDL during greenfield bootstrap is unsupported; affected relations may be
  skipped or fail repair
- `copy` scans selected table through PostgreSQL SQL path
- `base_backup` transfers cluster-sized backup even for one table
- `object_store` requires full wal-g backup and continuous archived WAL to selection point
- old object-store backup with intervening catalog changes can be rejected, use newer backup or `copy`
- `initial_load = "none"` never reconstructs rows which existed before selection and receive no later change

## Large values

Default `[toast] mode = "disabled"` cannot always reconstruct values stored
externally before replication window. Enable ClickHouse TOAST storage before
initial load when complete large-value history matters

Reused TOAST value IDs can leave ambiguous generations in backup chunk mirrors
when hint bits do not prove older chunks dead. CTID repair fixes baseline rows,
but later unchanged-pointer updates still depend on those mirrors. Chunk lookup
orders by `(ver, blkno, offnum)` for determinism, not generation correctness:
a newer generation at a lower TID can lose when versions tie

## Not an HA system

walshadow consumes PostgreSQL failover decisions, it does not make them. It
does not provide leader election, old-primary fencing, synchronous durability,
DNS movement, or promotion orchestration

Use [planned switchover protocol](failover.md) and retain independent source
backups
