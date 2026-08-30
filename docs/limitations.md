# Current limits

Review these limits before production use

## PostgreSQL

- PostgreSQL 16 or newer only
- shadow PostgreSQL major must match source major
- one source database per walshadow process
- `wal_level = logical` required
- every replicated table needs usable replica identity
- prepared transactions are not supported, `PREPARE TRANSACTION` changes can be lost
- sequence state is not replicated, values already stored in table rows still replicate
- non-default tablespaces are unsafe for bootstrap and managed-shadow lifecycle
- unplanned primary promotion is not supported

## ClickHouse

- destination uses ClickHouse Native protocol
- source column type changes require manual ClickHouse migration
- `time` mapping requires ClickHouse `Time64` support
- same-named tables from different PostgreSQL schemas need explicit destination mapping
- `base_backup` and `object_store` table loads publish with staging-table swap, database must support `EXCHANGE TABLES`
- backup rows inserted into staging do not fire destination materialized views, live rows copied back after swap can fire twice

## Ordering and consistency

- committed end state converges by source row key and `_lsn`
- updates from different tables inside one PostgreSQL transaction may become visible in ClickHouse at different moments
- restart can resend acknowledged-nearby rows, generated table engine deduplicates them during merge or `FINAL`
- destination queries without `FINAL` can observe multiple row versions until background merge
- bounded ClickHouse retry exhaustion stops daemon, supervisor restart continues from persisted floor

## Initial loads

- `copy` scans selected table through PostgreSQL SQL path
- `base_backup` transfers cluster-sized backup even for one table
- `object_store` requires full wal-g backup and continuous archived WAL to selection point
- old object-store backup with intervening catalog changes can be rejected, use newer backup or `copy`
- `initial_load = "none"` never reconstructs rows which existed before selection and receive no later change

## Large values

Default `[toast] mode = "disabled"` cannot always reconstruct values stored
externally before replication window. Enable ClickHouse TOAST storage before
initial load when complete large-value history matters

## Not an HA system

walshadow consumes PostgreSQL failover decisions, it does not make them. It
does not provide leader election, old-primary fencing, synchronous durability,
DNS movement, or promotion orchestration

Use [planned switchover protocol](failover.md) and retain independent source
backups
