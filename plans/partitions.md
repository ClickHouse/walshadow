# Partitioned tables

Declarative partitions send rows to root's destination. See
[table selection](../docs/table-selection.md#replicate-partitioned-tables)
and [schema changes](../docs/schema-changes.md#configure-partition-changes)
for supported behavior. Start in [routing](../src/emit/route.rs),
[DDL application](../src/emit/ch_ddl.rs), and
[catalog capture](../src/catalog/shadow_catalog.rs)

## Stale ancestor information

Partition descriptors store ancestor information when captured. Renaming
root, changing root's `REPLICA IDENTITY`, or attaching a partitioned table
under another root does not update its partitions' `pg_class` rows. Partitions
keep old ancestor information until next capture. Rows use old root name for
routing, and identity checks miss new root key

Recapture all descendants through `pg_inherits` when an ancestor's descriptor
changes. Check every partition's identity when root identity changes.
Coordinate root renames with [relation renames](schema.md#relation-renames)

## Partition key warning

`order_by` warning checks root's key columns, which include all partition
columns. Capture `pg_partitioned_table.partattrs` through a pgext projection
and mirror query to report only partition columns

## Disabled partition column

With `leaf = false`, replication stops when partition `TRUNCATE` arrives, or
when a detach or drop arrives with policy set to `drop`. Reject `drop` policies
at config load when partition column is disabled. This requires knowing
which tables are partitioned roots when resolving configuration

## Schema changes during initial load

Partition `TRUNCATE`, `DETACH`, or `DROP` during a load into same root adds
delete markers for rows already loaded. Rows loaded afterwards survive,
even though their load version precedes delete markers, because removal
did not see them. Reject these changes until load cancellation is defined.
See schema changes during initial load in [bootstrap](bootstrap.md)

## Deferred: copy from old destination

When an attached table already replicated independently, copy its ClickHouse
destination to avoid scanning source. This requires:

- proof that old destination contains all changes through attach position:
  initial load finished and replication had no gaps
- conversion between source and destination mappings: column names, system
  column names, and types
- rejection of old destinations without delete markers, since they retain
  rows deleted at source
- policy for old destination after copy

## Completion checks

Compare source root `SELECT` with destination `FINAL WHERE _is_deleted = 0`,
including `_leaf_oid`, for each case below. Include restart during a transaction:

- range and hash partition trees
- `DETACH PARTITION CONCURRENTLY`, routed as attached until second commit
- `TRUNCATE` listing both root and partitions, with tees on engines other than
  `ReplacingMergeTree`
- `DROP TABLE` of a whole tree under each drop policy
- foreign partitions skipped by streaming and root load
- root load configured for backup uses `copy` instead
- bootstrap from scratch with transactions open across backup end and pending
  rows staged separately for each partition
