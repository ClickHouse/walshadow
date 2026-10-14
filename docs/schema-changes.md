# Handle schema changes

walshadow observes committed PostgreSQL schema changes and orders ClickHouse
changes after earlier rows and before later rows

## Behavior matrix

| PostgreSQL change | ClickHouse behavior |
|---|---|
| `CREATE TABLE` | creates destination when table matches active scope |
| `ADD COLUMN` | adds mapped column automatically |
| `ADD COLUMN ... DEFAULT ...` | adds column and carries supported default |
| `RENAME COLUMN` | renames destination column unless config pins another target name |
| `DROP COLUMN` | drops automatically mapped destination column, custom target mappings can need cleanup |
| `ALTER COLUMN ... TYPE` | updates destination type unless config states one; `USING` is eventually consistent |
| `DROP NOT NULL` | makes automatically derived destination type `Nullable` |
| `SET NOT NULL` | preserves existing destination `Nullable` type |
| `TRUNCATE` | truncates destination table in source order |
| `TRUNCATE` of a partition | removes that partition's rows from root's destination |
| `DROP TABLE` | retains, warns, or drops destination based on policy |
| `DROP TABLE` of a partition | retains or removes that partition's rows based on `drop_table_strategy` |
| `ATTACH PARTITION` | loads attached table's rows into root's destination |
| `DETACH PARTITION` | retains or removes partition's rows based on `detach_strategy`; detached table then replicates on its own |
| `VACUUM FULL`, `CLUSTER`, rewriting `ALTER` | continues with rewritten source relation |

Table renames, schema moves, replica-identity changes, and switching to unlogged
storage are not automatically reconciled with destination routing and keys
Coordinate these changes with an explicit destination and mapping migration
None are rejected. Rows written after a table rename or schema move are
discarded unless a mapping already names new relation

Rewriting type changes log every live row with its new value. These rows
supersede stored versions through `_lsn` deduplication. Until they arrive,
values reflect what ClickHouse `MODIFY COLUMN` casts to. Conversions without
cast drop and re-add destination column, eventually filled as rows arrive

ClickHouse can reject key column type changes, including changes to replica
identity columns used in destination sort keys. It also refuses to drop or
rename sort key columns. With `REPLICA IDENTITY FULL` and no primary key, this
applies to every column used for sorting. Rejection stops replication
before column renames, additions, and drops from that schema event. Earlier
retypes from same event may already have succeeded

## Configure source table drops

Destination retention is default

```toml
[ch]
drop_table_strategy = "retain"
```

Accepted values:

- `retain`, keep destination silently
- `warn`, keep destination and emit warning
- `drop`, remove destination with `DROP TABLE IF EXISTS`

Override per source schema:

```toml
[namespace.staging]
drop_table_strategy = "drop"
```

Choose `drop` only when source owns destination lifecycle. ClickHouse
dependencies and downstream consumers remain operator responsibility

## Configure partition changes

A partition routed to its root has no separate destination. Its schema
changes apply to root's destination. `ALTER TABLE` on root updates that
destination once

walshadow uses `_leaf_oid` to find a partition's rows and source position to
limit removal to earlier rows. With default `ReplacingMergeTree`, it inserts
delete markers at removal position. Other MergeTree engines and tees use a
lightweight `DELETE`. If `leaf` column is disabled, replication stops on
partition `TRUNCATE` or on a detach or drop with policy set to `drop`

By default, `DETACH PARTITION` keeps existing rows in root's destination:

```toml
[ch]
detach_strategy = "retain"
```

Accepted values match `drop_table_strategy`. Use `drop` to remove detached
partition's rows, or `warn` to keep them and log a warning. Dropping a
partition follows `drop_table_strategy`; `drop` removes its rows and keeps
root's destination. Configure either policy in root's namespace:

```toml
[namespace.archive]
detach_strategy = "drop"
```

A detached table becomes an independent table. With `replicate_all`, it gets
its own destination for changes after `DETACH`

`ATTACH PARTITION` loads attached table's existing rows with `COPY` at source
position of attach, unless root sets `initial_load = "none"`. Any destination
it had before attaching remains but receives no further writes

Attaching a partitioned table under another root leaves its existing
partitions routed to previous root until their next schema change

## Migrate a rejected key type change

When ClickHouse rejects a key type change, coordinate manual migration:

1. Pause walshadow
2. Wait until `drain` and `emitter_ack` converge in `ctl status`
3. Alter or replace ClickHouse destination column
4. Add matching column type override when automatic mapping differs
5. Resume walshadow

```bash
walshadow-stream ctl pause
walshadow-stream ctl status
walshadow-stream ctl resume
```

Use staging table and swap when ClickHouse cannot alter existing type safely

## Pinned projections

Name-based column rules preserve automatic evolution. `attnum` rules pin full
projection, so newly added source columns stay excluded until config changes

Prefer name-based rules unless stable fixed projection is required
