# Query destination data

walshadow creates ClickHouse tables in configured `[ch] database` unless a
namespace or table rule chooses another database. Destination table name
defaults to source relation name

Map same-named tables from different PostgreSQL schemas to distinct ClickHouse
tables or databases to avoid collisions

## Generated shape

Automatically created tables use source row key for `ORDER BY` and
`ReplacingMergeTree` for convergence after updates, deletes, or daemon replay

Source columns are followed by four metadata columns:

| Column | Type | Meaning |
|---|---|---|
| `_lsn` | `UInt64` | source commit position and row version |
| `_xid` | `UInt32` | source transaction ID |
| `_commit_ts` | `DateTime64(6, 'UTC')` | source commit time |
| `_is_deleted` | `Bool` | delete marker |

## Read current state

Use `FINAL` when query must resolve outstanding row versions immediately

```sql
SELECT *
FROM cdc.orders FINAL
WHERE _is_deleted = 0
ORDER BY id;
```

Without `FINAL`, background merges converge versions asynchronously. For large
analytical queries, prefer application-specific `argMax` patterns or downstream
materialization when `FINAL` cost is too high

## Keep delete history

Default engine removes deleted rows during `FINAL` processing. Enable soft
deletes at startup to retain tombstones as latest versions

```toml
[ch]
soft_delete = true
```

Then query live state with `_is_deleted = 0`, or inspect deleted versions
without that filter

## Default type mapping

Common mappings include:

| PostgreSQL | ClickHouse |
|---|---|
| `boolean` | `Bool` |
| `smallint`, `integer`, `bigint` | `Int16`, `Int32`, `Int64` |
| `real`, `double precision` | `Float32`, `Float64` |
| `numeric(p,s)` | `Decimal(p,s)` up to precision 76, otherwise `String` |
| `text`, `varchar`, `char`, `name`, `bytea` | `String` |
| `date` | `Date32` |
| `time` | `Time64(6)` |
| `timestamp`, `timestamptz` | `DateTime64(..., 'UTC')` |
| `uuid` | `UUID` |
| `json`, `jsonb`, `inet`, `cidr`, `interval`, arrays, unknown types | `String` |

Nullable source columns become `Nullable(...)` unless used as ClickHouse sort
keys. ClickHouse deployments using PostgreSQL `time` columns must enable
`Time64` support

Override inferred type with name-based column rule, see
[Select tables](table-selection.md#rename-targets-or-columns)

## Large toasted values

Values stored externally by PostgreSQL need persistent chunk history when
references predate replication window. Enable ClickHouse-backed TOAST storage
for full reconstruction across bootstrap and restarts

```toml
[toast]
mode = "clickhouse"
```

Default `disabled` mode fills unrecoverable values with null or column default
and reports counters. Enable `clickhouse` before initial load for workloads with
large text, JSON, arrays, or bytea values
