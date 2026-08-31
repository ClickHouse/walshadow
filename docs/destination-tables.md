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

Rename these columns, drop the delete marker, or pin the sort key with
settings below

## Choose sort key

Set `order_by` to sort a destination table on chosen columns instead of source
row key. Name ClickHouse column names, after any rename:

```toml
[table.public.events]
order_by = ["tenant_id", "id"]
primary_key = ["tenant_id"]

[table.app."events_*"]
match = "glob"
order_by = ["tenant_id", "id"]
```

ClickHouse `PRIMARY KEY` chooses which sort-key prefix its sparse index covers
and enforces no uniqueness. `primary_key` must be a prefix of `order_by`.
walshadow ignores an invalid `primary_key`, logs a warning, and indexes whole
sort key. It also ignores an `order_by` naming a missing or `Nullable` column,
because ClickHouse rejects nullable sort keys, and falls back to source row key

Both settings apply when walshadow creates a table. walshadow never rekeys a
table ClickHouse already holds, so choose shape before first delivery, or run
`ALTER TABLE` in ClickHouse. With `replicate_all = true` a table can reach
ClickHouse before an exact source-side row arrives: keep custom shape in config
file, or in a pattern rule which matches before creation

Source-side rows carry same settings as `text[]`:

```sql
UPDATE walshadow.config_table
SET order_by = ARRAY['tenant_id', 'id'], primary_key = ARRAY['tenant_id']
WHERE namespace = 'public' AND relname = 'events';
```

## Rename metadata columns

`[system_columns]` renames appended columns for every table. walshadow reads it
at startup only:

```toml
[system_columns]
lsn = "_peerdb_version"
commit_ts = "_peerdb_synced_at"
is_deleted = "_peerdb_is_deleted"
```

Set same keys in a `[table.*]` block, or in a `config_table` row, to rename for
matching relations. Omitted keys inherit cluster-wide names. Names must be
unique and non-empty: config file fails validation, and a source-side row is
rejected with a warning, leaving cluster-wide names in place. TOAST mirror
tables keep fixed names

walshadow uses configured names in `CREATE TABLE` and `INSERT` statements, and
never renames a column in an existing ClickHouse table. Renaming for an existing
destination also needs `ALTER TABLE ... RENAME COLUMN` in ClickHouse

## Drop the delete marker

Set `is_deleted = false` for an append-only destination. This drops the marker
column and discards source `DELETE` rows, counting them in
`walshadow_emitter_deletes_discarded_total`:

```toml
[system_columns]
is_deleted = false               # cluster-wide

[table.app."events_*"]
match = "glob"
is_deleted = false               # this pattern only
```

Source-side rows use an empty string, `is_deleted = ''`, for same result

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
| `json`, `jsonb` | `JSON` |
| `hstore` | `Map(String, Nullable(String))` |
| `vector`, `halfvec` (pgvector) | `Array(Float32)` |
| `geography`, `geometry` (PostGIS) | `String`, WKT for 2-D points, PostgreSQL's own hex form otherwise |
| `<elem>[]` arrays | `Array(Nullable(<elem>))`; unknown elem → `Array(Nullable(String))`. One layer, so a multidimensional value needs an explicit nested override |
| `inet`, `cidr`, `interval`, unknown types | `String` |

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
