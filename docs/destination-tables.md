# Query destination data

walshadow creates ClickHouse tables in configured `[ch] database` unless a
namespace or table rule chooses another database. Destination table name
defaults to `<source namespace>_<source relation>`, so `public.orders` lands as
`public_orders` and `audit.orders` as `audit_orders` — same-named tables from
different PostgreSQL schemas never collide

## Name auto-created tables

A derived name is a template over `$database$` (source database), `$schema$`
(source namespace) and `$table$` (source relation). Default is
`$schema$_$table$`

`[stream] auto_create_name_all` sets it for every namespace, which is the one
to reach for beside `replicate_all`: a namespace with no entry of its own still
auto-creates under it

```toml
[stream]
replicate_all = true
auto_create_name_all = "$database$_$schema$_"   # audit.orders -> app_audit_orders
```

`auto_create_name` overrides it for one namespace

```toml
[namespace.audit]
auto_create = true
auto_create_name = "$database$_$schema$_$table$"   # audit.orders -> app_audit_orders

[namespace.shop]
auto_create = true
auto_create_name = "$table$_v2"                    # shop.orders -> orders_v2

[namespace.public]
auto_create = true
auto_create_name = "$table$"                       # public.orders -> orders
```

A template naming no `$table$` is read as a prefix, so `wh_` means
`wh_$table$` and `""` means `$table$`. Write `$$` for a literal `$`. An unknown
placeholder or an unterminated `$` is a config error, and in the
`config_namespace` overlay it is rejected with a warning, leaving the previous
value in place

| `auto_create_name` | `app`, `audit.orders` becomes |
|---|---|
| unset | `audit_orders` |
| `$schema$_` | `audit_orders` |
| `$database$_$schema$_$table$` | `app_audit_orders` |
| `$table$_v2` | `orders_v2` |
| `wh_` | `wh_orders` |
| `""` or `$table$` | `orders` |

Precedence is `target_table`, then the namespace's `auto_create_name`, then
`[stream] auto_create_name_all`, then `$schema$_$table$`. `target_table`
names a destination outright and ignores the template

The template applies when the table is created, so changing it leaves existing
destination tables where they are and starts writing to the new name

## Generated shape

Automatically created tables use source row key for `ORDER BY` and
`ReplacingMergeTree` for convergence after updates, deletes, or daemon replay
Row key uses primary key columns or index columns chosen by
`REPLICA IDENTITY USING INDEX`. With `REPLICA IDENTITY FULL` and no primary
key, it uses every column ClickHouse can sort. This enables
`allow_nullable_key` and uses first sort column as `PRIMARY KEY`

When an update changes row key, walshadow also writes a delete marker for old
key. Its version is one less than new row version. With default replica
identity, PostgreSQL logs only old key columns, so other columns in this marker
contain defaults or `NULL`

Source columns are followed by four metadata columns:

| Column | Type | Meaning |
|---|---|---|
| `_lsn` | `UInt64` | source commit position and row version |
| `_xid` | `UInt32` | source transaction ID |
| `_commit_ts` | `DateTime64(6, 'UTC')` | source commit time |
| `_is_deleted` | `Bool` | delete marker |

Rename these columns, drop the delete marker, pin the sort key, or pick engine
with settings below

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

Limit `order_by` to row key columns and columns that never change
Updating any other `order_by` column leaves old row in place. If `order_by`
includes non-key columns, changing row key also leaves old row unless
`REPLICA IDENTITY FULL` is set: other identity modes omit non-key values from
old-row delete markers

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

## Choose engine

walshadow creates tables as `ReplacingMergeTree(_lsn, _is_deleted)`. Set
`engine` to pick another ClickHouse engine:

```toml
[table.public.events]
engine = "Null"

[table.public.metrics]
engine = "CoalescingMergeTree"
```

walshadow renders `engine` verbatim, except an engine name ending in
`ReplacingMergeTree` gains walshadow's version and delete marker args. Only
engines ending in `MergeTree` get `ORDER BY`, `PRIMARY KEY`, and `SETTINGS`, as
other engines reject them. Engines other than `ReplacingMergeTree` do not
collapse updates, deletes, or replayed rows

Like sort key, `engine` applies only when walshadow creates a table. Source-side
rows set it in `config_table.engine`

## Route to named ClickHouse instances

One walshadow process can send rows from one PostgreSQL instance to multiple
ClickHouse instances. Keep `[ch]` as default destination and add named connections:

```toml
[ch]
host = "ch-primary.internal"
database = "cdc"

[ch.instances.archive]
host = "ch-archive.internal"
port = 9440
secure = true
user = "archive_writer"
password = "secret"
database = "history"

[table.public.orders]
replicate = true
target_instance = "archive"
tee = [
    { instance = "default", table = "orders" },
    { table = "orders_audit", engine = "MergeTree" },
]
```

Set `target_instance` to choose a named connection for a table. Both exact
and pattern rules support this setting. Omit it or use `"default"` to select
`[ch]`. Set `target_database` or a namespace database override to use a database
other than that connection's default

By default, a tee uses its main destination's instance and database. Set
`instance` to choose another connection and use its default database. Set
`database` to override that choice. In this example, orders and audit tables
use `archive`, and another copy of orders goes to `[ch]`

Named connections accept `host`, `port`, `database`, `user`, `password`, `secure`,
`tls_server_name`, and `compression`, with the same defaults as `[ch]`.
Credentials from `[ch]` are not reused for named connections. All destinations
share pipeline limits, pool sizes, retry settings, and column layout. Reload
TOML to update connection names and table mappings. `config_table` has no instance
column. `default` is reserved, and unknown names cause configuration errors

Each destination receives rows, initial loads, schema changes, `TRUNCATE`, and
`DROP TABLE` when configured. Each inserter opens connections as needed.
walshadow acknowledges source WAL after every destination receives the batch,
so an unavailable destination stops progress for all. Writes across instances
are not atomic: some destinations can receive rows before others, and retries
can send those rows again. TOAST mirrors stay on `[ch]` and supply values for
decoding rows sent to every instance

## Copy rows into more tables

Set `tee` to copy rows into additional ClickHouse tables. For example, keep
an audit log of every row version alongside a table that removes duplicates:

```toml
[table.public.orders]
replicate = true
tee = [
    { table = "orders_audit", engine = "MergeTree", order_by = ["id", "_lsn"] },
    { database = "replica", table = "orders" },
]
```

Each tee uses the same columns and metadata columns as its main destination
and receives the same schema changes. `database` defaults to the destination's
database. `engine`, `order_by`, and `primary_key` apply only when walshadow
creates a tee table, just as for the main destination. A `MergeTree` tee stores
updates as new rows and deletes as rows with `_is_deleted = true`

walshadow sends each batch to the main destination, then to each tee. It
acknowledges source WAL after every table receives the batch. After a restart,
walshadow may resend a batch. `ReplacingMergeTree` removes duplicate row
versions; other engines keep both copies. When reading those tables, remove
duplicates using row key and `_lsn`. Source `TRUNCATE` also empties tee tables.
Source `DROP TABLE` drops them when `drop_table_strategy = "drop"`

Initial loads from backups also write rows into tees. Replacing a destination
with its staging table affects only the main destination, so failed or retried
loads can leave extra rows in tees

Set `tee` on an exact `[table.*]` entry in TOML. Pattern entries reject it
because every matching source table would write to the same tee table.
`config_table` has no `tee` column. walshadow creates tee tables alongside the
main destination on source `CREATE TABLE`. It also creates them at startup
for entries that set `replicate = true` or specify `columns`. For other tables
already replicating, create tee tables before adding them to configuration

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
| `json`, `jsonb` | `String` |
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
references predate replication window. Default `[toast] mode = "clickhouse"`
stores TOAST chunks in ClickHouse mirrors for reconstruction across bootstrap
and restarts; see [value mode](configuration.md#value-mode) for `shadow` and
`disabled`. Other `[toast]` settings control
[buffering and connections](configuration.md#toast-buffering)

Each TOAST relation in a source database gets its own mirror in `[ch] database`,
named `pg_toast_<database oid>_<toast relation oid>`. Database OIDs keep mirror
names stable across `ALTER DATABASE ... RENAME` and separate mirrors from
databases that reuse relation OIDs.

Older binaries named mirrors `pg_toast_<toast relation oid>`. When following
only `[source] dbname`, startup renames these mirrors to include its database
OID. When following multiple databases, startup fails if any old mirror names
remain, because those mirrors may contain data from more than one database.
Start once following only their original source database to rename them, or
drop them and bootstrap tables with external TOAST values again.

Missing required mirror tables stop replication. Do not delete active mirrors
to reduce disk use. Relation-drop cleanup can leave empty mirror tables until
operator cleanup; retain them while any restart can still reread older referring
rows. Treat manual mirror cleanup as a recovery-state decision

Inline reconstruction still materializes each value. Values exceeding configured
limit fail instead of allocating without bound. See command help for TOAST
memory limits and [limits](limitations.md#large-values) for generation
ambiguity during backup
