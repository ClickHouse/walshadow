# Select tables

Choose broad automatic scope or explicit table scope. Exclusions always win
over broad patterns

Every selected table needs one of:

- primary key with default replica identity
- unique index selected by `REPLICA IDENTITY USING INDEX`
- `REPLICA IDENTITY FULL`

Tables with `REPLICA IDENTITY NOTHING`, or default identity without a primary
key, cannot replicate deletes correctly and fail preflight

`REPLICA IDENTITY FULL` without a primary key sorts destination rows by every
column ClickHouse can sort. Rows with identical sort values become one row,
and deleting one deletes all matches. ClickHouse rejects dropping, renaming,
or changing types of sort key columns, so those source changes stop
replication. Add a primary key or configure `order_by`; see
[sort key](destination-tables.md#choose-sort-key)

## Replicate all user tables

`replicate_all` defaults to `true`. It covers current and future user tables,
excluding `pg_*`, `information_schema`, and configured runtime-config schema.
`walshadow-stream init` writes no `[stream]` block, so config it writes starts
in this mode whichever tables you chose

```toml
[stream]
replicate_all = true

[table.public.audit_log]
replicate = false
```

Use this mode to replicate all tables by default. It covers the source database
`dbname` in `[source]` names; entries prefixed with another database name bring
that database in too, see [several databases](multi-database.md)

## Replicate an explicit set

Disable broad scope, then opt tables in. `replicate_all` is startup-only, so
change it in config file and restart daemon

```toml
[stream]
replicate_all = false

[table.public.orders]
replicate = true
initial_load = "copy"

[table.public.users]
replicate = true
initial_load = "copy"
```

Inspect and change scope while daemon runs:

```bash
walshadow-stream ctl tables
walshadow-stream ctl add public orders --initial-load copy
walshadow-stream ctl remove public audit_log
```

`ctl tables` marks replicated tables with `*`. `remove` stops future delivery
and retains destination table

## Choose initial load

Initial-load mode controls rows committed before table selection. It comes
from table block, `ctl add`, or config row. Table which reaches scope through
`replicate_all` alone gets no initial load: destination table auto-creates and
receives changes from start LSN onwards

| Mode | Existing rows | Source impact | Use when |
|---|---|---|---|
| `none` | skipped | none | destination already has baseline, or only future changes matter |
| `copy` | read with live SQL snapshot | table scan | normal table additions |
| `base_backup` | read from fresh physical backup | cluster-sized backup stream | SQL scan pressure is undesirable |
| `object_store` | read from latest wal-g backup plus archived WAL | archive reads | continuous compatible backup archive exists |

`walshadow-stream init` defaults to `--initial-load copy`. This mode scans only
selected table through PostgreSQL, which supplies visible rows and detoasted
values. Choose `--initial-load base_backup` or `--initial-load object_store`
to load existing rows from physical data

`base_backup` streams whole PostgreSQL cluster even when adding one table
because PostgreSQL backup protocol has no per-table filter

`object_store` requires `[backup]` configuration, full wal-g backup, and
continuous archived WAL coverage. Use `copy` when backup predates incompatible
schema changes or archive coverage has gaps

Backup modes resolve mapped external TOAST values from walked chunk mirrors.
Missing chunks or multixact visibility that backup cannot resolve stop the load;
retry with a fresher backup or explicitly select `copy`. Source SQL queries still
supply metadata and control replication. See
[initial-load limits](limitations.md#initial-loads)

## Select future tables by name

Use anchored glob or regular-expression rules

```toml
[stream]
replicate_all = false

[table.app."events_*"]
match = "glob"
replicate = true
initial_load = "copy"

[table.app."*_audit"]
match = "glob"
replicate = false
```

Supported match modes:

- `exact`, literal name and default mode
- `glob`, supports `*`, `?`, character classes, and choices
- `regex`, supports regular expressions without backreferences

Patterns match whole names. Exact entries apply after patterns. Explicit exact
entry can override pattern result, while matching exclusion blocks broader
opt-ins

## Replicate partitioned tables

Declarative partitions replicate into root table's destination, like
PostgreSQL publications with `publish_via_partition_root`. Partitions have no
separate destination. For nested partitions, root is topmost partitioned
table. Root settings determine which tables replicate, destination name, and
destination columns

```toml
[table.app.events]
replicate = true
initial_load = "copy"
order_by = ["tenant_id", "id", "created_at"]
partition_by = "toYYYYMM(created_at)"
```

By default, glob and regex entries skip partitions. `replicate_all` and broad
patterns include partitions through root settings. An exact entry for a
partition may set `replicate` and `route`; other settings stop replication

| Partition entry | Effect |
|---|---|
| none | rows go to root's destination |
| `replicate = false` | partition's rows are not replicated |
| `route = "self"` | partition, and every partition below it, gets its own destination and rules |

`route = "self"` on a glob entry gives matching partitions their own
destinations:

```toml
[table.app."events_hot_*"]
match = "glob"
route = "self"
```

Rows carry source partition OID in `_leaf_oid`, see
[metadata columns](destination-tables.md#generated-shape). Legacy `INHERITS`
children stay independent tables. Foreign partitions write no WAL and are
skipped. Unlogged partitions stop replication unless excluded with
`replicate = false`

Every partition must log all columns needed to identify deleted rows in
root's destination. Use a primary key or `REPLICA IDENTITY USING INDEX` index
covering those columns, or use `FULL` to log every column. `REPLICA IDENTITY`
does not apply recursively: if root uses `FULL` without a primary key, set
`FULL` on each partition too. Replication stops if a partition omits required
columns

Initial load of a partitioned table copies each partition separately with
`COPY … FROM ONLY`. Backup load modes use `copy` for partitions too: replacing
a shared destination with a staging table would discard other partitions'
rows

## Rename targets or columns

Map one table without pinning source shape:

```toml
[table.app.orders]
replicate = true
target_database = "warehouse"
target_table = "fact_orders"
columns = [
    { name = "customer_id", target = "account_id" },
    { name = "created_at", type = "DateTime64(6, 'UTC')" },
]
```

Name-based entries preserve automatic schema discovery. Each may override
ClickHouse name, type, or both

Use name patterns for type families:

```toml
[table.app."*"]
match = "glob"
replicate = true
columns = [
    { name = "*_at", match = "glob", type = "DateTime64(6, 'UTC')" },
]
```

Avoid `attnum` mappings unless destination projection must stay fixed. An
`attnum` mapping pins full projection, requires explicit ClickHouse names and
types, and will not automatically include unrelated source columns

## Replace NaN and infinity

ClickHouse `Decimal` cannot hold PostgreSQL `numeric` `NaN`, `Infinity`, or
`-Infinity`. By default, encountering any of these values stops replication
with an error identifying affected column. Map column to `String` to keep
original text, or choose replacements with `nan`, `pos_inf`, and `neg_inf`:

```toml
[table.app."*"]
match = "glob"
replicate = true
columns = [
    { name = "*_amount", match = "glob", nan = "0", pos_inf = "max", neg_inf = "min" },
]
```

| Value | Effect |
|---|---|
| `reject` | stop replication, default |
| `null` | write NULL, destination must be `Nullable(Decimal(...))` |
| `min`, `max` | write smallest or largest value of destination `Decimal(p,s)`, eg `-99999999.99` and `99999999.99` for `Decimal(10,2)` |
| decimal number | write a fixed value, such as `0` or `-1.5`, that fits destination decimal type without rounding |

Set each replacement independently. For example, a column rule setting only
`nan` keeps `pos_inf` and `neg_inf` from matching broader rules. Set `reject`
to stop on that value even when a broader rule supplies a replacement

Numeric replacements apply to `Decimal` columns during initial loads and
ongoing replication. They do not affect `String` columns or apply to column
defaults or array elements; see [ClickHouse limits](limitations.md#clickhouse)

For PostgreSQL `date`, `timestamp`, and `timestamptz`, `pos_inf` and
`neg_inf` also accept `reject` (default) or `null`. NULL substitution requires
`Nullable(Date32)` or `Nullable(DateTime64(...))`, for example:

```toml
columns = [
    { name = "expires_at", type = "Nullable(DateTime64(6, 'UTC'))", pos_inf = "null", neg_inf = "null" },
]
```

Temporal infinities stop replication by default. Temporal `min`, `max`, and
literal substitutes are unsupported. NULL substitution covers local row
encoding; oracle encoding and fast defaults do not support it. Finite timestamps
rescale exactly to destination precision; discarded nonzero digits and storage
overflow reject. Supported calendar ranges are not validated

Choose replacements that fit destination column type. For example, `null`
requires a nullable column, and `1.230` fits `Decimal(10,2)` but `1.234` does
not. An invalid setting is rejected when loading config. A recognized
replacement that does not fit stops replication when affected table first
receives rows. Review replacements after changing a column's type
