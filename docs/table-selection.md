# Select tables

Choose broad automatic scope or explicit table scope. Exclusions always win
over broad patterns

Every selected table needs one of:

- primary key with default replica identity
- unique index selected by `REPLICA IDENTITY USING INDEX`
- `REPLICA IDENTITY FULL`

Tables with `REPLICA IDENTITY NOTHING`, or default identity without a primary
key, cannot replicate deletes correctly and fail preflight

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

Use this mode when destination should track whole database by default

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
| `object_store` | read from latest wal-g backup plus archived WAL | no source data scan | continuous compatible backup archive exists |

`base_backup` streams whole PostgreSQL cluster even when adding one table
because PostgreSQL backup protocol has no per-table filter

`object_store` requires `[backup]` configuration, full wal-g backup, and
continuous archived WAL coverage. Use `copy` when backup predates incompatible
schema changes or archive coverage has gaps

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
