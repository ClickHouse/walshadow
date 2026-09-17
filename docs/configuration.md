# Configure walshadow

Use `walshadow-stream init` for first config, then choose file edits, live
control commands, or optional source-side config tables

## Connection URLs

Accepted PostgreSQL form:

```text
postgres://user:password@host:5432/database?sslmode=require&slot=walshadow
```

Accepted ClickHouse forms:

```text
clickhouse://user:password@host:9000/database
clickhouses://user:password@host:9440/database
```

Environment variables keep credentials out of command history:

```bash
export WALSHADOW_PG_URL='postgres://replicator:secret@source/app?sslmode=require'
export WALSHADOW_CH_URL='clickhouses://default:secret@clickhouse.example/cdc'
```

## Config file

Minimal standalone config:

```toml
[source]
host = "source.internal"
port = 5432
user = "replicator"
password = "secret"
dbname = "app"
sslmode = "require"
slot = "walshadow"

[ch]
host = "clickhouse.internal"
port = 9000
database = "cdc"
user = "default"
password = "secret"

[stream]
replicate_all = false

[table.public.orders]
replicate = true
initial_load = "copy"
```

Example above names its scope. Omit `[stream]` to replicate every user table,
including tables created later. `init` writes `[source]`, `[ch]`, and chosen
`[table.*]` blocks, leaving that broad default in place

Pass file with `--ch-config`. Loader also merges sibling directory formed by
replacing `.toml` with `.d`, for example `ch-config.d/*.toml`

Invalid values and incompatible mapping fields fail validation instead of
falling back silently. Loader ignores unknown keys, so check spelling here when
a setting has no effect

## Precedence

Highest value wins:

1. explicit CLI flag
2. optional source PostgreSQL config row
3. merged TOML files

Connection URLs provide defaults below config file values

Use `ctl show` to inspect effective config with passwords masked

```bash
walshadow-stream ctl show
```

## Live control

Common changes apply without restart:

```bash
walshadow-stream ctl add public orders --initial-load copy
walshadow-stream ctl remove public audit_log
walshadow-stream ctl pause
walshadow-stream ctl resume
walshadow-stream ctl source 'postgres://repl@new-primary/app?sslmode=require&slot=ws_new'
walshadow-stream ctl dest 'clickhouses://default@clickhouse.example/cdc'
walshadow-stream ctl reload
```

Apply several values atomically with TOML on stdin:

```bash
walshadow-stream ctl apply <<'EOF'
[ch]
flush_timeout_ms = 250
retry_max_attempts = 8

[namespace.events]
target_database = "event_store"
EOF
```

Control writes only `50-api.toml` in config fragment directory. Base config
stays unchanged. Invalid merged config is rejected and previous fragment is
restored

## Live and startup-only settings

Apply live:

- table and column rules
- per-table metadata column names, `order_by`, and `primary_key`, applied when
  walshadow creates a table, see [Query destination data](destination-tables.md)
- namespace destinations and drop policy
- pause state
- batch sizes, flush timeout, compression, and retry count
- source and ClickHouse endpoints

Require restart:

- `replicate_all`
- runtime-config schema
- cluster-wide `[system_columns]` names
- soft-delete mode and TOAST buffering settings
- worker-pool sizes and memory limits
- backup and shadow bootstrap choices

Run `walshadow-stream --help` for process and recovery flags. Run
`walshadow-stream ctl help` for current live-control surface

## Source-side runtime config

Optional config tables let DBAs change routing and batching with SQL, ordered
at same commit boundary as source data

Install tables, choosing schema if needed:

```bash
psql "$WALSHADOW_PG_URL" \
    -v walshadow_schema=walshadow \
    -f sql/runtime_config_install.sql
```

Enable schema in TOML:

```toml
[runtime_config]
schema = "walshadow"
```

Add a table from PostgreSQL:

```sql
INSERT INTO walshadow.config_table
    (namespace, relname, replicate, initial_load)
VALUES
    ('public', 'orders', true, 'copy');
```

`config_table` also carries destination shape: `order_by` and `primary_key` as
`text[]`, and `lsn`, `xid`, `commit_ts`, `is_deleted` for metadata column names.
See [Query destination data](destination-tables.md)

walshadow reads these tables but never writes them. Keep archive credentials
and bootstrap configuration in TOML, not source-side tables

## TOAST buffering

Tune chunk-store buffering independently of inserter count, restart daemon to apply

| `[toast]` setting | Default | Effect |
|---|---:|---|
| `mode` | `clickhouse` | Choose `clickhouse`, `shadow`, or `disabled` storage for external values |
| `put_batch_rows` | 65,536 | Seal chunk INSERT after this many rows |
| `put_batch_bytes` | 67,108,864 | Seal chunk INSERT after this many body bytes |
| `connections` | `ch.inserter_pool_size` | Limit concurrent chunk-store connections |

Both seals apply to bootstrap and WAL chunk writes. Each bootstrap drain holds
its own batch, so more lanes can multiply buffered chunk bodies. A byte seal
can overshoot by one chunk. Smaller seals reduce buffering and create more
ClickHouse parts; fewer connections reduce concurrent INSERT buffers but can
reduce throughput

For smaller buffers, retain connection parallelism and restore previous seals:

```toml
[toast]
put_batch_rows = 256
put_batch_bytes = 4194304
```

The three buffering settings require positive integers. Existing
`[bootstrap] lanes` and `[ch] inserter_pool_size` controls remain independent

### Value mode

`mode = "clickhouse"` stores large values in ClickHouse. This is default and
keeps value history outside shadow PostgreSQL

`mode = "disabled"` keeps no value store and writes no chunks. A value can
still be restored when its chunks appear in the same transaction's WAL.
Otherwise, it becomes NULL, or the column type's default when target is not
Nullable, and increments `toast_values_filled_default`. Bootstrap skips TOAST
relations, so existing external values load as NULL. Use this when ClickHouse
does not need large values and their storage cost is not justified

`mode = "shadow"` stores large values in shadow PostgreSQL instead of
ClickHouse. Choose it only when limits below are acceptable

**Costs and constraints:**

- Shadow stores TOAST heaps and indexes while it runs. For a TOAST-heavy
  database, these files may account for most database storage. User heaps
  present at bootstrap stream without being copied to disk
- Shadow also stores every relation created after bootstrap, including plain
  heaps. This increases shadow disk use and replay work
- Shadow can remove a value before ClickHouse writes its row during heavy lag,
  table maintenance, truncate, drop, or restart. Affected destination value
  becomes NULL, or column default when destination column is not Nullable
- Buffering settings above have no effect because this mode writes no chunks
- Changing mode requires a fresh bootstrap
- Only default tablespace is supported. Bootstrap refuses a mapped table whose
  large-value storage uses another tablespace. Moving or creating such a table
  after bootstrap is not supported

See [current limitations](limitations.md#shadow-value-mode) before enabling
this mode

### Value size cap

`[memory] inline_value_max` (default 1 GiB) caps decoded size of one external
value. walshadow does not fetch values over cap. By default, it writes NULL, or
column type's default for a non-Nullable column, and increments
`toast_values_filled_oversize`. Set `inline_value_overflow = "error"` to fail
transaction instead

```toml
[memory]
inline_value_max = 67108864
inline_value_overflow = "error"
```

Both settings apply to initial load and WAL processing in every value mode

### Memory budget

`[memory] resident_payload_max` controls backpressure for payload bytes held
while decoding and inserting rows. New work waits when budget is full. Default
is one half of cgroup memory limit, or host memory without a cgroup limit, with
a 512 MiB minimum. This default assumes a dedicated host or container. Set an
explicit limit in shared environments

Each decoder reserves `[memory] value_reserve` bytes (default 64 MiB) for value
resolution. Other pipeline work cannot use this memory. Total reserved memory
is `decoder_pool_size * value_reserve`

`inline_value_max` rejects values; it does not contribute to reserved memory.
A decode request larger than total reserved memory runs alone and may
temporarily take payload use above `resident_payload_max`. Such requests
increment `memory_budget_overshoots_total`; waits behind them increment
`memory_budget_big_leaf_waits_total`. Raise `value_reserve` when these waits are
common and more memory is available

```toml
[memory]
resident_payload_max = 1073741824
value_reserve = 67108864
```

Both settings apply at startup. Total reserved memory must fit within half of
`resident_payload_max`, and remaining memory must hold one pending batch.
walshadow rejects invalid combinations

## Backup archive

Configure wal-g-compatible object storage for object-store bootstrap, WAL
refill, or object-store table loads

```toml
[backup]
archive = "s3://my-bucket/walshadow"
region = "us-east-1"

[bootstrap]
mode = "object_store"
backup_name = "LATEST"
object_store_parallelism = 8
```

Supported archive schemes are `s3://`, `gs://`, and `file://`. Prefer ambient
cloud credentials. When using static S3 credentials, set both `access_key` and
`secret_key`
