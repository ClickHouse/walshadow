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
export WALSHADOW_SOURCE_URL='postgres://replicator:secret@source/app?sslmode=require'
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
- soft-delete and TOAST modes
- worker-pool sizes and memory limits
- backup and shadow bootstrap choices

Run `walshadow-stream --help` for process and recovery flags. Run
`walshadow-stream ctl help` for current live-control surface

## Source-side runtime config

Optional config tables let DBAs change routing and batching with SQL, ordered
at same commit boundary as source data

Install tables, choosing schema if needed:

```bash
psql "$WALSHADOW_SOURCE_URL" \
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
