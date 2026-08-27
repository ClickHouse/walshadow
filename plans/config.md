# Runtime configuration

Config resolver combines TOML, rows from PostgreSQL config tables, and explicit
CLI flags into one snapshot. Later layers override earlier ones, in this order:

1. TOML
2. Rows from `<schema>.config_*` tables
3. Explicit CLI flags

A DBA manages PostgreSQL config rows with SQL on source server. Walshadow reads
these changes from its existing WAL stream and applies each change at its commit
LSN. Config changes use same ordering barrier as DDL changes.

TOML defines bootstrap settings, including connection parameters. It is also
the only config source when PostgreSQL config tables are disabled. Explicit CLI
flags have highest priority so recovery options remain fixed during WAL replay.

## Opt-in

PostgreSQL config tables are disabled by default. Set
`[runtime_config] schema = "…"` to name their schema on source server. A typical
value is `walshadow`. Omitting this field or setting it to an empty string keeps
this feature disabled. In that case, daemon uses only TOML and CLI values. It
does not query config tables or treat changes to them specially. Existing
deployments therefore do not need these tables.

## TOML load

Config loading reads base TOML file and its `conf.d` fragments. CLI-derived
`[source]` values provide defaults beneath file values. Loader rejects wrong
types, out-of-range numbers, unknown values, and unknown keys. Each error names
its config path, so a misspelled value cannot silently select a default.

Validation also covers `[table.*]` and `[namespace.*]` blocks, `columns` arrays,
and rules involving multiple fields. Examples include choosing `attnum` or
`name`, preventing a pinned projection from also being a pattern, and requiring
a target or type where appropriate. `ctl apply` performs same validation before
reloading daemon.

TOML fields, corresponding CLI flags, and PostgreSQL rows share one set of
accepted values.

## Live and boot-only settings

These settings can change without restarting daemon:

- per-relation destination mapping, keyed on `(namespace, relname)`
- per-namespace defaults: `auto_create`, `target_database`,
  `drop_table_strategy`
- per-column ClickHouse names and types from TOML and `config_column`, applied when
  building mappings, DDL, and encoder plans
- `[ch] drop_table_strategy`, used as global fallback for `DROP TABLE`.
  Per-namespace settings override it
- `[ch] row_budget`, `byte_budget`, and `flush_timeout_ms`, which decide when to
  seal a batch. A new timeout applies to blocks opened after a change. An open
  block keeps its existing deadline
- `[ch] compression` and `retry_max_attempts`. Compression changes apply to
  next batch and require a reconnect. Retry changes apply to next attempt
- ClickHouse connection fields `[ch] host`, `port`, `database`, `user`,
  `password`, and `secure`. Connection reopens at a batch or apply boundary
- all `[source]` connection fields: `host`, `port`, `user`, `password`,
  `dbname`, `sslmode`, and `slot`. An omitted slot means slotless replication.
  Only TOML and CLI may change source connection because PostgreSQL config rows
  arrive through that connection. Pump changes feeds between chunks after
  verifying `system_id` and timeline. Backfills started after a change copy
  from new address. See [Source endpoint move](control.md#source-endpoint-move).
  Endpoint and slot change together because both require a reconnect and slot
  is selected during `START_REPLICATION`. `--slot` fixes slot name for process

Live batching, compression, and retry settings allow `config_global` changes to
take effect during streaming. Pipeline starts with config loaded at startup, so
startup and live resolution use same settings.

Other settings apply only at startup because they determine pipeline structure
or resource limits. These include toast store, `soft_delete`, pending capture
cost controls (`[stream] pending_max_boundaries_per_xact` and
`pending_max_hold_ms`; see [Pending capture](desc_log.md#pending-capture)),
`[memory] resident_payload_max`, `[memory] inline_value_max` (see
[Memory budget](emitter.md#memory-budget)), and backup archive (`[backup]`,
described below). Spill directory is set only with `--spill-dir`.

Slot name may change at runtime, but slot creation happens only at startup.
Walshadow creates configured startup slot if needed before preflight checks.
See [source.md](source.md). It does not create slots after startup. A slot
created during an endpoint change would reserve WAL only from current server
position and could not prove that required resume LSN is available. Operator
must create missing slot before changing endpoints, and pump rejects endpoint
change until slot exists. Walshadow leaves old slot in place so it can retain
WAL needed for rollback.

`target_database`, `soft_delete`, `[stream] replicate_all`, and
`[runtime_config] schema` determine DDL applicator scope at startup and do not
change on reload. Reloaded snapshots still contain per-table destinations.
`replicate_all` (default `true`) auto-creates and replicates every user table
outside system schemas (`pg_*`, `information_schema`, and configured runtime
config schema). An explicit `auto_create` namespace or a
`replicate = false` opt-out takes precedence.

## `[backup]` archive

`[backup]` configures storage used for object-store bootstrap, WAL refill, and
object-store backfill. URI scheme in `archive` selects storage backend. Settings
for other backends are ignored.

```toml
[backup]
archive = "s3://my-bucket/walshadow"   # required: s3://, gs://, or file://
region  = "us-east-1"                  # s3, optional (default us-east-1)
endpoint = "https://minio.internal"    # s3, optional
force_path_style = true                # s3, optional
access_key = "…"                       # s3, optional
secret_key = "…"
credentials_path = "/gcs-sa.json"      # gcs, optional
```

Set both `access_key` and `secret_key` to use static credentials. Omit both to
use IMDSv2.

Omitting this section disables archive. Object-store bootstrap and backfill then
return errors. If source WAL has been removed, restart stops with an error that
explains required operator action. Daemon does not create a new base backup to
replace missing archive coverage. See restart contract in
[bootstrap.md](bootstrap.md).

## `[bootstrap]` shadow seeding

`[backup]` selects archive location. `[bootstrap]` controls whether startup
seeds a shadow from that archive. These settings apply only at startup.
Matching `--bootstrap-*` flags override individual TOML fields. For example,
`--bootstrap-mode direct` changes mode but still reads `backup_name` from TOML.

```toml
[bootstrap]
mode = "object_store"            # off, direct, or object_store
backup_name = "LATEST"           # LATEST, or a literal base_… name
object_store_parallelism = 8     # default is min(4, num_cpus)
```

Default mode is `direct` when `--bootstrap-shadow-data-dir` is set. A fresh
start then performs an initial copy before streaming. Otherwise, default mode
is `off`. Runs using an external shadow have nothing to seed and reject any
other mode.

Loader rejects unknown modes and non-positive `object_store_parallelism`.
`backup_name` and `object_store_parallelism` apply only in `object_store` mode.
Other modes log a warning and ignore them.

`--bootstrap-shadow-data-dir` is available only as a CLI flag. It determines
whether daemon manages shadow lifecycle for this run, like `--start-lsn` and
`--ignore-cursor`. Keeping it out of TOML prevents an old config file from
changing a recovery decision.

## Name patterns

Set `match` on a table or column entry to choose how names are matched:

- `exact` matches a literal name and is default
- `glob` supports `*`, `?`, character classes such as `[a-z]`, and choices
  such as `{one,two}`
- `regex` supports regular expressions without backreferences

Glob and regex patterns match whole names. For example, `events_*` matches
`events_2026`, but `events` does not match `my_events`.

```toml
[table.app."events_*"]
match = "glob"
replicate = true
initial_load = "copy"
target_database = "warehouse"
```

```sql
INSERT INTO walshadow.config_table (namespace, relname, match, replicate)
VALUES ('app', 'events_*', 'glob', true);
```

Use glob for common prefix and suffix matches. Use regex when glob cannot
express a pattern, for example `v[0-9]+_.*`.

Walshadow applies matching patterns from least specific to most specific.
Longer combined namespace and table patterns are more specific. Exact entries
apply after patterns. A PostgreSQL config row overrides an identical TOML
pattern. If any matching pattern sets `replicate = false`, it blocks opt-ins
from other patterns. An exact entry may override that result.

`config_column.match` applies to namespace, table, and column names together.
Walshadow rejects and logs invalid patterns and unknown match modes. Other
valid rules remain active.

Pattern table rules also control scope:

- `replicate = false` prevents automatic creation and removes matching routes
- `replicate = true` enables automatic creation and requested initial loads
  for matching tables, including tables created later
- existing routes keep pinned column mappings
- patterns never include `pg_*`, `information_schema`, or runtime config
  schemas

## `<schema>.config_*` tables

Install config tables with
[`sql/runtime_config_install.sql`](../sql/runtime_config_install.sql). Set psql
variable `walshadow_schema` to choose schema name; default is `walshadow`.
Walshadow reads these tables but never writes them, so source access remains
read-only from daemon. Installation creates four tables:

- `config_global` contains one row, enforced by
  `id smallint PRIMARY KEY CHECK (id = 1)`. It configures `row_budget`,
  `byte_budget`, `flush_timeout_ms`, `compression`, `retry_max_attempts`,
  `drop_table_strategy`
- `config_namespace` uses `namespace` as its key. It configures
  `target_database`, `auto_create`, and `drop_table_strategy`
- `config_table` uses `(namespace, relname)` as its key. It configures `match`
  (`exact`, `glob`, or `regex`), `target_database`, `target_table`, `replicate`,
  and `initial_load` (`none`, `copy`, `base_backup`, or `object_store`). A NULL
  target database uses namespace default. A NULL target table uses source
  relation name. Key uses relation name instead of relfilenode because a
  forward-declared table has no relfilenode when config row is inserted
- `config_column` uses `(namespace, relname, attname)` as its key. It configures
  `match` (`exact`, `glob`, or `regex`) and `target_type`

Nullable config values use daemon default or TOML value when set to NULL. This
allows new config fields to be added without breaking a newer daemon against an
older installation. TOML `[table.*]` blocks accept same mode strings. Omitting a
TOML key has same effect as SQL NULL. All four tables use
`REPLICA IDENTITY FULL` for WAL decoding described below.

## Reading config writes

Walshadow recognizes a config change by resolved relation name: configured
schema and one of four table names. It does not rely on relfilenode, so
`TRUNCATE`, `VACUUM FULL`, and table rewrites require no config refresh. Config
changes are excluded from ClickHouse replication.

Each WAL record contains enough information to read a complete config row
without earlier in-memory state or a before-image lookup. With minimum supported
`wal_level=logical`, PostgreSQL logs complete new tuples for `INSERT` and
`UPDATE`. `REPLICA IDENTITY FULL` supplies row key for `DELETE`. Extra WAL cost
is small because config writes are infrequent and rows are small. Resolver
validates values when it merges config, not while decoding WAL.

## Apply at commit LSN

A config row takes effect once at its commit LSN. Walshadow orders it with heap
rows and catalog events using same LSN barrier used for DDL. Column changes also
invalidate cached encoder plans.

Routing state is fixed for an entire transaction. See Transaction planner in
[emitter.md](emitter.md). A config event does not affect transaction currently
being planned. Transactions planned before config commit use old routing state,
and transactions planned after it use new state. One transaction never mixes
routing versions, and a config change does not reroute rows from its own
transaction.

## Initial config load

At startup, Walshadow loads PostgreSQL config tables after catalog seed and
before starting WAL pump. WAL becomes its only source for later changes.
Initial loads for pinned TOML mappings start after PostgreSQL config is loaded,
so SQL include and exclude rules take precedence. See [add_table.md](add_table.md)
for backup-based initial load modes.

Initial load also verifies installation. If TOML names a schema that does not
contain expected config tables, daemon refuses to start. An installed but empty
schema uses TOML defaults and behaves like a TOML-only deployment.

## Merge and reload

Each merge starts with TOML, applies PostgreSQL config, then applies explicit CLI
flags. Resolver builds a complete replacement snapshot before publishing it.
Subscribers therefore see either old snapshot or new snapshot, never a partial
update. Config apply and concurrent reload are atomic relative to each other.

A reload reads TOML again and merges current PostgreSQL and CLI values.
Reloading ignores connection parameters from file. Read and parse errors keep
last valid snapshot active. SIGHUP and control socket command `config reload`
use same path described in [control.md](control.md). Reload remains available
for process lifetime, although a missing config-table installation still fails
startup. In metrics-only mode without `--ch-config`, reload does nothing.

Only explicitly supplied CLI flags override other config sources and continue
to do so after reload. Omitted flags leave selection to PostgreSQL config and
TOML. This rule currently applies to `--drop-table-strategy` and
`--ch-flush-timeout-ms`.

## Invalid runtime values

Resolver validates PostgreSQL config values during merge. If a value is
invalid, it rejects that value and keeps value selected before PostgreSQL layer.
WAL pump continues, and other valid keys still apply. Each rejection increments
a counter and writes a `WARN` log entry.

Validation checks following constraints:

- `drop_table_strategy` must use an accepted value
- `row_budget` and `byte_budget` must be positive and within supported ranges
- `flush_timeout_ms` and `retry_max_attempts` must be within supported ranges
- `compression` must name an accepted codec compiled into current build. For
  example, a build without zstd support rejects zstd during merge instead of
  failing when inserter reconnects
- `config_column.target_type` must parse as a ClickHouse type. Compatibility
  with source descriptor is checked later when building a plan, as described in
  [Column overrides](#column-overrides)

`config_table.target_database` and `target_table` may override destination only
for tables already mapped in TOML, which supplies column projection. A row for
an unmapped table would require deriving its columns, so Walshadow skips it and
logs a warning instead of sending an `INSERT` without columns. NULL leaves that
part of destination unchanged.

## Column overrides

Column rules use same precedence as name patterns. Walshadow applies least
specific patterns first and exact rules last. When TOML and `config_column`
define same rule, `config_column` takes precedence.

- TOML name rules may set ClickHouse column names and types. New tables and
  columns use these values in mappings and DDL. A custom type removes any
  generated default. Nullable columns are omitted from `ORDER BY`.
- `config_column` may change encoder type for an existing column. It cannot
  rename a ClickHouse column or alter its type.

Walshadow applies `config_column.target_type` in two stages because syntax and
source compatibility become known at different times:

1. During merge, target type must parse as a ClickHouse type. Resolver rejects
   a malformed type, increments rejection counter, logs a warning, and keeps
   last accepted override. Last accepted override may differ from current
   PostgreSQL row after an invalid update. If initial config load contains an
   invalid value, no earlier override exists and column keeps type derived from
   source descriptor. Deleting a config row explicitly clears its override.
2. During plan construction, source descriptor and target mapping are both
   available. Walshadow resolves `attname` to `attnum` and uses override only if
   it is compatible with wire encoding. Encoding does not perform arithmetic
   conversion. A Decimal source may use any Decimal, String, or signed
   `Int32`, `Int64`, `Int128`, or `Int256` as a scale-zero decimal. For example,
   `numeric(38,0)` may use `Int128`. A string-shaped source requires a
   string-shaped target. A fixed-width source may use a same-width, non-Decimal
   reinterpretation, such as `Int32` to `UInt32`. Incompatible conversions,
   such as `numeric` to `Float32` or `Int32` to `String`, keep default type and
   produce a warning.

Column changes run under reorder barrier. Barrier flushes batcher and clears its
plan cache before publishing new snapshot, so later rows rebuild plans from new
config. COPY and backup backfills read same snapshot and use same overrides.
Initial bootstrap of a new deployment uses only TOML because resolver is not
yet running.

Runtime overrides change only encoder projection. Operator must migrate an
existing ClickHouse column when its stored type needs to change.

## Mapping lifecycle

Publishing config rebuilds complete routing map from resolver inputs. Resolver
therefore stores every runtime mapping change. It maintains explicit opt-in
mappings and a derived layer for mappings created by `auto_create` or updated
from source `ALTER TABLE` changes.

Resolution starts with TOML, then applies derived mappings, then explicit
opt-ins. An explicit opt-in therefore overrides automatic derivation. Changes
from source `ALTER TABLE` are copied into derived layer and override original
TOML mapping. A SIGHUP reload cannot undo an applied source change. After a
restart, Walshadow derives state again from TOML and WAL replay.

With `drop_table_strategy = "drop"`, source `DROP TABLE` removes derived or
opt-in mapping so a later `CREATE TABLE` can derive new columns. If a PostgreSQL
config row still sets `replicate = true`, resolver keeps it as a declaration
for a table that does not yet exist. Recreating source table then materializes
mapping from new descriptor. A pinned TOML mapping remains because operator
manages it. Drop strategy recreates its ClickHouse destination from that
mapping, so a create, drop, and recreate sequence needs no separate ClickHouse
change.

## Related documentation

- [emitter.md](emitter.md) describes routing maps, namespace defaults, encoder
  plans, and resolved snapshot
- [shadow.md](shadow.md) describes catalog updates sent to DDL applicator and
  config refreshes from resolver
- [future/runtime_config_from_pg.md](future/runtime_config_from_pg.md) describes
  possible signal commands, config fields, degraded mode, and observability
