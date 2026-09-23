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

`[table.<schema>.<relname>]` names a table in source database selected by
`dbname` in `[source]`. Use `[database.<dbname>.table.<schema>.<relname>]` to
name a table in another database of the same cluster, which one process
replicates alongside. See [several databases](multi-database.md)

Pass file with `--ch-config`. Loader also merges sibling directory formed by
replacing `.toml` with `.d`, for example `ch-config.d/*.toml`

Invalid values and incompatible mapping fields fail validation instead of
falling back silently. Unknown keys in `[table.*]`, `[namespace.*]`, and
`[database.*]` are errors; other sections ignore them, so check spelling there
when a setting has no effect

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

`[ch] flush_timeout_ms` defaults to 200 and bounds how long a row waits before
its INSERT seals, so it sets both replication lag and part rate: one part per
table per window under continuous load. Raise it to coalesce more rows into
fewer, larger parts; lower it for tighter lag at the cost of merge pressure

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

ClickHouse tails bound sealed encoder buffers by startup `[ch] byte_budget`,
including spare vector capacity. Each batch targets that budget divided by
inserter count, keeping concurrent inserts within a byte limit. Partial batches
across tables share another `byte_budget`; a producer waiting to hand off a
sealed batch can hold one additional batch. A single oversized row may exceed
these limits. Oracle responses and native client buffers remain additional costs

Sealed-buffer permits stay held through resolution, retries and insert completion.
This pool is separate from decoded-payload admission to avoid circular waits.
Live byte-budget reductions lower flush thresholds; increasing sealed-buffer
capacity requires restarting walshadow

`walshadow_process_threads` reports OS threads

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

### Process-only deployment

Pass `--keep-shadow-running` or set `WALSHADOW_KEEP_SHADOW_RUNNING=true` to
leave managed shadow PostgreSQL running when walshadow exits. On startup,
walshadow checks recovery mode, data directory, restore command and bridge
configuration before adopting it, and fails startup rather than restarting
PostgreSQL. A changed shadow port or socket directory surfaces as a connection
failure during that check, not as a named mismatch. Extension replacement and
increases in bridge worker count still require a separately planned PostgreSQL
restart. `primary_conninfo` needs no restart: walshadow repoints an adopted
shadow at its own walsender by reload.

For systemd, install `deploy/walshadow.service.d/process-restart.conf` as
`/etc/systemd/system/walshadow.service.d/process-restart.conf` and run
`systemctl daemon-reload`. Launcher must `exec` walshadow, so MainPID receives
SIGTERM. Drop-in scopes signals to walshadow; stopping unit deliberately leaves
shadow running. Stop shadow explicitly with `pg_ctl` when retiring instance.
An old binary does not honor preservation flag. For first migration, install
new binary and drop-in, reload systemd, then signal only old MainPID with
`systemctl kill --kill-who=main --signal=SIGKILL walshadow`. Automatic restart
launches new binary and adopts running shadow. This uses last durable cursor;
old archive loop may have left it behind. Subsequent upgrades use normal
`systemctl restart walshadow` and graceful checkpointing.

`--archive-prefetch` controls concurrent archive downloads, default 4, range
1–64. Downloads overlap filtering and decoding, with ordered consumption and
a bounded queue. At most approximately `(prefetch + 2) * 16 MiB` of decoded WAL
is retained, plus decompressor and pipeline buffers. That memory is not charged
to `[memory] resident_payload_max`, so raise this only where the unmetered half
of the host has room. Segments decode straight into memory over the same
decrypt and decompress path as `wal-fetch`, so recovery writes no staging files
and an interrupted leg leaves none behind.

Archive recovery now runs through normal checkpoint, pause, metrics and signal
handling. Restart cursor remains bounded by durable WAL, shadow replay and
oldest unacknowledged transaction. Graceful exit drains pipeline and writes a
final checkpoint. Prefetched bytes never advance durable cursor.

Object-store table backfills honor `[bootstrap] object_store_parallelism`,
defaulting to 8 when absent. This controls concurrent parts, not ClickHouse
inserter count.

`[bootstrap] copy_fallback` defaults to `true`. Failed `base_backup` and
`object_store` table loads retry through source COPY, one table at a time per
failed pass. Set `copy_fallback = false` to leave failures pending until restart.
Fallback preserves original load LSN and persists COPY mode before reading source;
restarts resume recorded mode even if fallback is subsequently disabled. COPY
adds source database read load. Completed or publishing staging tables never
fall back. This option does not apply to greenfield bootstrap or publication
failures. Failed COPY remains pending until restart.

`walshadow_archive_fetch_seconds_total` sums fetch durations across concurrent
requests, including decrypt and decompression. `walshadow_archive_replay_seconds_total`
measures filter/dispatch time including downstream backpressure. Compare rates
with archive segments and queue depth to locate remaining serialization.

`walshadow_archive_wait_seconds_total` measures pump waits for prefetched WAL;
`walshadow_pump_queue_wait_seconds_total` measures sends blocked behind decoder.
These distinguish insufficient fetch concurrency from downstream backpressure.

### Object-store backfill restart

`spill/backup_backfill/backup_replay.json` carries an object-store pass across
a restart. It pins the backup name, the relation mappings and the staging table
UUIDs; a mismatch on any of them discards the file and restarts the pass.
Staging tables and TOAST mirrors survive, so a resumed pass keeps the rows an
earlier attempt inserted. ClickHouse may receive duplicate rows after a crash;
`ReplacingMergeTree` resolves them at unchanged row keys and `_lsn` values.

Every phase records only behind proven inserts, so a restart repeats work and
never skips it:

- page walk, per heap file. An archive part holding no `pg_xact` or
  `pg_multixact` segment and no unrecorded heap file is not fetched again.
  Transaction-status files are always re-read: the visibility gate rebuilds its
  whole view on each attempt
- deferred TOAST replay, as a byte offset into the spool
- WAL gap replay, as the lowest LSN a resumed replay must re-read. That is the
  oldest transaction still buffered when the last segment closed, so a
  transaction spanning the boundary replays whole

Checkpoint after completed batches, targeting 30 seconds between saves. Slow
fetches or inserts can extend that interval. Restart repeats archive
validation.

Older runs without a checkpoint still repeat the backup walk. Do not synthesize
an offset from read progress: prefetched rows may not have reached ClickHouse.
Passes producing undecided visibility rows restart their pass from whatever
walk progress they recorded.

A resumed pass logs what it skipped under `walshadow::backfill` at info:
`resuming backup page walk` names the recorded file count before the walk
starts, `backup page walk resumed` names the heap files declined and the
archive parts never fetched once it ends.

### Resumable COPY

`initial_load = 'copy'` reads the table in heap-block chunks, one
`COPY (SELECT … WHERE ctid >= … AND ctid < …)` per chunk under its own insert
tail. `backfills.toml` records the next block only once that tail proved its
rows durable, so a restart repeats at most one chunk. Repeated rows carry the
same `_lsn = S` and collapse.

`[bootstrap] copy_chunk_blocks` sets the chunk size in 8 KiB pages, defaulting
to 131072 (1 GiB, PostgreSQL's segment size). A table smaller than one chunk
issues exactly the single unqualified COPY it always did.

The cursor is paired with the relation's filenode. `VACUUM FULL`, `CLUSTER`,
`TRUNCATE` and rewriting `ALTER TABLE` relocate rows, so a filenode change
between chunks restarts the table rather than resuming into pages that no
longer hold the same rows. Chunk predicates need a TID range scan; leave
`enable_tidscan` on at the source.

COPY rows arrive detoasted, but later updates that leave a large value
unchanged carry its TOAST pointer, which resolves only from the chunk mirror.
In `clickhouse` value mode a COPY load therefore first copies the table's TOAST
heap into the mirror at `_lsn = S`. For a selection applied live, streaming
waits at the selection point until that copy finishes, so size maintenance
windows for large TOAST heaps. A load falling back from `base_backup` or
`object_store` copies it once COPY starts, after streaming has moved on;
updates applied in between can still store NULL. The source role needs `USAGE`
on schema `pg_toast` and `SELECT` on TOAST heaps, which `pg_read_all_data`
grants; without it the load fails and stays pending. `backfills.toml` records
the copy, so a restart does not repeat it.

The COPY session pins `TimeZone`, `DateStyle`, `IntervalStyle`,
`extra_float_digits` and `bytea_output` to the values the shadow bridge worker
uses, so values selected as text render as streamed rows do regardless of role
or database defaults.

Spill-dir state files `backfills.toml`, `toast_retires.toml` and
`visibility_carry.toml` record the source system identifier; boot rejects
files another source wrote. A file from an older build without the field loads
once and is rewritten with it. An unreadable `backfills.toml` stops boot:
removing it re-runs every load and can orphan `__wsstg` staging tables.
