# walshadow

Catalog-replay sidecar that turns a physical-WAL stream from PostgreSQL
into CDC for ClickHouse. Shadow PG runs as a recovery-mode standby fed
by walshadow's walsender, exposing source's catalog state for the decoder
without ever hosting user-heap data

For design, see [plans/overview.md](plans/overview.md) and component
docs indexed at [plans/INDEX.md](plans/INDEX.md); for diagrams,
[architecture/README.md](architecture/README.md); for future work,
[plans/future/INDEX.md](plans/future/INDEX.md)

## Quick start (docker)

Point walshadow at source PostgreSQL and destination ClickHouse:

```
git submodule update --init --recursive

export PG_MAJOR=17
export WALSHADOW_SOURCE_URL='postgres://replicator:secret@db.internal:5432/app?sslmode=require'
export WALSHADOW_CH_URL='clickhouse://default:secret@ch.internal:9000/cdc'

docker compose -f docker/docker-compose.yml build
docker compose -f docker/docker-compose.yml run --rm walshadow \
    init --all-tables
docker compose -f docker/docker-compose.yml up -d
docker compose -f docker/docker-compose.yml logs -f walshadow
```

Set `PG_MAJOR` to source PostgreSQL major. `init` validates both connections,
creates destination database, and selects source tables with row keys. Fix any
reported source requirements, then rerun it

Wait for `shadow caught up to bootstrap end_lsn`, change a selected source row,
then query matching ClickHouse table:

```
clickhouse-client --host ch.internal --database cdc --query \
    "SELECT * FROM users FINAL ORDER BY id"
```

See [docker/QUICKSTART.md](docker/QUICKSTART.md) for table selection and
teardown. Add browser status with provisioned Prometheus and Grafana:

```
docker compose -f docker/docker-compose.yml \
    -f docker/docker-compose.grafana.yml up -d
```

Open http://localhost:3000 to inspect health, lag, throughput, queues, memory,
backfills, and source-transition state

## Source PG requirements

Enforced at daemon boot by `src/preflight.rs`. Daemon refuses to start
when any of these fails:

- `server_version_num >= 160_000`, shadow major equals source major
- `wal_level = logical`
- every mapped relation has a row key for deletes: a PRIMARY KEY
  (`REPLICA IDENTITY DEFAULT`), `USING INDEX`, or `FULL`. `NOTHING` and
  keyless `DEFAULT` are rejected. `FULL` is accepted, not required

Skip with `--skip-preflight` only for recovery drills

## Recommended Configurations:

In case of backlog buildup, we need either a physical replication slot or some way to get
data from a cloud backup location. Otherwise walshadow would crash with "WAl not found error"
 
- `slot`, if set, names an existing physical replication slot
or
- `backup`, if set, would pull data from backup location like standby pg

## Running standalone

The easiest path: point the daemon at source PG + an empty data dir and let
it bootstrap its own shadow. First install the PG module so the shadow can
preload it (`make -C pgext install`; see [Building](#building-from-source)),
create the target CH database (walshadow makes tables, not databases), then:

```
walshadow-stream \
    --source-url postgres://replicator@source.example/app \
    --ch-url     clickhouse://default@ch.example:9000/cdc \
    --out-dir                   /var/lib/walshadow/wal \
    --spill-dir                 /var/lib/walshadow/spill \
    --shadow-socket-dir         /var/run/postgresql \
    --bootstrap-shadow-data-dir /var/lib/walshadow/shadow \
    --walsender-bind            127.0.0.1:6510 \
    --ch-config                 /etc/walshadow/ch.toml
```

Both URLs also read from `WALSHADOW_SOURCE_URL` / `WALSHADOW_CH_URL`, and both
decompose into the discrete `--host` / `--port` / … flags. `walshadow-stream
init` writes the config file; see [docker/QUICKSTART.md](docker/QUICKSTART.md).
`ch.toml` is the connection block:

```toml
[ch]
host = "localhost"
database = "analytics"
```

On first boot this base-backups source into the shadow dir, does a direct
initial copy of **every** table into ClickHouse, then tails changes live. A
restart resumes from the durable cursor — no re-copy. That's it; no per-table
config and no separately-managed shadow PG.

Notes:
- `--walsender-bind` needs an explicit non-zero port for a daemon-owned shadow
  (it is baked into shadow's `primary_conninfo`).
- `--bootstrap-shadow-data-dir` must be a new/empty dir; an initialized one
  resumes instead of re-bootstrapping.
- With neither `--ch-url` nor `[ch]` in `--ch-config`, the daemon stays
  metrics-only (no CH emission). Pass `--metrics-bind 127.0.0.1:9484` for a
  Prometheus scrape endpoint.
- `--ch-config` names a file that need not exist yet: the control socket
  writes its fragments into the sibling `ch-config.d/`.
- To manage shadow PG yourself, drop `--bootstrap-shadow-data-dir` (bootstrap
  defaults to `off` then, streaming only). See `walshadow-stream --help` for
  the full surface (bootstrap modes, walsender tuning, retention, etc.)

### Live control

`--control-socket` opens a management socket the same binary speaks:

```
walshadow-stream ctl status                 # lag, rows synced, pause state
walshadow-stream ctl tables                 # source tables, `*` = replicated
walshadow-stream ctl add public users       # opt in, CH table auto-creates
walshadow-stream ctl pause                  # freeze WAL consumption
walshadow-stream ctl source postgres://…    # repoint the source endpoint
walshadow-stream ctl help
```

Each verb applies to the running session, which reconfigures in place with
no restart. Mutations land in `ch-config.d/50-api.toml`, leaving
operator-owned config untouched. Details in
[plans/control.md](plans/control.md)

### CH emitter config

`[ch]` connection defaults: `port = 9000`, `user = "default"`, empty
`password`, `compression = "lz4"`, `secure = false`. With `[stream]
replicate_all` on (the default) a bare `[ch]` block replicates every user
table into `[ch] database` with high-throughput batching. To narrow scope:

```toml
[ch]
host = "localhost"
database = "analytics"

# Opt a table out (wins over replicate_all):
[table.public.audit_log]
replicate = false

# Or list explicitly — replicate only what you name:
[stream]
replicate_all = false

# Opt-in: shape comes from the source descriptor, CH table auto-creates
[table.public.orders]
replicate = true
initial_load = "copy"

# Pinned: exact columns, nothing outside this list replicates
[table.public.users]
replicate = true
initial_load = "none"
target_table = "users"
columns = [
    { attnum = 1, target = "id",    type = "UInt64" },
    { attnum = 2, target = "name",  type = "String" },
]
```

Table blocks take two key levels, `[table.<namespace>.<relname>]`; a name
carrying a dot or other TOML-special character quotes per key rules, e.g.
`[table.public."odd.name"]`.

`replicate_all` skips system schemas (`pg_*`, `information_schema`, the
`[runtime_config]` schema). `attnum` values match `pg_attribute.attnum`
(1-based) on the source relation; `type` is the CH destination type walshadow
advertises in the INSERT block. SIGHUP (or `ctl reload`) re-reads the file:
mappings swap atomically, and a changed source or destination endpoint is
redialled without a restart

Name a set of tables instead of one, with `match`:

```toml
[table.app."events_*"]              # each name part an anchored pattern
match = "glob"                      # exact (default) | glob | regex
replicate = true
initial_load = "copy"

[table.app."*_audit"]               # a guardrail: excludes even what
match = "glob"                      # a wider opt-in swept in
replicate = false
```

Everything a `config_table` row carries — destination, `replicate`,
`initial_load` — also takes `match = "glob"` / `"regex"`, which is how you
scope tables that do not exist yet: under `auto_create` / `replicate_all` a
relation reaches CH the first time it is seen, before a row naming it could
arrive. See [plans/config.md](plans/config.md).

Columns take the same treatment. A `columns` entry keys on `attnum` or on
`name`, never both, and one array holds one kind:

```toml
[table.app."*"]
match = "glob"
replicate = true
columns = [
    { name = "*_at", match = "glob", type = "DateTime64(6, 'UTC')" },
    { name = "legacy_id", target = "id" },   # rename, keep the bridge type
]
```

An `attnum` entry pins the projection outright, so it needs `target` + `type`
and only fits a block naming one relation. A `name` entry states the CH name
or type of a column the descriptor yields anyway: `target` and `type` are each
optional, a `match` entry may not set `target` (several columns cannot share
one CH name), and the whole array pins nothing — scope still comes from
`replicate` / `auto_create`. `config_column` rows take `match` the same way,
over the whole `(namespace, relname, attname)` key.


## Building from source

Workspace + two submodules:

```
git submodule update --init --recursive
cargo build --release
```

Default features pull `lz4`; `--no-default-features` for an
uncompressed-only build. `zstd` adds the ZSTD codec

Binaries land under `target/release/`:

- `walshadow-stream`, the daemon
- `walshadow-filter`, segment-level filter for offline WAL files
- `walshadow-classify`, record-level classifier for diagnostics

The PG module under `pgext/` is built separately via PGXS. It backs the
decode oracle and the catalog overlay reads:

```
make -C pgext install
```

Not an SQL extension: shadow's catalog is a read-only physical copy of
source's, so there is nowhere to run `CREATE EXTENSION`. Module must be
installed in shadow PG and loaded through `shared_preload_libraries`. Daemon
writes preload and `walshadow.*` settings for shadows it owns, then requires
worker socket. `--bridge-socket` defaults to
`<shadow-socket-dir>/walshadow-bridge.sock`

## Testing

```
make -C pgext
cargo nextest run --workspace --all-targets
cargo clippy --all-targets -- -D warnings
```

Integration tests under `tests/` need `initdb` + `pg_ctl` on `PATH`
and spin a transient shadow PG per case. Every shadow preloads the
module out of `pgext/`, so build it first: tests fail rather than skip
without it. Walshadow-side timeouts are
seconds-scale by design — long timeouts mask stalls rather than
surface them

CI runs the suite through [cargo-nextest](https://nexte.st), which
schedules across test binaries; `cargo test` still works but runs
binaries one at a time. Concurrency limits live in
`.config/nextest.toml`. Tests reserve TCP ports through
`tests/common/ports.rs`, so parallel runs — including several at once on
one machine — do not collide

## Repository layout

```
src/                walshadow daemon + library (submodules: emit/ source/ backfill/ decode/ catalog/ …)
src/bin/            CLI entry points (stream, filter, classify)
clickhouse-c-rs/    CH-Native client, separate submodule
pgext/              walshadow decode-bridge PG module (PGXS)
sql/                runtime-config overlay install SQL
architecture/       overview + internals diagrams
plans/              component design docs (overview.md is the baseline)
docker/             quickstart Compose file + Dockerfile
bench/              throughput / latency benchmark harnesses
tests/              integration suite
fixtures/wal/       golden WAL fixtures for offline tests
```
