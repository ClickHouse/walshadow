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

End-to-end demo wiring source PG → walshadow → ClickHouse. Step-by-step
in [docker/DEMO.md](docker/DEMO.md). Short form:

```
git submodule update --init --recursive
docker compose -f docker/docker-compose.yml up --build -d
docker compose -f docker/docker-compose.yml logs -f walshadow
```

Wait for the `shadow caught up to bootstrap end_lsn` line, then drive
changes on source and read them back from CH (full sequence in
[docker/DEMO.md](docker/DEMO.md))

For pgbench with Grafana dashboards plus live schema-change propagation:

```
docker compose -f docker/docker-compose.yml -f docker/docker-compose.demo.yml up --build -d
```

then open http://localhost:3000. Walkthrough in
[docker/DEMO.md](docker/DEMO.md)

## Source PG requirements

Enforced at daemon boot by `src/preflight.rs`. Daemon refuses to start
when any of these fails:

- `server_version_num >= 160_000`, shadow major equals source major
- `wal_level = logical`
- every mapped relation has a row key for deletes: a PRIMARY KEY
  (`REPLICA IDENTITY DEFAULT`), `USING INDEX`, or `FULL`. `NOTHING` and
  keyless `DEFAULT` are rejected. `FULL` is accepted, not required
- `--slot`, if set, names an existing physical replication slot

Skip with `--skip-preflight` only for recovery drills

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

The PG extension under `pgext/` is built separately via PGXS, only
needed for the decode oracle:

```
make -C pgext install
```

Loaded on shadow PG with `CREATE EXTENSION walshadow`. Absent extension
surfaces as `oracle fallback=N` on the status line; the daemon ships
raw on-disk bytes for `PgPending` types without it

## Running standalone

Minimum viable invocation against an existing source PG + shadow PG:

```
walshadow-stream \
    --host source.example \
    --user replicator \
    --out-dir /var/lib/walshadow/wal \
    --shadow-socket-dir /var/run/postgresql \
    --spill-dir /var/lib/walshadow/spill \
    --ch-config /etc/walshadow/ch.toml
```

Without `--ch-config` the daemon stays metrics-only (no CH emission).
Pass `--metrics-bind 127.0.0.1:9484` for a Prometheus scrape endpoint.
See `walshadow-stream --help` for the full surface (bootstrap modes,
walsender tuning, retention, etc.)

### CH emitter config

TOML, see [docker/ch-config.toml](docker/ch-config.toml) for a minimal
example. Shape:

```toml
[ch]
host = "localhost"
port = 9000
database = "default"
user = "default"
password = ""
compression = "lz4"

[table."public.users"]
target = "users"
columns = [
    { attnum = 1, target = "id",    type = "UInt64" },
    { attnum = 2, target = "name",  type = "String" },
]
```

`attnum` values match `pg_attribute.attnum` (1-based) on the source
relation; `type` is the CH destination type walshadow advertises in
the INSERT block. SIGHUP reloads mappings atomically; connection
params stay boot-only

## Testing

```
cargo test
cargo clippy --all-targets -- -D warnings
```

Integration tests under `tests/` need `initdb` + `pg_ctl` on `PATH`
and spin a transient shadow PG per case. Walshadow-side timeouts are
seconds-scale by design — long timeouts mask stalls rather than
surface them

## Repository layout

```
src/                walshadow daemon + library
src/bin/            CLI entry points (stream, filter, classify)
clickhouse-c-rs/    CH-Native client, separate submodule
pgext/              walshadow decode-bridge extension (PGXS)
architecture/       overview + internals diagrams
plans/              component design docs (overview.md is the baseline)
docker/             docker-compose demo + Dockerfile
tests/              integration suite
fixtures/wal/       golden WAL fixtures for offline tests
```
