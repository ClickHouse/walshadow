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

then open http://localhost:3000 for the Grafana dashboards, or
http://localhost:8088 for the click-to-drive UI — start the pgbench load,
insert rows, evolve the schema, and watch rowcount parity and the ClickHouse
schema converge. The stack comes up quiet; the load starts from the UI (or
add `--profile load` to the command above to start the standalone hammer with
the stack). Walkthrough in [docker/DEMO.md](docker/DEMO.md)

The demo overlay seeds the pgbench TPC-B schema on source and its
destination tables on CH through one-shot `source-init` / `ch-init`
services that walshadow waits on, so switching between the lean and demo
stacks needs no volume reset. To rebootstrap from scratch anyway:
`docker compose -f docker/docker-compose.yml -f docker/docker-compose.demo.yml down -v`

## Simplified demo (browser UI)

The click-to-drive surface: a row of buttons that write to `demo.users` on
the source and arm the pgbench load, with the downstream effect on
ClickHouse next to them. No psql, no Grafana queries, no login.

```
git submodule update --init --recursive

dc="docker compose -f docker/docker-compose.yml -f docker/docker-compose.demo.yml"
$dc up --build -d
```

First build is heavy (Rust release + PGXS shared object); later `up`s
reuse layers. Then open **http://localhost:8088**.

The page paints immediately — it gates on `service_started`, not on
walshadow's metrics port. Until bootstrap finishes, the walshadow tiles
read `—` and the status line says `walshadow bootstrapping — /metrics not
open yet`; the controls and the parity panel are live already, since they
only need source PG and ClickHouse. To watch bootstrap land:

```
$dc logs -f walshadow      # wait for: shadow caught up to bootstrap end_lsn
```

Six controls in one row — insert · rows · load · schema:

| control | what it does |
|---|---|
| **INSERT N ROWS** | `demo.users`: one server-side `INSERT … SELECT … FROM generate_series(…)`, chunked at 250k. Source rows step up; ClickHouse chases. |
| **UPDATE RANDOM ROW** | rewrites one existing row's email, picked by index range scan. The new version tops the rows feed; the older one is still below it — what CDC actually writes. |
| **DELETE RANDOM ROW** | deletes one existing row. The ClickHouse count drops by one while versions landed rises by one — the tombstone is itself a version, hidden by `FINAL WHERE _is_deleted = 0`. |
| **START / STOP WRITE LOAD** | arms the pgbench TPC-B hammer against the source (`-c 4 -j 2` by default), with live tps / latency under the button. Off until you press it. |
| **ADD COLUMN signup_ts** | live DDL — walshadow runs the `ALTER` on ClickHouse and auto-extends the column mapping, no config edit or restart. Bounded to 100 rows of `signup_ts = now()`. Disables itself once the column exists. |
| **DROP COLUMN** | `DROP COLUMN IF EXISTS signup_ts`, replicated too, so the schema beat repeats without a `down -v`. |

Below the controls: rowcount parity with one row per replicated table —
`demo.users` and the four pgbench TPC-B tables — each showing source rows vs
`count() FINAL WHERE _is_deleted = 0` on ClickHouse, the delta with a pill,
versions landed and the age of the last change. The counts are deduplicated so
they compare like with like against the source; the page's labels say what the
numbers mean rather than naming `FINAL`. Then the source and ClickHouse schemas
side by side, and the newest row versions by `_lsn` — undeduplicated, so every
CDC version shows. Apply lag and rows → CH /s ride in the header; the rest of
`/metrics` is Grafana's job.

With the write load armed, the throughput numbers are whole-pipeline —
pgbench included. What responds to *your* click is the `demo.users` parity
row, and `pgbench_history` is the row where a live delta shows up under load. For a quiet demo leave the load off, or press
STOP WRITE LOAD. (The standalone `pgbench` service still exists for a
browser-free run: `$dc --profile load up -d`, then `$dc stop pgbench` /
`$dc start pgbench`.)

Grafana on http://localhost:3000 is the time-series view of the same
stack. Full walkthrough, plus the CLI form of every button, in
[docker/DEMO.md](docker/DEMO.md).

Iterating on the UI itself: `docker/ui/app.py` and `docker/ui/index.html`
are bind-mounted, so `$dc restart ui` picks up edits without a rebuild.
Teardown: `$dc down -v --remove-orphans`.

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
replicate = true
initial_load = "none"
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
src/                walshadow daemon + library
src/bin/            CLI entry points (stream, filter, classify)
clickhouse-c-rs/    CH-Native client, separate submodule
pgext/              walshadow decode-bridge PG module (PGXS)
architecture/       overview + internals diagrams
plans/              component design docs (overview.md is the baseline)
docker/             docker-compose demo + Dockerfile
tests/              integration suite
fixtures/wal/       golden WAL fixtures for offline tests
```
