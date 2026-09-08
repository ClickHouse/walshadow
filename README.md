# walshadow

> [!WARNING]
> **Experimental**
>
> **Project status: Development Preview**
>
> walshadow is under active development and being hardened through real-world
> testing and feedback. We recommend validating it thoroughly with your
> workloads. Interfaces and behavior may evolve

walshadow replicates PostgreSQL rows into ClickHouse from physical WAL,
including initial load, continuous changes, schema evolution, restarts, and
planned source switchover

## Get Started

- Follow [Quickstart](docs/quickstart.md) to run PostgreSQL, ClickHouse, and
  walshadow locally with Docker Compose
- Follow [Getting started](docs/getting-started.md) to connect existing
  PostgreSQL and ClickHouse databases

PostgreSQL 16 or newer is required. Source and walshadow shadow must use same
PostgreSQL major version

## Documentation

- [Documentation index](docs/README.md)
- [Architecture](architecture/README.md)
- [Configuration](docs/configuration.md)
- [Table selection](docs/table-selection.md)
- [Destination tables](docs/destination-tables.md)
- [Schema changes](docs/schema-changes.md)
- [Operations](docs/operations.md)
- [Planned source switchover](docs/failover.md)
- [Current limitations](docs/limitations.md)

Engineering rationale lives in [plans](plans/INDEX.md), architecture diagrams
in [architecture](architecture/README.md), and open work in
[future plans](plans/future/INDEX.md)

## Build from source

Clone repository with recursive submodules, then build Rust workspace and
PostgreSQL module

```bash
# Clone source plus required pg-clickhouse-c and clickhouse-c submodules
git clone --recurse-submodules https://github.com/ClickHouse/walshadow.git
cd walshadow

# Build release binaries
cargo build --release

# Build and install PostgreSQL module with PGXS
make -C pgext install
```

Default features include LZ4. Use `--no-default-features` for uncompressed
builds or enable `zstd` for Zstandard support

Built binaries:

- `walshadow-stream`, replication daemon
- `walshadow-filter`, segment-level filter for offline WAL files
- `walshadow-classify`, record-level classifier for diagnostics

walshadow PostgreSQL module is not an SQL extension. Managed shadow loads it
through `shared_preload_libraries`

## Test

```bash
# Build PostgreSQL module before integration tests
make -C pgext

# Run test suite and lints
cargo nextest run --workspace --all-targets
cargo clippy --all-targets -- -D warnings
```

Integration tests require `initdb` and `pg_ctl` on `PATH`
