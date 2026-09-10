# Build and test

Build Rust binaries with `cargo build --release`. Build PostgreSQL module before
integration tests, using server headers for PostgreSQL major under test
See [module build guide](../pgext/README.md) for PG_CONFIG and installation

```bash
make -C pgext
make -C pgext faultshim.so
cargo nextest run --workspace --all-targets --locked
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
RUSTDOCFLAGS='-D warnings' cargo doc --no-deps --lib --bins
```

Integration tests start PostgreSQL and ClickHouse instances. Put matching
PostgreSQL tools and ClickHouse on PATH, and install extensions required by
selected tests. Some tests return early when prerequisites are missing, so a
passing result alone does not prove integration scenario ran. Inspect skip
output and check module was built

[CI workflow](../.github/workflows/ci.yml) defines supported test matrix and
dependency setup. [Nextest config](../.config/nextest.toml) bounds cluster-test
concurrency to limit memory and disk use

## WAL fixtures and coverage

Regenerate fixtures using PostgreSQL major being tested:

```bash
WALSHADOW_USE_LOCAL=1 fixtures/wal/classify/capture.sh
WALSHADOW_USE_LOCAL=1 fixtures/wal/filter/capture.sh
WALSHADOW_USE_LOCAL=1 fixtures/wal/xlog_switch/capture.sh
WALSHADOW_USE_LOCAL=1 fixtures/wal/vacuum_full_pg_depend/capture.sh
```

Run instrumented suite and generate reports from same execution:

```bash
make -C pgext coverage-build
cargo llvm-cov clean --workspace
cargo llvm-cov nextest --workspace --all-targets --locked --no-report --no-fail-fast
cargo llvm-cov report --summary-only
cargo llvm-cov report --lcov --output-path /tmp/walshadow-lcov.info
make -C pgext coverage-html
```

Install GCC, matching `gcov`, and `gcovr` for
[PG module coverage](../pgext/README.md#coverage) of `pgext/*.c` and `pgext/*.h`

CI merges coverage across PostgreSQL 16, 17, and 18. Use fresh line-coverage
reports to locate missing behavior; exported function counts can include generic
instantiations. Do not disable lints or coverage to make a report pass

Performance workloads and deployment commands live in [bench](../bench/README.md)
Keep performance comparisons separate from correctness tests

## Documentation

Keep operating instructions in docs, system explanations and diagrams in
[architecture](../architecture/README.md), and unfinished work in
[plans](../plans/INDEX.md). Remove completed plans. Link source for low-level
behavior instead of copying implementation into prose
