# pgext — walshadow PG module

Loadable module for shadow PG, built by PGXS. Read
[value architecture](../architecture/values.md) for role in replication and
[worker.c](worker.c) for protocol and failure handling. This guide covers
building and loading it

## Build

Needs PG server headers (`postgresql-server-dev-<N>` or equivalent) plus the
pinned dependency:

```sh
git submodule update --init --recursive pgext/pg-clickhouse-c
make -C pgext
```

That leaves `pgext/walshadow.so` in the tree, built against the `pg_config` on
PATH. Makefile header carries the non-default builds: `PG_CONFIG` for another
PG, `DESTDIR` install for a rootless prefix

## Coverage

Use GCC, matching `gcov`, and `gcovr` to gather C line, branch, and function
coverage from integration tests. Install `gcc` and `gcovr` on Debian/Ubuntu

```sh
make -C pgext coverage-build
cargo nextest run --test bridge --locked
make -C pgext coverage-html
```

`coverage-build` cleans previous objects and profiles, then rebuilds with GCC
`--coverage -fprofile-abs-path` and normal PGXS optimization. GCC/gcov accounts
for `siglongjmp` into `PG_CATCH`; LLVM source coverage can report executed catch
bodies as uncovered. Pass `PG_CONFIG` to select another PostgreSQL major, put
matching PostgreSQL binaries on PATH for tests
Run full workspace suite for broader coverage

Open `pgext/coverage/html/index.html`, or consume `pgext/coverage/lcov.info`
Reports cover `pgext/*.c` and `pgext/*.h`. Coverage notes (`*.gcno`) keep untouched
functions visible alongside execution counters (`*.gcda`)

GCC writes counters beside objects in `pgext/` and merges them across PostgreSQL
processes and test runs using file locking. Rust's `LLVM_PROFILE_FILE` does not
affect gcov output. PostgreSQL user needs write access to build directory
Stop test clusters before reporting or resetting profiles, normal process exit
writes coverage data. SIGKILL and immediate shutdown can lose counters from
terminated processes

`coverage-report` copies notes and counters into `coverage/raw` and produces
`coverage/coverage.json` plus LCOV. `coverage-html` also renders HTML from JSON
Use `make -C pgext coverage-reset` before repeating tests without rebuilding;
it removes counters and reports while preserving coverage notes
Override `COVERAGE_CC` on build command and `GCOV` on report command for versioned
tools, for example `COVERAGE_CC=gcc-14` and `GCOV=gcov-14`. Override `GCOVR` to
select a specific gcovr executable. Match gcov version to GCC

CI gathers `coverage-pgext-pg16`, `coverage-pgext-pg17`, and
`coverage-pgext-pg18` artifacts from instrumented workspace suite, each containing
raw notes and counters, gcovr JSON, LCOV, and HTML. C coverage has no percentage
gate yet

Restore ordinary build after gathering coverage:

```sh
make -C pgext clean
make -C pgext
```

## Loading

Not an extension: no control file, no SQL script, and `CREATE EXTENSION` can
never run on a shadow whose catalog is a physical copy of source's. Sole entry
point is `shared_preload_libraries = 'walshadow'`, which walshadow writes into
shadows it owns

An uninstalled build tree reaches PG through `dynamic_library_path`:

- daemon: `--bridge-lib-dir <repo>/pgext`
- tests: `pgext_dir()` asserts `walshadow.so` is present, so an unbuilt tree
  fails rather than silently skips

A `make install` into `$libdir` needs neither
