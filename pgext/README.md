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

Worker failure tests also need the fault shim, which is outside `all` so a
runtime image build never compiles it:

```sh
make -C pgext faultshim.so
```

`coverage-build` builds both

## Fault injection

`faultshim.c` builds `faultshim.so`, a test-only libc interposer. It is never
linked into `walshadow.so`, never installed, and reaches PostgreSQL only through
`LD_PRELOAD` on one test cluster's own `pg_ctl`, so concurrent tests share no
fault state. Wrappers act on calls made from `walshadow.so`, or on descriptors
that module created, which is what keeps postmaster and backend sockets out of
it. `pg_set_noblock` and `closesocket` live in the server binary, beyond
`LD_PRELOAD`'s reach, so `fcntl` and `close` are scripted by descriptor role
instead of by caller

Two files name the script and its counters:

- `WS_FAULT_SCRIPT`: one rule per line, `<op> <nth> <times> <action> <arg>`
  `nth` is a 1-based occurrence of that op, `times` 0 means every occurrence
  from `nth` on, `arg` is an errno for `fail` and a byte count for `short`
- `WS_FAULT_STATE`: fixed-size shared counters. Occurrence numbering survives a
  worker restart, so one arming can walk a chain of startups where each attempt
  fails one step further along

The fixture rearms by bumping a generation word, which reloads the script and
restarts occurrence counting. [`tests/common/pgext.rs`](../tests/common/pgext.rs)
holds the Rust side and duplicates the state layout; keep the op order in step

## Coverage

Use GCC, matching `gcov`, and `gcovr` to gather C line, branch, and function
coverage from integration tests. Install `gcc` and `gcovr` on Debian/Ubuntu

```sh
make -C pgext coverage-build
cargo nextest run --workspace --all-targets --locked
make -C pgext coverage-html
```

`coverage-build` cleans previous objects and profiles, then rebuilds with GCC
`--coverage -fprofile-abs-path` and normal PGXS optimization. GCC/gcov accounts
for `siglongjmp` into `PG_CATCH`; LLVM source coverage can report executed catch
bodies as uncovered. Pass `PG_CONFIG` to select another PostgreSQL major, put
matching PostgreSQL binaries on PATH for tests

`bridge`, `pgext_worker`, `pgext_listener`, `pgext_io_faults` and
`pgext_protocol` are the module's own binaries, but the oracle suites reach the
rest of `native.c`, so only the whole workspace covers all of it. A narrower
selection needs `COVERAGE_MIN_LINE=0 COVERAGE_MIN_BRANCH=0` on the report

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
raw notes and counters, gcovr JSON, LCOV, and HTML
Each PostgreSQL major must reach 100% line coverage, pinned in CI through
`COVERAGE_MIN_LINE=100`

`coverage-report` fails below `COVERAGE_MIN_LINE` or `COVERAGE_MIN_BRANCH`. The
whole workspace on PostgreSQL 17 covers every line and every function, so the
line gate is the whole of it. Branches stop short of that, on legs no test
reaches:

- `ereport` tests `errstart`, whose false arm wants a log level the cluster
  never runs at
- the header-only ClickHouse type accessors guard a null type their callers
  cannot pass
- `HEAP_XMAX_IS_LOCKED_ONLY` has a leg only a pg_upgraded tuple takes, and
  `TransactionXmin` bounds every xid a snapshot can hand `ws_xid_is_ours`
- `CHECK_FOR_INTERRUPTS` tests `InterruptPending`, which neither of the two
  handlers the worker installs raises

Restore ordinary build after gathering coverage:

```sh
make -C pgext clean
make -C pgext
make -C pgext faultshim.so
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
