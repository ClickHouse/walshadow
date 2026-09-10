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
