# Agent guide

Find build, test, lint and coverage commands in [development](docs/development.md).
Put operating guides in [docs](docs/README.md), system design in
[architecture](architecture/README.md) and unfinished work in [plans](plans/INDEX.md).

## Workflow

- Use Git only to inspect. An explicit maintainer request authorizes only the
  operation named. Leave edits in the working tree for the maintainer to
  review and land. They may be editing it too.
- Edit files in place. Emptying a file before rewriting it can lose source if
  the disk fills. Check free space before a full build, since `target/` can grow
  to hundreds of GB.

## Writing

- Let code explain itself. Use short comments for outside context such as
  PostgreSQL or ClickHouse behavior and wire formats. Leave out work history.
- Give direct instructions. Drop extra articles. Use commas instead of em dashes.
  Periods are optional.
- Cite upstream files by project and repo path, such as
  PG `src/include/access/transam.h`.
- Write plans in present tense. Leave out implementation status and history.
- Keep diffs small. Prefer deleting code when possible.

## Rust style

- For `Option` and `Result`, use `let ... else` to bind or exit, `map` and similar
  methods to compute a value, and `if let` for side effects. Use let chains for
  extra conditions. Keep `match` for three or more arms or when the second arm
  uses the value it extracts.
- Store namespace and name separately in structs, map keys, events and config.
  Join them into `ns.rel` or `db.table` only when building SQL.
- Keep lint and coverage checks enabled. Get maintainer approval before adding
  `#[allow]`, `#[coverage(off)]`, exclusions or lower thresholds.

## Module dependencies

Keep `src/` file dependencies free of cycles. Count `crate::`, `super::` and
`self::` paths outside tests, plus `pub use child::*` in `mod.rs`. Use this order
as a guide and depend only on earlier layers. A directory can mix layers.

1. Basic types and helpers (`pos`, `record`, `schema`, `atomic_stats`, `ch`,
   `ch::types`, `decode::page`, `decode::datum`, `toast::fetched`,
   `backfill::backup_source`, `ops::bridge`, `emit::pipeline::{fatal, row_ledger}`)
2. Config types and rules (`mapping`, `runtime_config`, `table_rules`,
   `column_rules`, `emitter_config`)
3. Codecs and shared records (`emit::column`, `emit::stats`, `backfill::tuple`,
   `backfill::backfill_types`, `pg`, `ops::oracle`)
4. Loaders and streams (`config`, `ch::conn`, `mapping::derive`,
   `source::{wal_stream, source_feed}`)
5. Writers, metrics and page walking (`emit::ch_emitter`, `emit::ch_ddl`,
   `backfill::{backfill_staging, backup_page_walk}`, `ops::metrics`)
6. Pipeline stages (`emit::pipeline::{planner, encode, batcher, resolver, inserter, tail, ack}`)
7. Backup scanning and startup (`backfill::{backup_checkpoint, visibility_pending}`,
   `emit::pipeline::bootstrap`)
8. Pipeline coordination (`emit::pipeline` in `mod.rs`, `emit::pipeline::reorder`)
9. Backfill drivers (`backfill::{copy_backfill, backup_backfill, visibility_gate}`)
10. Binaries in `src/bin/`

Keep `config`, pipeline stages and backfill drivers out of layers 1 to 3. If two
files need each other, move the shared code to the lowest file both already
depend on or to a new leaf file. A re-export still counts as a dependency.
An inherent `impl` can live in a higher layer than its type. Move tests with the
code they cover.

After adding imports across modules, run
`find src -name '*.rs' | awk -f ci/module_cycles.awk`. It prints each cycle
with the imports that form it.

## Testing

- Run integration tests with `WALSHADOW_REQUIRE_TOOLS=1`.
- Build pgext with `make -C pgext test-deps PG_CONFIG=<bindir>/pg_config` for the
  tested PostgreSQL version. Rebuild before debugging widespread bridge
  handshake failures, which indicate a stale `.so`.
- Make harnesses fail if `pgext/walshadow.so` is missing. Skip only for missing
  external tools.
- Integration tests should finish in seconds. Stop hung tests, read daemon
  stderr and leave timeouts alone.
- Use `anyhow::ensure!` inside `let result = async { ... }` in daemon end-to-end
  tests. `assert!` panics before the harness can print daemon stderr.
- Test changes to decoding or catalogs on PostgreSQL 16 through 19 since WAL
  differs by version. Invalidate the cache broadly before deciding what to do
  with decoded results.
- Keep 100% line coverage for Rust across majors combined and for pgext C on
  each major. Cover new branches in the same change. Prefer unit tests with stubs.
- Make `harness = false` benchmarks print `<name>: benchmark` for `--list` so
  nextest can list them.

## Design rules

- Use `_lsn` and `ReplacingMergeTree(_lsn)` to guarantee the final state.
  Avoid ordering guarantees during a drain if they reduce batching.
- Require walshadow to manage its shadow cluster and run its bridge worker.
- Caching is last resort, don't specialize for redundant benchmarking workloads.
