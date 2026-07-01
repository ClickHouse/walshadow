# plans/future, line coverage to 100%

Drive `cargo llvm-cov` line coverage from current baseline to 100%, using
`coverage/lcov.info` and `coverage/target/llvm-cov/html/` as generated work
lists

Exact source locations belong in generated reports. Do not pin source line
numbers in this plan; they go stale after edits and test additions. Anchor work
on files, functions, branches, and observable behavior

## Baseline

Use line coverage as target metric. Function coverage helps find untouched
routines, but hides missed match arms inside already-hit functions

Refreshed 2026-06-26 from CI `line coverage (pg17)` job (run 28219022837).

- **Target:** 89.49% line coverage (17602 / 19670), 2068 missed lines from
  `cargo llvm-cov report --summary-only`. Use this denominator for
  `--fail-under-lines`
- **Secondary:** 88.36% function coverage (1639 / 1855), region 88.51%
  (27228 / 30762). llvm-cov counts each source function once and marks it hit
  if any instantiation ran. Use only for prioritising untouched functions
- **Ignore:** `lcov.info` `FN` / `FNDA` rows. lcov export emits one `FN` per
  monomorphization per crate, so generic functions and closures are counted
  repeatedly. Collapse by `(file, start-line)` to reproduce llvm-cov shape
  before drawing any conclusion from FN counts

Real work is **2068 missed executable lines**, not the inflated monomorphized
`FN` count.

Regenerate locally with CI coverage recipe, PG 17 and ClickHouse running, see
`.github/workflows/ci.yml` `coverage:` job:

```sh
cargo llvm-cov clean --workspace
WALSHADOW_USE_LOCAL=1 cargo llvm-cov --workspace --all-targets --locked --no-report -- --nocapture
cargo llvm-cov report --summary-only
cargo llvm-cov report --lcov --output-path coverage/lcov.info
```

## Missed-line shape

Missed-line counts from the 2026-06-26 run (Lines column), main blockers:

| area | missed lines | work |
|---|---:|---|
| `src/bin/*` | 476 | decide coverage denominator, then test or exclude binaries (`stream` 461, `filter` 11, `classify` 4) |
| `ch_emitter.rs` | 185 | config arms, CH connection/retry, flush, observer, debug variants |
| `heap_decoder.rs` | 224 | value matrix, partial tuple, varlena, dropped-column arms |
| `xact_buffer.rs` | 228 | snapshot, TOAST, spill/detoast, observer-error paths |
| `oracle.rs` + `ch_ddl.rs` + `shadow_*` | 306 | live control-plane and schema-event paths |
| remaining library tail | 649 | fixture gaps, pure helpers, defensive error arms |
| **total** | **2068** | summary TOTAL row denominator |

Treat this as denominator pressure, not function backlog. `heap_decoder.rs` is
98.44% function-covered but still misses 224 lines; `bin/stream.rs` alone
misses 461 lines (`pipeline/inserter.rs` 26, `queueing_record_sink.rs` 37,
`shadow_stream.rs` 72 are the next library blockers). Broad match arms and the
`stream` binary dominate

Literal 100% line coverage needs one denominator decision for CLI binaries,
decoder/emitter matrix tests, and deterministic fault injection for defensive
OS and transport paths. No residual glue can remain under literal 100% gate

## Tier B, fixtures and in-proc harnesses

Use existing in-proc and fixture paths. Extend
`tests/common/inproc_harness.rs`, fixture replay, or module-local tests

- **Pipeline tail variants:** batcher deadline trip, inserter `send_with_retry`
  + reconnect arms, ack collector `Trailing` / gap cases, `RecordSink`
  `on_idle` / `on_close`, and `Display` / `fmt`
- **`wal_stream` helper sinks:** exercise `CountingRecordSink::on_record`,
  `CollectingBytesSink` chunk / segment-boundary handling, and `Record`
  helpers. Delete helpers if dead after confirming no non-test caller
- **`filter_segment.rs` error closures:** feed corrupt records through
  `tests/filter_round_trip.rs`, including bad `page_magic` and unparseable body
  paths
- **`fpi.rs` decompress errors:** corrupt LZ4 and ZSTD FPI fixtures to exercise
  both error closures
- **`ch_ddl.rs` rename/drop-column logic:** drive ALTER RENAME and DROP COLUMN
  through existing DDL test path in `tests/ddl_replicates.rs`
- **`xact_buffer.rs` detoast and observer errors:** drive truncated/bad TOAST
  through the subxact / large-xact harnesses and inject observer failures where
  callbacks already exist
- **Live-client accessors:** once DDL harness builds a `DdlApplicator`,
  assert `config` and `config_mut`. Holds a live `AsyncClient`, so
  pure unit construction is not practical
- **`queueing_record_sink.rs` worker-stopped branch:** build sink with inner
  `RecordSink` that errors on `on_record`, push batch so worker exits, then
  `flush` again so `tx.send` fails and "worker stopped" error surfaces.
  "Already closed" arm remains Tier D because public `close()` consumes `self`

## Tier C, live PG and ClickHouse

Extend existing `WALSHADOW_USE_LOCAL` e2e tests. Keep live-state cases in
existing harnesses rather than adding binaries

- `shadow_catalog.rs::seed_from_source` and dependent closures: drive via
  `tests/catalog_seed.rs` / `tests/shadow_catalog.rs`
- `ch_emitter.rs::reconnect`: explicit CH drop mid-stream
- `shadow_stream.rs` listeners and COPYDATA enqueue path: drive walshadow
  walsender through `tests/walsender_pg18_walreceiver.rs` /
  `tests/shadow_lifecycle.rs`
- `source_feed.rs` connect paths: exercise unix, TCP, and TLS transports once
- `backfill_bootstrap.rs` drain arms and `backup_source*` start / finish /
  tablespace symlink: extend `tests/bootstrap_*_e2e.rs` with tablespace-bearing
  source
- Catalog-bound accessors: assert `shadow_catalog`
  `last_observed_replay` and `xact_buffer` `with_subxact_tracker` /
  `subxact_tracker` where existing catalog e2e tests already have real
  `ShadowCatalog`
- `preflight.rs::run` `BadShadowVersion` arm: point validator at shadow
  returning non-integer `server_version_num`, or extract parse into a pure
  helper and unit-test it

## Tier D, hard tail decisions

Remaining misses are executable CLI lines, match arms inside called decoder
functions, and defensive error closures. Each missed executable line needs one
outcome: execute it in CI, refactor it into testable code, or remove it from
configured denominator

1. **CLI denominator, 476 missed lines** (`stream` 461, `filter` 11,
   `classify` 4). `filter`/`classify` are
   near the floor; `stream` is the whole decision. Choose coverage or exclusion
   explicitly. `stream` coverage means bootstrap off/direct/object-store paths,
   CH emitter + DDL setup, metrics-only observer, oracle wrapper on/off,
   retention loop, object-store WAL fetch, autospawn success/failure, and
   shutdown hooks. Prefer moving pure helpers into library modules before
   testing
2. **Decoder/value matrix, `heap_decoder.rs`.** Varlena short / 4-byte /
   unsupported-tag / invalid-UTF-8 / truncation, dropped columns, missing
   defaults per mapped type, prefix/suffix partial-tuple exits, and the
   array-element `Char` mapping (`char[]` fixture). Do not exclude product
   decoder code
3. **Emitter/DDL/config arms across `ch_emitter.rs` and `ch_ddl.rs`.** Config
   parser errors and coercions (`port`, budgets, retry fields, namespace/table/
   column shape errors, `attnum` overflow), `ColumnBuf` debug variants,
   idle/close/deadline flush variants, retry and reconnect success/failure, DDL
   rename/drop/type-change branches, and mapping mutation. Use unit, in-proc,
   and live-CH harnesses
4. **Live control plane.** Validation hard-error, `seed_from_source`
   empty/known/new relation/not-found/error paths, TCP walsender listener,
   COPYDATA enqueue error, TCP/TLS source transports, direct/object-store
   bootstrap, tablespace symlink, object-store WAL fetch, and shadow autospawn.
   Extend existing e2e tests where state is real; add injectable clients or
   failing readers/writers where deterministic faults are needed
5. **Defensive OS/transport/fault lines.** Add explicit fault injection for
   remaining `with_context` closures, fs failures, socket handoffs, malformed
   impossible states, and lock/fault paths. Known examples include
   `metrics.rs` listener accept failure and `queueing_record_sink.rs`
   "already closed" `ok_or_else`, unreachable from public API because
   `close()` consumes `self`. Stable `rustc 1.96` plus
   `cargo-llvm-cov 0.8.7` does not expose stable line-level `#[coverage(off)]`;
   cargo-llvm-cov documents it as `coverage_nightly` gated. On stable, practical
   exclusions are report-level file regexes, so prefer small injection points
   over excluding product files

## Endgame options

- **True source 100% line coverage:** no report exclusions. Requires tests or
  fault injection for every item above, including all binaries and all
  defensive failures
- **Practical 100% line gate:** exclude any intentional non-product files (none
  currently); cover or refactor existing
  binaries; add decoder/emitter/control-plane matrix tests; add deterministic
  fault injection; gate configured denominator with `--fail-under-lines 100`
- **High-water floor:** if stream/CLI or defensive product-file exclusions are
  accepted, state scope in CI and docs. This means "100% of configured
  denominator", not source line coverage

## Sequencing and verification

1. Finish Tier B fixture and in-proc tests
2. Land config parser and DDL fixtures from Tier D, dense coverage work
3. Extend Tier C live e2e tests
4. Decide binary denominator, cover or exclude CLI paths
5. Add decoder matrix, emitter/DDL matrix, then fault injection for remaining
   defensive lines
6. Add `--fail-under-lines N` to CI coverage job once baseline is accepted,
   then raise it after each tier

After each tier, rerun llvm-cov recipe, diff summary, and regenerate
`coverage/lcov.info` so work list shrinks visibly. Do not add tests that only
walk closures; every test must assert observable behavior
