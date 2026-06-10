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

- **Target:** 84.62% line coverage (13993 / 16536), 2543 missed lines from
  `cargo llvm-cov report --summary-only` and HTML totals. Use this denominator
  for `--fail-under-lines`
- **Secondary:** 84.11% function coverage (1307 / 1554), region 83.89%.
  llvm-cov counts each source function once and marks it hit if any
  instantiation ran. Use only for prioritising untouched functions
- **Ignore:** 54% (1917 / 3550) from `lcov.info` `FN` / `FNDA` rows. lcov
  export emits one `FN` per monomorphization per crate, so generic functions
  and closures are counted repeatedly. Collapsing `lcov.info` by
  `(file, start-line)` reproduces llvm-cov shape: 1186 / 1417 = 83.7% for
  `src/` alone, excluding `clickhouse-c-rs`

Real work is **2543 missed executable lines**, not 231 uncovered functions and
not 1633 monomorphized `FN` rows

Regenerate locally with CI coverage recipe, PG 17 and ClickHouse running, see
`.github/workflows/ci.yml` `coverage:` job:

```sh
cargo llvm-cov clean --workspace
WALSHADOW_USE_LOCAL=1 cargo llvm-cov --workspace --all-targets --locked --no-report -- --nocapture
cargo llvm-cov report --summary-only
cargo llvm-cov report --lcov --output-path coverage/lcov.info
```

## Missed-line shape

Current HTML totals show main blockers:

| area | missed lines | work |
|---|---:|---|
| `src/bin/*` | 815 | decide coverage denominator, then test or exclude binaries |
| `ch_emitter.rs` | 342 | config arms, CH connection/retry, flush, observer, debug variants |
| `heap_decoder.rs` | 264 | value matrix, partial tuple, varlena, dropped-column arms |
| `xact_buffer.rs` | 223 | snapshot, TOAST, spill/detoast, observer-error paths |
| `oracle.rs` + `ch_ddl.rs` + `shadow_*` | 370 | live control-plane and schema-event paths |
| remaining library tail | 529 | fixture gaps, pure helpers, defensive error arms |
| **total** | **2543** | HTML totals row denominator |

Treat this as denominator pressure, not function backlog. `heap_decoder.rs` is
96.08% function-covered but still misses 264 lines; `bin/stream.rs` alone
misses 477 lines. Error closures remain, but broad match arms and binaries
dominate

Literal 100% line coverage needs one denominator decision for CLI binaries,
decoder/emitter matrix tests, and deterministic fault injection for defensive
OS and transport paths. No residual glue can remain under literal 100% gate

## Tier A, pure units, DONE

Implemented 2026-06-04. Added 18 `#[test]`s in owning modules' `mod tests`,
asserting observable output, exact strings, error variants, and counter values
rather than walking lines. `cargo test --lib`: 365 passed. `cargo clippy --lib
--tests`: 0 warnings. Re-run llvm-cov recipe before using baseline table above;
it predates this work

Covered:

- `backup_sink.rs`: `CatalogFilenodes::len`, `is_empty`,
  `MultiplexSink::lander_stats`
- `ch_ddl.rs`: `DdlConfig::with_drop_strategy`
- `ch_emitter.rs`: `decimal_type_error`, `is_retryable`,
  `From<EmitterError> for DecoderSinkError`
- `xact_buffer.rs`: `inflight_snapshot` for empty and parked xid states, both
  `From<XactBufferError>` impls (`SinkError`, `DecoderSinkError`).
  `XactBuffer::new` was already covered
- `backfill_bootstrap.rs`: `BootstrapConfig::with_catalog_filenodes`
- `backup_page_walk.rs`: `PageWalkSink::source_lsn`
- `streaming_walker.rs`: `StreamingWalker::seg_size`
- `oracle.rs`: `OracleObserver::inner_mut`, using literal `Oracle` with
  `client: None` inside `mod tests`
- `type_bridge.rs`: `render_pg_time`, `render_pg_timetz`,
  `render_pg_timestamp`, including out-of-range fallback
- `heap_decoder.rs`: `decode_cstring` Text / Bytea / Truncated arms,
  `CHAROID` to `Char` scalar branch in `decode_one_value`. Array-element
  `Char` mapping still needs `char[]` fixture in decoder matrix

## Tier B, fixtures and in-proc harnesses

Use existing in-proc and fixture paths. Extend
`tests/common/inproc_harness.rs`, fixture replay, or module-local tests

- **Pipeline tail variants:** cover batcher deadline / `FlushAll` /
  budget trips, inserter `send_with_retry` + reconnect arms, ack
  collector `Trailing` / gap cases, `RecordSink` `on_idle` /
  `on_close`, and `Display` / `fmt`.
  `tests/emitter_budget_flush.rs` and `tests/emitter_native_types.rs` already
  build the tail in-proc; add deadline-trip and trailing-ack assertions there
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
- **`xact_buffer.rs` detoast and observer errors:** feed truncated pglz and bad
  lz4 TOAST through subxact / large-xact harnesses; inject observer failures
  where callbacks already exist
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
- `oracle.rs::validate` and `reconnect`: use `tests/oracle.rs` with pgext
  bridge up. Validate needs sampled row; reconnect needs forced shadow bounce
- `ch_emitter.rs::connect` and `reconnect`: cover with live-CH emitter e2e,
  including explicit CH drop mid-stream
- `shadow_stream.rs` listeners and COPYDATA enqueue path: drive walshadow
  walsender through `tests/walsender_pg18_walreceiver.rs` /
  `tests/shadow_lifecycle.rs`
- `source_feed.rs` connect paths: exercise unix, TCP, and TLS transports once
- `backfill_bootstrap.rs` drain arms and `backup_source*` start / finish /
  tablespace symlink: extend `tests/bootstrap_*_e2e.rs` with tablespace-bearing
  source
- Catalog-bound accessors: assert `shadow_catalog` `has_pending_sweep` /
  `last_observed_replay` and `xact_buffer` `with_subxact_tracker` /
  `subxact_tracker` where existing catalog e2e tests already have real
  `ShadowCatalog`
- `preflight.rs::run` `BadShadowVersion` arm: point validator at shadow
  returning non-integer `server_version_num`, or extract parse into pure helper
  and unit-test it in Tier A

## Tier D, hard tail decisions

After Tiers A-C, remaining misses are executable CLI lines, match arms inside
called decoder functions, and defensive error closures. Each missed executable
line needs one outcome: execute it in CI, refactor it into testable code, or
remove it from configured denominator

1. **CLI denominator, 815 missed lines.** `bin/latency_bench.rs` misses 291
   lines, `bin/stream.rs` misses 477, `bin/classify.rs` misses 25, and
   `bin/filter.rs` misses 22. Exclude `src/bin/latency_bench.rs`; it is manual
   benchmark code, not normal shipped behavior. For shipped binaries, choose
   coverage or exclusion explicitly. `stream` coverage means bootstrap
   off/direct/object-store paths, CH emitter + DDL setup, metrics-only
   observer, oracle wrapper on/off, retention loop, standby/listener config
   writers, object-store WAL fetch, autospawn success/failure, shutdown hooks,
   and `parse_lsn` good/bad shapes. Prefer moving pure helpers into library
   modules before testing. For `classify` and `filter`, add CLI smoke tests for
   JSON/human output, quiet/noisy output, short-read/end-of-file paths, and
   failure exits
2. **Decoder/value matrix, 264 missed lines in `heap_decoder.rs`.** Drive
   fixed-width values (`float4`, `float8`, `oid`, date/time/timestamp/
   timestamptz/timetz, uuid, interval success/error), varlena shapes (short,
   4-byte, compressed, external TOAST, unsupported tag, invalid UTF-8,
   truncation), dropped columns, missing defaults for each mapped type, and
   prefix/suffix partial tuple exits. Do not exclude product decoder code
3. **Emitter/DDL/config arms, 430 missed lines across `ch_emitter.rs` and
   `ch_ddl.rs`.** Cover config parser errors and coercions (`port`, budgets,
   retry fields, namespace/table/column shape errors, `attnum` overflow),
   `ColumnBuf` debug variants, idle/close/deadline flush variants, retry and
   reconnect success/failure, DDL rename/add/drop/type-change branches, and
   mapping mutation. Use unit, in-proc, and live-CH harnesses
4. **Live control plane.** Cover extension present/absent, oracle reconnect and
   query error, validation match/mismatch/error, `seed_from_source`
   empty/known/new relation/not-found/error paths, Unix and TCP walsender
   listeners, COPYDATA enqueue error, unix/TCP/TLS source transports,
   direct/object-store bootstrap, tablespace symlink, object-store WAL fetch,
   and shadow autospawn. Extend existing e2e tests where state is real; add
   injectable clients or failing readers/writers where deterministic faults are
   needed
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
- **Practical 100% line gate:** exclude intentional non-product files, at
  minimum `src/bin/latency_bench.rs`; cover or refactor shipped binaries; add
  decoder/emitter/control-plane matrix tests; add deterministic fault injection;
  gate configured denominator with `--fail-under-lines 100`
- **High-water floor:** if stream/CLI or defensive product-file exclusions are
  accepted, state scope in CI and docs. This means "100% of configured
  denominator", not source line coverage

## Sequencing and verification

1. Re-run llvm-cov after Tier A and update baseline
2. Finish Tier B fixture and in-proc tests
3. Land config parser and DDL fixtures from Tier D, dense coverage work
4. Extend Tier C live e2e tests
5. Decide binary denominator, cover or exclude shipped CLI paths
6. Add decoder matrix, emitter/DDL matrix, then fault injection for remaining
   defensive lines
7. Add `--fail-under-lines N` to CI coverage job once refreshed baseline is
   accepted, then raise it after each tier

After each tier, rerun llvm-cov recipe, diff summary, and regenerate
`coverage/lcov.info` so work list shrinks visibly. Do not add tests that only
walk closures; every test must assert observable behavior
