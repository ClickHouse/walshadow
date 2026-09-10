# Reach 100% line coverage

Drive workspace line coverage to 100%, including product binaries and defensive
paths. Use meaningful behavioral assertions, testable boundaries, and removal of
confirmed dead code. Keep lints and existing zero-coverage-file gate enabled
Do not exclude files or disable coverage without human approval

## Measurement and gate

Regenerate coverage with [development recipe](../docs/development.md#wal-fixtures-and-coverage)
and current [CI matrix](../.github/workflows/ci.yml). Use merged PostgreSQL
16/17/18 line data for project target; retain per-major reports to find version
branches. Require every expected major's artifact before accepting merged result
An early-returned integration test does not establish coverage of its scenario

Generate work list from `merged.lcov` and HTML artifact. Record covered and total
executable lines for revision under test, no historical percentage serves as
current baseline. Use function coverage only to find untouched routines; LCOV
function entries may repeat generic instantiations and do not count missed lines

Add a measured high-water line floor to merged job, raise after each completed
tier, and finish with a 100% gate. Check integer covered/total equality at final
gate so rounded percentages cannot hide misses. A per-major `--fail-under-lines`
check is not equivalent to merged coverage across version-specific branches

Audit denominator differences between native llvm-cov summaries, per-major LCOV
`LF`/`LH` totals, and merged unique `(source file, DA line)` entries before adding
thresholds. Derive each floor from matching report format

Measure [PG module coverage](../pgext/README.md#coverage) separately from Rust
line target. Refresh C baseline from `coverage-pgext-pg16/17/18` artifacts, merge
per-major C reports, and choose a C coverage floor. Retain live/fault tests and
C sanitizer campaigns to verify module behavior beyond line execution

## Work list

Recheck each candidate against fresh report before adding tests. Anchor on files,
functions, and behavior rather than source line numbers

Prioritize staged backfill restart around prepare/publish/swap/copy-back, then
direct/object-store handoff and gap replay. Exercise real state transitions
through live harnesses and assert recovery outcomes

| Tier | Candidate gaps | Evidence |
|---|---|---|
| Pure units | Config coercion/errors, namespace/table/column validation, attnum overflow, mapping mutation, cursor corruption | Assert accepted values or precise rejection, round-trip persisted state |
| Fixtures and in-process pipeline | Deadline/idle/close flush, retry/reconnect, acknowledgement gaps and trailing events, worker failure | Assert emitted batches, contiguous durable frontier, and retained work after error |
| WAL and tuple matrix | Bad page magic/body, cross-page records, LZ4/ZSTD FPI corruption, short/4-byte varlena, invalid UTF-8, truncation, partial tuples, dropped columns, defaults, arrays | Assert decoded values or error without partial publication |
| Transaction and TOAST | Truncated chunks, detoast misses, spill failures, observer errors, subtransactions | Assert transaction disposition, cleanup, and restart floor |
| Live control plane | Catalog seed/refresh, shadow lifecycle, DDL, source unix/TCP/TLS, shadow transport, keepalive timeout | Exercise actual client state and verify failure/recovery |
| Bootstrap and archives | Direct/object-store modes, backup start/finish, tablespaces, archive fetch, concurrent writes | Compare exact final rows and durable handoff, reject missing evidence |
| CLI | `stream`, `filter`, `classify` setup, argument errors, startup failure, shutdown | Run process or extract cohesive testable setup, assert exit and externally visible state |
| Defensive tail | Failed reads/writes, disk full, fsync/rename, listener failure, socket handoff, cancellation | Inject deterministic faults and prove no premature acknowledgement or lost retry state |

Reuse [in-process harness](../tests/common/inproc_harness.rs), WAL fixtures, and
existing live tests. For stopped-worker coverage, make inner sink fail, then
exercise subsequent queue send/flush and assert propagated failure. Use paused
time for deadlines and controlled readers/writers for transport faults

Keep CLI orchestration in scope: bootstrap off/direct/object-store, emitter and
DDL setup, metrics/oracle options, retention, archive fetch, shadow startup, and
shutdown hooks. Extract pure helpers only where they express useful behavior
Do not split files merely to make exclusions easier

Review unreachable public states before injecting impossible ones. For example,
a consuming close API may make an already-closed branch redundant. Remove only
after proving no caller or recovery path needs it; otherwise expose a narrow
fault boundary and assert contract

## Sequencing and completion

1. Enforce [CI prerequisites](verification.md#enforce-ci-prerequisites), audit
   native/merged denominator discrepancy, refresh baseline, and list missed branches
2. Extend live harnesses for staged backfill restart, handoff, and gap replay
3. Close pure-unit, fixture, schema, bootstrap, and transport gaps
4. Exercise CLI orchestration and deterministic OS/transport failures
5. Regenerate reports after each tier, raise floor, and retain regression inputs
6. Enforce literal 100% merged line coverage once no missed executable lines remain

Use [semantic fuzzing](fuzzing.md) to discover interactions, then convert useful
findings into deterministic coverage tests. Coverage proves execution, recovery
and semantic oracles must still prove outcomes
