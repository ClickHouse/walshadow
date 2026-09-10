# Reach 100% line coverage

Drive workspace line coverage to 100%, including product binaries and defensive
paths. Use meaningful behavioral assertions, testable boundaries, and removal of
confirmed dead code. Keep lints and existing zero-coverage-file gate enabled
Do not exclude files or disable coverage without human approval

## Baseline and gate

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

## Measured baseline, 2026-09-10

Latest successful [CI run 34503434541](https://github.com/ClickHouse/walshadow/actions/runs/34503434541)
at review covers commit `aad06973561e9bec389298d4843510695ba6b075`, newer than
planning checkout `402d4db2aab2a40dc3fb8788a339e7b82829c9fb`. All three major
jobs and merged job passed. Counts below come from downloaded `coverage-merged`
artifact's `merged.lcov`, checked against per-major artifacts. Recompute every
row from unique `(source file, DA line)` entries to match merged job's denominator

| Report | Covered lines | Executable lines | Missed lines | Coverage |
|---|---:|---:|---:|---:|
| PostgreSQL 16 | 43,608 | 47,744 | 4,136 | 91.3371% |
| PostgreSQL 17 | 43,610 | 47,744 | 4,134 | 91.3413% |
| PostgreSQL 18 | 43,613 | 47,744 | 4,131 | 91.3476% |
| Merged | 43,614 | 47,744 | 4,130 | 91.3497% |

Native llvm-cov summary uses a different denominator in this run:
[PG18 job](https://github.com/ClickHouse/walshadow/actions/runs/34503434541/job/102959545815)
reports 45,400 / 50,091 lines, 4,691 missed, **90.64%**. Per-major LCOV `LF`/`LH`
totals also differ from unique `DA` entries; merge script regenerates totals
from `DA`. Audit this discrepancy before introducing a native llvm-cov threshold
Do not apply 91.3497% merged baseline to `cargo llvm-cov --fail-under-lines`

Report contains 97 source files. This is CI's instrumented denominator, not proof
that separately built PostgreSQL C module or dependencies are covered. Keep
module behavior in live/fault tests and C sanitizer campaigns; report additional
instrumentation separately rather than conflating it with Rust line target

Merge gains only one line over PG18 in this run, so missing major coverage does
not explain bulk of backlog. Top eight files account for 2,284 missed lines,
55.3% of total. Prioritize staging and initial-load recovery alongside CLI
orchestration, rather than inheriting old decoder/emitter-only ranking

| File | Missed / executable | Coverage | First behavior to exercise |
|---|---:|---:|---|
| `src/bin/stream.rs` | 723 / 3,495 | 79.31% | `run_session`, runtime-config seed, bootstrap, recover/reconnect, retention/archive fetch |
| `src/backfill/backup_backfill.rs` | 339 / 1,012 | 66.50% | `walk_and_ship`, object-store pass, gap pre-scan/replay |
| `src/backfill/copy_backfill.rs` | 284 / 812 | 65.02% | Staged publish, swap/resume, backup pass, opt-in lifecycle |
| `src/xact/xact_buffer.rs` | 236 / 3,511 | 93.28% | Record dispatch, image inserts, invisible stash, commit resolution |
| `src/decode/heap_decoder.rs` | 219 / 1,401 | 84.37% | Varlena, tuple payload, missing defaults, MULTI_INSERT |
| `src/backfill/backfill_staging.rs` | 198 / 238 | 16.81% | Prepare/rebuild/copy-back, exchange, schema fingerprint/UUID probes, retries |
| `src/backfill/bootstrap_oracle.rs` | 145 / 326 | 55.52% | Provisioning, extension discovery, pg_dump validation/failure |
| `src/emit/ch_emitter.rs` | 140 / 2,131 | 93.43% | Typed buffer formatting, fixed bytes, value encoding, nulls/config |
| Remaining 89 files | 1,846 / 34,818 | 94.70% | Control plane, shadow lifecycle, init, transitions, transport, durable logs |

Next largest gaps: `ops/control.rs` 99, `catalog/shadow.rs` 98, `ops/init.rs` 90,
`source/transition.rs` 80, `source/shadow_stream.rs` 72, and
`backfill/backup_page_walk.rs` 65. Use report source at pinned commit to select
branches, line numbers can shift in working tree

Start with staged backfill restart around prepare/publish/swap/copy-back, then
direct/object-store handoff and gap replay. Exercise these through live harnesses
to cover real state transitions; deleting missed error branches or invoking
accessors without behavior assertions does not establish recovery correctness

## Work list

Recheck each candidate against fresh report before adding tests. Anchor on files,
functions, and behavior rather than source line numbers

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
2. Close pure-unit and fixture gaps, especially dense decoder/config matrices
3. Extend live harnesses for schema, bootstrap, transport, and restart gaps
4. Exercise CLI orchestration and deterministic OS/transport failures
5. Regenerate reports after each tier, raise floor, and retain regression inputs
6. Enforce literal 100% merged line coverage once no missed executable lines remain

Use [semantic fuzzing](fuzzing.md) to discover interactions, then convert useful
findings into deterministic coverage tests. Coverage proves execution, recovery
and semantic oracles must still prove outcomes
