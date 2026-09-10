# Parser and schema fuzzing

Use generated inputs to find failures fixed regressions miss. Keep malformed
byte parsing separate from valid PostgreSQL programs, since they need different
oracles and failure reports

## Parser targets

Start with WAL record parsing and relation-location extraction, then filter
round trips and no-op replacement. Expand to tuple/value decoding, full-page
images, protocol frames, and persisted state once first targets produce useful
coverage

Seed from existing WAL captures and unit vectors. Validate lengths before
allocation and bound memory per input. A malformed input may return an error;
it must not panic, allocate without limit, or produce invalid rewritten WAL
For round trips, compare semantic fields and verify checksums. Mutations intended
to reach record bodies need valid outer framing and CRCs

Instrument C code as well as Rust when testing ClickHouse protocol and type
conversion boundaries. Rust-only sanitizer coverage cannot establish C safety
Keep minimized crash inputs as deterministic regressions

## Schema programs

Begin with deterministic cases in [schema plan](schema.md). Give each program
an expected outcome: converge, reject before destination effects, obey explicit
policy, or reproduce a named known gap. A known gap must remain visible and must
not silently become expected success

Generate small transactions combining DDL, DML, savepoints, commit, rollback,
config changes, and restart. Track source objects by identity so names can change
without losing reference to a table. Keep unsupported types and deployment
features explicit in generator capabilities

Compare three results: final source/destination row state, destination schema
and mapping, and progress/recovery state. Use FINAL and deletion filtering for
destination row comparison. Account for deliberate projections, retained tables,
and type conversions instead of comparing raw SQL text

Run same program uninterrupted and with restarts at selected boundaries. Persist
seed, SQL, config, source/destination snapshots, relevant WAL, and logs on failure
Shrink transactions and statements while preserving failure and dependencies
Minimize values and schema only after reproducing same semantic outcome

## Pure planning tests

Extract a pure DDL planning seam when needed to test a whole transition before
effects. Compare ordered operations, resulting mapping, and policy decisions
against a small reference model. Keep execution tests for effects a pure model
cannot prove, including connection failure and partial DDL success

## Run and finish

Keep PR checks deterministic: build targets and replay committed regressions
Run sustained campaigns separately with bounded processes, memory, disk, and
input duration. Preserve corpus across runs and report crashes rather than
letting a supervisor hide them through endless restarts

Judge progress by new transitions and failure boundaries exercised, reproducible
bugs, and minimized regressions. Raw execution count and line coverage alone do
not prove useful semantic coverage

## Target ownership and input models

Add separate nightly fuzz harness while preserving stable product CI and coverage
scope. Both `wal-rus` and `clickhouse-c-rs` are external dependencies in current
Cargo manifest; standalone parser/client harnesses belong upstream, while local
targets exercise walshadow integration. Check public entry points before copying
old sketches that assumed a workspace client crate or manual value codecs

| Target family | Input construction | Oracle |
|---|---|---|
| WAL record/page stream | Raw bytes plus structure-aware header/CRC repair; retain parser across pages | Bounded parse, continuation correctness, locations and checksums |
| Filter/no-op rewrite | Real captured segments, mutations across page/segment boundaries | Reparse output, preserve lengths/LSNs, validate CRC and intended routing |
| Tuple/descriptor pair | Generate column shape and matching tuple framing together | Value/default/drop/varlena behavior without descriptor-mismatch-only rejection |
| Cursor, descriptor log, spill | Valid encoded seeds plus corruption/truncation | Round-trip identity or explicit corruption rejection, no partial recovery |
| Native block/type/protocol | In-memory wire source, nested types, malformed frames | Bounded parsing, Rust/C memory safety, round-trip semantics where supported |

Seed from captured WAL per major, individual records/pages, persisted-state
encoders, and existing unit vectors. Keep live corpus/artifacts outside source
control, retain minimized regressions. Verify C translation units receive
sanitizer and coverage instrumentation as well as Rust; inspect actual dependency
build flags and avoid claiming C coverage from Rust instrumentation alone

Generate SQL programs from typed relation/column IDs, current OID/generation,
attnums, keys, persistence, partition relationships, expected rows, mapping, and
lifecycle policy. Add prepare/resolve only in explicitly supported campaign
Render legal operations from state; separate PostgreSQL syntax/dependency-error
campaign from replication semantics. Share [DDL planning seam](schema.md) instead
of maintaining a competing planner extraction

Measure legal operation pairs before increasing depth: DDL/DML order, same/separate
transaction, commit/rollback/savepoint/prepare, rewrite class, key changes,
nullable/inline/TOAST, mapped/excluded/auto-created, drop policy, restart cut, and
PostgreSQL major. Distinguish rejected-before-effects, partial effects, acknowledged
mismatch, policy divergence, and generator rejection in artifacts

Supervisor must bound target processes, memory, disk, and input duration, preserve
corpus on restart, and surface crashes. Promote found cases into deterministic
regressions for [100% coverage work](coverage100.md) without treating percentage
as substitute for semantic matrix
