# Production verification

Extend existing tests around observable failures and recovery. Use current
coverage reports to find gaps instead of keeping historical line counts or
lists of already covered functions. Build and test commands live in
[development guide](../docs/development.md)

## Verify value fidelity and recovery

Preserve minimized SQL, effective config,
generated destination DDL, server/build versions, process exit reason, and
durable positions in regression fixtures. Run failure cases independently;
later stale queries after daemon exit do not establish additional failures

WAL probes at `1267db7`, PostgreSQL 18.6 / ClickHouse 26.8.1.951, establish
compressible TEXT fidelity through 128 MiB and configured 64 MiB cap behavior
See [verified value gaps](value_coercion.md). Promote those probes into persistent
regressions; remaining investigation concerns daemon recovery and wider matrix
Distinguish policy rejection, panic, OOM kill, and query timeout. For rejection,
prove durable progress cannot skip failed work, unchanged restart fails again,
and corrected policy plus restart converges. Coordinate diagnostics and policy
with [value coercion](value_coercion.md), UI checks with
[runtime configuration](runtime_config.md#operator-health-and-recovery)

Extend existing TOAST and type suites with:

- An 8 KiB, 32 MiB, and 128 MiB ladder for TEXT, JSON/JSONB, INTEGER[], and TEXT[],
  plus values just below, at, and above configured decoded-size cap. Account for
  PostgreSQL representation overhead. Exercise compressible and incompressible
  payloads, NULL/default overflow and error policy, spill, and restart
- Exact source/destination lengths and content digests for strings; element
  counts, order, NULL elements, and content for arrays. Test streaming and
  supported initial-load/value-mode combinations. Check resident memory and
  progress when one value exceeds normal batch or reserved-memory budgets
- Extend 32 MiB JSONB verification beyond successful compressible String case:
  `to_jsonb(repeat('x', 33554432))` and
  `jsonb_build_object('v', repeat('x', 33554432))` match canonical source lengths
  and MD5s after WAL replay. Test incompressible bodies, cap boundaries, explicit
  native JSON mapping, and restart. Do not infer native JSON semantics from
  automatic String mapping or row count

For a fixture containing 8 KiB, 32 MiB, and 128 MiB TEXT rows, assert total
length of `167780352` bytes after all inserts succeed. Compare every expected
key and value separately rather than combining nullable, differently typed
lengths with `greatest`

Reproduce with `public.doc(id int PRIMARY KEY, meta text, body text)`, default
EXTENDED storage, and `REPLICA IDENTITY FULL`, using
[TOAST harness](../tests/toast_e2e.rs). Insert separate transactions containing
`repeat('x', 8192)`, `repeat('x', 33554432)`, and `repeat('x', 134217728)`, then
switch WAL and drain. Compare source `length(body), md5(body)` against destination
`length(body), lower(hex(MD5(body)))` under `FINAL WHERE _is_deleted = 0`
Repeat with `inline_value_max = 67108864` and each overflow policy. Harness
`expect` panics on returned errors do not establish daemon panics

Cover transaction and trigger scenarios where regression coverage is missing:
TRUNCATE between writes to two tables in one spilling transaction, including
abort and restart; BEFORE-trigger rewrites and suppressed UPDATE/DELETE; and
AFTER-trigger audit rows. Derive expected audit rows from fixture operations
and assert final heap effects rather than SQL command tags. Keep numeric boundary,
signed-zero, and default checks in [value coercion](value_coercion.md#acceptance)

Isolate repeated runs by source and destination identities. If reusing tables,
wait for cleanup to replicate and verify source emptiness: DELETE can be vetoed
by a BEFORE trigger and can itself generate audit rows. Source DROP/recreate
under retain policy does not clear destination; test that behavior separately
with [schema lifecycle](schema.md). Avoid destination-only resets while old WAL
or backfill remains active

## Pin WAL layouts

Extend generated fixtures with commit records combining subtransactions,
dropped statistics, relation locators, invalidations, origins, and prepared
transaction fields. Add MULTI_INSERT batches. Assert parsed fields and tuple
contents per major, rather than only record counts or catalog fraction

Start with [transaction parser](../src/decode/wal_xact.rs),
[fixture capture](../fixtures/wal/classify/capture.sh), and
[fixture tests](../tests/classify_fixture.rs)

## Prove outage recovery

Contiguous ClickHouse acknowledgements and transaction-aware restart floor
already prevent cursor advancement past undurable work. Do not add a second
committed-spill recovery path without a failing case demonstrating need

Extend [restart harness](../tests/kill_restart.rs) with ClickHouse outage through
retry exhaustion, supervisor restart, and recovery after connectivity returns
Cover a transaction spanning WAL segments, partial batch success, and later
inserts completing before an earlier insert. Compare final rows and durable
progress with uninterrupted execution. Verify missing retained WAL fails clearly

Prepared COMMIT and ABORT already dispatch through ordinary drain/discard logic,
and open transactions constrain restart floor. Verify full daemon restart after
PREPARE, followed by COMMIT PREPARED or ROLLBACK PREPARED, including subtransactions
and spill. Existing prepared-DDL tests do not establish every data-recovery case
Include a prepared transaction predating bootstrap before broadening production
support. Choose new persistence only if retained-WAL recovery proves insufficient

## Close remaining coverage gaps

Run schema restart and reload cases from [schema plan](schema.md). Inject failed
reads, truncated values, disk errors, and connection loss at existing boundaries
Assert errors, retained state, and eventual recovery, not execution alone

Add explicit walsender keepalive timeout and transport regressions where current
round trips cover behavior only indirectly. Cover source TCP/TLS and shadow
connection loss separately

Reach [100% line coverage](coverage100.md), raising floor from a freshly measured
baseline after meaningful behavioral tests land. Keep product code, binaries,
and defensive paths in scope
Do not exclude code or disable coverage to reach a percentage without human
approval. Existing zero-coverage-file gate and lints remain required

## Recovery implementation decisions

Retained-WAL replay remains first recovery strategy for ClickHouse retry
exhaustion and prepared transactions. Test recovery with current cursor and
retention rules before deciding a new durable queue is necessary

If committed work must survive independently of source WAL, persist complete
transaction envelope before releasing replay history: original row versions,
schema/control ordering, TOAST dependencies, route snapshot, and completion
state. Partial INSERT success must replay with original identities and versions
Delete durable work only after acknowledgement and checkpoint make restart safe
Do not treat files in startup-cleared spill directory as a recovery journal

If prepared state needs independent persistence, key ownership by source identity
and prepared xid; retain GID for correlation instead of assuming finishing
record's header xid identifies buffered transaction. Include subxids, earliest
required WAL, descriptor history, chunks, and spill ownership in checkpoint
Test GID reuse after resolution, old cursor versions, missing spill, and repeated
restart before both COMMIT PREPARED and ROLLBACK PREPARED

Coordinate with [failover fence](failover.md): ordinary abandoned state must not
clear prepared state, and xid reuse on descendant must not recover ancestor
scratch. Coordinate with [pending bootstrap rows](bootstrap.md) when PREPARE predates
backup redo, where replay inside backup window cannot reconstruct earlier rows

Normalize differential-oracle locale and timezone inputs. Pin or record source
and shadow tzdata versions when timestamp text differs, then distinguish
environment mismatch from decoding failure before accepting a regression oracle
