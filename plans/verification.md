# Production verification

Extend existing tests around observable failures and recovery. Use current
coverage reports to find gaps instead of keeping historical line counts or
lists of already covered functions. Build and test commands live in
[development guide](../docs/development.md)

## Enforce CI prerequisites

CI already installs PostgreSQL and ClickHouse, builds module, regenerates WAL
fixtures, and runs PostgreSQL 16/17/18 jobs. Remove claims that these suites are
not wired into CI

Make missing binaries, module, extensions required by a job, and WAL fixtures
fail that job. Runtime early returns currently let some unavailable tests look
successful. Keep optional local skips explicit. Verify active PostgreSQL major
matches matrix job and coverage merge receives every expected major

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
scratch. Coordinate with [bootstrap carry](bootstrap.md) when PREPARE predates
backup redo, where replay inside backup window cannot reconstruct earlier rows

Normalize differential-oracle locale and timezone inputs. Pin or record source
and shadow tzdata versions when timestamp text differs, then distinguish
environment mismatch from decoding failure before accepting a regression oracle
