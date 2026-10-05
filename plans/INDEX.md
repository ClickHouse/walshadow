# Planned work

Keep only unfinished work here. Describe problem, next step, constraints, and
evidence needed to finish. Check code and tests before treating an old proposal
as a missing feature

Current behavior belongs in [docs](../docs/README.md). System design belongs in
[architecture](../architecture/README.md). Existing function signatures, wire
layouts, implementation walkthroughs, and exhaustive metric lists belong in source. Keep
proposed interfaces, persistence ordering, alternatives, and acceptance matrices
here when other workstreams need them before implementation exists

Read [shared implementation constraints](coordination.md) before changing WAL
publication, durable progress, physical identity, or pipeline concurrency

## Production readiness

Start with small guards against silent divergence and tests of restart behavior
Prove pending-row recovery before relying on backup loads under concurrent
writes. A documented limitation does not imply code rejects it

| Plan | Next step |
|---|---|
| [Value coercion](value_coercion.md) | Reject Decimal precision loss and signedness corruption; fix String NaN defaults; prove rejection recovery |
| [Runtime configuration](runtime_config.md) | Locate owning UI and reproduce stale/dead state; expose existing controls there |
| [Schema changes](schema.md) | Fix routing after renames and handling of replica identity changes; define policy for sort key changes; reject unlogged tables before destination changes |
| [Tablespaces](tablespaces.md) | Reject unsafe layouts before bootstrap, then add complete support |
| [Catalog completeness](catalog.md) | Stop when a surviving relation has lost buffered payload |
| [Bootstrap visibility](bootstrap.md) | Prove pending-row recovery across load modes and restart |
| [Verification](verification.md) | Prove value fidelity and restart without skipping WAL |
| [100% line coverage](coverage100.md) | Close fixture, live-system, CLI, and fault-path gaps, then enforce 100% |

## Further work

| Plan | Reason to take it up |
|---|---|
| [Multi-database loads and metrics](multi_database.md) | Extend heap-page bootstrap and backup loads beyond primary database, attribute metrics |
| [Fuzzing](fuzzing.md) | Find parser and schema-transition interactions beyond fixed regressions |
| [Performance](performance.md) | Establish healthy sustained WAL baseline with exact reconciliation before tuning |
| [Failover](failover.md) | Fence unplanned promotion and prove remaining archive/restart fault cases |
| [Shadow TOAST reclamation](shadow_toast.md) | Keep historical values readable under lag and restart |
| [Replay callback](custom_rmgr.md) | Reduce measured command-boundary capture stalls |
| [Dependencies](dependencies.md) | Replace generic protocol code when an adapter preserves behavior |
| [Tier 2 containers](tier2.md) | Remove shadow round trips for array, map, and vector columns |
| [Optional capabilities](extensions.md) | Meet a concrete routing, export, vector, or durability requirement |

Remove completed proposals instead of keeping a second implementation reference
Keep unresolved acceptance tests even when their proposed implementation has
been replaced by another design. Treat sketches as proposals, verify source
before choosing names or replacing existing mechanisms
