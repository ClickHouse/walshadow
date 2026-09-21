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
Resolve bootstrap visibility before relying on backup loads under concurrent
writes. A documented limitation does not imply code rejects it

| Plan | Next step |
|---|---|
| [Schema changes](schema.md) | Reject unsupported transitions before destination effects |
| [Tablespaces](tablespaces.md) | Reject unsafe layouts before bootstrap, then add complete support |
| [Catalog completeness](catalog.md) | Stop when a surviving relation has lost buffered payload |
| [Bootstrap visibility](bootstrap.md) | Preserve tuples whose transaction outcome is still unknown |
| [Verification](verification.md) | Enforce CI prerequisites and prove outage, restart, and WAL-version behavior |
| [100% line coverage](coverage100.md) | Close fixture, live-system, CLI, and fault-path gaps, then enforce 100% |

## Further work

| Plan | Reason to take it up |
|---|---|
| [Multi-database loads and metrics](multi_database.md) | Extend heap-page bootstrap and backup loads beyond primary database, attribute metrics |
| [Fuzzing](fuzzing.md) | Find parser and schema-transition interactions beyond fixed regressions |
| [Performance](performance.md) | Locate bottlenecks before changing concurrency or allocation |
| [Runtime configuration](runtime_config.md) | Add source-side commands and explain effective settings |
| [Failover](failover.md) | Continue after unplanned promotion or across archived timeline changes |
| [Shadow TOAST reclamation](shadow_toast.md) | Keep historical values readable under lag and restart |
| [Replay callback](custom_rmgr.md) | Reduce measured command-boundary capture stalls |
| [Dependencies](dependencies.md) | Replace generic protocol code when an adapter preserves behavior |
| [Tier 2 containers](tier2.md) | Remove shadow round trips for array, map, and vector columns |
| [Optional capabilities](extensions.md) | Meet a concrete routing, export, vector, or durability requirement |

Remove completed proposals instead of keeping a second implementation reference
Keep unresolved acceptance tests even when their proposed implementation has
been replaced by another design. Treat sketches as proposals, verify source
before choosing names or replacing existing mechanisms
