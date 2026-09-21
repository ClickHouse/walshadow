# Complete multi-database loads and observability

Extend current per-database routing work to heap-page bootstrap, backup-based
table loads, and metrics. Stage 1 covers live replication and `copy` for
non-primary databases; keep remaining load paths explicit until verified

Use [shared constraints](coordination.md) for physical identity and durable
progress, and [bootstrap visibility](bootstrap.md) for undecided tuples

## Heap-page bootstrap

Build a catalog map for every followed database and route bootstrap rows by
database in [bootstrap drain](../src/emit/pipeline/bootstrap.rs). Select matching
mapping rules and type-conversion bridge using tuple's database identity
Preserve database identity through deferred TOAST replay and completion tracking

Include every followed database in temporary PostgreSQL instance provisioned by
[bootstrap oracle](../src/backfill/bootstrap_oracle.rs). Restore each database's
schema and required types separately, then connect conversion workers to matching
database. Do not let single-bridge fallback convert another database's values

Prove existing rows from primary and non-primary databases reach configured
destinations. Cover matching schema/table names and overlapping relation and type
OIDs with distinct layouts, external values, deferred rows, and restart during
bootstrap. Assert row contents and durable progress across handoff to live WAL

## Backup-based table loads

Support `initial_load = "base_backup"` and `initial_load = "object_store"` for
each followed database. [Backup backfill](../src/backfill/backup_backfill.rs)
currently requires one target database per request set through `target_db_oid`

Partition requests by database or explicitly extend that contract. Preserve
per-database catalog-skew checks, descriptor lookup, physical locators, and route
selection; removing mixed-database rejection alone does not establish support
Keep unsupported requests rejected before destination effects until covered

Exercise both modes for non-primary databases and loads spanning multiple
databases. Include overlapping relation OIDs, concurrent WAL, catalog skew in
target versus unrelated databases, and interrupted loads followed by restart
Assert destination isolation, final rows, staging cleanup, and resume safety
Retain [visibility acceptance cases](bootstrap.md#completion) for each load mode

## Database labels on metrics

Add source database labels to metrics with database-owned work in
[metrics exporter](../src/ops/metrics.rs). Carry attribution through counters and
gauges, including shared bridge statistics, rather than relabeling aggregate
values once per database

Keep cluster WAL and process-wide resource metrics aggregate. Define label
identity consistently and bound series by configured databases, avoiding table
names or arbitrary config values. Preserve cumulative counters across bootstrap
handoff and document changed series for dashboard consumers

Verify two databases with independent activity produce distinct series, shared
work is counted once, and single-database operation retains useful metrics
