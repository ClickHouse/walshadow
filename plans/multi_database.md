# Complete multi-database loads and observability

Extend [multi-database replication](../docs/multi-database.md) to heap-page
bootstrap, backup-based table loads, and remaining pipeline metrics

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

## Remaining metric attribution

Bridge, descriptor, catalog-capture, and config metrics already have database
labels in [metrics exporter](../src/ops/metrics.rs). Attribute remaining shared
pipeline counters where work belongs to one database, carrying identity through
decode, resolution, and insertion instead of relabeling aggregate values

Keep cluster WAL and process-wide resources aggregate. Reuse configured database
labels and preserve cumulative counters across bootstrap handoff. Verify shared
work is counted once and independent database activity produces distinct series
Document changed series for dashboard consumers
