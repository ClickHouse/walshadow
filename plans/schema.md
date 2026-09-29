# Safe schema changes

Some PostgreSQL changes update captured descriptors without updating destination
routing or schema. Others only warn, then let rows continue under an old mapping
Make each supported transition converge and reject unsupported transitions
before applying that transaction's destination changes

Start in [schema comparison](../src/schema.rs),
[catalog capture](../src/source/catalog_capture.rs), and
[DDL application](../src/emit/ch_ddl.rs). Current operator contract lives in
[schema changes](../docs/schema-changes.md)

## Relation renames

Table renames, schema moves, and schema renames change only relation name
Schema diff compares columns alone, so no schema event fires. Routes are keyed
by name, so later rows miss them and discard as unmapped without warning

Carry relation name changes in schema diff and rekey mapping from old to new
name at that event. Keep destination table name, with ClickHouse `RENAME TABLE`
as opt-in. Decide explicitly:

- New name outside scope, such as a glob that stops matching, a move to an
  unreplicated schema, or an explicit mapping naming only old name: scope
  follows source OID, or rename applies drop policy
- Rename into scope from an unreplicated name needs initial load
- Column rules and `order_by` resolve by current name; bind them into mapping
  at derivation so rename cannot fall back to defaults
- Relation created under old name must not inherit renamed mapping

## Row keys

UPDATE emits only new tuple. When an update changes destination key, row under
old key never receives a delete marker. PostgreSQL logs old key
(`XLH_UPDATE_CONTAINS_OLD_KEY`), or old tuple under `REPLICA IDENTITY FULL`
Emit delete for old key plus insert for new row. Test `UPDATE t SET id = id + 1`

`REPLICA IDENTITY FULL` without primary key derives no key, so destination sorts
on `_lsn` and updates and deletes never converge. Sort on whole row instead:

- Every update changes key, so this depends on key-change split above
- Identical rows collapse to one, and deleting one duplicate deletes all
  Document or reject; `ctid` moves on update and rewrite, so cannot break ties
- Nullable columns need `allow_nullable_key`; exclude or reject unsortable
  types such as `Map` and `JSON`
- Keep primary index small with a short `PRIMARY KEY` prefix
- Column add extends key in same `ALTER`; drop or retype of a key column
  requires rebuild

Detect replica-identity changes and key column removal. Neither reaches schema
diff. Preserve a usable destination row key or require rebuild

## Other transitions

Reject unsupported changes while planning commit, before first destination
effect. Stop with relation name, change, and required operator action. A
warning followed by incomplete routing is not rejection

| Transition | Required decision or test |
|---|---|
| Drop NOT NULL, then insert NULL | Pinned destination type only warns, and emitter writes type default for NULL into non-Nullable columns. Reject instead of substituting |
| Create an unlogged table or switch to unlogged | Persistence never reaches schema diff. Reject replication scope that cannot receive its row changes |
| Attach a populated partition | Define leaf routing and initial load before accepting parent scope |
| Drop and recreate a name | Test retained destination rows under retain and warn policies |
| Drop with CASCADE or a refused RESTRICT | Apply chosen dependent-object policy and preserve unaffected mappings |

Column DDL must address destination columns by mapped name. `DROP COLUMN`
names old source column, so a column with a configured destination name stays
in ClickHouse. `RENAME COLUMN` renames to new source name while mapping takes
column rule's target name

Cover column rename and drop landing in ClickHouse, and `CLUSTER`. Compare
schema-change tests against canonical source rows, not literals

## Column retype

Type changes without rewrite emit no replacement rows. Examples include
widening `numeric(10,2)` to `numeric(12,2)` and `timestamp(3)` to
`timestamp(6)`. Verify destination conversions preserve values

Plan key rebuilds through bootstrap staging swap ([bootstrap](bootstrap.md),
[staging](../src/backfill/backfill_staging.rs)): create staging with new schema,
route complete set of rewrite rows there, then exchange tables at commit
For transitions requiring rebuild without rewrite, such as replica identity or
configured `ORDER BY` changes, populate staging through backfill at an LSN

Test or document remaining cases:

- Soft-deleted and superseded versions receive no rewrite rows; they keep cast
  values or defaults
- Whole-table rewrites can spill transactions and retain multiple destination
  versions until merges; `MODIFY` mutations compete with inserts
- Volatile `ADD COLUMN` defaults converge through rewrite rows
- `REFRESH MATERIALIZED VIEW` also fills transient heap; verify handling of
  refreshed rows and removal of rows absent from new contents

Cover `text` to `int` with values ClickHouse cannot parse, key column `int` to
`bigint` including ClickHouse refusal, volatile `ADD COLUMN` defaults, retype
plus rename in one statement, DML after rewriting `ALTER` in same transaction,
restart during rewrite transaction, and rewrite beyond spill threshold. Compare
canonical source rows against destination `FINAL`

## Restart and external drift

Use [descriptor history](../src/catalog/desc_log.rs) and
[mapping updates](../src/config.rs); do not add another baseline cache or
persistence format

Verify ADD, RENAME, DROP COLUMN, and table drop while daemon is stopped, followed
by restart without a warm-up write. Pinned mappings fold schema changes only in
memory, so verify a column added before restart still routes after it. Repeat
ALTER followed by config reload for both pinned projections and
source-configured tables. Never re-add a column operator deliberately excluded

Separately decide whether walshadow should reconcile destination changes made
outside replication, such as a manually dropped table or column. Boot
re-creation of mapped tables and columns already undoes such drops without
policy. ClickHouse can prove destination existence, but cannot tell whether an
absent source column was deliberately excluded. Any reconciliation must respect
mapping policy and stored source history

## Planning seam and baseline tests

Return typed mutations for every change, not only retypes, and keep them
separate from rendered SQL so [fuzzing](fuzzing.md) can compare rename,
add/drop/modify column, table lifecycle, and rebuild decisions. Add a
disposition: apply, no destination effect, explicit policy, or unsupported

ClickHouse DDL can fail after earlier operations succeeded, so persist or
reconstruct enough evidence to retry or stop explicitly; pure validation cannot
make remote DDL atomic. Keep [runtime config](runtime_config.md) changes in same
ordered snapshot

Pin warm/cold equivalence by clearing caches between capture and diff, then
comparing events, SQL, and resulting mapping. Configured projection and
ClickHouse columns cannot reconstruct omitted source columns or distinguish
intentional exclusion from drift

Repeat restart and reload matrix across explicit table mappings, namespace
auto-create, source-side opt-in, retained drops, and drop/recreate generations
If destination existence probes are added, keep them separate from source
baseline, including missing destination table versus deliberately excluded column

Coordinate destructive DDL with active COPY [bootstrap loads](bootstrap.md),
which write into live tables without notice of schema events. Cancel or restart
loads through an explicit generation boundary; do not publish old load rows
under new mapping just because resolver has republished

## Completion

Every transition has a tested outcome: convergence, explicit retention policy,
or rejection before its destination effects. Restart and config reload preserve
that outcome. Turn unexplained descriptor differences into failures in
[semantic fuzzing](fuzzing.md)
