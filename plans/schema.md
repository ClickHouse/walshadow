# Safe schema changes

Some PostgreSQL changes update captured descriptors without updating destination
routing or schema. Others only warn, then let rows continue under an old mapping
Make each supported transition converge and reject unsupported transitions
before applying that transaction's destination changes

Start in [schema comparison](../src/schema.rs),
[catalog capture](../src/source/catalog_capture.rs), and
[DDL application](../src/emit/ch_ddl.rs). Current operator contract lives in
[schema changes](../docs/schema-changes.md)

## Reproduced gaps

Checked revision `1267db7` on PostgreSQL 18.6 and ClickHouse 26.8.1.951
Use [schema harness](../tests/schema_evolution_cdc.rs) with namespace auto-create,
fresh `probe` schema per case, and `pg_switch_wal()` before and after transition
Drain pipeline, then compare source with `FINAL WHERE _is_deleted = 0`
All cases below drain successfully despite different final rows

| Setup and transition | Observed destination | Next step |
|---|---|---|
| Create `t(id int PRIMARY KEY, v text)`, insert `(1,'before')`, rename to `renamed`, insert `(2,'after')` | Old destination contains only `(1,'before')` | Emit relation-identity event and rekey route before following rows |
| Create unlogged keyed table, insert two rows | Destination exists but contains no rows | Reject unlogged scope before destination creation |

Drop/recreate under default retain also leaves both generations' rows. Treat
that as selected retention policy, not unexplained loss; test explicit warn/drop
policies separately. Column rename/drop without target-name overrides, `CLUSTER`,
and automatic nullable widening converged in these probes. Keep mapped-column, pinned-type,
restart, and combined-transaction cases below

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

Keep destination sort key aligned with replica identity:

- Changes to replica identity and removal of key columns do not trigger schema
  events. A table created before setting `REPLICA IDENTITY FULL` keeps
  `ORDER BY _lsn`. Preserve a usable destination row key or require a rebuild
- If configured `order_by` includes a non-key column, updating that column
  leaves old row in place. Changing row key also leaves old row unless identity
  is `FULL`, because other modes log only old key values. Reject this
  configuration or require `FULL`

### Sort key column changes

ClickHouse rejects dropping or renaming sort key columns, and type changes
that require more than a metadata update (`ALTER_OF_COLUMN_IS_FORBIDDEN`).
This affects primary key columns and, with `REPLICA IDENTITY FULL` and no
primary key, every column used for sorting. Rejection fails `ddl apply` and
stops replication

Restart replays schema event from `emitter_ack`. Each column `ALTER` uses
`IF [NOT] EXISTS`, so manually rebuilding destination with new schema should
let replay skip completed changes. Verify this for `MODIFY COLUMN`: ClickHouse
also rejects replaying it on a rebuilt key column. Operator docs cover only
type changes; document drops and renames too. Type changes report rebuild
instructions, but drops and renames report raw server errors

When whole row acts as key, changing its columns changes row identity.
Rewriting stored rows is required to update their keys. Handle each operation:

| Operation | Proposed handling |
|---|---|
| rename | Keep old ClickHouse column name as a configured name would. Replication stays correct, but destination name differs from source. Apply to primary key columns too; no policy needed |
| add | Adding without `MODIFY ORDER BY` lets delete marker and new row share a key, so newer version wins. Rows differing only in new column become one row. Alternatively, extend key in same `ALTER` (`ADD COLUMN …, MODIFY ORDER BY (…, c)`) without rebuilding |
| drop | Apply configured policy |
| change type | Apply configured policy |

Add a policy per table or namespace, such as `sort_key_change`, defaulting to
`fail`:

- `fail`: stop and report affected table, column, and rebuild steps
- `keep`: on drop, keep ClickHouse column and fill it with defaults. Rows
  inserted after drop replicate correctly. Updating or deleting earlier rows
  leaves stale versions because old-row delete markers lack dropped values
  and cannot match old keys. On type change, keep old ClickHouse type and
  convert incoming values. Reject values it cannot represent; see
  [value coercion](value_coercion.md)
- `rebuild`: when processing schema event, create a replacement table with
  explicit key clauses (`CREATE … AS` copies old keys). Copy selected columns
  with `INSERT … SELECT`, preserving `_lsn`, version, and delete columns.
  Swap tables with `EXCHANGE TABLES`, then drop old table. See
  [column retype](#column-retype) for staging and swap steps

Rebuild constraints:

- Pause pipeline while ClickHouse copies rows; disk usage doubles during copy
- `EXCHANGE TABLES` requires an Atomic or Replicated database
- Materialized views and grants refer to old table UUID; verify views survive swap
- After a crash, inspect destination schema during replay to resume rebuild
- If ClickHouse `CAST` differs from PostgreSQL conversion, including `USING`,
  rewritten rows get different key values and leave duplicates

Do not rely on dropped column values remaining in PostgreSQL rows. Although
`FULL` logs those values and decoder could use old type to build matching
delete markers, `VACUUM FULL` and table rewrites clear them without emitting
replacement rows. Later delete markers would no longer match stored rows

Test each policy with column drops, renames, and type changes on tables using
whole rows as keys. Then update and delete rows inserted before and after each
change. Restart during rebuild and compare source rows with destination `FINAL`

## Other transitions

Reject unsupported changes while planning commit, before first destination
effect. Stop with relation name, change, and required operator action. A
warning followed by incomplete routing is not rejection

| Transition | Required decision or test |
|---|---|
| Drop NOT NULL, then insert NULL | Pinned destination type only warns, and emitter writes type default for NULL into non-Nullable columns. Reject instead of substituting |
| Create an unlogged table or switch to unlogged | Persistence never reaches schema diff. Reject replication scope that cannot receive its row changes |
| Attach a populated partition | Define leaf routing and initial load before accepting parent scope |
| Drop and recreate a name | Verify warn/drop policies and restart preserve chosen generation policy |
| Drop with CASCADE or a refused RESTRICT | Apply chosen dependent-object policy and preserve unaffected mappings |

Column DDL must address destination columns by mapped name. `DROP COLUMN`
names old source column, so a column with a configured destination name stays
in ClickHouse. `RENAME COLUMN` renames to new source name while mapping takes
column rule's target name

Cover configured target names through column rename/drop, including restart
Compare schema-change tests against canonical source rows, not literals

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

Existing `alter_column_type_converges_through_rewrite` passes with source
`'abc'` rewritten through `USING length(s)` and integer widening. Extend it with
key column `int` to `bigint` including ClickHouse refusal, volatile `ADD COLUMN`
defaults, retype plus rename in one statement, DML after rewriting `ALTER` in same transaction,
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
