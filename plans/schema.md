# Safe schema changes

Some PostgreSQL changes update captured descriptors without updating destination
routing or schema. Others only warn, then let rows continue under an old mapping
Make each supported transition converge and reject unsupported transitions
before applying that transaction's destination changes

Start in [schema comparison](../src/schema.rs),
[catalog capture](../src/source/catalog_capture.rs), and
[DDL application](../src/emit/ch_ddl.rs). Current operator contract lives in
[schema changes](../docs/schema-changes.md)

## First changes

Detect table renames, schema moves, replica-identity changes, and persistence
changes alongside column changes. Decide whether a change affects a replicated
relation before rejecting it. Include destination sorting keys and explicit
column mappings in validation

Validate all changes in a transaction before executing any destination DDL
For unsupported cases, stop with relation name, change, and required operator
action. A warning followed by incomplete routing is not rejection

| Transition | Required decision or test |
|---|---|
| Rename table, move table to another schema, rename schema | Move routing by source identity and choose destination rename policy, or reject |
| Widen or replace a column type | Converge through rewrite rows, see [column retype](#column-retype-through-rewrite-rows) |
| Drop NOT NULL, then insert NULL | Make destination nullable or reject, never silently substitute a default |
| Change replica identity or remove a destination key column | Preserve a usable destination row key or require rebuild |
| Create an unlogged table or switch to unlogged | Reject replication scope that cannot receive its row changes |
| Attach a populated partition | Define leaf routing and initial load before accepting parent scope |
| Drop and recreate a name | Test retained destination rows under each drop policy |
| Drop with CASCADE or a refused RESTRICT | Apply chosen dependent-object policy and preserve unaffected mappings |

Keep supported controls beside rejection cases: column add, rename, drop,
fast defaults, truncate, rewrite, abort, and savepoint rollback. Test DML on both
sides of each change. Compare canonical source and destination rows as well as
destination schema

## Column retype through rewrite rows

Rewriting `ALTER TABLE` fills transient heap `pg_temp_<oid>` through
`heap_insert`, then swaps filenodes at command end. Its `pg_class.relrewrite`
identifies owner relation (PostgreSQL `src/backend/commands/tablecmds.c`,
`ATRewriteTable`). At `wal_level=logical`, each insert logs full new tuple,
including `USING` results. Logical decoding skips these rows through
`relrewrite` checks in `src/backend/replication/logical/reorderbuffer.c`.
`VACUUM FULL` and `CLUSTER` instead write pages through `rewriteheap.c`

Commit resolution associates rewritten filenode with owner. During fold,
ignore command-boundary descriptors with another OID on that filenode and
route rows under owner's commit descriptor
([transaction buffer](../src/xact/xact_buffer.rs)). Owner's schema event uses
new filenode's storage creation position, before rewrite rows, so destination
DDL runs first. Rewrite rows cover every live row with newer `_lsn` values
and supersede stored versions through deduplication. Until then, stored
versions retain cast values or column defaults

| Destination change | Current selection |
|---|---|
| `MODIFY COLUMN` | integer conversions, conversions to `String`, or no source rewrite |
| `DROP COLUMN` then `ADD COLUMN` | other conversions during rewrite; avoid mutations stalled by unparseable stored values |
| manual migration | ClickHouse rejects operation, such as unsupported key column retype |

Detect rewrite by comparing old and new filenodes. Type changes without
rewrite emit no replacement rows. Examples include widening `varchar(10)` to
`varchar(20)`, `numeric(10,2)` to `numeric(12,2)`, and `timestamp(3)` to
`timestamp(6)`. Verify destination conversions preserve values in these cases

Plan key rebuilds through bootstrap staging swap ([bootstrap](bootstrap.md),
[staging](../src/backfill/backfill_staging.rs)): create staging with new schema,
route complete set of rewrite rows there, then exchange tables at commit.
Current implementation stops on ClickHouse refusal and requires manual
migration. For transitions requiring rebuild without rewrite, such as replica
identity or configured `ORDER BY` changes, populate staging through backfill at
an LSN

Test or document remaining cases:

- Soft-deleted and superseded versions receive no rewrite rows; retain cast
  values or defaults
- Whole-table rewrites can spill transactions and retain multiple destination
  versions until merges; `MODIFY` mutations compete with inserts
- Volatile `ADD COLUMN` defaults converge through rewrite rows
- `REFRESH MATERIALIZED VIEW` also fills transient heap; verify handling of
  refreshed rows and removal of rows absent from new contents

Cover `int` to `bigint USING w * 10`, `int` to `text`, `text` to `int` with
values ClickHouse cannot parse, key column `int` to `bigint`, volatile
`ADD COLUMN` defaults, retype plus rename in one statement, DML after `ALTER`
in same transaction, restart during rewrite transaction, and rewrite beyond
spill threshold. Compare canonical source rows against destination `FINAL`

## Restart and external drift

Descriptor history is durable and source mapping changes already survive
resolver republish. Do not add another baseline cache or persistence format
Use [descriptor history](../src/catalog/desc_log.rs) and
[mapping updates](../src/config.rs) as current implementation

Verify ADD, RENAME, DROP COLUMN, and table drop while daemon is stopped, followed
by restart without a warm-up write. Repeat ALTER followed by config reload for
both pinned projections and source-configured tables. Never re-add a column
operator deliberately excluded

Separately decide whether walshadow should reconcile destination changes made
outside replication, such as a manually dropped table or column. ClickHouse
can prove destination existence, but cannot tell whether an absent source
column was deliberately excluded. Any reconciliation must respect mapping
policy and stored source history

## Planning seam and baseline tests

Extract a pure transition planner from DDL application when implementing guards
Proposed inputs are previous/current `RelDescriptor`, selected `TableMapping`,
and immutable config snapshot. Return ordered typed mutations, mapping delta,
and disposition: apply, no destination effect, explicit policy, or unsupported
Keep mutation intent separate from rendered SQL so [fuzzing](fuzzing.md) can
compare rename, add/drop/modify column, table lifecycle, and rebuild decisions

Plan every schema event in source transaction before executing first destination
effect. Publish mapping delta only after corresponding DDL succeeds. ClickHouse
DDL can fail after earlier operations succeeded, so persist or reconstruct enough
evidence to retry or stop explicitly; pure validation cannot make remote DDL
atomic. Keep [runtime config](runtime_config.md) changes in same ordered snapshot

Pin warm/cold equivalence by clearing caches between capture and diff, then
comparing events, SQL, and resulting mapping. Preserve column identity through
attnum and source history: a rename must remain rename, not DROP plus ADD
Configured projection and ClickHouse columns cannot reconstruct omitted source
columns or distinguish intentional exclusion from drift

Repeat restart and reload matrix across explicit table mappings, namespace
auto-create, source-side opt-in, retained drops, and drop/recreate generations
If destination existence probes are added, keep them separate from source
baseline, including missing destination table versus deliberately excluded column

Coordinate destructive DDL with active [bootstrap loads](bootstrap.md). Cancel
or restart staging through an explicit generation boundary; do not publish old
load rows under new mapping just because resolver has republished

## Completion

Every transition has a tested outcome: convergence, explicit retention policy,
or rejection before its destination effects. Restart and config reload preserve
that outcome. Turn unexplained descriptor differences into failures in
[semantic fuzzing](fuzzing.md)
