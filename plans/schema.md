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
| Widen or replace a column type | Prove destination representation still fits, otherwise require migration |
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
