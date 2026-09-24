# Runtime configuration extensions

Source config tables, commit-ordered changes, table opt-in, initial loads,
column rules, and destination sorting keys already exist. Current syntax and
precedence live in [configuration](../docs/configuration.md). Do not duplicate
that surface here

## Source-side commands

Add WAL-carried commands only for operations that do not fit stored config,
such as flush, reseed, or ignoring replication of a particular transaction
Local pause, resume, reload, and endpoint changes already have control commands

If using pg_logical_emit_message, filter source database identity before parsing
configured prefix. Restrict global commands to a configured administrative
database with appropriate connection privileges. Transaction-local ignore must
come from source transaction it affects. Define authorization before exposing
destructive commands such as slot removal

Apply transactional commands at commit. Preserve savepoint rollback, abort, and
replay behavior. An ignored transaction suppresses its destination row and DDL
effects, while shadow still replays catalog changes needed for later decoding
Advance acknowledgement only through normal ordered completion

Classify commands by replay safety. Persist completion for non-idempotent actions
before restart can replay them, with crash tests around action and checkpoint
Do not assume a single last-seen LSN solves partial external effects

## Explain and recover config

Add an explanation of which layer supplied a resolved value. Prefer bounded
diagnostics over metrics labeled with arbitrary keys or values. Show stale
source-config state separately from unchanged but healthy configuration

Define behavior when WAL delivery stalls. Lack of config writes does not itself
prove stale configuration. Do not silently discard known source settings or
switch precedence on a timer. Preserve operator control through local commands
and make any fallback an explicit, tested policy

Test restart and failover with config changes, including settings present only
on an abandoned branch. Resolve against timeline history, not a bare numeric
LSN comparison across branches

## Complete destination table definition

Accept complete destination `CREATE TABLE` definitions, including
column types and clauses, codecs, engine arguments, sorting and primary keys,
partitioning, sampling, TTL, indexes, projections, and table `SETTINGS`. Preserve
configured clauses beyond existing mapping fields
Support both TOML and source config tables

Choose one canonical interface, such as a `create_table` SQL field
alongside source-to-target mappings. Define interaction with existing structured
table and column options: reject conflicts or define explicit precedence. Keep
structured column tuning useful when operators prefer generated DDL

Parse and validate one `CREATE TABLE` statement before executing DDL. Bind
destination identity explicitly and define handling of database names, cluster
clauses, and object references. Reject unsupported statement forms with actionable
diagnostics. Validate mapped columns, writable and computed columns, defaults
for omitted columns, system columns, and engine/key requirements for replication

Derive wire types from declared columns and check source conversions independently
of storage clauses. Preserve accepted definition in resolved config snapshots
used by streaming and initial loads. If bootstrap uses staging tables, define
how staging and final publication preserve configured schema and settings

Define behavior for an existing destination: compare semantic definitions and
report drift. `CREATE IF NOT EXISTS` does not validate existing schema. Coordinate
config edits and source schema changes with [schema planning](schema.md). Classify
each difference as supported ALTER, explicit migration, or rejection; never drop
and recreate populated tables implicitly. Preserve ordered application and replay
behavior across partial DDL failures

Test complete definitions through creation, existing-table validation, backfill,
source schema evolution, reload, and restart. Check destination metadata as well
as replicated insert/update/delete results. Cover conflicting config fields,
unsupported clauses, and invalid mappings before publishing new routing

## Target type and column tuning

Extend column rules beyond target name and target type with destination storage
tuning, starting with `CODEC` for `String` and other compatible types. Expose
matching controls in TOML and source config tables, with existing pattern
matching, precedence, and effective-value diagnostics

Keep target type available independently for type validation and wire encoding
Represent codec chains and parameters as separate column metadata rather than
appending unchecked SQL to target type. Evaluate further column options, such
as column `SETTINGS` and TTL, individually with explicit supported scope

Define inheritance, explicit reset to destination defaults, and invalid-value
behavior. Validate codec names, parameters, chains, and type compatibility
against supported ClickHouse versions before publishing config. Distinguish
column storage codecs from INSERT wire compression

Carry resolved tuning through CREATE TABLE and ADD COLUMN. Coordinate changes
to existing columns with [schema planning](schema.md): define supported ALTER
operations, commit ordering, retry behavior, and operator action for unsupported
changes. State whether changes affect future parts or require explicit rewriting
of existing data; do not silently schedule mutations

Test string codecs, parameterized codec chains, incompatible
types, precedence and reset, generated DDL, existing-table changes, and restart
or replay after partial DDL failure. Verify destination metadata and unchanged
replicated values. Document syntax and migration behavior once implemented

## Additional settings

Evaluate per-query ClickHouse settings, column exclusion, and
per-table truncate policy only where current rules cannot express required
behavior. Creation-time settings must not pretend to migrate existing tables
Validate unsupported changes before routing new rows

If config workloads become large, batch resolver publication for one transaction
without changing event order. Report unresolved forward declarations clearly;
do not expire intentional declarations merely because they are old

Finish each extension with operator documentation, precedence and validation
tests, replay tests, and a clear failure response. Keep archive credentials and
startup discovery settings outside source config tables

## Signal implementation sketch

Parse logical-message body with checked lengths and PostgreSQL layout for active
major, including padding before variable payload. Read database identity before
interpreting prefix/payload. Resolve configured administrative database name at
attach, then bind signal authorization to verified source identity across restart
Bound payload and diagnostics; do not label metrics with arbitrary command text

Route transactional commands through transaction buffer and ordered control
drain. Nontransactional commands need explicit semantics against in-flight work;
receipt ordering is not commit ordering. Avoid source-side resume that depends
on reading WAL through a pump already stopped by pause, retain local control path

For `ignore-transaction`, require transactional message from affected source
database with no xid argument. Store flag on owning transaction/subtransaction
state so rollback-to-savepoint clears it and committed child state propagates it
At commit suppress destination rows and DDL, release spill safely, and register
normal zero-row completion. Preserve shadow catalog replay and descriptor
evidence needed by later transactions. Define whether source config effects are
also suppressed before enabling mixed config/ignore transactions

An optional SQL helper can record audit row and emit message in same transaction
Keep daemon read-only on source config. For destructive one-shot commands, use
durable intent/completion and replay-safe action identity; numeric last-seen LSN
cannot prove external action completed atomically with checkpoint

Explain effective values through bounded status output containing winning layer
and relevant commit boundary. Track delivery health separately from last config
write. Test old/new config-table schema compatibility, bulk config writes in one
transaction, and abandoned-branch settings with [failover](failover.md)

Keep destination connection definitions local if [fan-out](extensions.md#multiple-clickhouse-destinations)
lands, allow logical route rules in source overlay. Freeze resolved route/schema
snapshot for backfill and coordinate changes through [schema planning](schema.md)
