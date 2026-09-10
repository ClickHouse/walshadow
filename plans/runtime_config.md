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

## Additional settings

Evaluate per-query ClickHouse settings, engine selection, column exclusion, and
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
