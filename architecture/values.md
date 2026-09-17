# Large values and type conversion

![TOAST and PostgreSQL conversion](values.svg)

PostgreSQL tuples can contain compressed values or external TOAST pointers
Decoding a pointer requires historical chunks, while decoding an unfamiliar type
requires PostgreSQL's type machinery. These are separate operations

## Resolve external values

Chunks written in same transaction provide fastest reconstruction path. An
unchanged pointer may instead refer to data predating current WAL window, which
requires a persistent chunk store

Current ClickHouse mirrors track chunk births and deletions by physical tuple
location, with record LSN as version. Lookup is bounded by referring record's
position so later chunk reuse cannot automatically replace an earlier value
Reconstruction validates sequence and expected size before decompression

One lookup covers many values of one mirror at one bound, which is how deferred
bootstrap referrers resolve: per-value as-of aggregation is unchanged, wide
batches split at a fixed width, and splits run across store connections

Destination merges can remove old chunk versions. Current miss handling relies
on supersession by later main-row versions; it does not authorize filling values
when an entire required mirror is missing. Bootstrap visibility and reused
generations have additional limits documented in
[large values](../docs/limitations.md#large-values)

TRUNCATE orders a mirror wipe with destination truncation. DROP cleanup waits
until persisted restart floor passes possible older referrers. A live
acknowledgement alone is insufficient because restart could still reread a
pre-drop row. Rewrite barriers handle changed physical generations without
unconditionally wiping history needed by earlier reads

## Convert unfamiliar types

After decompression, values outside local codec set are sent to PostgreSQL
module in batches. PostgreSQL interprets source type and module produces Native
columns for destination insertion. Batch conversion avoids a database round trip
per row and keeps type semantics in PostgreSQL

Conversion failure stops batch with column and row context. Module pins output
settings to make conversion reproducible. Greenfield bootstrap uses a temporary
PostgreSQL instance because managed shadow is not ready yet

## Alternative value modes

Shadow mode reads external values from PostgreSQL's own TOAST heaps instead of
writing chunks to a mirror. See
[shadow TOAST architecture](shadow-toast.md),
[large-value limitations](../docs/limitations.md#shadow-value-mode) and
[shadow TOAST plan](../plans/shadow_toast.md)

Disabled mode keeps no value store. A value can still be restored when its
chunks appear in same transaction's WAL. Otherwise, it becomes NULL, or column
type's default when target is not Nullable. See
[disabled mode](../docs/configuration.md#value-mode)

## Implementation

Start in [TOAST resolver](../src/toast/resolver.rs),
[retirement ledger](../src/toast/toast_retire.rs),
[conversion client](../src/ops/oracle.rs), and
[PostgreSQL module](../pgext/worker.c). Build instructions live in
[module guide](../pgext/README.md), PostgreSQL-backed storage in
[shadow TOAST architecture](shadow-toast.md)
