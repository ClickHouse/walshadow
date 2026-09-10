# Shared implementation constraints

Use this map when sequencing future work. Detailed proposals belong in linked
plans; these contracts identify changes that cannot be designed independently

| Shared seam | Workstreams | Contract to preserve |
|---|---|---|
| WAL publication before decoder dispatch | [Replay callback](custom_rmgr.md), [shadow TOAST](shadow_toast.md), [performance](performance.md) | Arm or classify before publishing bytes, then prove every decoder replay wait remains reachable under backpressure |
| Filtered archives and manifests | [Replay callback](custom_rmgr.md), [shadow TOAST](shadow_toast.md), [failover](failover.md) | Recover original record meaning, gate destructive bytes on every recovery path, validate branch and fork prefix before publication |
| Durable resume floor | [Bootstrap](bootstrap.md), [verification](verification.md), [shadow TOAST](shadow_toast.md), [destinations](extensions.md#multiple-clickhouse-destinations) | Account for unresolved transactions, carry, deferred reads, queued work, and every required destination before retention or cleanup advances |
| Full physical identity | [Tablespaces](tablespaces.md), [catalog](catalog.md), [shadow TOAST](shadow_toast.md) | Key by tablespace, database, and filenode; preserve relation OID and generation separately for logical ownership and retirement |
| Schema planning and route snapshots | [Schema](schema.md), [runtime config](runtime_config.md), [fuzzing](fuzzing.md), [destinations](extensions.md#multiple-clickhouse-destinations) | Validate transaction before effects, retain source baseline separately from destination projection, publish mapping after successful application |
| Bootstrap jobs and deferred storage | [Performance](performance.md), [bootstrap](bootstrap.md), [shadow TOAST](shadow_toast.md) | Assign completion identity before parallel decode, retain original load version, make deferral safe for concurrent writers and restart |
| Timeline crossing | [Failover](failover.md), [runtime config](runtime_config.md), [verification](verification.md) | Fence abandoned ordinary state, retain prepared state when supported, compare positions through lineage |
| Recovery evidence | [Coverage](coverage100.md), [fuzzing](fuzzing.md), all storage changes | Test crashes around persistence and publication, assert rows and durable progress, retain minimized regressions |

## Resolve before composing publication gates

Current byte-before-record ordering allows decoder waits to observe WAL already
sent to shadow. A replay callback permits receipt ahead while holding redo;
TOAST reclamation instead withholds destructive records until older reads finish
Neither feature can inherit deadlock freedom from current ordering unchanged

Draw wait dependencies for a transaction needing catalog replay beyond a held
TOAST boundary, a full decoder queue, and a blocked destination. Choose draining,
durable materialization, or explicit rejection before permitting that cycle
Bound arm slots, record queues, staged archives, retained shadow WAL, and deferred
tuples independently, then measure combined memory and disk budgets

Keep raw witness WAL separate from filtered shadow WAL. Witness durability,
shadow replay, destination acknowledgement, and resume safety prove different
things. Do not reuse one watermark merely because each is represented as an LSN

## Persistence changes

For every new carry file, manifest, cursor field, or durable destination queue,
specify identity, format version, write/fsync/publication order, startup recovery,
and cleanup condition. Test old-format startup and interrupted replacement
Never recover by scanning anonymous scratch files and assuming they are complete

Persist enough route and schema evidence to reproduce pending effects. A future
destination queue or prepared-transaction snapshot must preserve descriptor
history and TOAST dependencies as well as row bytes

Land immediate rejection guards independently of larger support work. Keep
guards until combined path passes its acceptance matrix, then move settled
behavior into architecture and operator docs
