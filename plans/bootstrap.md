# Transactions crossing bootstrap

Retain undecided backup tuples until their transactions settle. Row WAL can
predate backup redo while commit or abort lands after handoff, leaving replay
unable to reconstruct those rows

See [visibility gate](../src/backfill/visibility_gate.rs),
[tuple visibility](../src/decode/visibility.rs), and
[pending storage](../src/backfill/visibility_pending.rs) for implementation,
[initial loads](../docs/limitations.md#initial-loads) for operator limits

## Preserve undecided rows

`tuple_visibility` returns `Defer` for in-progress inserts, deletes, or multixact
updaters. `deferred_xids` identifies required outcomes: insert must commit,
delete must abort. Resolve multixacts to updater xids before storage so
settlement needs only transaction status

Use one `<table>__wspending` sibling per destination on plain
`MergeTree ORDER BY tuple() PRIMARY KEY tuple() PARTITION BY tuple()`.
Name every key clause because ClickHouse `CREATE … AS` inherits omitted keys.
Plain MergeTree preserves competing UPDATE versions; ReplacingMergeTree could
collapse rows sharing a key and load version before visibility is known

Store rendered destination rows plus three metadata columns:

- `_ws_xmin`: xid whose commit is required, zero when already proven
- `_ws_xmax`: xid whose abort is required, zero when no deleter remains
- `_ws_infomask`: original tuple hint bits behind those reductions

Promote survivors with `INSERT INTO <target> SELECT … FROM <pending>` at original
load version so later streamed changes win. Select only rows resolved by current
round to avoid repeating earlier promotions. Drop pending table once all deciding
xids settle, discarding aborted inserts and committed deletes

Replay from oldest buffered WAL record for later changes; missing required
history remains an error. Include pending rows in
[TOAST reclamation](shadow_toast.md) safety accounting

## Pending row durability

Write pending rows and persist ledger before clearing bootstrap marker. For
staged loads, publish swap before recording pending manifests so EXCHANGE cannot
discard promoted rows. Failed passes leave no entry; retry rebuilds pending tables

`{spill_dir}/visibility_carry.toml` retains its existing filename and `carry` key
for restart compatibility. Record relation `(namespace, relname)`, destination
database and table, original `start_lsn`, and outstanding, committed, and aborted
xids. Retain known outcomes across rounds for rows waiting on both insert and
delete. Replace ledger atomically and reject corrupt state

Promote before updating ledger; drop pending table before removing its entry.
Destination dedup absorbs promotions repeated after a crash

## Settling

Fold live `XLOG_XACT_COMMIT` / `XLOG_XACT_ABORT` outcomes, including subtransactions,
into ledger. At boot, recover outcomes from shadow transaction logs for records
absent from resumed WAL

Retain required transaction history. Vacuum freezes surviving tuples and removes
aborted tuples before truncating `pg_xact`, providing evidence for on-page
visibility decisions. Pending copies receive no such updates: missing status
leaves rows unpublished and xids outstanding. Report these through
`walshadow_pending_undecidable_xids` and a boot log

Resolve mapped TOAST values through walk's chunk store before rendering pending
rows. Missing chunks fail pending writes just as they fail main drain

`XLOG_RUNNING_XACTS` could bound outstanding xids, as in PostgreSQL hot standby.
Extend `parse_running_xacts_next_xid` to read xid array and respect
`subxid_overflow`. Records arrive from bgwriter and checkpoints; do not depend on
forcing one

Coordinate [parallel bootstrap decode](performance.md) completion with pending
rows and deferred TOAST. Scratch writes alone cannot acknowledge deferred tuples

## Completion

Exercise INSERT, UPDATE, and DELETE begun before backup redo and inside backup
window, committing or rolling back after handoff. Cover multixact updaters,
subtransactions, deferred external values, and restart before settlement. Run
direct and object-store cases with explicit retained-history assumptions

Assert row contents, deletion state, restart position, and pending table cleanup.
Keep per-table load rejection coverage separate

DDL during initial load remains unsupported. Define cancellation or restart for
destructive and type-changing DDL before adding support, preserving staging and
convergence boundaries

## Reduce source SQL reads

Backup-based initial loads issue no source SQL scans for user rows. Explicit
`initial_load = "copy"` still scans selected table and remains `init`'s default.
Baseline external values in backup modes resolve from walked chunk mirrors,
so reused TOAST generations need a physical proof of age: backup transaction
logs and tuple-location order do not
establish it, and no source read compensates. Keep WAL replay ordered behind
required chunk history

Eliminating all source SQL also requires descriptors from landed catalogs, an
OID-consistent type converter without source pg_dump, and alternatives for slot,
preflight, and runtime-config queries. Keep these dependencies explicit. Landing
all user heaps in shadow to serve COPY would change it into a full replica
