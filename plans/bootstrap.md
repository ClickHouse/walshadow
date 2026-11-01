# Transactions crossing bootstrap

Prove pending-row recovery across load modes and transaction outcomes. Row WAL
can predate backup redo while commit or abort lands after handoff, leaving replay
unable to reconstruct those rows

Pending tables, durable outcome ledger, live settlement, and boot recovery exist
See [initial-load architecture](../architecture/bootstrap.md),
[pending storage](../src/backfill/visibility_pending.rs), and
[initial-load limits](../docs/limitations.md#initial-loads)

## Retain deciding evidence

Define recovery when required transaction status ages out. Pending copies receive
neither vacuum's frozen hints nor removal of aborted tuples, so missing status
leaves rows unpublished indefinitely. Include pending rows in
[TOAST reclamation](shadow_toast.md) safety accounting

`XLOG_RUNNING_XACTS` could bound outstanding xids, as in PostgreSQL hot standby.
Extend `parse_running_xacts_next_xid` to read xid array and respect
`subxid_overflow`. Records arrive from bgwriter and checkpoints; do not depend on
forcing one

Coordinate [parallel bootstrap decode](performance.md) completion with pending
rows and deferred TOAST. Scratch writes alone cannot acknowledge deferred tuples

## Completion

[Pending-row tests](../tests/bootstrap_pending_shapes_ch.rs) cover updates,
deletes, multixact updaters, subtransactions, and external values during direct
bootstrap. Add transactions begun during backup. Repeat for object-store
bootstrap and per-table backup loads, stating which WAL history must remain
available

Restart before settlement and after promotion but before ledger persistence.
Interrupt pending-table cleanup and staged publication. Cover outcomes arriving
between replay cut and ledger registration, including failure of source status
query. Assert exact rows, deletion state, original load version, restart position,
and pending-table cleanup. Keep per-table load rejection coverage separate

DDL during initial load remains unsupported. Define cancellation or restart for
destructive and type-changing DDL before adding support, preserving staging and
convergence boundaries

## Reduce source SQL reads

Backup-based initial loads issue no source SQL scans for user rows. Explicit
`initial_load = "copy"` still scans selected table and remains `init`'s default.
Backup rows read external values from chunk mirrors populated during page walk.
Greenfield bootstrap skips a row whose value cannot be reassembled only when
backup-window replay wrote a matching row version. Per-table loads resolve
deferred values before gap replay, so these failures still stop loading.
Resolve them after gap replay to allow skipping rows already replayed there.
Keep WAL replay behind required chunk writes

Eliminating all source SQL also requires descriptors from landed catalogs, an
OID-consistent type converter without source pg_dump, and alternatives for slot,
preflight, and runtime-config queries. Keep these dependencies explicit. Landing
all user heaps in shadow to serve COPY would change it into a full replica
