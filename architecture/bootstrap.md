# Initial loads

![Bootstrap data paths](bootstrap.svg)

An initial load combines existing rows with changes arriving while they are
read. A physical backup is a mixture of page states, so walking heap pages alone
cannot produce a consistent destination

## Greenfield bootstrap

Backup files feed two paths: catalog and recovery files build shadow, while
selected user pages become candidate destination rows. A visibility gate uses
tuple hints, backup transaction logs, and backup-window WAL outcomes to decide
which rows belong in initial state

Concurrent WAL replay covers changes during backup. Walked rows carry an older
coverage version so later committed changes win at destination. Bounded channels
and spill constrain memory while backup and insertion proceed independently

Shadow is not ready during this phase. A temporary PostgreSQL instance built
from source schema converts values that need PostgreSQL's type machinery

Rows carrying external values wait for the chunk mirror the walk and the window
leg write. Window page images refill chunks a page copied mid-write lost, every
tuple an image carries and not only the one its record names, so resolution
waits for both every walk lane and the window leg. A value the mirror cannot
reassemble stops the load rather than substituting one

Rows written before backup redo can remain undecided after handoff. Retain them
in pending tables beside destinations until commit or abort decides visibility.
Promote survivors at original coverage version so later streamed changes win

Handoff waits for required insertion and recovery work, then persists restart
position before steady streaming advances it. Transactions still open can lower
resume position into backup window. See
[remaining visibility work](../plans/bootstrap.md)

## Per-table loads

`initial_load = "copy"`, selected by default by `init`, scans selected table
through PostgreSQL for visible rows and detoasted values. Backup-based loads
use page walk and WAL replay without replacing existing shadow. Destination
staging separates partial initial state from published table

Replay of archived WAL between backup and selection point follows the branch the
stream proved, cross-checked against archived history. Reject backups whose redo
or finish lies outside that branch, including loads without gap replay

For backup-based table loads, a durable ledger tracks load and swap progress
After staged rows are ready, table exchange publishes them and live changes are
reconciled. Restart must distinguish a swap that has already happened from one
still pending; otherwise retry can replace newer destination state

Staging changes what destination materialized views observe. See
[table selection](../docs/table-selection.md) and
[initial-load limits](../docs/limitations.md#initial-loads) before deployment

## Implementation

Start in [bootstrap](../src/backfill/backfill_bootstrap.rs),
[window replay](../src/backfill/bootstrap_window.rs),
[visibility gate](../src/backfill/visibility_gate.rs),
[pending visibility](../src/backfill/visibility_pending.rs),
[table backfill](../src/backfill/backup_backfill.rs), and
[staging](../src/backfill/backfill_staging.rs)
