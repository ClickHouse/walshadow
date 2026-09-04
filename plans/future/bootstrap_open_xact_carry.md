# bootstrap: carry tuples of open transactions

Greenfield gate drops tuples whose deciding transaction remains open at
handoff. Inserts can remain absent and deletes can remain live. Slot replay
only repairs records retained inside backup window
([bootstrap.md](../bootstrap.md)).

## Invariant that makes it fixable

Backup checkpoint flushes changes below redo before file copy. Crossing
transactions therefore have:

- records at or above redo, replayable from `open_floor` with a slot
- records below redo, represented by deferred walked tuples

Persist deferred tuples until shadow observes transaction outcome.

## Gate changes

- Return `Defer` for in-progress `xmin`, `xmax`, or multixact updater
- Move deferred tuples into persistent carry spool and record xids
- Record undecided `(rfn, xid)` pairs for pending relations. Delay relation
  repair until all recorded xids settle
- Keep per-table load behavior unchanged: `Undecidable::Abort` gates deferrals

## Persistence

Store beside bootstrap marker in shadow data directory:

- `DeferredSpool` carry file
- manifest containing `start_lsn`, xids, and pending relations

Write before removing marker; remove once carry and pending sets empty. Do not
use startup-cleared spill directory.

## Settle task

Run after handoff and on startup when carry state exists:

1. Build `PgXactView` from shadow `pg_xact` and `pg_multixact`
2. Wait when no carried xid changed
3. Resolve newly settled tuples and relations; rewrite remaining carry

Keep `start_lsn` tag. On commit, emit inserted versions and withhold deleted
ones. On abort, emit deleted versions and drop inserts.

## What stays

Keep `open_floor` and `BootstrapHandoff::resume_lsn`. In-window deletes still
need slot replay because only WAL carries them.

## Tests

Unit: carry in-progress `xmin` and `xmax`; filter pending xid hints; round-trip
manifest; shrink spool after settlement.

E2E: cross bootstrap without `--slot` using INSERT, UPDATE, and DELETE, cover
COMMIT and ROLLBACK. Restart once between handoff and outcome.
