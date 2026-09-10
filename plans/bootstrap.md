# Transactions crossing bootstrap

Backup bootstrap can finish while transactions affecting walked tuples remain
open. Replaying from oldest buffered WAL record covers changes inside backup
window, but earlier inserts can remain absent and earlier deletes can remain
visible. Increasing handoff wait reduces exposure without proving completeness

Current visibility decisions live in
[visibility gate](../src/backfill/visibility_gate.rs) and
[tuple visibility](../src/decode/visibility.rs). Current operator limitations
live in [initial loads](../docs/limitations.md#initial-loads)

## Preserve undecided rows

Treat an in-progress inserting transaction, deleting transaction, or multixact
updater as undecided. Keep its tuple and deciding transaction IDs until outcome
is known. Do not combine undecided tuples with rows proven invisible

Persist carried tuples and pending relation repairs before declaring bootstrap
complete. State must survive startup cleanup and record original load boundary
If persistent carry is not available, refuse completion while required visibility
remains unknown, keeping enough state for a safe retry

After handoff and on restart, settle carried tuples as transaction outcomes
become available. Emit committed inserts and aborted deletes; discard aborted
inserts and committed deletes. Keep original load version so later streamed
changes win. Delay relation repair when unresolved transactions can still change
its result

Retain WAL replay from oldest buffered record. Carry and replay cover different
parts of crossing transaction, and neither replaces other. Missing required
history remains an error

## Carry implementation

Change `resolve_phase` so `Visibility::Defer` survives final visibility pass
instead of sharing discard path with `Skip`. Retain deciding xmin, xmax, or
multixact updater IDs with raw tuple and full physical identity. Preserve
pending-relation xid hints even when relation repair replaces individual tuples

Extend [deferred spool](../src/backfill/spool.rs) or introduce durable carry
format beside bootstrap marker, outside startup-cleared scratch. Manifest needs
source/timeline identity, original `start_lsn`, outstanding xids, pending relation
generations, and spool reference. Fsync data and manifest before clearing marker
Use atomic replacement when shrinking carry; a crash must leave old or new
complete state, never a manifest pointing at partially rewritten tuples

On handoff and startup, rebuild `PgXactView` from shadow transaction logs and
settle only tuples whose deciding outcomes are available. Retain unresolved
entries, preserve original row version, and clean carry only after emitted rows
are durable. Retain required transaction-status history or reject if it has aged
out. Include carry in [TOAST reclamation](shadow_toast.md) safety accounting

If [parallel bootstrap decode](performance.md) lands first, carry and deferred
TOAST paths need coordinated writers and completion accounting. A worker cannot
acknowledge a deferred tuple merely because it wrote restart-unsafe scratch

## Completion

Exercise INSERT, UPDATE, and DELETE begun before backup redo and inside backup
window, with both commit and rollback after handoff. Cover multixact updaters,
subtransactions, relation repair, and restart before settlement. Run direct and
object-store cases with explicit retained-history assumptions

Assert final row contents, deletion state, restart position, and eventual carry
cleanup. Keep per-table load rejection behavior covered separately

DDL during initial load remains a separate unsupported case. Before adding
support, define how a destructive or type-changing DDL cancels or restarts an
active table load without exposing partial staging results. Do not silently
restart against a newer snapshot without preserving convergence boundaries

## Reduce source SQL reads

Removing automatic COPY repair requires a physical visibility proof for retained
tuples and reused TOAST generations. Backup transaction logs and tuple-location
order alone do not establish generation age. Replace missing evidence before
removing source reads, and keep WAL replay ordered behind required chunk history

Eliminating all source SQL also requires descriptors from landed catalogs, an
OID-consistent type converter without source pg_dump, and alternatives for slot,
preflight, and runtime-config queries. Keep these dependencies explicit. Landing
all user heaps in shadow to serve COPY would change it into a full replica
