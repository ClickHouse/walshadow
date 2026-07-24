# Intra-xact descriptor timeline

Per-record layout fidelity inside a catalog-dirty transaction: describe
every deferred record with the descriptor its tuple was actually written
under, instead of fencing the span where that is unprovable.

## What the fence leaves on the table

Capture samples the catalog once, at the commit boundary. A physically
incompatible in-place transition therefore publishes
`Ambiguity[first_touch, next_lsn)` and the drain fails closed for every
stashed record inside it ([desc_log.md](../desc_log.md),
[xact.md](../xact.md)). The fence is sound and, after the benign/physical
split, near-unreachable — PG rewrites the relation for every ALTER that
moves tuple layout, and a rotation skips the predicate. What stays
unavailable is the middle ground: a record that provably postdates the
layout change still fails closed, because nothing in the boundary's
evidence says *where* inside the interval the change landed.

## Why the two cheap routes don't work

**Ask at commit time.** The information is not there. `BoundaryInfo` carries
the tree's first catalog touch and per-oid first pg_class touch; neither
bounds the mutation's position, and narrowing `through_lsn` to the tree's
*last* catalog touch trades a fence miss for a false positive — a second
DDL statement after the DML re-widens the interval, and for oids known only
from commit relcache invals the tree's touch span does not attribute the
change to that transaction at all.

**Reconstruct from WAL.** Extending `pg_class_decoder` to pg_attribute
would give per-relation attribution and a change position. It fails on the
operation that matters: catalogs are excluded from
`RelationIsLogicallyLogged`, so `log_heap_update` prefix-compresses catalog
updates, and `attrelid` — column 1, offset 0 — sits inside the elided
prefix that shares bytes with the old tuple. Recovering it needs the old
tuple, i.e. a TID-keyed pg_attribute row cache maintained from the same WAL
stream plus a boot snapshot: a historic relcache, reimplemented.

## Shape

PG marks intra-xact command boundaries for free. At `wal_level=logical`,
`LogLogicalInvalidations` writes `XLOG_XACT_INVALIDATIONS` at each command
end, and a relation's layout can only change at a command boundary. So:

1. **Subdivide the hold.** The pump already holds publication at catalog
   commits until shadow replays through `next_lsn`. Extend that to
   `XLOG_XACT_INVALIDATIONS` records of a dirty xact — one hold per DDL
   command, not per record.
2. **Read the in-flight state.** At a hold at LSN `L` the shadow has
   replayed exactly through `L`, so the xact's catalog rows are on-page but
   uncommitted; no MVCC snapshot sees them. A pgext function scanning with
   `SnapshotAny`, filtered to `xmin == the writing xid` and not deleted by
   it, reproduces a historic snapshot for that one transaction and yields
   the descriptor as of `L`. This reuses PG's own tuple interpretation
   rather than duplicating per-major catalog layouts.
3. **Store a timeline.** The batch grows from one entry per changed
   relation to one per (relation, command boundary), each `valid_from` at
   its command's end. Replay-from-log keeps working unchanged: entries are
   already the durable record of the verdict.
4. **Resolve per record.** `resolve_stash` stops resolving one descriptor
   per filenode and instead hands the drain the filenode's timeline; each
   stashed record folds under the entry covering its own `source_lsn`. The
   ambiguity fence stays as the backstop for whatever the timeline cannot
   cover (a relation the pgext read fails on, an unmodelled catalog shape).

## Cost

One extra hold + one shadow query per DDL command inside a dirty xact.
Nothing changes for clean transactions, and `BEGIN; CREATE TABLE; COPY;`
pays for the CREATE, not for the COPY's records. Against today's model the
new expense is holding at non-commit positions, which delays publication of
successor bytes in the same way commit boundaries already do.

## Open questions

- Subxacts: an aborted subtree's command boundaries must drop with it, same
  as `DirtyTree::drain_tree` handles observations today.
- Concurrent writers: an `SnapshotAny` scan sees other transactions'
  uncommitted rows too. The xid filter handles the common case; a relation
  written by two in-flight xacts needs the AccessExclusiveLock argument
  spelled out, or a fence for the residue.
- pgext availability: the oracle path is optional today. A timeline that
  only works when the extension is installed needs the fence to remain the
  fallback, not a hard dependency.
