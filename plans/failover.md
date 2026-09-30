# Failover beyond planned switchover

[Planned switchover](../docs/failover.md) already handles controlled descendant
timeline crossing, including restart and stable endpoints. Remaining work covers
unplanned promotion and remaining archive/restart fault cases. See
[recovery architecture](../architecture/recovery.md)

## Unplanned promotion

Order a transaction-state fence after all complete ancestor records and before
descendant records. Finish preceding commits and aborts, then discard ordinary
transactions abandoned by promotion. Clear their tuple, TOAST, spill, pending
descriptor, subtransaction, and creation-marker state. Audit speculative catalog
tracking before letting new-branch records use it

Prepared transactions can survive promotion. Keep them an explicit unsupported
case until [prepared-transaction recovery tests](verification.md) establish a
safe transition. Do not discard them as ordinary open transactions

Handle promotion through an incomplete WAL record. Parser and shadow must agree
on overwritten continuation and exact fork position. Never reinterpret a torn
ancestor tail as ordinary descendant data

Reject transitions if ClickHouse already received abandoned-branch effects
beyond fork. Continuing there needs a separate compensation or rebuild design
Unrelated systems, sibling timelines outside stored lineage, and missing WAL
remain errors

## Archives and backups

Do not reimplement archive lineage or ancestor-backup replay. PostgreSQL 18.6 /
ClickHouse 26.8.1.951 tests at `1267db7` pass for ancestor-backup bootstrap,
[backup gap replay across promotion](../tests/backfill_gap_across_promotion.rs),
and [control-plane recovery](../tests/control_plane_e2e.rs): descendant copies
when ancestor segments are partial, two forks within one segment, source slot
ahead of replay, archive history discovery after source death, and restart inside
fork barrier

Archive discovery after source death proves recovery within already accepted
branch; it does not authorize crossing onto an unproved live source. Retain that
distinction when extending recovery

Prove slotless pause resumes after source recycles WAL. Add restart with an
unsealed descendant segment and archive-only shadow recovery, verifying either
durable fork prefix or explicit wait. Test missing history separately from
missing segment, including direct backups taken from a standby

## Transition implementation

Represent transaction fence as ordered control item in record stream, acknowledge
it from worker after all complete ancestor records drain. Truncate undispatched
bytes past verified fork before archive-seal barrier. Never mutate transaction
buffer concurrently from reconnect task. Fence clears ordinary transaction and
subtransaction ownership, speculative catalog tracking, pending capture holds,
TOAST, markers, and spill accounting before descendant dispatch

Coordinate xid reuse and prepared-state handling with [recovery tests](verification.md)
Descriptor timelines inside transactions are separate from WAL timeline IDs;
clearing pending descriptor entries must preserve committed durable history

For torn records, audit `XLP_FIRST_IS_OVERWRITE_CONTRECORD` and
`XLOG_OVERWRITE_CONTRECORD` handling in parser and shadow recovery. Reproduce
aborted continuation evidence expected by PostgreSQL reader, accepting page flag
alone cannot make shadow cross safely. Test failure without shadow PANIC/FATAL,
then enable only after source and shadow agree on overwritten LSN

Keep fork-prefix durability explicit when descendant segment is unsealed
Coordinate atomic archive publication with [TOAST gate](shadow_toast.md) and
custom-record [provenance](custom_rmgr.md)

Sequence ordinary-state fence, overwritten-continuation support, and remaining
fork-prefix durability proofs. Keep refusal until each enabled case advances
source, shadow, and durable floor consistently

## Completion

Exercise controlled and abrupt promotion, incomplete records, open ordinary and
prepared transactions, restart around each crossing boundary, slot loss, and
archive-only recovery. Compare final rows with a surviving source branch and
assert no state from abandoned transactions leaks through

Keep history and transition metadata until every relevant durable floor passes
them. Status must distinguish missing proof, missing WAL, and pending archive
seal. Document newly supported cases only after corresponding fault tests pass

Leader election, primary fencing, DNS movement, and promoting schema-only shadow
into an application primary remain outside this plan
