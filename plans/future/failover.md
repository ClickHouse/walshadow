# Source timeline failover

Operator-driven switchover is built, restart and stable endpoints included
([../failover.md](../failover.md)): pause below the fork, prove the
frontier and the branch, promote, resume, and commit a resume position on
the descendant behind a barrier. This doc covers what that leaves out —
unplanned promotion, archive-side timeline resolution, and
ancestor-timeline base backups

Source-consumer continuity only. Walshadow is not an HA orchestrator or a
durability witness; WAL relay into a lagging full PostgreSQL standby is
[sync_commit_witness.md](sync_commit_witness.md)

Out of scope throughout:

- leader election, old-primary fencing, DNS or proxy orchestration
- switching to an unrelated system identifier, or accepting a sibling
  timeline when the stored branch is absent from live history
- compensating ClickHouse data already published from an abandoned branch
  beyond the fork
- manufacturing WAL missing from source and archive
- promoting the schema-only shadow into an application primary
- preserving prepared transactions before
  [two_phase_commit.md](two_phase_commit.md) lands

## Slotless pause windows

Slot mode keeps a pause window safe with source-local WAL and proves the
target's slot before the promotion ([../failover.md](../failover.md)
§Slot). A slotless switchover can instead catch up from a continuous S3
archive after `wal_keep_size` recycles the source WAL, which needs the
archive resolution below: request ancestor WAL from the promoted primary
while retained, fall back to timeline-aware segment names, and keep the
current missing-WAL failure when neither covers the floor

## Unplanned promotion

The barrier carries over unchanged — it is about walshadow's own pipeline,
not about how `F` was chosen — but it runs *after* the truncation of
undispatched bytes past `F`, since the archive-seal condition can only be met
once the partial record holding the segment is dropped. What stays
failover-only is the fence and the contrecord

### Transaction-state fence

Replaces the `open_xact_at_fork` refusal. A timeline transition is ordered
record-stream control, not a mutation racing the decoder worker: add a
control item after every complete ancestor record and wait for worker
acknowledgment. At the fence:

- finish every commit or abort record dispatched before the switch
- abandon unresolved ordinary transactions from the ancestor, removing
  their raw heap records, TOAST chunks, and resident/spill accounting from
  `XactBuffer`
- clear `PendingCatalog` speculative slots, the dirty transaction tree,
  unresolved subtransaction mapping, and smgr markers owned by abandoned
  transactions
- reset pending boundary holds
- audit the catalog tracker for uncommitted `pg_class` observations, and
  reseed conservatively from the new source or a caught-up shadow
- advance resume-safe accounting past the discarded uncommitted records

No uncommitted ordinary transaction survives a source promotion. Prepared
transactions do — PostgreSQL can finish them on the new primary — so keep
them an explicit refusal until gxid-keyed buffering
([two_phase_commit.md](two_phase_commit.md)) defines the behavior, and
distinguish `prepared_xact_present` from `open_xact_at_fork`. The
descriptor log receives only committed shapes, so pending slots drop
without durable compensation

### Overwrite contrecord

Replaces the `overwrite_contrecord_at_fork` refusal. Abrupt promotion
after a torn record moves `F` below bytes already received: end of log
becomes `missingContrecPtr`, the descendant's fork page carries
`XLP_FIRST_IS_OVERWRITE_CONTRECORD` (0x0008), and its first record is
`XLOG_OVERWRITE_CONTRECORD`, which a replaying server FATALs on unless its
own reader saw the same aborted contrecord at that LSN. Page-header
parsing accepts flags `0x0007`, so such a page reads as corrupt rather
than as a typed transition. Accept the flag, and reproduce the aborted
contrecord for the shadow so its reader agrees

## Archive paths

- the archive fallback fetches segments at the stream's timeline, so a
  fork inside the range reads as a missing segment. Name each segment from
  the chain the way boot names the floor's — per segment, so the fork
  segment resolves to the descendant and an absence below it is a real gap
  (`tli_of_segment`, [../failover.md](../failover.md) §Lineage). Fetch and
  validate history files from the archive first
- never substitute `<name>.partial`. The ancestor's last partial segment
  is archived under that name and recovery never reads it
  (`CleanupAfterArchiveRecovery`), so neither filtered output nor archive
  lookup may depend on it
- the fork segment's verified prefix becomes durable only when the segment
  seals or the daemon flushes its partial, so an archive-only shadow
  reconnecting inside the fork segment has nothing to read until
  descendant WAL fills it. Make the prefix durable under the descendant
  name at the crossing, and give the wait its own state — parked at `F`
  waiting for a seal, not generic replay lag. `archive_timeout` on the
  source bounds it by forcing a segment switch

## Ancestor-timeline base backup

Every replay that starts from a base backup inherits the backup's
timeline, and that timeline is an ancestor whenever a promotion happened
after the backup. Same crossing, different driver: the fork sits inside a
bounded replay range rather than arriving on a live socket

Where a fixed boot timeline appears:

- object-store bootstrap and add-table backfill take the timeline from the
  backup name (`parse_timeline_from_backup_name`) and walk `SegmentName`
  forward with it (`fetch_gap_segments`), so a fork inside the gap range
  aborts the pass
- direct bootstrap takes `StartInfo.timeline` from `BASE_BACKUP`; a backup
  served by a standby carries that standby's timeline
- gap pre-scan and gap replay each build a `WalStream` for one timeline,
  so both need the crossing primitive the live pump uses
- a shadow seeded from that backup starts at its `pg_control` timeline and
  must cross the same fork during catchup

Rules: resolve the timeline through parsed history, per segment for names
and per LSN for which branch wrote a record; fetch history files before
segments (wal-rus's WAL fetch already routes `*.history`
uncompressed); name the fork segment with the descendant timeline and
treat the absent ancestor name as expected; treat the backup's start and
stop timeline as lineage input rather than identity, rejecting a backup
whose timeline is absent from live history
(`backup_timeline_not_ancestor`); place history files in the filtered dir
before the shadow needs them

Bootstrapping directly from a standby is the interesting subcase: backup,
first WAL, and shadow all start on the ancestor, so a promotion afterwards
makes the crossing mandatory before the first opt-in boundary can pass

## Remaining protocol edges

- `start_physical_replication` requires CopyBoth. PostgreSQL skips COPY
  and answers with the next-timeline result when the requested historic
  timeline already ends at the requested position. Nothing asks for that
  position — boot resolves per segment onto the descendant, and a reconnect
  where the chain ends the branch routes to the crossing instead
  ([../failover.md](../failover.md) §Reconnect) — so it stays a protocol
  error rather than a handled `StartOutcome::TimelineEnd`
- `source_received` is the highest `server_wal_end` seen on the socket,
  bookkeeping that gates nothing and sits above the consumed frontier by
  whatever the source flushed but had not sent. Only its frozen pause copy
  is a decision input
- staged pending-catalog timeline work is orthogonal: its "timeline" means
  relation descriptor versions inside one transaction, not WAL
  `TimeLineID`

## Invariants the unbuilt work must hold

- transition metadata survives until every durable floor passes it, and
  the GC cutoff stays at or below that floor on the same branch
- the descriptor log's covered range never claims a branch history cannot
  place
- the shadow sees the same identifier and the same branch chain, receives
  all filtered WAL through the ancestor fork before any descendant WAL,
  and can recover from filtered archive plus history without a byte gap
- repeated prefix bytes reach the transition verifier only, never the
  filter or decoder twice
- a fork reported behind an already-dispatched record poisons the stream

## Failure policy

Reasons the built crossing does not yet emit: `wal_missing`,
`shadow_transition_failed`, `prepared_xact_present`,
`overwrite_contrecord_at_fork`, `backup_timeline_not_ancestor`. Retry stays
limited to connection and transient storage errors, and a terminal refusal
parks the pump ([../failover.md](../failover.md) §Refusals)

## Observability

`walshadow_timeline_abandoned_xacts_total` and
`walshadow_timeline_abandoned_bytes_total` arrive with the fence. An
archive-only shadow parked at `F` waiting for a seal must read as that,
distinct from the replay lag it looks like today

## Tests

Pure protocol:

- immediate end result without CopyBoth
- reject a missing field, malformed LSN, non-increasing timeline, and
  missing command completion

`WalStream` and transition:

- walk multiple timelines inside one 16 MiB segment, final name using the
  newest branch while every suppressed prefix matches its own
- reject a complete dispatched record beyond the fork

Transaction state, with the fence:

- ordinary uncommitted transaction at the fork drops raw and TOAST state,
  pending descriptor slots, subxact and dirty-tree state, spill accounting
- xid reuse on the descendant cannot see ancestor state
- prepared transaction refuses explicitly

Real PostgreSQL, primary plus streaming standby:

- two switches before walshadow catches up, floor one and two timelines
  behind the live source
- `-m immediate` stop: `open_xact_at_fork` until the fence lands,
  automatic abandonment after
- slot whose `restart_lsn` is above the resume position
- shadow crossing through `restore_command` only, including a reconnect
  inside the unsealed fork segment that parks at `F`, serves no
  `.partial`, and crosses once the segment seals
- source or archive WAL gap rejects without advancing the cursor; foreign
  system identifier rejects
- shadow log free of `end-of-recovery record` PANIC and
  `mismatching overwritten LSN` FATAL throughout

Ancestor-timeline base backup:

- object-store bootstrap whose backup timeline is an ancestor, fork inside
  the gap range, fork segment resolved under the descendant name and no
  `.partial` read
- direct bootstrap served by a standby, promoted before the pump starts
- backup timeline absent from live history rejects
- backup-seeded shadow crosses to the descendant during catchup

## Landing sequence

1. transaction-state abandonment fence, replacing `open_xact_at_fork`
2. overwrite-contrecord acceptance and reproduction for the shadow
3. timeline-aware archive resolution and fork-prefix durability
4. timeline-aware bootstrap and backfill replay
5. promote settled behavior into [../source.md](../source.md),
   [../ops.md](../ops.md), and [../shadow.md](../shadow.md); reduce the
   source-primary section in [risks.md](risks.md) to remaining deployment
   constraints

Keep fail-closed behavior until source, persistence, and shadow pieces
compose for the case being enabled: partial support that advances the
source timeline without moving the durable floor and shadow branch is
unsound

## Acceptance

Full failover is complete when:

- walshadow behind a fork follows the promoted descendant automatically,
  from both a same-connection promotion and an HA-endpoint reconnect
- an unclean promotion abandons uncommitted ancestor state and accepts an
  overwritten contrecord instead of refusing
- restart from an ancestor floor works while the live source is one or
  more timelines ahead
- the shadow follows the identical chain without a catalog replay stall
- a valid descendant preserves manifest, descriptor log, and ClickHouse
  progress, while missing lineage, missing WAL, an unsafe slot, or
  publication past the fork fails before any cursor is adopted
- ancestor-timeline base backups replay across a fork inside their gap
  range
