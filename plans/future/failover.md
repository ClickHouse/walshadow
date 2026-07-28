# Source timeline failover

Keep CDC live when source endpoint moves to a promoted PostgreSQL
primary on a descendant timeline while walshadow still reads an
ancestor timeline. Continue from durable floor, consume remaining
ancestor WAL through fork point, switch timelines, and resume without
`--ignore-cursor`, rebootstrap, or skipped WAL

This plan covers source-consumer continuity. It does not make
walshadow an HA orchestrator or durability witness. WAL relay into a
lagging full PostgreSQL standby remains
[sync_commit_witness.md](sync_commit_witness.md)

## Target scenario

```
old primary, TLI 4                    promoted primary, TLI 5

        WAL through A ────────────────┐
                                     ├── fork at F ── TLI 5 WAL ── H
walshadow durable floor R ───────────┘
                     R < F < H
```

Walshadow reconnects at `R` while `IDENTIFY_SYSTEM` reports timeline
5. Timeline 4 belongs to timeline 5 history. PostgreSQL can serve
`START_REPLICATION ... R TIMELINE 4`, stop at `F`, report timeline 5
and its start position, then serve timeline 5

Support both ways transition becomes visible:

1. Existing connection serves a standby that gets promoted. Walsender
   turns requested timeline historic, reaches fork point, then ends
   COPY with next-timeline result
2. Connection drops and HA endpoint resolves to promoted primary.
   `IDENTIFY_SYSTEM` reports newer timeline before walshadow resumes
   ancestor stream

Walk more than one generation. A consumer on timeline 2 may reconnect
to a primary on timeline 5 and cross `2 → 3 → 5` one reported
transition at a time

## Scope

Automatic continuation requires all of:

- unchanged PostgreSQL system identifier
- stored timeline present in live primary's history
- resume position on that ancestral branch, at or before its fork
- required WAL retained by source slot, `pg_wal`, or configured archive
- promoted primary carrying configured physical slot, when slot mode
  is enabled
- no externally visible walshadow publication from abandoned branch
  beyond fork point

Reject rather than guess when any proof fails

Out of scope:

- leader election, old-primary fencing, DNS or proxy orchestration
- switching to unrelated system identifier
- accepting sibling timeline when stored branch is absent from live
  history
- compensating ClickHouse data already published from an abandoned
  branch beyond fork point
- manufacturing WAL missing from source and archive
- promoting schema-only shadow into application primary
- preserving prepared transactions before
  [two_phase_commit.md](two_phase_commit.md) lands

## Current behavior

Current paths fail closed, but cannot continue:

- `SourceFeed::reconnect` rejects any `IDENTIFY_SYSTEM` timeline
  mismatch before issuing `START_REPLICATION`
- `SourceFeed::next_chunk` maps backend `CopyDone` to `Ok(None)`;
  daemon treats it as terminal shutdown and never reads
  `next_tli` / `next_tli_startpos`
- `start_physical_replication` requires CopyBoth. PostgreSQL may return
  immediate next-timeline result when requested historic timeline
  already ends at requested position
- `SourceRecovery`, `WalStream`, archive lookup, segment naming,
  `ShadowStreamState`, and manifest writer retain boot timeline
- shadow-facing walsender exposes empty timeline history and cannot
  finish one timeline into next
- manifest and descriptor-log identity treat timeline change as
  foreign source; `--ignore-cursor` adopts live timeline by discarding
  continuity
- no transition fence retires uncommitted xid-scoped state abandoned
  at fork

Staged pending-catalog timeline work is orthogonal. Its "timeline"
means relation descriptor versions inside one transaction, not
PostgreSQL WAL `TimeLineID`

## Correctness invariants

### Lineage

System identifier owns artifacts. Timeline identifies selected branch
through those artifacts

- system identifier mismatch stays fatal
- same system identifier is necessary, not sufficient
- live history must prove stored timeline is ancestor
- history entry must prove resume LSN belongs to stored timeline
- every transition must increase timeline ID and match server-reported
  switch LSN
- sibling branch or backward timeline report stays fatal

### Byte and record continuity

- feed bytes to `WalStream` exactly once in increasing branch order
- accept repeated prefix bytes only inside transition verifier, never
  send them through filter or decoder twice
- emit no complete record from abandoned ancestor suffix beyond fork
- require last dispatched record end at or below switch LSN
- discard only partial, undispatched record bytes beyond switch LSN
- poison stream when server reports fork behind dispatched record

PostgreSQL can have `sentPtr > switch_lsn` when promotion catches a
partially received WAL record. Such suffix cannot replay on new branch.
Transition logic must truncate it before accepting descendant bytes

### Durable progress

Timeline associated with durable resume floor, not furthest byte
received. Pipeline may already ingest timeline 5 while emitter floor
still belongs to timeline 4

- persist `(floor_tli, floor_lsn)` atomically
- restart requests `floor_tli` at `floor_lsn`, even when live primary
  reports newer timeline
- advance `floor_tli` only after floor crosses corresponding fork
- retain transition metadata until every durable floor passes it
- keep GC cutoff at or below same floor on same branch

### External publication

Automatic switch is safe only while live branch fork is not behind
externally durable publication

- reject when `emitter_ack_lsn > switch_lsn` on abandoned branch
- audit schema-event acknowledgment against same rule
- reject when descriptor history or other durable side effects beyond
  fork cannot be rolled back without touching ClickHouse
- allow received but undispatched partial bytes past fork to truncate
- allow uncommitted buffered transactions at or before fork to abandon

Target lagging case has resume and publication frontiers below fork,
so no compensation is needed

### Shadow continuity

- shadow sees same system identifier and selected timeline chain
- history file content matches source history
- shadow receives all filtered WAL through ancestor fork before
  descendant WAL
- fork-containing segment has correct descendant timeline identity
- catalog boundary on descendant timeline cannot pass until shadow
  replay follows transition
- shadow reconnect at any point can recover from filtered archive plus
  timeline history without byte gap

## Source replication protocol

### Events

Replace `Option<WalChunk>` end signal with explicit protocol outcome:

```rust
enum SourceEvent<'a> {
    Wal(WalChunk<'a>),
    TimelineEnd {
        finished_tli: u32,
        next_tli: u32,
        switch_lsn: u64,
    },
    Shutdown,
}
```

Distinguish controlled server shutdown from historic-timeline end.
`CopyDone` alone does not make distinction; finish COPY exchange and
parse following response:

- one row with `next_tli`, `next_tli_startpos` means timeline end
- CommandComplete without row means controlled stream termination
- malformed row, missing command completion, or protocol regression
  poisons connection

When backend sends `CopyDone`:

1. send frontend `CopyDone`
2. read RowDescription/DataRow when present
3. validate exactly one row and at least two fields
4. parse `next_tli` and PostgreSQL LSN text
5. consume CommandComplete and ReadyForQuery
6. return `TimelineEnd`

`START_REPLICATION` needs matching start outcome:

```rust
enum StartOutcome {
    Streaming,
    TimelineEnd {
        next_tli: u32,
        switch_lsn: u64,
    },
}
```

Historic request at or after fork may skip CopyBoth and return result
immediately. Feed this through same transition path

### Reconnect

Change reconnect contract from "live timeline must equal requested"
to:

1. connect and run `IDENTIFY_SYSTEM`
2. require system identifier equality with manifest
3. if live timeline equals requested, start normally
4. if live timeline is newer, fetch `TIMELINE_HISTORY <live_tli>`
5. parse history and prove requested timeline ancestry at resume LSN
6. issue `START_REPLICATION` using requested timeline, not live timeline

Let PostgreSQL walsender enforce branch membership again. Client-side
history validation protects persistent artifacts before stream starts;
server-side validation protects each replication request

Persist exact history bytes returned by source. Do not synthesize
history from timeline numbers

### Transition coordinator

Add one owner for mutable source branch state:

```rust
struct TimelineCursor {
    system_id: String,
    stream_tli: u32,
    next_lsn: u64,
    history: TimelineHistory,
}
```

`SourceRecovery` reads cursor instead of storing boot timeline.
Archive lookup, reconnect, status logging, manifest snapshots, and
metrics read same owner

On `TimelineEnd`:

1. stop source ingestion
2. validate `finished_tli == stream_tli`
3. validate `next_tli > finished_tli`
4. validate switch LSN against history and publication frontier
5. establish ordered pipeline transition fence
6. prepare fork segment for descendant bytes
7. install source history and downstream shadow transition
8. change `stream_tli`
9. request descendant timeline
10. resume ingestion after prefix validation

Repeat until stream reaches live timeline

## Fork-segment handling

Always restart descendant timeline from fork segment start, matching
PostgreSQL `pg_receivewal` behavior:

```text
segment_start = align_down(switch_lsn)
```

Do not pass repeated prefix through `WalStream`. Transition adapter:

1. retain current segment prefix already consumed on ancestor
2. truncate undispatched ancestor suffix after switch LSN
3. request descendant from `segment_start`
4. compare repeated `[segment_start, switch_lsn)` bytes against retained
   prefix
5. fail on any mismatch
6. suppress matching prefix
7. feed bytes from `switch_lsn` onward through normal `WalStream::push`

This provides three properties:

- detects corrupt or sibling branch despite matching system ID
- avoids double classification and duplicate xid-buffer mutations
- builds complete descendant fork segment using verified common prefix

`WalStream` needs `transition_timeline(next_tli, switch_lsn)`:

- require `dispatched_lsn <= switch_lsn <= next_lsn`
- truncate only walker state beyond switch LSN
- clear partial-record continuation state
- keep filter state established by complete records before fork
- change segment identity before fork segment seals
- preserve cumulative metrics

Segments fully before fork retain ancestor filename. Fork-containing
segment seals under descendant timeline because prefix is byte-equal
and suffix belongs to descendant. Persist source history beside
filtered archive so PostgreSQL recovery can select it

Test multiple switches inside one 16 MiB segment. Final segment name
must use newest selected timeline while each suppressed prefix matches
every intermediate branch

## Transaction-state fence

Timeline transition is ordered record-stream control, not direct
mutation racing decoder worker. Add control item after all complete
ancestor records and wait for worker acknowledgment

At fence:

- finish every commit or abort record dispatched before switch
- abandon unresolved ordinary transactions from ancestor
- remove their raw heap records, toast chunks, and resident/spill
  accounting from `XactBuffer`
- clear `PendingCatalog` speculative slots
- clear dirty transaction tree and unresolved subtransaction mapping
- clear smgr markers owned by abandoned transactions
- reset pending boundary holds
- audit catalog tracker for uncommitted pg_class observations; reseed
  conservatively from new source or caught-up shadow
- advance resume-safe accounting past discarded uncommitted records

No uncommitted ordinary transaction survives source promotion. Prepared
transactions differ, PostgreSQL can preserve and later finish them on
new primary. Keep failover unsupported when prepared state is present
until gxid-keyed buffering from
[two_phase_commit.md](two_phase_commit.md) defines transition behavior

Descriptor log receives only committed shapes, so pending slots drop
without durable compensation. If any durable batch lies beyond reported
fork, reject automatic switch

## Manifest and descriptor history

### Manifest

Bump schema. Separate ownership from branch cursor:

```toml
[source]
system_id = 7334001234567890123

[wal]
floor_timeline = 4
stream_timeline = 5

[lsn]
floor = "0/6A000000"
source_received = "0/6B123456"
```

Exact field layout may keep current top-level `floor`; invariant is
atomic pairing of floor LSN and floor timeline

Store discovered timeline transitions or history-file references so
restart and archive recovery can resolve `timeline_at(floor)`. Keep
`stream_timeline` diagnostic; never use it in place of
`floor_timeline`

Boot:

1. load system ID, floor timeline, floor LSN, and known history
2. identify live source
3. reject system ID mismatch
4. fetch live history when timeline differs
5. prove floor timeline ancestry and floor membership
6. start at persisted floor timeline and walk forward

Remove `--ignore-cursor` requirement for valid descendant. Preserve
flag as explicit rebaseline for unprovable or intentionally discarded
state

### Descriptor log

Timeline cannot remain scalar foreign-source identity. Bind log to:

- PostgreSQL major
- system identifier
- database OID
- WAL segment size
- selected lineage through each covered LSN

Record timeline transitions durably in descriptor-log metadata or
shared manifest history. On open, prove every durable descriptor batch
belongs to live branch. Reject log whose covered head extends beyond
fork onto sibling branch

Migration from existing format can treat header timeline as lineage
origin. Accept newer live timeline only when source history contains
origin through log's covered range

## Shadow-facing replication

Extend wal-rus server surface from single timeline to supplied history:

- dynamic `IDENTIFY_SYSTEM` current timeline
- exact `TIMELINE_HISTORY <tli>` filename and bytes
- per-connection requested timeline
- historic stream cutoff
- server `CopyDone`
- next-timeline result after client `CopyDone`
- immediate result when requested position already reaches fork

Source transition coordinator publishes history before advertising new
timeline. Existing shadow connection finishes ancestor stream at fork,
then follows descendant using normal PostgreSQL walreceiver behavior

Keep filtered bytes available for lagging shadow:

- sealed ancestor segments before fork
- descendant fork segment with verified common prefix
- descendant segments after fork
- history files required to select them

Disconnect fallback may remain for broken clients, but normal path must
complete PostgreSQL timeline protocol. Blind socket close risks
walreceiver repeatedly requesting old timeline without learning next
timeline

Boundary gate remains final safety check. Do not capture descendant
catalog state until shadow reports replay on descendant branch through
boundary LSN

## Slot and archive requirements

Slot mode:

- require configured slot on promoted primary
- verify slot type is physical
- verify slot can serve stored floor, including ancestor history
- prefer PG 17 failover slots synchronized to standby
- support pre-PG-17 operator-created slot only with explicit position
  validation
- never create missing failover slot at current head and call
  continuation successful

Slotless mode:

- request ancestor WAL from promoted primary while retained
- fall back to archive by timeline-aware segment name
- fetch and validate history files from archive
- retain current missing-WAL failure when neither source nor archive
  covers floor

Archive recovery must advance timeline rather than treating missing next
ancestor segment as generic archive exhaustion. Resolve absence against
known switchpoint:

- before switchpoint, missing segment is real gap
- at switchpoint, select descendant fork segment
- after switchpoint, fetch descendant timeline

## Failure policy

Expose typed terminal reasons:

- `system_id_changed`
- `timeline_not_descendant`
- `resume_past_fork`
- `published_past_fork`
- `history_missing`
- `history_malformed`
- `fork_prefix_mismatch`
- `slot_missing`
- `slot_too_new`
- `wal_missing`
- `shadow_transition_failed`
- `prepared_xact_present`

Keep retry only for connection and transient storage errors. Lineage,
prefix, publication, and slot-position failures require operator action

Never fall back from failed lineage proof to live head automatically

## Observability

Add:

- `walshadow_source_timeline`
- `walshadow_floor_timeline`
- `walshadow_timeline_switches_total`
- `walshadow_timeline_switch_failures_total{reason}`
- `walshadow_timeline_transition_seconds`
- `walshadow_timeline_switch_lsn` diagnostic gauge or structured log
- `walshadow_timeline_prefix_bytes_verified_total`
- `walshadow_timeline_abandoned_xacts_total`
- `walshadow_timeline_abandoned_bytes_total`
- shadow timeline and replay timeline in status snapshot

Log system ID, old timeline, new timeline, switch LSN, resume LSN,
publication frontier, slot, and history source at every transition.
Never log credentials or full connection strings

## Tests

### Pure protocol

- parse historic end row after backend `CopyDone`
- distinguish controlled shutdown without row
- handle immediate end result without CopyBoth
- reject missing field, malformed LSN, non-increasing timeline, and
  missing CommandComplete
- parse PostgreSQL history with comments, multiple ancestors, and
  non-consecutive timeline IDs
- prove ancestor membership at positions before, at, and after fork

### WalStream

- switch at segment boundary
- switch mid-segment
- discard ancestor partial record beyond fork
- reject complete dispatched record beyond fork
- verify repeated descendant prefix and suppress redispatch
- reject one-byte prefix mismatch
- walk multiple timelines inside same segment
- seal fork segment under descendant name
- preserve filter statistics and catalog tracker state from committed
  prefix

### Transaction state

- committed transaction before fork drains normally
- ordinary uncommitted transaction at fork drops raw and toast state
- pending descriptor slots drop
- subxact and dirty-tree state clear
- spill accounting returns to zero
- xid reuse on descendant cannot see ancestor state
- prepared transaction causes explicit unsupported failure

### Persistence

Crash and restart at each edge:

1. before ancestor CopyDone
2. after next-timeline result, before transition metadata write
3. after history persistence, before descendant request
4. while verifying fork prefix
5. after source stream changes timeline while durable floor remains
   ancestor
6. after floor crosses fork
7. after shadow changes timeline, before next manifest cadence

Every restart resumes `(floor_tli, floor_lsn)` and reaches same output

### Real PostgreSQL

Build primary plus streaming standby fixture:

- pause walshadow behind planned promotion point
- promote standby and redirect source endpoint
- verify old timeline drains to fork, new timeline continues, and
  ClickHouse reaches expected rows
- promote server serving existing connection, verify CopyDone result
  path without endpoint change
- switch twice before walshadow catches up
- force fork in middle of WAL segment
- leave ordinary transaction open across promotion
- restart walshadow before and after fork
- verify shadow recovery reaches descendant timeline and catalog DDL
  after promotion applies
- run with synchronized failover slot
- verify missing or too-new slot fails with typed reason
- verify same-system sibling timeline rejects
- verify unrelated system identifier rejects
- verify source or archive WAL gap rejects without cursor advance

Assert:

- no manual `--ignore-cursor`
- no rebootstrap
- no duplicate decoder-side state mutation
- no committed source row skipped
- no abandoned transaction emitted
- manifest floor and timeline pair stay crash-safe
- descriptor log remains usable after restart
- filtered segment names and history files match PostgreSQL recovery
  expectations

## Landing sequence

1. Add history parser and source protocol outcomes, keep current
   timeline mismatch rejection
2. Add lineage-aware manifest and descriptor-log migration
3. Add fork-prefix verifier and `WalStream` transition primitive
4. Add ordered transaction-state abandonment fence
5. Make source recovery and archive lookup timeline-aware
6. Add shadow-facing history and end-of-timeline protocol
7. Wire coordinator, metrics, and typed failures
8. Enable automatic descendant continuation after real-PG crash matrix
   passes
9. Promote behavior into `plans/source.md`, `plans/ops.md`, and
   `plans/shadow.md`; reduce source-primary section in `risks.md` to
   remaining deployment constraints

Keep existing fail-closed behavior until all source, persistence, and
shadow pieces compose. Partial support that advances source timeline
without moving durable floor and shadow branch is unsound

## Acceptance

Feature is complete when:

- walshadow behind fork follows promoted descendant automatically
- same-connection promotion and HA-endpoint reconnect both work
- restart from ancestor floor after live source advances works
- shadow follows identical timeline chain without catalog replay stall
- valid descendant preserves manifest, descriptor log, and CH progress
- missing lineage, missing WAL, unsafe slot, or publication past fork
  fails before cursor adoption
- full test matrix covers segment, transaction, crash, slot, and
  multiple-timeline boundaries
