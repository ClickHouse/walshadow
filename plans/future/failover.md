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

## Planned switchover

Operator-driven promotion is the primary supported path, and the only
one whose safety does not rest on how the promotion happened to go.
Operator owns the ordering, so walshadow's consumed frontier is held
below the fork before the fork exists:

```text
old primary   writes ──────────────────► stop
                    R     C  P
target        replay ────────────────────► P ── promote ──► F ── TLI 5 ──►
walshadow     consume ───► pause at C ········ resume ─► drain C→F ─► TLI 5

              R = durable floor, C = frozen consumed frontier,
              P = frozen received frontier (source head), F = fork
              R <= C <= P <= F, arranged before promotion, proved after
```

`C` and `P` answer different questions and neither substitutes for the
other. `C` is where resume asks the promoted target to start. `P` is the
source head walshadow last heard about, which the target must reach before
promotion. Freezing only one of them either understates what the target
owes or misstates the resume position

Protocol:

1. pre-create the physical slot on the promotion target, verify its
   `restart_lsn` is at or below walshadow's floor
2. `ctl apply` `[stream] paused = true`, read back both frozen pause LSNs
3. stop writes on the old primary (demote, or `pg_ctl stop -m fast`)
4. wait until target replay reaches `pause_received_lsn`, until target
   receive equals target replay, and until replay covers the old primary's
   final durable record
5. point walshadow's source at the target, then promote it. A stable
   address (VIP, proxy, DNS) makes this one step the HA layer already
   owns; a direct address is `ctl apply` `[source] host = …`, adopted
   live while paused. Repoint before promoting either way, so the target
   is still on the ancestor timeline when walshadow dials it
6. `ctl apply` `[stream] paused = false`

Steps 1, 2, and 4 carry the proof obligations. Rest is orchestration
walshadow does not participate in

Step 5's repoint costs no restart and no re-read: the pump swaps its
`SourceFeed` at `WalStream::next_lsn` ([control.md](../control.md)
§Source endpoint move). Its identity gate demands the manifest's
`system_id` *and* timeline, which is why the repoint goes before the
promotion — after it, the target reports timeline 5 and the swap refuses,
leaving the crossing to the transition coordinator. Repointing first also
collapses the two transition shapes into one: walshadow is holding a
connection to the server that gets promoted, so the walsender ends the
ancestor stream with the next-timeline result

### What the pause buys

Pause stops consumption, not production. Fork `F` is created in step 5,
after the frontier freeze, so `received <= F` is arranged rather than
hoped for:

- `drain <= consumed <= received <= F`, so no ClickHouse row comes from
  WAL the descendant branch never had, and `published_past_fork` cannot
  fire
- no complete ancestor record beyond `F`, so no dispatched-record
  truncation and no orphan ancestor suffix
- target replayed through `F`, so its `nextXid` is above every xid
  walshadow observed; the descendant cannot reuse an xid still keyed in
  `XactBuffer`
- clean shutdown leaves no partial record at `F`, so the descendant
  carries no `XLOG_OVERWRITE_CONTRECORD`

Unpaused consumption inverts the first point: a target promoted while
behind walshadow leaves rows in ClickHouse from a branch that no longer
exists, with no automatic path to remove them. Pause bounds that
exposure to `C`; step 4 removes it

### Frontier proof before promotion

Pause bounds `received`, it does not prove the target reached it. Prove
it in step 4, while rollback is still free:

- `pause_received_lsn` from `ctl status` or `walshadow_pause_received_lsn`
- target `pg_last_wal_replay_lsn()` at or above it
- target `pg_last_wal_receive_lsn()` equal to that replay LSN, so
  nothing received sits unapplied

That much is walshadow's own obligation, and all of it: `consumed <= F`
is what keeps ClickHouse free of rows from a branch the promoted primary
never had. Whether the old primary's last writes survive is the
switchover's obligation, not walshadow's — WAL lost above `F` was never
consumed, so ClickHouse still converges on the promoted primary

Prove that half too, because a switchover wants it anyway, and the check
differs by how writes stopped:

- demoted: old primary `pg_current_wal_flush_lsn()` equal to target replay
- cleanly stopped: `pg_controldata` on the stopped primary's data dir
  reports the shutdown checkpoint as `Latest checkpoint location`; require
  target replay at or past it. The primary is down, so nothing races the
  read

PostgreSQL already narrows this window without being asked. Fast shutdown
writes the shutdown checkpoint, *then* wakes walsenders to drain, and the
postmaster waits for them to exit
(`src/backend/postmaster/postmaster.c`); `WalSndDone` exits only once the
standby has confirmed flush of everything sent
(`src/backend/replication/walsender.c`). So a completed `pg_ctl stop -m
fast` with the target streaming implies the target holds the whole tail.
The check catches what that argument does not cover: a target not
streaming at stop time, `wal_sender_timeout` firing mid-drain, or
`pg_ctl` giving up

With those, `C <= P <= F` holds before timeline 5 exists. Post-promotion
lineage verification then confirms rather than gambles, and still fails
closed on its own terms

### Resume in place, whether or not the address moved

All three shapes resume from memory. Only the last one is a restart, and
nothing forces it:

**Same endpoint, live resume.** `paused = false` re-enables the
`feed.next_chunk` arm. The socket to the stopped primary is dead, so the
arm errors and `SourceRecovery::recover` runs: reconnect,
`IDENTIFY_SYSTEM` reports timeline 5, prove timeline 4 ancestry at
`WalStream::next_lsn`, request timeline 4, drain `[C, F]`, take the
end-of-timeline result, cross, continue. `WalStream` keeps filter and
`CatalogTracker` state in memory, so no WAL is re-read and no catalog
reseed happens. This is the cheap path and the one to optimize for

**Endpoint change, live resume.** `[source]` reloads live
([control.md](../control.md) §Source endpoint move), so step 5's repoint
is a `ctl apply`, not a restart. The pump swaps its `SourceFeed` between
chunks at `WalStream::next_lsn`: nothing re-read, nothing reseeded, and
`pause_consumed_lsn` is exactly what the new address is asked to serve.
Taken while the target is still a standby, this lands walshadow on the
first shape above — the connection it holds is the one that gets
promoted, so the crossing arrives as a next-timeline result rather than a
reconnect

The swap is not the mechanism that crosses the fork, and cannot be: its
gate is `system_id` plus timeline equality, which a promoted primary
fails by construction. An endpoint applied after promotion sits unadopted
(`source_swap_pending`) until the transition coordinator, which owns the
timeline change, takes it

**Restart.** Still supported, and unchanged: boot resumes at
`(floor_tli, floor_lsn)` and re-reads `[R, F)` on the ancestor before
crossing. Re-emitted rows dedupe on `_lsn`, so the cost is throughput and
a wider ancestor-WAL retention requirement, not correctness. This is the
fallback when the daemon died anyway, not the price of moving an address

A stable endpoint (VIP, proxy, DNS) still keeps the switchover shortest —
one fewer step, and no window where config names an address the pump has
not adopted (`walshadow_source_endpoint_swap_pending`)

### Fence at the fork is empty under clean shutdown

Ancestor WAL between `P` and `F` resolves every transaction that was
open at pause: `pg_ctl stop -m fast` rolls back live sessions, and each
xid-assigned transaction writes its abort record before the shutdown
checkpoint. So the transaction-state fence has nothing to abandon, and
outstanding boundary holds at `F` belong to committed transactions and
release once shadow crosses

Treat that as a checked condition, not an assumption: unresolved
ordinary transactions present when the ancestor stream ends at `F` mean
the shutdown was not clean (`-m immediate`, crash, or a promotion that
raced live writes). Until the fence lands, that is a typed terminal
failure rather than silent abandonment

### Rollback

Nothing is adopted before step 6, so the switchover has a free abort path
at every step up to resume. Rollback is semantic, not byte identity: pause
stops consumption, not the pipeline, so floor, `emitter_ack`, descriptor
batches, manifest cadence, and shadow replay keep advancing behind the
frozen frontier. What an aborted promotion guarantees is that no descendant
artifact exists and no cursor passed bytes already consumed on the
ancestor:

- `stream_timeline` unchanged, no history bytes in the filtered dir under a
  descendant name
- no fork segment sealed under a descendant name
- no descriptor-log entry claiming descendant lineage
- every artifact still valid for ancestor resume

### Operator-visible state

`ctl status` reports enough for steps 4 and 5 without a second tool:

- `pause_consumed_lsn` — `WalStream::next_lsn` frozen when the pump
  observed the pause. Resume asks the source for exactly this position
- `pause_received_lsn` — `source_received` frozen at the same instant, so
  the promotion gate reads the source head walshadow last heard about.
  A live value cannot be compared against a promotion decision
- `source_timeline`, `floor_timeline`, `floor`
- `source_received`, `drain`, `emitter_ack`, `shadow_replay`
- `source_swap_pending` — config names an endpoint the pump has not
  adopted. Promoting while this is true promotes out from under a
  connection walshadow still holds to the old address

Keep `[stream] paused` as the only control input. A dedicated switchover
verb adds a state machine whose only job is what pause plus the checks
above already do

## Scope

Automatic continuation requires all of:

- unchanged PostgreSQL system identifier
- stored timeline present in live primary's history
- resume position on that ancestral branch, at or before its fork
- required WAL retained by source slot, `pg_wal`, or configured archive
- promoted primary carrying configured physical slot, when slot mode
  is enabled
- no walshadow work dispatched toward ClickHouse from abandoned branch
  beyond fork point, measured by `drain` rather than `emitter_ack`

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
- a controlled shutdown's bare CommandComplete falls through
  `next_chunk`'s unrecognized-message arm, so the stop surfaces as the
  EOF behind it: a connection error `SourceRecovery::recover` retries
- `start_physical_replication` requires CopyBoth. PostgreSQL may return
  immediate next-timeline result when requested historic timeline
  already ends at requested position
- `SourceRecovery`, `WalStream`, archive lookup, segment naming,
  `ShadowStreamState`, and manifest writer retain boot timeline
- page-header parsing accepts flag bits `0x0007`, so a descendant fork
  page carrying `XLP_FIRST_IS_OVERWRITE_CONTRECORD` reads as a corrupt
  header instead of a typed unsupported transition
- `[stream] paused` freezes consumption but publishes no frozen frontier
  LSN, so an operator cannot compare the pause point against target
  replay without reading metrics scraped at an unrelated instant
- `source_received` is the highest `server_wal_end` seen on the socket,
  documented as bookkeeping that gates nothing, and sits above the
  consumed frontier by whatever the source had flushed but not sent
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

`emitter_ack` cannot carry this proof. It is the contiguous-done
watermark, and the pipeline completes out of order by design
(`src/emit/pipeline/ack.rs`): a later sequence can be drained into
ClickHouse while an earlier stalled one pins the watermark below it. An
`emitter_ack <= switch_lsn` that passes says nothing about rows from a
higher commit LSN already sitting on the abandoned branch

Use `drain` instead — highest commit LSN drained out of `XactBuffer`,
so above every commit whose rows could have reached ClickHouse. It
over-rejects, by the width of the in-flight window, and needs no state
the manifest does not already carry. Fail closed is the right bias for a
path with no compensation behind it

- reject when `drain_lsn > switch_lsn` on abandoned branch
- audit schema-event acknowledgment against same rule; a DDL applied to
  ClickHouse is as durable as a row
- reject when descriptor history or other durable side effects beyond
  fork cannot be rolled back without touching ClickHouse
- allow received but undispatched partial bytes past fork to truncate
- allow uncommitted buffered transactions at or before fork to abandon

Target lagging case has resume and publication frontiers below fork,
so no compensation is needed. Planned switchover never reaches this
check with anything to reject: pause freezes the frontier before `F`
exists, so nothing past the fork was consumed, let alone dispatched

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

## PostgreSQL mechanics

Facts the design leans on, with the PostgreSQL sources that fix them:

- promotion writes a lightweight `XLOG_END_OF_RECOVERY` at `F`, not a
  checkpoint. Its redo PANICs when the replaying server has not already
  switched to the record's timeline
  (`src/backend/access/transam/xlog.c`). A shadow fed descendant bytes
  down an ancestor stream dies there, so the shadow-facing end-of-
  timeline protocol is mandatory, not politeness
- a mid-segment fork copies the ancestor segment up to
  `XLogSegmentOffset(F)` into the descendant-named file and zero-fills
  the rest (`XLogInitNewTimeline`). The copy is raw, so page headers in
  the prefix keep the ancestor `xlp_tli`
- recovery accepts those pages: the check is
  `tliInHistory(latestPageTLI, expectedTLEs)`
  (`src/backend/access/transam/xlogrecovery.c`), membership in history,
  not equality with the file's timeline. So keep prefix bytes verbatim;
  rewriting `xlp_tli` is both unnecessary and wrong
- a range inside the fork segment resolves to the descendant filename.
  `XLogFileReadAnyTLI` skips a timeline only when the segment precedes
  its `begin` segment, and walsender's segment open sets the timeline to
  `sendTimeLineNextTLI` when the requested segment is the fork segment
  (`src/backend/replication/walsender.c`). One sealed fork segment under
  the descendant name serves both sides of `F`
- the ancestor's last partial segment is archived as `<name>.partial`
  and never read by recovery (`CleanupAfterArchiveRecovery`). Neither
  filtered output nor archive lookup may depend on it
- `START_REPLICATION ... TIMELINE <historic>` rejects only
  `switchpoint < startpoint`, deliberately allowing a start at the fork
  segment's beginning. End of timeline returns `next_tli` and
  `next_tli_startpos`, where the position is the switchpoint itself
- history files are lines of `<tli>\t<hi>/<lo>\t<reason>` with `#`
  comments and strictly increasing timeline IDs; each entry's `begin` is
  the previous line's switchpoint, and the target timeline's own entry
  has an open end. Timeline 1 has no history file
  (`src/backend/access/transam/timeline.c`)
- physical slots are not synchronized to standbys. PG 17 slot sync
  covers logical slots created with `failover = true`
  (`src/backend/replication/logical/slotsync.c`), so a physical slot on
  a promotion target exists only if an operator created it
- `pg_create_physical_replication_slot(name, true)` reserves at the last
  checkpoint or restartpoint redo, not the current position
  (`ReplicationSlotReserveWal`, `src/backend/replication/slot.c`), which
  is why a slot pre-created on the target sits behind its replay LSN
- abrupt promotion after a torn record moves `F` below bytes already
  received: end of log becomes `missingContrecPtr`, the descendant's
  fork page carries `XLP_FIRST_IS_OVERWRITE_CONTRECORD` (0x0008), and
  its first record is `XLOG_OVERWRITE_CONTRECORD`, which a replaying
  server FATALs on unless its own reader saw the same aborted contrecord
  at that LSN. Planned switchover with a clean shutdown never produces
  this; the general path must handle it or fail with a typed reason

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

Three ways a physical stream ends. Backend `CopyDone` separates them:
only historic-timeline end sends one.

**Historic-timeline end.** `XLogSendPhysical` sends backend `CopyDone` at
the switchpoint, then the loop waits for the frontend's before
`StartReplication` writes its result
(`src/backend/replication/walsender.c`):

1. send frontend `CopyDone`
2. read RowDescription/DataRow when present
3. validate exactly one row and at least two fields
4. parse `next_tli` and PostgreSQL LSN text
5. consume CommandComplete and ReadyForQuery
6. return `TimelineEnd`

**Controlled server shutdown.** `WalSndDone` sends CommandComplete
directly, still inside COPY, flushes, and calls `proc_exit`. No backend
`CopyDone` precedes it and no ReadyForQuery follows it: CommandComplete
then EOF. A parser that enters result mode only on backend `CopyDone`
reads this as an unexpected streaming message, then blocks until the
socket closes

**Client-initiated stop.** Frontend `CopyDone` first. Backend answers with
its own, then CommandComplete and ReadyForQuery, carrying a next-timeline
row only when the request was historic

Malformed row, missing command completion, or protocol regression poisons
connection

`Shutdown` is a reconnect, not a daemon exit. The same-endpoint switchover
shape ends its ancestor connection exactly this way — the old primary
stops while walshadow is paused — and resume must dial the promoted
target, not terminate. `SourceRecovery::recover` owns it, same as a
dropped socket. Reserve termination for operator intent

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
6. request descendant timeline from the fork segment's start
7. verify repeated prefix against retained ancestor bytes
8. make the fork segment's verified prefix durable under the descendant
   name, with the history bytes fsynced beside it
9. change `stream_tli` and advertise the transition downstream
10. resume ingestion

Repeat until stream reaches live timeline

Advertisement is last on purpose. Everything before step 9 is reversible
inside walshadow; a `fork_prefix_mismatch`, a missing segment, or a source
error there ends as a typed failure with the shadow still parked on the
ancestor. Advertise before verifying and the shadow learns about a
transition walshadow may then reject, with no way to unsay it. Step 9 is
also what the shadow handoff order requires: history bytes and fork
segment durable before the ancestor stream is allowed to end

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

Seal the fork segment once, and resolve reads of it to that one name:
walshadow's own archive lookup and the shadow-facing walsender both map
an ancestor-timeline request whose segment is the fork segment onto the
descendant file, matching PostgreSQL's own rule. Copy the prefix bytes
verbatim, including ancestor `xlp_tli` in page headers, because recovery
validates page timelines against history membership

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

Handoff order is load-bearing:

1. write source history bytes into the filtered dir as
   `<descendant>.history`, fsync
2. make the fork segment's verified prefix durable under the descendant
   name, as the in-progress `.partial` the sink already writes
3. end the shadow's ancestor stream at `F` with the next-timeline result
4. serve `TIMELINE_HISTORY <descendant>` from the same bytes
5. serve descendant bytes: live tail off the stream, sealed segments off
   disk

History before stream end, because a shadow that reads its history file
too early assumes a parentless timeline and then cannot find any
ancestor-named segment. Retention must never trim `*.history` from the
filtered dir; `recovery_target_timeline = 'latest'` discovery and every
later restart probe them through `restore_command`

Step 2 makes the prefix durable, not servable. A segment becomes servable
when it seals at its boundary, and the fork segment cannot seal until
descendant WAL fills it, which is after the crossing. So the two shadow
paths cross the fork differently:

**Live tail.** The connected shadow crosses on the stream. It takes the
next-timeline result at `F` and follows descendant bytes as they arrive,
never touching the unsealed fork segment. This is the normal path and the
one to keep working

**Archive only.** A shadow crossing through `restore_command` waits for
the fork segment to seal, because a `.partial` must never be served —
recovery would read the zero tail as a valid page. The wait lasts until
the promoted primary writes past the fork segment's boundary, so an idle
primary stalls a reconnecting shadow at `F` for as long as it stays idle.
`archive_timeout` on the source bounds it by forcing a segment switch.
Supported, and worth the typed wait state rather than a silent stall

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
- require an operator-created slot on the promotion target, under the
  same name as `[source] slot`, so `ensure_physical_slot` adopts it
  instead of creating one; no PostgreSQL version synchronizes physical
  slots to a standby
- verify the target slot's `restart_lsn` is at or below the floor; a slot
  reserved at the target's last restartpoint redo usually is, but a
  lagging consumer inverts it
- never create a missing slot at current head and call continuation
  successful

`max_slot_wal_keep_size` on the target can invalidate a pre-created slot
during a long pause, so the pause window lives inside that budget or the
run is slotless with an archive. After switchover the slot left on the
old primary pins WAL on a node that may rejoin as a standby; dropping it
is part of the switchover, not cleanup that can wait

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

## Ancestor-timeline base backup

Every WAL replay that starts from a base backup inherits the backup's
timeline, and that timeline is an ancestor whenever a promotion happened
after the backup. Same crossing, different driver: the fork sits inside
a bounded replay range rather than arriving on a live socket

Where a fixed boot timeline appears today:

- object-store bootstrap and add-table backfill take the timeline from
  the backup name (`parse_timeline_from_backup_name`) and walk
  `SegmentName` forward with it (`fetch_gap_segments`), so a fork inside
  `[B_redo, S]` surfaces as a missing segment and aborts the pass
- direct bootstrap takes `StartInfo.timeline` from `BASE_BACKUP`; a
  backup served by a standby carries that standby's timeline
- gap pre-scan and gap replay each build a `WalStream` for one timeline,
  so both need the same transition primitive as the live pump
- `SourceRecovery`'s archive fallback fetches segments at the boot
  timeline
- a shadow seeded from that backup starts at its `pg_control` timeline
  and must cross the same fork during catchup

Rules:

- resolve the timeline per LSN through parsed history, mirroring
  `tliOfPointInHistory`, never once per run
- fetch history files before segments, from the archive (wal-rus's WAL
  fetch already routes `*.history` uncompressed) or from a live source
  with `TIMELINE_HISTORY`
- name the fork segment with the descendant timeline, and treat the
  absent ancestor name for that segment as expected rather than a gap
- never substitute `<name>.partial`; recovery ignores those files, so
  replay must too
- treat the backup's start and stop timeline as lineage input, not
  identity: reject a backup whose timeline is absent from live history,
  accept an ancestor
- place history files in the filtered dir before the shadow needs them,
  so a backup-seeded shadow can select the descendant timeline

Bootstrapping directly from a standby is the interesting subcase: the
backup, the pump's first WAL, and the shadow all start on the ancestor,
and a promotion any time afterwards makes the crossing mandatory before
the first opt-in boundary can pass

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
- `open_xact_at_fork` — unresolved ordinary transactions when the
  ancestor stream ends, so the promotion was not a clean switchover.
  Becomes automatic abandonment once the fence lands
- `overwrite_contrecord_at_fork` — descendant begins by overwriting an
  aborted contrecord, so the fork sits below received bytes
- `backup_timeline_not_ancestor` — base backup's timeline is absent from
  live history

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
- `walshadow_pause_consumed_lsn` and `walshadow_pause_received_lsn`, both
  frozen when the pump observes the pause
- shadow timeline and replay timeline in status snapshot
- an archive-only shadow parked at `F` waiting for the fork segment to
  seal reads as that, not as generic replay lag

Log system ID, old timeline, new timeline, switch LSN, resume LSN, `drain`
as the publication ceiling, slot, and history source at every transition.
Never log credentials or full connection strings

## Tests

### Pure protocol

- parse historic end row after backend `CopyDone`
- classify a bare CommandComplete then EOF, with no backend `CopyDone` and
  no ReadyForQuery, as controlled shutdown, and route it to reconnect
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

### Planned switchover

Same primary-plus-standby fixture, driven through the six-step protocol:

- full run on a stable endpoint: pause, `-m fast` stop, target replay
  reaches `pause_received_lsn` and the stopped primary's shutdown
  checkpoint, promote, resume; ClickHouse matches source and no operator
  flag is needed
- both pause LSNs stay put while the pipeline keeps draining;
  `pause_consumed_lsn` equals the resume request LSN, and
  `pause_received_lsn` sits at or above it with a gap whenever the source
  had flushed WAL it had not yet sent
- transaction open at pause: its abort record arrives before `F`, the
  fence abandons nothing
- `-m immediate` stop: `open_xact_at_fork` until the fence lands,
  automatic abandonment after
- target promoted while behind the consumed frontier: `published_past_fork`,
  cursor unchanged, ClickHouse untouched
- same rejection with `emitter_ack` below the fork and `drain` above it,
  from a stalled early sequence behind a completed later one, so the
  ceiling is proved to be `drain`
- promotion aborted after pause: resume continues on the ancestor, frozen
  frontier unchanged and no descendant artifact written, while the
  manifest's floor and ack fields advance from draining
- endpoint change applied live between pause and resume (`ctl apply`
  `[source] host`), the swap taken before promotion and the crossing
  driven by the walsender's next-timeline result on that same connection
- endpoint change with a daemon restart between pause and resume, floor
  an ancestor while the live source is one and two timelines ahead
- endpoint applied after promotion: the swap's timeline gate refuses,
  `source_swap_pending` stays set, and the stream keeps draining the
  ancestor rather than half-adopting the descendant
- shadow crossing with the walsender stopped, `restore_command` only,
  covering history discovery and fork-segment selection. Reconnect inside
  the unsealed fork segment: the shadow parks at `F`, no `.partial` is
  served, and it crosses once descendant WAL seals the segment
- live-tail crossing over the same fork while the fork segment is still
  unsealed, so the connected shadow never depends on the seal
- shadow log free of `end-of-recovery record` PANIC and
  `mismatching overwritten LSN` FATAL across every case
- old primary's slot dropped, target slot adopted rather than recreated

### Ancestor-timeline base backup

- object-store bootstrap whose backup timeline is an ancestor of the
  opt-in boundary, fork inside the gap range
- gap fetch resolves the fork segment under the descendant name and does
  not read `.partial`
- direct bootstrap served by a standby, promotion before the pump starts
- backup timeline absent from live history rejects
- backup-seeded shadow crosses to the descendant during catchup

## Landing sequence

Switchover is the shippable subset: it needs no fence and no
contrecord handling, because a clean shutdown resolves every
transaction and leaves no torn record. Land it first, with every
condition it relies on checked rather than assumed.

1. Add history parser and source protocol outcomes, keep current
   timeline mismatch rejection
2. Add lineage-aware manifest and descriptor-log migration
3. Add fork-prefix verifier and `WalStream` transition primitive
4. Make source recovery and archive lookup timeline-aware
5. Add shadow-facing history and end-of-timeline protocol
6. Freeze and publish both pause LSNs, add the switchover status fields
7. Wire coordinator, metrics, and typed failures, with
   `open_xact_at_fork` and `overwrite_contrecord_at_fork` terminal
8. Enable automatic descendant continuation after the switchover matrix
   passes on real PostgreSQL

Unplanned failover then converts the two terminal reasons into working
paths:

9. Add ordered transaction-state abandonment fence, replacing
   `open_xact_at_fork`
10. Accept the overwrite-contrecord page flag and reproduce the aborted
    contrecord for the shadow, replacing `overwrite_contrecord_at_fork`
11. Make bootstrap and backfill replay timeline-aware for
    ancestor-timeline backups
12. Promote behavior into `plans/source.md`, `plans/ops.md`, and
    `plans/shadow.md`; reduce source-primary section in `risks.md` to
    remaining deployment constraints

Keep existing fail-closed behavior until source, persistence, and shadow
pieces compose for the case being enabled. Partial support that advances
source timeline without moving durable floor and shadow branch is
unsound

## Acceptance

Switchover is complete when:

- the six-step protocol runs with no operator flag beyond
  `[stream] paused`, on a stable endpoint and across an endpoint change,
  neither shape needing a restart
- both pause LSNs are readable and frozen, so the promotion decision is
  checkable before it is taken and the resume position is not guessed from
  a source-head snapshot
- a clean shutdown abandons no transaction, and an unclean one fails with
  a typed reason instead of guessing
- an aborted promotion adopts no descendant artifact and leaves every
  cursor valid for ancestor resume, without demanding byte identity from
  artifacts the draining pipeline keeps rewriting
- shadow crosses the fork through both the walsender and
  `restore_command`, with no PANIC or FATAL

Full failover is complete when:

- walshadow behind fork follows promoted descendant automatically
- same-connection promotion and HA-endpoint reconnect both work
- restart from ancestor floor after live source advances works
- shadow follows identical timeline chain without catalog replay stall
- valid descendant preserves manifest, descriptor log, and CH progress
- missing lineage, missing WAL, unsafe slot, or publication past fork
  fails before cursor adoption
- full test matrix covers segment, transaction, crash, slot, and
  multiple-timeline boundaries
