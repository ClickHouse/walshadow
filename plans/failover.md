# Source timeline crossing

Source endpoint moving to a promoted PostgreSQL primary on a descendant
timeline is a continuation, not a rebootstrap. Walshadow drains the
ancestor branch through fork point `F`, proves the descendant repeats the
same bytes below `F`, hands the shadow the same crossing, and resumes:
no `--ignore-cursor`, no re-read past the ancestor tail, no skipped WAL

A restart anywhere across a crossing is ordinary, because the crossing
commits a resume position on the descendant before anything else moves onto
it: every window either side of that commit resumes on the branch the floor
names and reaches the same output
([`tests/control_plane_e2e.rs`](../tests/control_plane_e2e.rs)). Unplanned
promotion and ancestor-timeline base backups are
[future/failover.md](future/failover.md)

Scope is operator-driven switchover — writes stop before promotion, so
every transaction resolves and no torn record survives

## Operator protocol

```text
old primary   writes ──────────────────► stop
                    R     C  P
target        replay ────────────────────► P ── promote ──► F ── TLI 5 ──►
walshadow     consume ───► pause at C ········ resume ─► drain C→F ─► TLI 5

              R = durable floor, C = frozen consumed frontier,
              P = frozen received frontier (source head), F = fork
              R <= C <= P <= F, arranged before promotion, proved after
```

1. pre-create the physical slot on the promotion target, when slot mode is
   on. Its name is the target's to choose, since step 4 repoints
   `[source] slot` along with the address, but it exists before step 4:
   walshadow creates no slot it was not booted with, and one created at the
   target's head reserves nothing below it. Physical slots are never
   synchronized to a standby at any PostgreSQL version; PG 17 slot sync
   covers logical slots created with `failover = true`
   (`src/backend/replication/logical/slotsync.c`)
2. `ctl apply` `[stream] paused = true`, read both frozen pause LSNs back
   from `ctl status`
3. stop writes on the old primary (demote, or `pg_ctl stop -m fast`)
4. repoint `[source]` at the target with `ctl apply` (`slot` too, when the
   target's slot is named differently), adopted live while paused, *before*
   promoting it
5. wait for `ctl status` `promotion_ready = true`, which is target replay
   at or past `pause_received_lsn`, target receive level with target
   replay, and the target still in recovery. `promotion_blocked_on` names
   the first term that fails
6. promote the target
7. `ctl apply` `[stream] paused = false`

Steps 2, 4, and 5 carry the obligations; the rest is orchestration
walshadow does not join. Repoint before the wait, not after: the gate
reads the target through the connection the repoint moved, so it needs no
second tool and no second credential. Replay covering the old primary's
final durable record is the same statement as replay covering
`pause_received_lsn`, since the frozen head is what the source last
reported

`C` and `P` answer different questions. `C` is where resume asks the
promoted target to start. `P` is the source head walshadow last heard
about, which the target must reach before promotion. Freezing one
without the other either understates what the target owes or misstates
the resume position

Repoint before promote because it collapses the two transition shapes into
one: walshadow holds a connection to the server that gets promoted, so the
walsender ends the ancestor stream with a next-timeline result rather than
dropping it, and the promotion gate has somewhere to read from

A stable endpoint (VIP, proxy, DNS) skips step 4 and still crosses, with no
restart. Every reconnect proves continuity rather than equality
(§Reconnect), so an address that comes back promoted is a fork to cross,
not a stranger to refuse

## What pause freezes

Pause stops consumption, not production. `F` is created in step 6, after
the freeze, so `received <= F` is arranged rather than hoped for:

- `drain <= consumed <= received <= F`, so no ClickHouse row comes from
  WAL the descendant branch never had
- no complete ancestor record beyond `F`, so no dispatched-record
  truncation and no orphan ancestor suffix
- target replayed through `F`, so its `nextXid` is above every xid
  walshadow observed; the descendant cannot reuse an xid still keyed in
  `XactBuffer`
- clean shutdown leaves no partial record at `F`, so the descendant
  carries no `XLOG_OVERWRITE_CONTRECORD`

Unpaused consumption inverts the first point: a target promoted while
behind walshadow leaves rows in ClickHouse from a branch that no longer
exists, with no path to remove them. Pause bounds that exposure to `C`,
step 5 removes it

Everything behind the frozen frontier keeps moving — floor,
`emitter_ack`, descriptor batches, manifest cadence, shadow replay — so
an aborted promotion resumes on the ancestor with no descendant artifact
written, not with byte-identical artifacts

A restart inside the pause comes back paused, since the pause is config,
and re-freezes both numbers rather than restoring them (§Surfaces). Between
steps 3 and 4 it comes back to an endpoint that is down, so boot waits for
the source instead of exiting: `ctl` and `/metrics` bind before the first
connection attempt, and the repoint that ends the wait is applied to the
daemon doing the waiting

Fast shutdown narrows step 5 without being asked: it writes the shutdown
checkpoint, *then* wakes walsenders to drain, and the postmaster waits
for them to exit (`src/backend/postmaster/postmaster.c`); `WalSndDone`
exits only once the standby confirmed flush of everything sent
(`src/backend/replication/walsender.c`). The check catches what that
argument misses — a target not streaming at stop time,
`wal_sender_timeout` firing mid-drain, `pg_ctl` giving up

## Crossing

[`src/source/transition.rs`](../src/source/transition.rs). The walsender
says the branch ended by closing COPY on a historic timeline. The pump
answers that `CopyDone` (`SourceFeed::end_historic_stream`), leaving the
connection in simple-query mode, then proves the fork, waits at a barrier,
and crosses. `Switchover::probe` proves one fork per call and touches
nothing:

1. `IDENTIFY_SYSTEM` for the system identifier and the live timeline
2. fetch and parse `TIMELINE_HISTORY` for every timeline in
   `(finished, live]`
3. refuse unless the chain begins the finished branch where walshadow's own
   chain does (§Lineage), and read `switch_lsn` and `next_tli` out of the
   live history
4. refuse unless `next_lsn <= switch_lsn`

The pump then holds that proof while the pipeline drains to the fork
(§Barrier). Once it opens, `Switchover::cross`:

5. refuse unless `drain_lsn <= switch_lsn` and no ordinary transaction is
   open
6. write each `<tli>.history` into the filtered dir, fsynced
7. prove the configured slot on the descendant (§Slot), then
   `START_REPLICATION` from the fork segment's start
8. digest the repeated prefix and compare, holding bytes past `F` unpushed
9. **commit** the resume position: manifest `floor` at the fork segment's
   start, `[source] timeline` at `next_tli` with the fork as its
   switchpoint, then publish it to the pruners
10. `WalStream::transition_timeline`: seal every complete ancestor segment,
    drop bytes past `switch_lsn` with any partial record they carried, take
    the fork segment's identity to `next_tli`
11. advertise the crossing to shadow clients
12. push the held descendant bytes and resume

The identifier is re-proved on every attempt, not only on the operator's
repoint: a retry dials the endpoint again, and a fork on a segment boundary
repeats no bytes for the prefix digest to catch a foreign cluster with

The commit is the hinge, and it is why a restart never sees two branches.
Above it nothing has moved — the stream is still the ancestor's at `F` —
so a failure or a kill re-crosses from where the branch ended, and every
retryable refusal lands there by construction. Below it the floor, the
stream, and the shadow are the descendant's. Only `[9, 11)` is mixed, and
in the harmless direction: the floor names the descendant while the shadow
still sits at `F` on the ancestor

Advertising eleventh is load-bearing in both directions. Later, and a
client still reading the ancestor is handed a descendant record, which
recovery PANICs on (a checkpoint or end-of-recovery record carries its own
timeline, `src/backend/access/transam/xlog.c`). Earlier, and the shadow is
told about a fork whose floor still names the ancestor

Both fork proofs read the decoder's view, so the pump-side queue flushes
and drains first (bounded; past the bound the buffer's own view answers,
which reads a still-queued record as an open transaction and refuses —
the fail-closed direction)

The switchpoint comes from history, never from the next-timeline result
row. History has to be parsed anyway, to prove lineage and to hand the
shadow the exact bytes PostgreSQL wrote; a row restating one switchpoint
would be a second source for the same fact

## Barrier

The fork is the one moment when the input is provably closed — the
ancestor's walsender said so — so it is the cheapest place to make every
consumer agree. `ForkBarrier::pending` reports what is still behind, and
the pump holds the proof and keeps looping until nothing is:

- `resume_safe_lsn` at or past the fork segment's start, so no transaction
  below the committed floor is still in flight to ClickHouse
- shadow apply at or past `F`. The shadow gets there without being told
  anything — replay only needs bytes it already has — and the wait prods
  it with reply-requested keepalives, since a walreceiver answers
  unprompted only when its flush position moves
- the fork segment's start covered by `filter_durable` or by the floor
  already persisted, so the committed position is backed by fsynced
  segments

`[fork segment start, F)` is deliberately not in the barrier: a restart
re-reads that window from the descendant's own copy of the prefix, so a
transaction in flight there is re-read, not lost. What *would* be lost is
a transaction below the segment boundary, which is exactly the first
condition

Unbounded, and reported instead: the source has stopped producing, so
waiting costs nothing that is moving, while crossing anyway would put the
floor on the descendant with ClickHouse behind it. The wait couples the
handover to ClickHouse and shadow liveness — a stalled destination holds
the crossing open, with the descendant's WAL pinned by the slot (or
`wal_keep_size`) meanwhile. Progress goes to the log every two seconds
with a `waiting_on` label

## Committed resume position

`floor = align_down(F)` is not an optimistic jump. The floor's contract is
that a restart from it loses nothing, which the barrier proves, and the
fork segment's start is a sealed boundary already covered by
`filter_durable`, so nothing about durability accounting changes

PostgreSQL serves that position on the descendant. `StartReplication`
bounds the startpoint from below only for a historic timeline; naming the
current one clears `sendTimeLineIsHistoric` and the sole remaining check
is `FlushPtr < startpoint`
(`src/backend/replication/walsender.c`). The descendant-named fork segment
holds the ancestor prefix verbatim, copied by `XLogFileCopy(newTLI,
endLogSegNo, endTLI, endLogSegNo, XLogSegmentOffset(endOfLog))`
(`XLogInitNewTimeline`)

The floor never walks back. `align_down(emitter_ack)` reaches the
committed position only once descendant WAL fills the fork segment, so the
status loop takes the higher of the two; a rewind (`--start-lsn`,
`--ignore-cursor`) lowers it by seeding the published floor at the rewind
point instead

## Fork segment

The descendant always restarts at the fork segment's start, matching
`pg_receivewal`, because PostgreSQL copies the ancestor prefix into the
descendant-named file and zero-fills the rest
(`XLogInitNewTimeline`). Those repeated bytes are verified, not re-fed:
`WalStream` rolls a CRC32C over raw input per segment
(`WalStream::fork_prefix`) and the crossing compares the descendant's
copy against it, then feeds only bytes from `switch_lsn` on. So no record
is classified or xid-buffered twice, and a branch sharing the system
identifier but not the WAL is caught

Digest rather than byte compare: the walker buffer holds the *filtered*
stream, where dropped records are NOOP-rewritten in place, so it
disagrees with the source for reasons unrelated to lineage. A rolling CRC
costs 4 bytes of state and localises a mismatch to the fork segment

Segments wholly below `F` keep ancestor names. The fork segment seals
under the descendant name, once, its prefix byte-equal and its suffix
descendant. Prefix bytes stay verbatim, ancestor `xlp_tli` in page headers
included: recovery validates page timelines by history membership,
`tliInHistory(latestPageTLI, expectedTLEs)`
(`src/backend/access/transam/xlogrecovery.c`), not by equality with the
file's timeline

A restart from the committed floor re-reads `[align_down(F), F)` and
rewrites the same descendant-named file, so the archive stays continuous
whichever side of a restart wrote it

## Shadow handoff

The shadow-facing walsender serves a history rather than one timeline
([source.md](source.md)): dynamic `IDENTIFY_SYSTEM` timeline, exact
`TIMELINE_HISTORY` bytes per branch it knows, per-connection requested
timeline, historic cutoff at that branch's switchpoint, backend
`CopyDone` then next-timeline result. A `START_REPLICATION` for a timeline
it cannot place is refused rather than answered with another branch's
bytes

Order on the wire: history files into the filtered dir first, then the
crossing, then the descendant. A shadow that reads its history file too
early assumes a parentless timeline and cannot find any ancestor-named
segment; discovery probes IDs one at a time, so a gap in the chain stops
it short (`findNewestTimeLine`). Retention never trims `*.history` —
unrecognized filenames are left alone
([`src/ops/retention.rs`](../src/ops/retention.rs))

The connected shadow crosses on the wire and never touches the unsealed
fork segment: it takes the next-timeline result at `F` and follows
descendant bytes as they arrive. Its walreceiver fetches
`TIMELINE_HISTORY` for every timeline between its own and the one
`IDENTIFY_SYSTEM` reports, writes those files into its `pg_wal`, and asks
the startup process where to go
(`src/backend/replication/walreceiver.c`,
`WalRcvFetchTimeLineHistoryFiles`). `restore_command` is the path for a
shadow that was not connected across the fork

The catalog boundary gate stays the final check: no descendant catalog
state is captured until shadow replay follows the transition

## Reconnect

Every fresh source connection proves continuity, never equality
(`resume_source_feed`). A swapped `[source]`, a dropped socket, and a
recovery all take the same four steps:

1. `IDENTIFY_SYSTEM`, and refuse a system identifier the artifacts do not
   belong to
2. fetch `TIMELINE_HISTORY` for the live timeline, whenever it is above 1
3. refuse unless that chain begins the requested branch where walshadow's
   own chain does, and still places the resume LSN on it
4. `START_REPLICATION` on the *requested* branch, never on the live one

A live timeline above the requested one is the whole point: a stable
endpoint gives walshadow nothing else to notice a promotion by. The
walsender serves the historic branch to its switchpoint and ends it there,
so the reconnect hands the crossing a fork rather than a refusal, and step
5 of the protocol becomes optional rather than load-bearing

A drop *at* the switchpoint takes the same path without a reconnect:
`START_REPLICATION` for a historic timeline at its own switchpoint answers
with the next-timeline result instead of opening COPY, so the pump reads a
stream that ends where the chain says the branch does as the crossing
arriving early

## Slot

Slot mode keeps the pause window safe with source-local WAL, so the slot
has to be on the server the window is spent against. Walshadow proves it,
at every reconnect and again at the crossing, and creates none: a slot
created here would reserve at the target's head
(`ReplicationSlotReserveWal`, `src/backend/replication/slot.c`) and call
that continuation successful

- absent, or present but not physical, is `slot_missing`
- `restart_lsn` above the position being resumed is `slot_too_new`. A slot
  pre-created with `immediately_reserve` sits at the last checkpoint or
  restartpoint redo, which is behind a caught-up consumer and ahead of a
  lagging one; the remedy for the second is to catch up, since no slot can
  be created below the current redo
- `restart_lsn` above the durable floor is a warning, not a refusal: this
  connection is served, and the first standby status pulls the reserve back
  down (`PhysicalConfirmReceivedLocation`). Until it does, a restart would
  ask for segments the slot does not pin

The crossing proves the slot on the descendant against the fork segment's
start, which is the position it is about to commit. `[source] slot` is
live-reloadable, so the name proved is the one configured now, not the one
the daemon booted with

## Lineage

The system identifier owns artifacts; the timeline selects a branch
through them. Both selections come from parsed history
([`src/source/timeline.rs`](../src/source/timeline.rs)), never from a
timeline number on its own, and both are per position rather than once per
run:

- **per LSN** (`tli_of_point`, mirroring `tliOfPointInHistory`) answers
  which branch *wrote* a record; a switchpoint belongs to the descendant
- **per segment** (`tli_of_segment`, mirroring `XLogFileReadAnyTLI`)
  answers which branch's *file* holds a position. A fork copies the
  ancestor prefix into a descendant-named file, so the whole fork segment
  is the descendant's, and PostgreSQL skips a timeline only when the
  segment precedes that timeline's own first one

The floor is a resume position, so it resolves per segment: the committed
`align_down(F)` names the descendant while sitting below `F`, and asking
for a descendant-named segment below the fork — which the source does not
have — cannot happen

The manifest pairs the floor with the floor's branch
([ops.md](ops.md) §Manifest). `[source] timeline` is what a restart
requests at `floor`; the barrier keeps `[wal] stream_timeline` equal to it,
so the second is a cross-check rather than a lagging value. `system_id`
mismatch stays fatal; a timeline difference is a question for history, not
for equality

A branch is a number *and* a switchpoint, and the manifest stores both
(`[source] timeline_begin`). Numbers are not unique across branches: two
standbys of one primary, promoted independently, are both timeline 2 under
one system identifier, and a chain places either of them. So the number
alone lets a sibling pass every proof, while the switchpoint the source
reports for that number disagrees with the one walshadow crossed at —
`sibling_branch`. Boot, every reconnect, and each crossing check the same
equality. Storing it is the one place walshadow writes a switchpoint of its
own beside PostgreSQL's: the live chain cannot answer for a branch the live
server is not on, so nothing else carries the fact forward. `0/0` above
timeline 1 reads as unrecorded rather than as a switchpoint, and the next
manifest write records the real one

Boot fetches the live history whenever the source is past timeline 1,
proves the stored branch serves the aligned start, and starts there. So a
restart before a crossing resumes on the ancestor and crosses on its own,
and a restart after one resumes on the descendant at the fork segment's
start. The chain is also what places artifacts written on an earlier
branch: the descriptor log's header timeline is accepted when the chain
holds it, since a crossing moves the resume branch while the log stays
where it was. A missing history file is `history_missing`, not a resume as
though the branch never forked. `--ignore-cursor` remains the explicit
rebaseline for state that cannot be proved

## Refusals

Every reason below is terminal except source and storage trouble, which
retries. Lineage, prefix, and publication proofs need an operator, and
nothing falls back to the live head automatically:

- `timeline_not_descendant` — live timeline is not above the stream's, or
  history does not place the stream's branch
- `foreign_system_id` — the endpoint answers for another cluster
- `sibling_branch` — the source begins the stream's branch somewhere else,
  so it shares a number with it and nothing more
- `history_missing`, `history_malformed`
- `resume_past_fork` — consumed frontier sits above `switch_lsn`
- `published_past_fork` — `drain` sits above `switch_lsn`.
  `emitter_ack` cannot carry this proof: the pipeline completes out of
  order ([`src/emit/pipeline/ack.rs`](../src/emit/pipeline/ack.rs)), so a
  stalled early sequence pins it below rows already inserted from a later
  one. `drain` over-rejects by the in-flight window, which is the right
  bias for a path with no compensation behind it
- `open_xact_at_fork` — ordinary transactions unresolved where the
  ancestor ends, so the shutdown was not clean
- `fork_prefix_mismatch` — descendant's copy of the prefix is not the WAL
  the ancestor served, including a prefix truncated or no longer retained
- `slot_missing`, `slot_too_new` — §Slot
- `source` — connection, history-persistence, or resume-commit failure

A terminal refusal parks the pump; it does not exit the daemon. Every one
of them lands before the commit, so the floor is still on the ancestor and
a supervisor restart would boot cleanly, re-cross, and re-fail — a loop
that answers nothing. Parked, `ctl status` reports `crossing_blocked_on`
and the refusal's own words in `crossing_detail`, `/metrics` keeps serving,
and the source connection stays untouched at the fork. `[stream] paused`
takes the crossing decision back and clears the park, so the operator fixes
what the reason named, pauses, and resumes to prove the fork again

Most reasons are fixed where they are: `slot_missing` and `slot_too_new`
are answered on the target, a repoint onto the wrong endpoint by repointing
again. Only lost lineage — `foreign_system_id`, `sibling_branch`,
`history_missing`, `history_malformed`, or a `fork_prefix_mismatch` from
WAL the target no longer retains — leaves nothing to continue from, and
that is a rebootstrap

A barrier that never opens is not a refusal either: the crossing stays
pending and says what it is waiting on

A refused reconnect is the same vocabulary from the same proofs, counted
under the same `reason` label and reported as `source_swap_blocked_on`. It
costs the swap, not the stream: the feed already streaming stays up

A clean `pg_ctl stop -m fast` reaches none of them: it rolls live sessions
back and each xid-assigned transaction writes its abort before the
shutdown checkpoint, so the ancestor tail between `P` and `F` resolves
every transaction open at the pause

## Surfaces

`ctl status` carries the whole switchover surface
([control.md](control.md) §Verbs): the cluster `source_system_id`, which
orchestration reads back to confirm which one a repoint landed on; the
branches `source_timeline`,
`floor_timeline`, `shadow_served_timeline`, `shadow_replay_timeline`; the
refusals `source_swap_blocked_on`, `crossing_blocked_on`,
`crossing_detail`; the gate `promotion_ready`, `promotion_blocked_on`,
`target_in_recovery`, `target_replay_lsn`, `target_receive_lsn`; and the
LSNs `pause_consumed_lsn`, `pause_received_lsn`, `floor`,
`source_received`, `drain`, `emitter_ack`, `shadow_replay`

Both `pause_*` freeze when the pump observes the pause and read `0/0`
otherwise, so a promotion decision compares against a frontier that cannot
move under it. A restart mid-pause re-freezes them — conservatively, since
consumed drops back to the floor and received re-derives from the live head
— which moves numbers a decision was already taken from, so
`pause_refrozen` says the pair has to be read again

The two shadow branches are what separate a shadow that followed the chain
from one that merely survived: served is what the walsender advertises,
replay is the oldest branch an attached client is still reading

`[stream] paused` is the only control input; a dedicated switchover verb
would add a state machine for what pause plus these checks already do

Metrics: `walshadow_source_info{system_id}` — a label, since a 64-bit
identifier does not survive a float sample — plus
`walshadow_source_timeline`, `walshadow_floor_timeline`,
`walshadow_shadow_served_timeline`, `walshadow_shadow_replay_timeline`,
`walshadow_timeline_switches_total`,
`walshadow_timeline_switch_failures_total{reason}`,
`walshadow_timeline_switch_lsn`,
`walshadow_timeline_prefix_bytes_verified_total`,
`walshadow_timeline_transition_seconds_total`,
`walshadow_crossing_wedged`, `walshadow_pause_consumed_lsn`,
`walshadow_pause_received_lsn`

`walshadow_timeline_switch_lsn` comes off the chain at boot, so it reports
the fork a restart resumed across rather than zero

Every transition logs system id, both timelines, switch LSN, resume LSN,
committed floor, `drain` as the publication ceiling, verified prefix bytes,
and slot. The proof, each barrier wait with its `waiting_on` label, and the
commit log separately, so a crossing that pauses says where. Never
credentials or full connection strings

`ctl status` and `/metrics` keep answering across the barrier, and across a
parked crossing: the pump holds the fork proof rather than blocking inside
the crossing, so the loop publishes every tick. The pair it publishes is re-read at publish time
rather than reused from the top of the iteration, so the crossing's own
iteration reports the committed floor and branch instead of the values it
started with

## Stream end shapes

`SourceEvent` splits the three ways a physical stream ends, since only one
of them is a crossing
([`src/source/source_feed.rs`](../src/source/source_feed.rs)):

- **`TimelineEnd`** — backend `CopyDone` at the switchpoint of a historic
  request. The loop waits for the frontend's before `StartReplication`
  writes its result (`src/backend/replication/walsender.c`)
- **`Shutdown`** — `WalSndDone` sends a bare `CommandComplete` from inside
  COPY, then exits: no backend `CopyDone` before it, no `ReadyForQuery`
  after. A parser that leaves COPY only on `CopyDone` reads this as a
  stray streaming message and blocks until the socket closes. It is a
  reconnect, not a daemon exit
- **error / drop** — `SourceRecovery::recover`, source first then archive
  ([source.md](source.md))
