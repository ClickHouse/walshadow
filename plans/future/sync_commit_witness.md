# synchronous-commit WAL witness

Promote walshadow into source PG's `synchronous_standby_names`
quorum as an RPO=0 durability witness. Topology is `primary +
full-PG-standby + walshadow` with both replicas listed under `ANY 1
(walshadow, fullpg)`. Primary's commit unblocks the moment *either*
replica acks the flush LSN. In steady state walshadow wins the race
— buffering bytes durably is strictly faster than full-PG running
recovery against a real data dir — so primary's commit durability
survives primary loss without the full standby having to keep pace.
On primary loss, walshadow relays the surviving WAL tail to the
lagging full-PG standby before stepping back

## Topology

```
                     +-------------+
                     |   Primary   |   wal_level=logical
                     +------+------+   synchronous_commit=on
                            |          synchronous_standby_names=
                            |            'ANY 1 (walshadow, fullpg)'
              +-------------+--------------+
              | (sync slot, fast acker)    | (sync slot, slow acker)
              v                            v
        +-----+-----+                +-----+-----+
        | walshadow |                |  Full PG  |
        |  daemon   |                |  standby  |
        +-----------+                +-----------+
```

Both replicas receive the same WAL stream. The quorum form changes
which ack primary waits for, not who gets the bytes. Configuration is
load-bearing for two reasons:

- **Walshadow as fast quorum member.** Primary's commit latency
  bounded by walshadow's flush, not full-PG's apply. Cheap
  durability witness without paying full-standby latency on commit
  path
- **Walshadow loss degrades, doesn't block.** Under `ANY 1`,
  walshadow crashing means full-PG's ack starts satisfying the
  quorum. Primary's commit latency rises (to full-PG's flush
  latency) but commits still land. Contrast with
  `synchronous_standby_names='walshadow'` — walshadow becomes a
  single point of blocking; any hiccup blocks primary writes until
  operator removes it. Quorum form is the only operationally honest
  shape

## What walshadow already has

- `START_REPLICATION PHYSICAL` against source with slot keepalive
  and standby status messages (`r` packets carrying `(write_lsn,
  flush_lsn, apply_lsn)`). Today advances conservatively against
  shadow PG's replay LSN
- Local filesystem path receiving every WAL byte source emits,
  pre-filter. Today the path is in-memory between `SourceFeed` and
  filter; making it durable is the load-bearing change

## What it doesn't have

- A **durable unfiltered WAL archive** co-located with daemon.
  Filter writes filtered segments under `--out-dir` (shadow's
  `pg_wal/`); original unfiltered bytes discarded once filter
  classification is done. Witness role demands persistent
  unfiltered archive sized to bridge a failover window
- Any path promoting shadow PG into a write-capable primary, because
  shadow has no user-heap data files. Promotion in the PG sense
  (writeable, applications connect, accept new xacts) is not the
  goal. Goal is **WAL relay during the failover window**, not
  application traffic
- A "feed WAL outward" path. wal-rus's replication client is
  receive-only against source today; sender-side walsender doesn't
  exist

## Sequence on primary loss

1. Primary dies. Walshadow's slot has acked flush LSN F1 (the LSN
   quorum has been winning on); full-PG standby's flush LSN F2
   trails by replication lag. `ANY 1` guarantees only that *some*
   replica reached primary's commit LSN — walshadow in steady state
   — and says nothing about full-PG's position beyond best-effort.
   Gap (F1 − F2) is the failover bridge walshadow must close before
   full-PG can promote without data loss
2. Operator (or HA orchestrator) confirms primary loss and
   instructs walshadow to enter **relay mode**
3. Walshadow connects to surviving full-PG standby and ships WAL
   bytes between F2 and F1, byte-identical to primary's own WAL
   emission. Two implementation shapes:
   - **`restore_command` shim.** Full-PG's `restore_command` is
     configured to fetch from walshadow's unfiltered archive
     directory. Standby pulls segments at its own cadence. Simplest;
     needs walshadow's archive to expose a fetch endpoint (HTTP,
     NFS, or shared volume)
   - **walshadow-as-primary.** Walshadow opens a walsender-
     compatible session to full-PG and pushes WAL via streaming.
     Requires implementing server side of `START_REPLICATION
     PHYSICAL` (issuer of bytes, not consumer). Mirrors wal-rus's
     receive-side state machine but matching send-side isn't
     implemented today
4. Full-PG standby reaches F1, declares consistency, promotes via
   `pg_ctl promote`
5. Walshadow re-attaches its physical slot to the newly-promoted
   primary. Slot must be pre-created on (former) standby via
   failover-aware replication slots (PG 17+) or manual pre-create on
   standby before failover

## Disposition of shadow PG after failover

Three operator-selectable outcomes:

| outcome | shadow data dir | relationship to new primary |
|---|---|---|
| **rebind** | unchanged | shadow keeps replaying catalog WAL from new primary's slot; relfilenodes stay coherent because new primary inherited old primary's relfilenodes |
| **rebuild** | `rm -rf` + shadow reinit (initdb or BASE_BACKUP) | fresh shadow against new primary; loses cache warmth, gains clean state |
| **promote-bridge** | shadow promotes briefly to feed WAL via PG's own walsender, then demotes back to standby | sidesteps walshadow-side relay implementation but burdens shadow with brief write-primary role |

Default per topology:

- If walshadow ran with `relfilenode_pin` and new primary is former
  full-PG standby (inherited exact relfilenodes via streaming
  replication), **rebind** is correct and zero-cost
- If new primary diverged at any point (operator promoted an
  unrelated cluster), **rebuild** is the only safe option

`promote-bridge` is mechanically simplest because PG's existing
walsender does byte-pushing for free; cost is shadow temporarily has
to be a real, writeable primary, which requires user-heap files for
any relation failover-recovery WAL touches and opens door to
accidental writes against shadow. Requires a BASE_BACKUP-driven (not
schema-only) shadow data dir, so incompatible with MiB-scale
schema-only shadow

## What this buys

- **RPO = 0 at full-PG-apply-lag cost.** A `primary + async full-PG`
  topology loses any commit primary flushed but hadn't yet streamed
  past full-PG's apply LSN. Promoting walshadow into the quorum
  closes that gap: every commit primary acks has been durably
  flushed on *some* replica, and walshadow is the cheap one keeping
  pace
- **Fast commit ack without a hot standby.** Full-PG doesn't have
  to keep up with primary's commit cadence to provide RPO=0;
  walshadow's "buffer bytes, fsync, ack" loop is strictly lighter
  than full-PG's "buffer + apply + ack". Operator gets sync-commit
  durability at near-async commit latency
- **Lossless promotion of a lagging standby.** Full-PG can lag by
  minutes and still recover lossless because walshadow holds the
  WAL tail and ships it post-failover. Full-PG's lag budget
  decouples from primary's commit-latency budget
- **Graceful walshadow failure.** Under `ANY 1`, walshadow going
  down falls back to full-PG ack on the quorum. Commit latency
  rises to full-PG's flush latency but primary doesn't block

## What this costs

- **Walshadow on the commit path (when it's the quorum winner).**
  Primary's commit ack waits for walshadow's flush in steady state.
  RTT-bounded; lighter than full-PG flush but heavier than zero.
  Walshadow failure degrades gracefully rather than blocking
- **Durable unfiltered archive.** Walshadow must persist unfiltered
  WAL bytes long enough to bridge a failover (operator-configurable
  retention, default a few hours). Adds disk-budget line distinct
  from shadow's `pg_wal/` (which today only holds filtered bytes).
  Equally the archive is what makes walshadow a meaningful flush
  target — without on-disk persistence, "flush ack" wouldn't
  survive a walshadow crash
- **Failover orchestration outside walshadow.** Walshadow can do
  the relay but doesn't detect primary loss, nominate promotion
  target, or fence the old primary. Orchestrator (Patroni, repmgr,
  custom) owns that. Walshadow exposes "I have WAL up to LSN F1,
  ready to ship to <host:port>" and a "begin relay" RPC
- **Sender-side walsender implementation.** If `restore_command`-shim
  shape (3a above) is chosen, cost is small — walshadow exposes
  HTTP / file-fetch endpoint for segments and full-PG drives pull
  cadence. If push-via-walsender shape (3b) is chosen, walshadow
  needs fresh send-side of replication protocol, ~1000+ LOC mostly
  on wal-rus
- **Slot positioning around failover.** Walshadow's slot lives on
  primary; surviving full-PG standby needs walshadow's slot
  recreated (or pre-created and `pg_replication_slot_advance`'d
  after promotion). PG 17 failover-aware slots simplify this;
  earlier majors need manual operator script
- **Quorum-membership reporting.** Operator needs visibility into
  which replica is currently satisfying the quorum on each commit —
  if walshadow silently stops being the fast acker (archive disk
  full, walshadow GC pause), failover-bridge WAL retention may
  diverge from operator expectations. Surface via metric:
  per-replica `last_acked_lsn` plus `quorum_winner_xid_count` over
  a rolling window

## Why deferred

Significant new failure-mode surface. Walshadow today is a passive
CDC sink — failure modes are bounded to "stop emitting to CH" and
recovery is "restart the daemon, resume from cursor". Witness role
puts walshadow on primary's commit path: walshadow flush latency
becomes primary commit latency, walshadow archive disk full becomes
a quorum-degradation event, walshadow GC pause becomes a tail-latency
spike for production xacts. Each adds a class of incident operators
must learn to diagnose

Requires:

- Quorum metrics. Per-replica `last_acked_lsn`,
  `quorum_winner_xid_count`, archive disk pressure, flush latency
  histograms. Operator dashboard non-trivial
- Sender-side walsender on walshadow (or `restore_command` HTTP
  endpoint, depending on shape chosen). Both shapes are net-new
  code; sender-side walsender is the bigger lift but composes more
  naturally with PG's existing replication assumptions
- Durable unfiltered archive with retention policy. Distinct from
  shadow's `pg_wal/`. Retention must be operator-tunable because
  failover-bridge window varies by deployment
- Failover orchestration integration. Walshadow exposes RPCs but
  doesn't drive the failover sequence; HA layer (Patroni, repmgr)
  must learn walshadow's RPC vocabulary

None of the existing surfaces need this. CDC pipeline is the primary
mission; witness role is a second use of the same WAL receiver and
shadow PG, but with substantially different operational shape.
Sequence: lands naturally after slot-keepalive + metrics surface
because metrics for "walshadow flush LSN vs primary commit LSN" are
exactly the durability witness telemetry. A full-data-dir
BASE_BACKUP on shadow is a soft prerequisite for the
`promote-bridge` post-failover disposition; not a prereq for the
more conservative `rebind` / `rebuild` paths

Sizing (evaluation-grade): ~600 LOC walshadow side for durable
unfiltered archive + relay-mode state machine + RPC surface, plus
wal-rus upstream lift sized to 4a (modest, ~200 LOC) or 4b (large,
~1000+ LOC) depending on shape

## Dependencies

- Slot keepalive + metrics surface (ops.md) — soft, supplies the
  telemetry shape witness role needs
- Full-data-dir BASE_BACKUP on shadow — soft, only for the
  `promote-bridge` disposition
- Sender-side walsender in wal-rus (if push shape chosen) — hard for
  3b, not needed for 3a
- Durable unfiltered archive in walshadow — hard for both shapes
- Failover orchestration layer (operator-provided, Patroni/repmgr/
  custom) — hard, walshadow doesn't fence or detect primary loss
  itself

## Open question

Interaction with PG's existing `synchronous_commit` machinery, and
walshadow's candidacy in `synchronous_standby_names`. PG identifies
sync standbys by `application_name` advertised in the walsender
connection. Walshadow already opens a `START_REPLICATION PHYSICAL`
session; setting `application_name` is one line. But PG's quorum
machinery makes assumptions about what a sync standby *is*:

- Does PG validate that the named standby is replaying the WAL it
  acks? Walshadow flushes bytes durably but never applies them in
  the PG sense — there's no recovery process running against the
  archive. PG's `pg_stat_replication` exposes `flush_lsn` and
  `replay_lsn` separately; walshadow can advertise `flush_lsn` and
  leave `replay_lsn` at the same value (since walshadow's "apply"
  is degenerate: the archive *is* the applied state for witness
  purposes). Need to confirm PG doesn't reject this shape
- Does the quorum logic care about *which* standby wins? `ANY 1
  (walshadow, fullpg)` should treat walshadow's flush ack identical
  to full-PG's flush ack from primary's POV; quorum is satisfied
  on first ack. Verify experimentally — sync_commit machinery has
  edge cases around standby reconnection that may matter
- What happens if walshadow's `application_name` collides with a
  re-attaching old replica name during failover orchestration? Need
  a naming convention that survives slot recreation. Operator-
  configurable `walshadow.witness_application_name` with sensible
  default (`walshadow-witness-<hostname>`)

Resolution before any witness work lands: build a minimal
reproduction (PG primary + walshadow with the witness branch + a
streaming full-PG standby, all on loopback), confirm `ANY 1 (...)`
quorum acks against walshadow's `application_name`, verify
`pg_stat_replication` rows behave as expected. ~2 days of bench work
ahead of any production-shape engineering
