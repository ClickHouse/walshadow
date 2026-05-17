# FUTURE — speculative roles for shadow PG beyond CDC

Evaluation. Two future-potential capabilities that walshadow's existing
machinery (shadow PG + raw physical WAL receiver) already prefigures
but no current phase pursues. Status: **not committed work, not yet
sized**. Recorded here so neither idea quietly turns into scope creep
inside another phase.

Both build on the same observation: walshadow already runs (a) a live
schema mirror against source's catalog WAL, and (b) a physical WAL
receiver that sees every byte source emits. Today walshadow consumes
those two for CDC into ClickHouse. Each idea below puts the same two
streams to a second use.

1. [Schema-only restore](#1--schema-only-restore). Ship shadow's
   catalog as a `CREATE TABLE … / CREATE INDEX … / CREATE SEQUENCE …`
   payload (or as a hollow data directory of empty heaps + indexes)
   for a third-party cluster.
2. [Synchronous-commit WAL witness](#2--synchronous-commit-wal-witness).
   In a `primary + full-PG-standby + walshadow` topology with
   both replicas in `synchronous_standby_names` under
   `ANY 1 (walshadow, fullpg)`, walshadow wins the quorum in
   steady state (it's strictly faster to ack than the full
   standby) so primary's commit durability survives primary loss
   without the full standby having to keep pace. Walshadow then
   relays the surviving WAL tail to the lagging full-PG standby
   before stepping back.

---

## 1 — Schema-only restore

Shadow PG already holds a live, WAL-replayed mirror of source's
catalog (pg_class, pg_attribute, pg_type, pg_index, pg_constraint,
pg_depend, pg_namespace, pg_proc-for-defaults, …). Today nothing
consumes that beyond [`ShadowCatalog`](../src/shadow_catalog.rs)'s
runtime lookups. Three downstream consumers are plausible:

* **Dev / test cluster bootstrap.** Operator wants "source's schema as
  of LSN X" loaded onto an empty PG instance, no data. Today they run
  `pg_dump --schema-only` against source, which costs source CPU and
  IO and requires production credentials. Shadow already has it.
* **Bootstrap material for a fresh full-PG replica.** Pairs with
  §2 below: when the surviving full-PG node needs to be re-seeded
  from scratch (or the prior full-PG standby is destroyed and
  replaced), shadow's schema is the obvious starting point ahead
  of a data-bearing `BASE_BACKUP`.
* **CH schema sync.** Phase 7's emitter maps PG relations to CH
  tables via TOML config today. A future "auto-create CH tables"
  mode could ride shadow's catalog: when a new relation appears on
  source, shadow sees the catalog row, walshadow synthesises the
  matching CH `CREATE TABLE` and issues it before the first data
  block lands.

### Two output shapes

**A. SQL payload.** Walk shadow's catalog, emit DDL text:

* `CREATE SCHEMA …` for every namespace except system schemas
* `CREATE TYPE … AS ENUM` / composite types
* `CREATE SEQUENCE …` with `last_value` / `is_called` from
  `pg_sequence`
* `CREATE TABLE …` with column list (`pg_attribute` ordered by
  attnum, skipping dropped), `NOT NULL` (`attnotnull`), defaults
  (`pg_attrdef`), check constraints (`pg_constraint contype = 'c'`),
  storage clauses
* `ALTER TABLE … ADD CONSTRAINT … PRIMARY KEY / UNIQUE / FOREIGN KEY`
  for `contype IN ('p','u','f')`, ordered so referenced tables
  exist first (topo-sort on `pg_constraint.confrelid`)
* `CREATE INDEX … USING <am> (…) WHERE …` for `pg_index` rows that
  aren't the constraint-backed ones already emitted
* `CREATE TRIGGER` / `CREATE RULE` if in scope, else explicitly
  declared out of scope
* `GRANT` from `pg_authid` / ACLs if in scope

Closest analogue: `pg_dump --schema-only`. Shadow runs the same PG
major as source, so feeding shadow's output back into `psql` on a
matching-major target is supported by PG itself.

Lift: shadow PG can already produce this directly. `pg_dump
--schema-only -h <shadow-socket> -d <shadow-db>` works today because
shadow holds the catalog rows post-replay. The walshadow-side work
is wrapping that invocation: a `Shadow::dump_schema(target_lsn) ->
String` that gates on `wait_for_replay(target_lsn)` and shells out
to `pg_dump`. ~50 LOC plus the gating logic shadow already has.

Wins over `pg_dump` against source:
* Zero load on source PG. Catalog reads happen against shadow's
  socket on the same host as walshadow.
* LSN-scoped: dump reflects source's schema at exactly `target_lsn`,
  not "whenever pg_dump's transaction snapshot fired". Useful for
  reproducing a debug state.
* Doesn't require production credentials on the dumping host.

Caveats:
* `pg_dump`'s output assumes the target PG can run its `SELECT
  pg_catalog.set_config(...)` preamble; same constraint as
  upstream.
* Trigger functions live in `pg_proc.prosrc`, which is a catalog
  table walshadow's filter classifies as catalog (good), but
  function bodies referencing other schemas might fail to compile
  on a target that doesn't have those schemas yet. Standard
  `pg_dump` ordering problem; not walshadow's to solve.
* Extension state. `pg_extension` is catalog, but
  `CREATE EXTENSION foo` runs the extension's install script which
  is not in shadow's catalog. Shadow knows "extension foo is
  installed at version X"; the target must have foo's package
  installed for the dump to apply.

**B. Hollow data directory.** Same end state expressed differently:
walshadow ships a fully `initdb`'d, catalog-populated, schema-aware
data directory with every user heap / index file present but
zero-row. Effectively shadow's own data dir, post-prune of any
relfilenodes that recovery happened to touch with WAL.

This is what the [BASEBACKUP §"Disk budget: B. Strip user heap
post-fetch"](BASEBACKUP.md) path produces as a side effect. Shape
re-cap:

* Spin shadow in normal (non-recovery) mode briefly
* Per `pg_class`: any `oid >= FirstNormalObjectId` non-catalog
  relfilenode → truncate to one empty page (or unlink; recovery
  doesn't touch these because the filter NOOPs their WAL)
* Tag the directory with `.walshadow-schema-only` sentinel
* `pg_ctl stop`, tar the data dir

Target operator does `tar -xf schema.tar -C /var/lib/postgresql/data
&& pg_ctl start`. PG comes up with empty tables matching source's
schema. No `psql` replay, no DDL ordering surprises, indexes already
present (and valid, because they're empty).

Wins over (A):
* No DDL replay step on the target. Fastest "schema present, ready
  for inserts" path.
* Sequences carry their `last_value` byte-exact (sequence state
  lives in the heap file, not just in `pg_sequence`).
* Extension on-disk state (function `oid`s, type `oid`s in
  `pg_proc` / `pg_type`) is byte-exact; subsequent WAL or COPY
  loads against the target line up without OID-remap surprises.

Caveats:
* Major-version-locked. (A)'s SQL payload survives minor-version
  drift on the target; (B)'s data dir doesn't (`pg_control` is
  major-pinned).
* Storage on shadow during the prune pass: schema-only shadow is
  MiB-scale, but the prune sequence needs shadow to spin in
  read-only mode first, which means recovery must catch up to
  `target_lsn` before the prune. Acceptable in steady state.
* OID skew on the target. Source's `pg_class.oid` propagates
  exactly (which is the win), but a target that's already been
  `initdb`'d with conflicting OIDs can't re-use the same data dir.
  Operator workflow is "fresh empty volume + shadow's tar".

### Recommendation

(A) lands first as a 1-command shim around `pg_dump`. (B) is the
natural follow-up once [BASEBACKUP §Use Case 1B](BASEBACKUP.md)'s
prune pass exists — at that point (B) reduces to "skip step C
(`enable_standby_recovery`), tar what's on disk".

Neither blocks Phases 5–10. Sequencing: defer past Phase 10
unless §2 below promotes both ideas together.

### Sourcing decisions deferred

* Whether (A) emits a single payload or a stream of `CREATE …`
  statements addressable by relation. Latter pairs with the CH
  auto-create case; the former is enough for cluster bootstrap.
* `pg_dump`'s `--format=custom` / `--format=directory` shapes for
  (A). Standard `pg_dump` flags pass through; no walshadow
  decision needed.
* Locale / collation / encoding mismatches between shadow and the
  target. Document as operator-precondition; matches `pg_dump`'s
  own posture.

---

## 2 — Synchronous-commit WAL witness

Topology:

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
        |  +-------+|                +-----------+
        |  |shadow ||                (commits land here too,
        |  |  PG   ||                 but applied lazily)
        |  +-------+|
        +-----------+
```

Three nodes, both replicas listed in `synchronous_standby_names` under
the `ANY 1 (walshadow, fullpg)` quorum form. Primary's commit
unblocks the moment *either* replica acks the flush LSN. In steady
state walshadow wins the race — it's just buffering bytes durably,
not running recovery against a full data dir — so the full-PG
standby trails walshadow's acked LSN by replication-apply lag. Both
replicas are still receiving the same WAL stream; the quorum form
only changes which ack primary waits for, not who gets the bytes.

The configuration is load-bearing for two reasons:

* **Walshadow as the fast quorum member.** Primary's commit latency
  is bounded by walshadow's flush, not by full-PG's apply. Cheap
  durability witness without paying full-standby latency on the
  commit path.
* **Walshadow loss degrades, doesn't block.** Under
  `ANY 1 (walshadow, fullpg)`, walshadow crashing simply means
  full-PG's ack starts satisfying the quorum. Primary's commit
  latency rises (to full-PG's flush latency) but commits still
  land. Contrast with `synchronous_standby_names='walshadow'` —
  walshadow becomes a single point of blocking, and any walshadow
  hiccup blocks primary writes until operator removes it from the
  list. The quorum form is the only operationally honest shape.

### What walshadow already has

* `START_REPLICATION PHYSICAL` against source, with slot keepalive
  and standby status messages (`r` packets carrying
  `(write_lsn, flush_lsn, apply_lsn)`). Today walshadow advances
  these conservatively against shadow PG's replay LSN
  ([PLAN.md §"Phase 10"](PLAN.md#phase-10--operational)).
* A local filesystem path that receives every WAL byte the source
  emits, pre-filter. Today that path is in-memory between
  [`SourceFeed`](../src/source_feed.rs) and the filter; making it
  durable is the load-bearing change.

### What it doesn't have

* A *durable* unfiltered WAL archive co-located with the daemon.
  Filter today writes filtered segments under `--out-dir` (shadow's
  `pg_wal/`). The original unfiltered bytes are discarded once the
  filter classification is done.
* Any path that promotes shadow PG into a write-capable primary,
  because shadow has no user-heap data files. Promotion in the
  PG sense (writeable, applications connect, accept new xacts) is
  not the goal here. The goal is **WAL relay during the failover
  window**, not application traffic.
* A "feed WAL outward" path. wal-rs's replication client is
  receive-only against source today.

### Sequence on primary loss

1. Primary dies. Walshadow's slot has acked flush LSN F1 (the LSN
   the quorum has been winning on); full-PG standby's flush LSN F2
   trails by replication lag. `ANY 1` guarantees only that *some*
   replica reached primary's commit LSN — walshadow in steady
   state — and says nothing about full-PG's position beyond "best
   effort". Gap (F1 − F2) is the failover bridge walshadow must
   close before full-PG can promote without data loss.
2. Operator (or HA orchestrator) confirms primary loss and
   instructs walshadow to enter **relay mode**.
3. Walshadow connects to the surviving full-PG standby and ships
   WAL bytes between F2 and F1, byte-identical to what primary
   would have shipped. Two implementation shapes:
   * **`restore_command` shim.** Full-PG's `restore_command` is
     configured to fetch from walshadow's unfiltered archive
     directory. Standby pulls segments at its own cadence.
     Simplest; needs walshadow's archive to expose a fetch endpoint
     (HTTP, NFS, or shared volume).
   * **walshadow-as-primary**. Walshadow opens a
     `walsender`-compatible session to full-PG and pushes WAL via
     streaming. Requires implementing the server side of
     `START_REPLICATION PHYSICAL` (issuer of bytes, not consumer).
     Roughly mirrors wal-rs's existing receive-side state machine,
     but the matching send-side isn't implemented today.
4. Full-PG standby reaches F1, declares consistency, promotes via
   `pg_ctl promote`.
5. Walshadow re-attaches its physical slot to the newly-promoted
   primary. Slot must be pre-created on the (former) standby via
   `failover-aware replication slots` (PG 17+) or via a manual
   pre-create on the standby before failover. Two options match
   [PLAN.md pitfall #9](PLAN.md#9-source-primary-failover).

### Disposition of shadow PG after failover

Three operator-selectable outcomes:

| outcome | shadow data dir | walshadow's relationship to new primary |
|---|---|---|
| **rebind** | unchanged | shadow keeps replaying catalog WAL from new primary's slot; relfilenodes stay coherent because new primary inherited old primary's relfilenodes |
| **rebuild** | `rm -rf` + Phase 3 reinit or [BASEBACKUP](BASEBACKUP.md) | fresh shadow against new primary; loses cache warmth, gains clean state |
| **promote-bridge** | shadow promotes briefly to feed WAL via PG's own walsender, then demotes back to standby | sidesteps walshadow-side relay implementation but burdens shadow with brief write-primary role |

Default per topology:
* If walshadow ran with `relfilenode_pin` and the new primary is
  the former full-PG standby (which inherited the exact relfilenodes
  via streaming replication), **rebind** is correct and zero-cost.
* If the new primary diverged at any point (e.g. operator promoted
  an unrelated cluster), **rebuild** is the only safe option.

`promote-bridge` is mechanically the simplest because PG's existing
walsender does the byte-pushing for free; the cost is that shadow PG
temporarily has to be a real, writeable primary, which (a) means it
needs user-heap files for any relation the failover-recovery WAL
touches and (b) opens the door to accidental writes against shadow.
Practically: this mode requires the BASEBACKUP-driven (not
schema-only) shadow data dir, so it's incompatible with the
MiB-scale shadow that Phase 3 ships.

### What this buys

* **RPO = 0 at full-PG-apply-lag cost.** A `primary + async
  full-PG` topology loses any commit primary flushed but hadn't
  yet streamed past full-PG's apply LSN. Promoting walshadow into
  the quorum closes that gap: every commit primary acks has been
  durably flushed on *some* replica, and walshadow is the cheap
  one keeping pace.
* **Fast commit ack without a hot standby.** Full-PG doesn't have
  to keep up with primary's commit cadence to provide RPO=0;
  walshadow's "buffer bytes, fsync, ack" loop is strictly lighter
  than full-PG's "buffer + apply + ack". Operator gets sync-commit
  durability at near-async commit latency.
* **Lossless promotion of a lagging standby.** Full-PG can lag by
  minutes and still recover lossless, because walshadow holds the
  WAL tail and ships it post-failover. Full-PG's lag budget
  decouples from primary's commit-latency budget.
* **Graceful walshadow failure.** Under `ANY 1`, walshadow going
  down falls back to full-PG ack on the quorum. Commit latency
  rises to full-PG's flush latency but primary doesn't block —
  contrast with naming walshadow as the sole sync standby, where
  walshadow becomes a primary-blocking single point of failure.

### What this costs

* **Walshadow on the commit path (when it's the quorum winner).**
  Primary's commit ack waits for walshadow's flush in steady
  state. RTT-bounded; lighter than full-PG flush but heavier than
  zero. Walshadow failure degrades gracefully (see above) rather
  than blocking.
* **Durable unfiltered archive.** Walshadow must persist the
  unfiltered WAL bytes long enough to bridge a failover (operator-
  configurable retention, default a few hours). Adds a disk-budget
  line distinct from shadow's `pg_wal/` (which today only holds
  filtered bytes). Equally, the archive is what makes walshadow a
  meaningful flush target — without on-disk persistence,
  walshadow's "flush ack" wouldn't survive a walshadow crash.
* **Failover orchestration outside walshadow.** Walshadow can do
  the relay but does not detect primary loss, nominate the
  promotion target, or fence the old primary. Orchestrator
  (Patroni, repmgr, custom) owns that. Walshadow exposes "I have
  WAL up to LSN F1, ready to ship to <host:port>" and a "begin
  relay" RPC.
* **Sender-side `walsender` implementation.** If the
  `restore_command`-shim shape (4a above) is chosen, the cost is
  much smaller — walshadow exposes an HTTP / file-fetch endpoint
  for segments and full-PG drives the pull cadence. If the
  push-via-walsender shape (4b) is chosen, walshadow needs a
  fresh send-side of the replication protocol, ~1000+ LOC mostly
  on wal-rs ([BASEBACKUP §"Library surface to shape on wal-rs"](BASEBACKUP.md)
  sets the precedent for that boundary).
* **Slot positioning around failover.** Walshadow's slot lives on
  primary; the surviving full-PG standby needs walshadow's slot
  recreated (or pre-created and `pg_replication_slot_advance`'d
  after promotion). PG 17 failover-aware slots simplify this;
  earlier majors need manual operator script.
* **Quorum-membership reporting.** Operator needs visibility into
  *which* replica is currently satisfying the quorum on each
  commit — if walshadow silently stops being the fast acker (e.g.
  archive disk full, walshadow GC pause), failover-bridge WAL
  retention may diverge from what the operator expects. Surface
  via metric: per-replica `last_acked_lsn` plus
  `quorum_winner_xid_count` over a rolling window.

### Sequencing against existing phases

Independent of Phases 5–10 — none of them touch
synchronous-commit semantics or sender-side replication. Sits
naturally after Phase 10 (slot keepalive, metrics surface)
because the metrics for "walshadow flush LSN vs primary commit
LSN" are exactly the durability witness telemetry.

[BASEBACKUP](BASEBACKUP.md) is a soft prerequisite for the
`promote-bridge` post-failover disposition (needs full data dir
on shadow). Not a prereq for the more conservative `rebind` /
`rebuild` paths.

Sizing (very rough, evaluation-grade): ~600 LOC walshadow side
for durable unfiltered archive + relay-mode state machine + RPC
surface, plus wal-rs upstream lift sized to 4a (modest, ~200
LOC) or 4b (large, ~1000+ LOC) depending on shape.

### Out of scope here

* Multi-primary / multi-master setups. Walshadow's catalog
  invariants assume a single source-of-truth primary at any one
  time.
* Quorum commit (multiple sync standbys). PG handles that on the
  primary side via `synchronous_standby_names = ANY n (…)`;
  walshadow joining a quorum is identical to single-sync from
  walshadow's perspective.
* Cross-region replication. Walshadow being on the commit path
  means primary's commit latency depends on walshadow's RTT;
  cross-region walshadow witness is feasible but only useful if
  the application tolerates that latency.
