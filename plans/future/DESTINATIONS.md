# N:M ClickHouse destination mapping

Route N source relations to M ClickHouse destinations (distinct
endpoints / databases): fan-out (one source table → several CH
clusters) and fan-in (several source tables → one CH table). Large
net-new work, gated on the emitter growing a destination abstraction.

Companion [TABLESPACES.md](TABLESPACES.md) covers source-tablespace
correctness; its *Tablespace as an emitter-visible attribute* section
(§3 there) exposes `spc_node`, one of the routing keys §2 below can
match on. That's the only coupling — this proposal stands alone.

## 0. Today: 1 destination, baked in

The emitter holds exactly one CH connection: `Emitter.client:
Client<'static>` (`src/ch_emitter.rs:1312`) built from a single
`EmitterConfig { host, port, database, user, password }`
(`src/ch_emitter.rs:119-153`). `DdlApplicator` opens a second client to
the *same* endpoint (`src/ch_ddl.rs:143`). A relation maps to one
`TableMapping { target: String, columns }` (`src/ch_emitter.rs:264`),
keyed by `"<namespace>.<relname>"` in `route()`
(`src/ch_emitter.rs:1460`); `target` is copied verbatim into
`INSERT INTO {target} ... FORMAT Native` in `TablePlan::build`
(`src/ch_emitter.rs:580`). `NamespaceMapping.target_database`
(`src/ch_emitter.rs:283`) only picks a CH *database* on the one
endpoint, not an endpoint. There is no list of endpoints anywhere.

## 1. The baseline alternative: just run more daemons

Before designing in-process N:M, state the honest baseline: **run K
walshadow daemons, each with its own replication slot, each routing a
disjoint or overlapping relation subset to one destination.** This
gives true failure isolation (a dead CH cluster stalls only its own
daemon's slot) and zero new code. Its cost is K physical WAL reads on
the source and K shadow Postgres instances.

Integrated N:M is worth building only when the **shared cost
dominates** — one WAL read + one shadow + one decode feeding many
sinks. That holds when: WAL volume is high, destinations overlap
heavily (same tables to several places), or shadow/decode CPU is the
bottleneck. When destinations carry disjoint, low-overlap relation
sets, multiple daemons is the better answer. The design below should
not pretend otherwise; it earns its complexity on the shared-read case.

## 2. Routing model

Generalize the relation→target lookup from "one target string" to
"a routing decision producing zero or more `(destination, target)`
pairs." A route predicate matches on, in precedence order:

1. explicit per-table rule (qualified name) — finest
2. per-namespace rule
3. per-tablespace rule (`spc_node` from
   [TABLESPACES.md](TABLESPACES.md) §3; `0` = default)
4. global default destination

Predicates resolve to a **set** of destinations (fan-out) each with a
target table name. Fan-in (N source tables → 1 CH table) needs no new
mechanism: two source rules name the same `(destination, target)`;
`_lsn`/`_xid` synthetic columns already disambiguate provenance and
`_lsn` dedup keeps end-state correct (per
`[[walshadow-eventual-consistency]]`), provided the union schema is
compatible (operator's responsibility, validated at DDL time).

Tablespace as a routing key is supported but **weak**: tablespace is a
physical-storage choice (which disk), rarely aligned with logical
routing intent. Namespace or per-table rules are usually the right key.
The one real case is tablespace-per-tenant layouts; support it, don't
lead with it.

## 3. Destination abstraction

```rust
struct DestinationId(String);            // operator-named, stable

struct Destination {
    id:        DestinationId,
    endpoint:  ChEndpoint,               // host, port, user, password, compression
    database:  String,                   // default CH db for this dest
    insert:    Client<'static>,          // INSERT pump
    ddl:       DdlApplicator,            // per-dest, separate connection
    budgets:   EmitterBudgets,           // rows/bytes/deadline, per-dest
    ack_lsn:   AtomicU64,                // highest fully-sealed LSN to this dest
}
```

`Emitter` grows `dests: HashMap<DestinationId, Destination>` and the
per-table encoder state becomes per `(DestinationId, table)`. `route()`
fans the decoded row to each matched destination's encoder. Sealing,
budget trips, and reconnect (`src/ch_emitter.rs:1788`) all become
per-destination — a stall or reconnect on one destination must not
block sealing on another (modulo §4).

DDL fans out: a `SchemaEvent` for a relation routed to destinations
{A,B} applies to A's and B's `DdlApplicator` independently
(`src/ch_ddl.rs`), each gated by its own `await_ready`. Auto-create and
`drop_table_strategy` resolve per destination.

## 4. The hard part: ack accounting and slot advance

The slot advances on `min(shadow_replay, emitter_ack)` (overview /
`plans/ops.md`). With M destinations, `emitter_ack` becomes
`min` over all M destination `ack_lsn`. Therefore:

> **The slowest (or dead) destination bounds slot advance, and source
> WAL accumulates until it catches up.**

This is the central, unavoidable tension of single-slot fan-out. One
physical slot can be released only to the LSN every consumer has
durably accepted. Three responses, none free:

1. **Couple and stall (default).** `min` across destinations; a dead
   destination eventually triggers the existing CH-bounce retry budget
   (`plans/future/ch_bounce_recovery.md`) and kills the daemon, cursor
   resumes on restart. Simple, but one bad destination DoSes the
   source's WAL retention. Acceptable when destinations are co-located
   and equally trusted
2. **Decouple via spill.** Let a lagging destination fall behind by
   buffering its pending rows to the spill area
   (`{spill_dir}/dest-<id>/...`), advancing the slot on the *fast*
   destinations while the slow one drains from spill. Bounds source
   bloat at the cost of unbounded local disk for the laggard. Needs a
   per-destination spill budget and a kill switch when it's exceeded.
   This is the only way to get real isolation from one slot, and it is
   substantial work — effectively a per-destination durable queue
3. **Separate slots.** = §1's multiple-daemon baseline. Rejected for
   the integrated path; if you want independent slots, run independent
   daemons

Recommend shipping (1) first with a loud `walshadow_dest_lag_bytes{dest}`
metric and a per-destination `max_lag_bytes` that fails fast, then
(2) only if a deployment proves it needs isolation without paying for
multiple WAL reads.

## 5. Cursor schema

Cursor records a single emitter ack today (`plans/ops.md` five-field
layout; see also `[[inproc-harness-large-xact]]`). N:M needs
**per-destination ack LSN** persisted so restart resumes each
destination from its own position rather than re-emitting from the
global min:

```
destinations: Vec<{ id: String, ack_lsn: Lsn }>
```

Crucially, `_lsn` dedup makes per-destination cursors **forgiving**:
re-emitting rows a destination already has is idempotent (Replacing/
dedup on `_lsn`), so cursor precision trades disk/bandwidth, not
correctness. A coarse cursor (single global min) is *correct* but
re-ships data to ahead-destinations on restart; the per-destination
list is a bandwidth optimization, not a correctness requirement. Bump
cursor version; old cursors restore with a single-destination list
(missing field → `[{id:"default", ack_lsn: legacy_ack}]`).

## 6. Config surface

Destinations are connection descriptors — like `[ch]` today, they
describe how to reach an external system, so they stay in TOML/CLI, not
in the source-PG overlay (same boundary as
`[[runtime-config-from-pg]]` draws for connection params):

```toml
[destination.warehouse]
host = "ch-warehouse"; port = 9000; database = "wh"; ...
[destination.analytics]
host = "ch-analytics"; port = 9000; database = "an"; ...
```

Routing *rules* (which relation → which destination) are logical, not
connection state, so they belong in the runtime-config overlay once it
lands: extend `config_table` / `config_namespace` (see
`plans/future/runtime_config_from_pg.md` §1) with a `destinations
text[]` column. Until the overlay ships, rules live in TOML
`[table.…]` / `[namespace.…]` blocks with a new `destinations` field
(absent → the single default destination, preserving today's behavior
byte-for-byte). This makes §0 the zero-destination-config special case
of §2, so existing TOML keeps working unchanged.

## 7. What does not change

- decode, filter, shadow, xact buffer, TOAST reassembly — all upstream
  of the emitter, destination-agnostic. N:M is an emitter-and-cursor
  change
- per-destination ordering and dedup semantics are identical to today's
  single-destination guarantees; eventual consistency via `_lsn`
  (`[[walshadow-eventual-consistency]]`) holds per destination
- a single source table to a single destination is unchanged

## Phasing

- **§2 + §3** routing model + `Destination` abstraction, default-only
  config so behavior is unchanged, behind a feature gate
- **§4 response (1)** coupled-stall + `walshadow_dest_lag_bytes` metric
- **§5** cursor schema bump
- **§6** TOML rules, then overlay column once runtime-config lands
- **§4 response (2)** per-destination spill — only if isolation demand
  is proven

## Dependencies

- §6's logical routing rules depend on
  `plans/future/runtime_config_from_pg.md` for the overlay column;
  TOML-only rules have no dependency
- §5 reuses the cursor version-bump discipline from
  `plans/future/two_phase_commit.md` (old cursor restores cleanly into
  new daemon)
- tablespace routing key (§2.3) depends on
  [TABLESPACES.md](TABLESPACES.md) §3 exposing `spc_node` on the
  `RelDescriptor`; the other routing keys (namespace, table) have no
  such dependency

## Open questions

- **Per-destination DDL divergence.** If destination A auto-creates a
  table and B has `drop_table_strategy = retain` with a pre-existing
  incompatible schema, the two destinations diverge. Surface per-dest
  DDL outcomes; do not let one destination's DDL failure stall the
  other's INSERT pump
- **Fan-in schema conflicts.** Two source tables → one CH table with
  incompatible column sets. Validate union compatibility at route-plan
  build (`TablePlan::build`) and reject with a clear metric rather than
  emitting malformed blocks
- **Slot-advance starvation under spill (§4.2).** A destination that
  never catches up grows spill without bound; the `max_lag_bytes`
  kill must fire before disk fills, and the operator must be told which
  destination forced it
- **Is the integrated path ever right?** §1 is the honest default.
  Pin the crossover with measurement: WAL volume × destination overlap
  where one-read-fan-out beats K daemons. Don't build §3+ until a real
  deployment lands past that line
