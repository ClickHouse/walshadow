# PHASE15 — propagate source schema changes through to CH

Source DDL touches `pg_class` / `pg_attribute` / `pg_type` / `pg_index`,
the catalog tracker
([`src/catalog_tracker.rs:182`](../src/catalog_tracker.rs)) fires
[`ShadowCatalog::invalidate`](../src/shadow_catalog.rs), the next
descriptor lookup re-reads from shadow PG. Source-side coverage is
clean: shadow's catalog tracks source's catalog within ms thanks to
PHASE13's streaming wire

PHASE14 closes two CH-side gaps that ride catalog signals: read-time
defaults ([phase14/01](phase14/01-read-time-defaults.md)) and
`TRUNCATE` ([phase14/03](phase14/03-truncate.md)). After PHASE14
lands, the remaining holes are the **shape-mutating DDLs**
(`ALTER TABLE ADD/DROP/RENAME COLUMN`, `CREATE TABLE`) plus
`DROP TABLE` — every DDL that the static-TOML mapping cannot model
without operator intervention. They all want the same plumbing: a
generalised catalog-event channel out of `ShadowCatalog` plus a
PG → CH type bridge

Today's CH-side mapping is **static TOML**
([`src/ch_emitter.rs:106-365`](../src/ch_emitter.rs)) loaded once at
boot, SIGHUP-reloadable per [PHASE10](PHASE10.md). For shape-mutating
DDL that means:

- `ALTER TABLE foo ADD COLUMN c int` on source requires operator to
  pre-add `c` to TOML **and** run `ALTER TABLE foo ADD COLUMN c Int32`
  on CH before the DDL ships, otherwise post-ALTER rows lose `c`
  (silent `NULL` per
  [`src/ch_emitter.rs:417-426`](../src/ch_emitter.rs)). PHASE8's
  add-column drill pins this — the test pre-declares the post-ALTER
  shape in its mapping
- `ALTER TABLE foo DROP COLUMN c` flips `RelAttr.dropped = true`;
  mapping still references the now-dropped attnum, encoder keeps the
  CH column populated with whatever the stale descriptor returns
- `ALTER TABLE foo RENAME COLUMN c TO d` is invisible — TOML's
  `target_name` is operator-owned, source rename doesn't touch it
- `CREATE TABLE pub.new_t (...)` requires operator to extend TOML
  + run CH `CREATE` before any source insert lands, otherwise the
  daemon errors with `no table mapping` on first row
- `DROP TABLE pub.foo` heap-deletes `pg_class`/`pg_attribute` rows;
  `pg_class_decoder` invalidates the descriptor cache, but the
  emitter is never told the relation went away. CH dest keeps its
  table; operator-side schema drift accumulates. PHASE8 named this
  as a followup, no code lands today

Operator's contract today: every shape-mutating DDL = source `ALTER` +
CH `ALTER` + TOML edit + SIGHUP, all coordinated by hand. `DROP TABLE`
is worse — CH-side stays around forever unless the operator notices

Goal: source DDL on a tracked relation echoes to CH automatically,
end-to-end, with the tail of the workload landing against the
post-DDL CH shape inside one xact boundary of the source ALTER's
commit. TOML stops being the mapping source-of-truth for tracked
namespaces; remains an override + per-column rename channel

## Coordination with PHASE14 + post-PHASE14 topology shift

PHASE14 closed (commit `142681a`). The relevant artefacts §1–§6
consume:

- **`RelAttr.missing_text: Option<String>`** (not `missing_value` as
  this plan originally said). PG `attmissingval` is `anyarray`, so
  shadow fetch casts to `text` & the field carries the typoutput form.
  `heap_decoder::missing_value_for(att) → ColumnValue` is the
  text → typed resolver. §3 needs a further `ColumnValue → CH SQL
  literal` writer (the emitter today encodes via codecs, not SQL; no
  existing function renders a `ColumnValue` as a `DEFAULT` clause)
- **`HeapOp::Truncate`** + the pre-decode intercept in
  [`BufferingDecoderSink::on_record`](../src/decoder_sink.rs) — §6's
  pg_class heap_delete intercept lives at the same fan-out point
- **Subxact lineage** ([phase14/05](phase14/05-subxact-rollback.md))
  — `XactBuffer::commit/abort` k-way merges per-subxid buffers in
  source_lsn ASC order. §6's `DrainEntry::Catalog` events must thread
  the same merge so catalog events nested under an aborting sub roll
  back too. Catalog events buffered under a subxid that aborts must
  drop; catalog events under a sub that commits drain in WAL position
  alongside the sub's heap writes

Post-PHASE14 the pump → emitter chain restructured behind
[`QueueingRecordSink`](../src/queueing_record_sink.rs) (commits
`b6cf510` + `a546be5`). Today's topology:

```
pump task                              worker task
─────────                              ───────────
RecordBytesSink (shadow wire bytes)    BufferingDecoderSink → XactBuffer → Emitter
   ↓                                       ↑
   └─── mpsc<Vec<Record<'static>>> ───────┘
```

The pump task only stamps wire bytes through the shadow `RecordBytesSink`;
decoder, xact buffer, & emitter run in a sibling worker task draining
the mpsc. PHASE15 §1's event channel + §2's applicator gate must live
**inside the worker task** — blocking the pump on a CH ALTER ack
deadlocks the shadow-wire feed that the catalog refetch in §1 depends
on. `advance_idle(lsn)` (the post-`a546be5` quiescent-stream ack)
means catalog events on a low-traffic source need an idle path too
— see §6 below

DROP TABLE moved from PHASE14 into PHASE15 §6 because the
event-channel + xact-buffer-drain plumbing it needs is the same
plumbing §1 + §2 + §6 already ship together

## Strategy

Six pieces. §1 + §2 are the substrate (event channel + applicator);
§3 is the type bridge that gates the per-DDL drills; §4 + §5 + §6 are
the per-DDL drills themselves (ADD/DROP/RENAME COLUMN, CREATE TABLE,
DROP TABLE)

### 1. Schema-change event channel

`ShadowCatalog::invalidate`
([`src/shadow_catalog.rs:353`](../src/shadow_catalog.rs)) bumps a
generation counter + drops cache. No consumer is told **what**
changed. The CH applicator needs to know which relation's descriptor
diverged so it can diff old vs new and emit one ALTER per diff; the
DROP TABLE drill in §6 needs the same channel shape for the
"relation went away" signal

Extend `ShadowCatalog` with a per-relation event channel:

```rust
pub enum SchemaEvent {
    /// First time we see this oid (CREATE TABLE, attach-time
    /// discovery, or post-DROP re-CREATE). `desc` is the freshly
    /// fetched descriptor
    Added { desc: Arc<RelDescriptor> },
    /// Descriptor diff against the previously-cached version. The
    /// diff is *resolved* — emitter doesn't re-walk attributes,
    /// just consumes the action list
    Changed {
        old: Arc<RelDescriptor>,
        new: Arc<RelDescriptor>,
        diff: SchemaDiff,
    },
    /// `pg_class` lookup returned zero rows for an oid we used to
    /// know about. Carries the last-known name so consumers can
    /// route DROPs without a now-impossible re-fetch
    Dropped { oid: Oid, qualified_name: Arc<str> },
}

pub struct SchemaDiff {
    pub added_columns: Vec<RelAttr>,
    pub dropped_columns: Vec<i16>,         // attnums
    pub renamed_columns: Vec<(i16, String, String)>, // attnum, old, new
    pub type_changes: Vec<(i16, RelAttr)>, // see §3 on rejection
}

impl ShadowCatalog {
    pub fn subscribe(&mut self) -> tokio::sync::mpsc::Receiver<SchemaEvent>;
}
```

Wire-up:

- `invalidate` keeps today's coarse-fire semantics for the decoder
  (cache clears, next lookup re-fetches)
- Descriptor fetch path
  ([`fetch_by_filenode`](../src/shadow_catalog.rs)) compares against
  the previously-cached descriptor by oid; builds `SchemaDiff` +
  emits `Added` / `Changed`
- `pg_class_decoder` ([`src/pg_class_decoder.rs`](../src/pg_class_decoder.rs))
  gets a heap_delete branch that pulls `relname` + `relnamespace`
  out of the to-be-deleted tuple **before** the cache invalidation
  trigger fires, surfaces them through `ShadowCatalog` as a
  `Dropped { oid, qualified_name }`. Sourcing the name from the
  decoder's own read of the dying tuple sidesteps the cache-miss
  problem — a relation walshadow has never queried via shadow still
  has its name available off the WAL record itself
- Backstop: a `sweep_dropped` loop ticked off generation bumps polls
  shadow's `pg_class` for the previously-known oid set; any oid that
  disappears without a corresponding decoder-side `Dropped` event
  (e.g. a drop that landed during a reconnect window) emits
  `Dropped` with `qualified_name` resolved off the last-cached
  descriptor
- Channel is `mpsc::channel(64)` per subscriber. Subscriber runs
  inside the worker task that owns the `BufferingDecoderSink`; the
  decoder fan-out is local to that task, so a slow applicator only
  stalls the worker (back-pressuring the queueing channel), never
  the pump's wire-bytes path
- Rename detection is `position-match + name diff` on `RelAttr`:
  the dropped + added attnums correlate when the attnum order is
  preserved and one column's name changed. Heuristic; reverts to
  `dropped + added` when ambiguous

Why through `ShadowCatalog` and not `CatalogTracker`? The tracker
sees WAL records but not the resolved descriptor — it knows
something in `pg_class` moved, not what the post-ALTER tuple shape
is. Resolution requires the libpq round-trip through shadow that
`ShadowCatalog` already owns

### 2. CH-side DDL applicator

New module `src/ch_ddl.rs`. Owned by the worker task (same task as
`BufferingDecoderSink` + `XactBuffer` + `Emitter`); a separate
applicator task is rejected because the post-DDL `await_ready` gate
would otherwise cross a task boundary the worker already snapshots
synchronously. Applicator subscribes to `ShadowCatalog`'s
`SchemaEvent` channel & opens its own `clickhouse_c_rs::Client`
(separate from the emitter's per-replica connection so DDL doesn't
ride the INSERT pump's backpressure path), converting each event
into the corresponding CH SQL:

| event | CH SQL | notes |
|---|---|---|
| `Added` | `CREATE TABLE IF NOT EXISTS ...` | skipped when no mapping rule covers the namespace; see §5 |
| `Changed.added_columns` | `ALTER TABLE ... ADD COLUMN ...` | one ALTER per added column, in attnum order |
| `Changed.renamed_columns` | `ALTER TABLE ... RENAME COLUMN ...` | applied before ADD/DROP so the post-rename name is the canonical one |
| `Changed.dropped_columns` | `ALTER TABLE ... DROP COLUMN ...` | guarded — see open question on data-preserving drops |
| `Changed.type_changes` | rejected, surfaces as error | see §3 — type widening is a future drill |
| `Dropped` | `DROP TABLE ...` | gated on `--drop-table-strategy`, see §6 |

DDL is idempotent: `IF NOT EXISTS` / `IF EXISTS` everywhere, so a
daemon restart that re-fires `Added` events for already-extant tables
is a no-op. CH-side DDL replication (`ON CLUSTER`) is a config flag,
not the default; v1 assumes single-shard CH

**Ordering invariant** the applicator must hold: a `Changed` event
for relation R must commit on CH **before** the first INSERT that
encodes against R's new shape lands on CH. Implementation:

- Applicator owns a `tokio::sync::Notify` keyed on `(rfn,
  generation)`, fired when the in-flight DDL for that
  rfn-at-generation acks
- Emitter's `route_with_retry`
  ([`src/ch_emitter.rs`](../src/ch_emitter.rs)) calls
  `applicator.await_ready(rfn, generation).await` before the first
  `send_data` of any block under that descriptor generation. Both
  emitter & applicator are worker-task-local — the await is a same-task
  notify wait, not a cross-task channel hop
- A descriptor that hasn't moved (no `Changed` event since the
  applicator booted) is "ready" trivially — the gate only blocks
  when an unacked DDL is in flight for that exact rfn+generation
- Worst-case latency = CH ALTER round-trip (~ms on MergeTree). Bumps
  p99 on the post-ALTER xact, not a correctness break

**`TupleObserver` widening for catalog events.** `XactBuffer::commit`
takes `observer: &mut O: TupleObserver` & calls `observer.on_tuple` per
`CommittedTuple`, `observer.on_xact_end` per commit
([`src/xact_buffer.rs:708`](../src/xact_buffer.rs)). Catalog events
buffered alongside user-data writes (see §6) need a parallel callback;
extend the trait with `on_schema_event(&SchemaEvent) -> Result<()>`.
The k-way merge in `commit` iterates `DrainEntry` in `source_lsn`
order (§6), dispatching `Tuple` → `on_tuple`, `Catalog` →
`on_schema_event`. Observer impl in `Emitter` calls the
worker-local applicator from `on_schema_event`, then the gate fires
for the next `on_tuple` to await

### 3. Type-system bridge

`TableMapping.ColumnMapping.target_type` is a raw CH type string the
operator writes
([`src/ch_emitter.rs:267`](../src/ch_emitter.rs)). For automated DDL
the daemon needs the inverse: given a `RelAttr` (PG type_oid +
typmod + nullable + PHASE14 §1's `missing_text`), produce a CH type
string + a default-expression literal

New `src/type_bridge.rs`. Greenfield — no existing PG → CH classifier
to extend (the plan's earlier reference to a `src/type_map.rs` was
incorrect; today's type knowledge sits ad-hoc inside `ch_emitter.rs`'s
`TypeAst::parse` & `heap_decoder::missing_value_for`'s match arms).
The bridge is the canonical mapping table:

| PG type | typmod meaning | CH type |
|---|---|---|
| `int2/4/8` | — | `Int16/32/64` |
| `bool` | — | `Bool` |
| `numeric(p,s)` | `((p << 16) | s) + VARHDRSZ` | `Decimal(p,s)`, fallback `String` if `p > 76` |
| `varchar(n)` / `bpchar(n)` | `n + VARHDRSZ` | `String` (no CH length cap) |
| `text` / `bytea` | — | `String` |
| `timestamp` / `timestamptz` | precision | `DateTime64(p, 'UTC')` |
| `date` | — | `Date32` |
| `uuid` | — | `UUID` |
| `jsonb` / `json` | — | `String` (json-in-string; CH `JSON` opt-in via config) |
| `array(T)` | per-element | `Array(T_ch)` (recurse) |
| `inet` / `cidr` | — | `String` |
| user composite / domain | — | `String` (fallback, matches PHASE9's `PgPending` path) |

Nullable mapping: `RelAttr.not_null = false → Nullable(T)`. Primary
key + replident-default columns stay non-nullable per CH MergeTree
ordering-key rules. Default expressions: if PHASE14 §1's
`missing_text` is `Some`, route through
`heap_decoder::missing_value_for(att) → ColumnValue`, then through a
new `type_bridge::column_value_to_sql_literal(&ColumnValue, &CHType)
-> String` writer that renders the literal in CH SQL form (`'…'` for
strings, numeric forms for ints/decimals, `toDateTime64('…', 6,
'UTC')` for timestamps, etc.). Emitted as
`DEFAULT <literal>` in the CH `ADD COLUMN` clause. PG's
`PgPending` fallback (Tier 3) renders as a `DEFAULT` against the
text form via CH `parseDateTime…` / `toNumeric…` casts where the
target type has a string-input form; types lacking one fall through
to `UnsupportedType` & operator-side TOML override. Otherwise let
CH apply its type-default

The literal writer is the load-bearing new surface — the existing
emitter encodes values via the Tier 1/2 codec stack into the CH
native wire format, never as SQL text — so it lives in
`type_bridge` alongside the type matrix, not in the emitter

Failure mode: a PG type with no bridge entry surfaces an
`UnsupportedType { oid, type_name }` error from §1's
`Added`/`Changed` event handler. Applicator logs + skips the DDL
(does not crash). Operator either adds a TOML override mapping for
the column or installs a typmap plugin. Either way, the daemon
continues serving traffic for already-bridged relations

This is the chunk that gates everything else: §4 + §5 both assume
`type_bridge::map(rel_attr) → (CHType, Option<DefaultExpr>)` exists

### 4. ALTER TABLE ADD/DROP/RENAME COLUMN

Smallest end-to-end drill. Source:

```sql
ALTER TABLE public.orders ADD COLUMN ship_at timestamptz;
INSERT INTO public.orders (..., ship_at) VALUES (..., now());
```

Flow:

1. Source WAL writes pg_class + pg_attribute heap records, catalog
   tracker fires `invalidate`
2. Next decoder lookup for `orders` rfn re-fetches descriptor;
   `ShadowCatalog::fetch_by_oid` notices the new `RelAttr`, builds
   `SchemaDiff { added_columns: [ship_at], .. }`, emits
   `SchemaEvent::Changed`
3. §2 applicator dequeues, calls `type_bridge::map` →
   `DateTime64(6, 'UTC')`, runs
   `ALTER TABLE default.orders ADD COLUMN ship_at Nullable(DateTime64(6, 'UTC'))`
4. ALTER ack arrives, applicator fires the `(rfn, new_generation)`
   notify
5. Emitter's `await_ready` returns; INSERT xact drains through the
   post-ALTER `TablePlan` rebuilt from `(rfn, new_generation)`

Pre-existing rows on CH read NULL for `ship_at` (CH `Nullable`
default). When the source ALTER carries `DEFAULT k`, PHASE14 §1's
`missing_text` carries k's typoutput form through `RelAttr`; §3's
bridge resolves it via `missing_value_for` + the literal writer & emits
`DEFAULT k` in the CH `ADD COLUMN` so pre-existing CH rows resolve to
k matching source's read-time-default behaviour

DROP COLUMN flow is the mirror: `dropped_columns` triggers
`ALTER TABLE ... DROP COLUMN ...` — see open question on retention
semantics. RENAME COLUMN runs before ADD/DROP so position-matched
renames don't trip the diff heuristic into a drop+add pair

Edge case: the INSERT xact may commit on source **before** the
applicator's ALTER ack returns from CH. The `await_ready` gate (§2)
holds the drain. Bounded by CH ALTER latency (~ms)

### 5. CREATE TABLE + namespace tracking

Today's mapping is whitelist: only namespaces named in TOML
replicate. CREATE TABLE in an unmapped namespace stays unmapped.
PHASE15 keeps the whitelist semantics but extends them to a
**namespace pattern** match so a tracked namespace auto-discovers
new tables

New config block:

```toml
[namespace."public"]
target_database = "default"
auto_create = true       # default false: CREATE on source → no-op on CH
type_overrides = [        # optional per-column overrides
  { table = "events", column = "payload", type = "JSON" },
]
order_by_default = "(_lsn)"   # ORDER BY clause when no PK on source
engine_default = "ReplacingMergeTree(_lsn)"
```

`auto_create = true` enables the §2 applicator's `Added` path for
that namespace. Per-table TOML (`[table."public.foo"]`) still
overrides — explicit column-list wins over auto-derivation

**Resolver shape for PHASE16 reuse.** Today the emitter's
`MappingHandle` is `Arc<RwLock<HashMap<String, TableMapping>>>`
([`src/ch_emitter.rs:971`](../src/ch_emitter.rs)) swapped wholesale on
SIGHUP. PHASE16 §3 wants `watch::Receiver<Arc<ResolvedConfig>>`
fed by a precedence merge of CLI > WAL-config > TOML. PHASE15
lands `auto_create` / `type_overrides` / `order_by_default` /
`engine_default` consumable through the watch shape from day one:

```rust
pub struct ResolvedConfig {
    pub tables:     HashMap<String, TableMapping>,
    pub namespaces: HashMap<String, NamespaceMapping>, // §5
    pub global:     GlobalMapping,                     // §6's drop_strategy + future PHASE16 keys
}
```

Today's only producer is the TOML loader; PHASE16 plugs the
WAL-config decoder + CLI override at the same merge point without
changing emitter/applicator consumers. Emitter
snapshots `*watch_rx.borrow()` at xact-start (post-PHASE16 §4
ordering — config flips inside an xact apply to the next xact);
applicator does the same at `Added`/`Changed` dispatch. SIGHUP
republishes the merged snapshot; the `RwLock` swap path goes
away

CREATE-TABLE SQL the applicator renders:

```sql
CREATE TABLE IF NOT EXISTS default.orders (
    id Int64,
    ...
    _lsn UInt64,
    _xid UInt32,
    _op Enum8('insert' = 1, 'update' = 2, 'delete' = 3),
    _commit_ts DateTime64(6, 'UTC')
) ENGINE = ReplacingMergeTree(_lsn)
  ORDER BY (id);
```

`ORDER BY` derivation: source PK columns when present; falls back to
`order_by_default` from config. PG composite PK becomes CH tuple
order-key in declaration order. No PK + no `order_by_default` →
applicator logs + skips with `NoOrderByKey`

Operator's flow becomes: declare the namespace once, every CREATE
TABLE on source materialises on CH within a round-trip. Explicit
TOML stays available for per-column renames, type pinning,
exclusion (`skip = true` on a `[table.…]` block)

Bootstrap: `ShadowCatalog::seed_from_source` on first connect emits
one `Added` per relation in a tracked namespace, so a daemon
attached to a non-empty source materialises the schema before any
data lands. Pairs with the [Phase 12](PLAN.md#phase-12--backfill-bridge)
backfill bridge: backfill consumes the post-CREATE CH shape, no
separate "set up CH first" step

### 6. DROP TABLE

Source `DROP TABLE t` heap-deletes the `pg_class` row for `t`,
heap-deletes the `pg_attribute` rows, drops the relfilenode files.
`pg_class_decoder` sees the heap_delete + invalidates the descriptor
cache today, but nothing surfaces to the emitter. CH-side schema
drift accumulates one table at a time. Closes the
[PHASE8](PHASE8.md) followup

Flow:

1. `pg_class_decoder` heap_delete branch (per §1) pulls
   `(rel_oid, namespace, name)` out of the dying tuple, emits
   `SchemaEvent::Dropped { oid, qualified_name }` into the channel
2. The xact buffer's drain queue carries the event alongside
   per-row writes — see "ordering against in-flight writes" below
3. §2 applicator dequeues at drain time, matches `qualified_name`
   against the `MappingHandle`. Three policy paths via
   `--drop-table-strategy`:

| `--drop-table-strategy` | mapped relation | unmapped relation |
|---|---|---|
| `retain` (default) | drop the in-memory encoder; CH stays | log INFO, no-op |
| `drop` | `DROP TABLE IF EXISTS <dest>` + drop encoder | log INFO, no-op |
| `warn` | log WARN, drop encoder; CH stays | log WARN, no-op |

Per-table TOML overrides the global flag
(`[table."ns.foo"] on_drop = "drop"`). Default `retain` because a
silent CH-side drop is operationally surprising; operators
explicitly opt in once they've vetted the upstream-DROP semantics
against their CH consumers

**Ordering against in-flight writes.** DROP TABLE on source
completes after every heap write the operator issued before it; PG
xact ordering keeps them atomically before the catalog DELETE.
walshadow's xact buffer
([`src/xact_buffer.rs`](../src/xact_buffer.rs)) drains writes in
WAL order; the catalog DELETE arrives at the same xact's commit.
The emitter must process writes-then-drop within the drain. Lift
the per-xid buffer entry (today `SpillEntry` carrying tuple bytes
+ chunk fragments) so it also carries catalog events keyed on
`source_lsn`:

```rust
enum DrainEntry {
    Tuple(CommittedTuple),
    Catalog(SchemaEvent),
}
```

`XactBuffer::commit`'s k-way merge across per-subxid buffers
([`src/xact_buffer.rs:776`](../src/xact_buffer.rs)) iterates
`DrainEntry` in `source_lsn` ASC order; tuples route through
`observer.on_tuple`, catalog events through
`observer.on_schema_event` (§2's `TupleObserver` widening). Pre-DROP
writes land on the still-extant CH dest, then the DROP runs

**Subxact abort interaction.** PHASE14 §5 lets a sub-xact buffer
its own writes keyed on `subxid`; `XactBuffer::abort` discards the
sub's buffer entirely. Catalog events stamped under that subxid
must drop with the heap writes — the `pg_class_decoder` heap_delete
intercept inserts the `Dropped` event into the **subxid's**
per-xid buffer, not a global side-channel, so abort cleanup is
automatic. CREATE-then-rollback-savepoint sequence: an `Added`
event followed by a sub-rollback discards both, no CH-side effect

**Quiescent-stream path.** `XactBuffer::advance_idle(lsn)`
(post-`a546be5`) advances `emitter_ack_lsn` when no xact is
active. Catalog events on truly idle streams (DROP TABLE in a
single-stmt autocommit on an otherwise-quiet source) still arrive
through a one-tuple xact's commit path; no separate idle dispatch
needed. If a future workload demonstrates DROP-only xacts being
held up by the queueing worker's batch threshold (64 records),
revisit with a per-event flush hint on the queueing sink

Risks + edge cases:

- **CREATE TABLE then DROP TABLE in the same xact.** Single xact
  drain runs after commit; the `Added` event from the CREATE
  precedes the `Dropped` from the DROP in WAL order. Applicator
  sees both; if `auto_create` is on, it runs CREATE then DROP. If
  `auto_create` is off, neither fires CH-side (the mapping never
  knew about the relation). Verified by an integration test
- **`DROP TABLE a CASCADE` removing dependents.** Each dependent
  relation emits its own pg_class heap_delete; §1 fans out one
  `Dropped` per relid. Applicator handles them independently.
  Constraints between dependents are CH no-ops anyway
- **Drop of an unmapped relation.** Logged at INFO, no CH side
  effect. Operator's TOML stays valid; future re-CREATE under the
  same qualified name produces an `Added` that the mapping
  resolves normally
- **Re-CREATE under the same qualified name after a `retain`-mode
  drop.** §1 emits `Added` for the new oid; §2 runs
  `CREATE TABLE IF NOT EXISTS` which ack-skips (CH dest still
  exists). Subsequent `Changed` events reconcile any shape drift
  through ALTER. End state matches "source DROP + CREATE" cleanly
  even though CH never saw the DROP

## What stays (anti-goals)

Pre-1.0: backwards compat with existing TOML / `MappingHandle` /
emitter signatures isn't a goal. Existing tests rewrite as the
shape changes; the namespace + resolver edits land as one cut,
not an additive layer

- **No CH `Replicated` engine assumption**. DDL applicator runs
  one ALTER per replica via the emitter's existing per-replica
  client. CH-cluster fan-out (`ON CLUSTER`) is a config flag, not
  the default
- **PG triggers / generated columns / partitioning DDL** stays in
  the "known correctness gaps" bucket. Partitioned-table support
  in particular touches multiple subsystems (decoder ATTACH/DETACH
  PARTITION handling, applicator routing) and lands as its own
  phase
- **Decoder catalog API** stays at one round-trip per refetch.
  The event channel is additive on the producer side
  (`ShadowCatalog::fetch_by_*` emits a `SchemaEvent` after the
  fetch); the existing `relation_at` consumer path stays
  unchanged
- **No retroactive schema migration**. A `Changed` event reshapes
  the CH dest forward only. Old rows in CH already encoded under
  `old`'s shape keep their column values; new rows encode under
  `new`. CH `Nullable` semantics + `DEFAULT` on the added column
  cover the read side. Backfilling pre-ALTER rows with the new
  column's value is out of scope (mutation, slow, operator-driven)
- **TRUNCATE + read-time defaults**. PHASE14's territory. PHASE15
  consumes PHASE14 §1's `RelAttr.missing_text` through §3's type
  bridge but doesn't reshape TRUNCATE plumbing — TRUNCATE rides the
  `HeapOp::Truncate` path PHASE14 §3 lit up, not the catalog
  channel §1 introduces

## Open questions

- **Type widening on `ALTER COLUMN TYPE`.** PG allows
  `ALTER COLUMN x TYPE bigint USING x::bigint` against an int4
  column. CH `MODIFY COLUMN` accepts int32 → int64 via mutation.
  Cross-family changes (text → int) don't map cleanly. PHASE15
  rejects type changes with `UnsupportedSchemaChange`; operator
  handles the migration out-of-band. A future phase can extend §3
  with a widen-only matrix
- **DROP COLUMN with retained data on CH.** PG flips
  `pg_attribute.attisdropped = true` and emits NULLs for the
  column on subsequent reads. CH `DROP COLUMN` is destructive
  (loses every row's value). Two operator-visible behaviours:
  `drop` = `ALTER TABLE ... DROP COLUMN` (data loss accepted),
  `stop_encoding` = `ALTER COLUMN x MODIFY DEFAULT NULL` + stop
  emitting the column (column stays for historical rows). Config
  switch: `namespace.<ns>.drop_columns = "stop_encoding" | "drop"`.
  Default `stop_encoding` for v1 — silent destructive DROP is
  surprising
- **Schema-event ordering across multiple xacts**. Source can
  commit two DDLs in flight inside the same WAL window (e.g.
  `ADD COLUMN c1` xact, `ADD COLUMN c2` xact, both before
  walshadow's next descriptor refresh). `ShadowCatalog::invalidate`
  collapses both into one cache drop; the next fetch sees the
  post-c2 shape, §1 emits a single `Changed { old: 2-col, new:
  4-col, diff: { added: [c1, c2] } }` event. Applicator handles it
  as two ALTERs in attnum order. Matches the "atomic two-DDL
  block" semantic an operator would expect
- **Race: source CREATE TABLE + immediate INSERT in same xact.**
  Source emits pg_class write, then immediately writes user-heap
  records for the new oid, all inside one xact. Catalog tracker
  fires invalidate; descriptor refetch under
  `relation_at(rfn, commit_lsn)` finds the new oid (shadow has
  replayed the catalog write via the wire-driven gate). §1 emits
  `Added`; §2 applicator races against the decoder's attempt to
  route the INSERT. The `await_ready(rfn, generation)` gate from
  §2 holds; the INSERT routes after CH `CREATE TABLE` acks.
  Verify under a single-xact CREATE+INSERT integration test
- **Foreign-key / unique constraints**. CH has no FK enforcement.
  Mapping PG constraints onto CH = no-op for engine constraints,
  could emit comments. Out of scope; document as "constraints
  drop on the floor"
- **DDL on shadow itself.** Shadow PG replays source DDL via the
  wire (PHASE13 streaming). Any DDL walshadow issues (none today,
  PHASE15 doesn't add any either) would risk a shadow / source
  divergence. Strict invariant: walshadow never writes to shadow.
  The DDL applicator writes to CH only
- **Rename heuristic false positives.** Position-match + name-diff
  collapses to RENAME, but a DROP-then-ADD at the same position
  with a different name is indistinguishable from RENAME at the
  catalog level. PG's WAL distinguishes (rename writes one
  pg_attribute heap_update; drop+add writes a heap_delete + a
  heap_insert) but `ShadowCatalog::fetch_by_oid` sees only the
  post-state. With PHASE14 §5's subxact lineage now collapsing
  same-xact drop+add into a single descriptor refetch, the false-
  positive surface widened. Mitigation: per-table opt-out
  `[table."ns.foo"] schema_diff = "no_rename"` forces every
  attnum delta to render as drop+add. Default stays heuristic-on
  because pure-rename DDL is the common case
- **Spill format version bump.** `HeapOp::Truncate` (PHASE14 §3)
  + `DrainEntry::Catalog` (this phase) both add tags without a
  spill schema rev. Resume contract is "wipe on startup" so it's
  academic, but bump the format version to v2 with the §6 work
  for honesty — PHASE14 retro already flagged this as cleanup

## Acceptance

- **§4 drill**: `ALTER TABLE x ADD COLUMN c text` on source,
  followed by INSERTs that populate `c`, lands on CH with the new
  column auto-added and the new rows containing the supplied
  values. No TOML edit, no CH DDL run by the operator
- **§4 default drill**: `ALTER TABLE x ADD COLUMN c int DEFAULT 7`
  against a non-empty source table. CH `c` reads 7 for every
  pre-ALTER row (via PHASE14 §1's `missing_text` carried through
  §3's bridge) **and** the CH column ddl is auto-applied via §2.
  End-to-end this is PLAN §1's gating drill, with PHASE14
  responsible for the read-time-default decode and PHASE15
  responsible for the auto-DDL
- **§4 drop drill**: `ALTER TABLE x DROP COLUMN c`. Under
  `drop_columns = "stop_encoding"` (default), CH `c` stops
  receiving new values; existing rows preserve their `c`. Under
  `drop_columns = "drop"`, CH `c` disappears
- **§4 rename drill**: `ALTER TABLE x RENAME COLUMN c TO d`. CH
  column rename runs cleanly; subsequent INSERTs populate `d`.
  Rename heuristic test: position-match + name-diff = RENAME;
  unrelated attnum change = drop + add
- **§5 drill**: `CREATE TABLE pub.new_t (id int, body text)` in a
  namespace with `auto_create = true`. CH dest gets the matching
  table within one xact commit; subsequent INSERTs land cleanly.
  No-PK variant exercises `order_by_default`
- **§6 drop drill**: CREATE TABLE, INSERT 50 rows, DROP TABLE.
  Under `--drop-table-strategy=drop`: CH `EXISTS TABLE <dest>`
  returns false after drain. Under `=retain` (default): CH
  `EXISTS TABLE <dest>` returns true, `count(*) == 50`. Variant:
  `DROP TABLE a CASCADE` removes a parent + two FK-dependent
  tables; CH-side dependents drop cleanly under `=drop`. Variant:
  CREATE+DROP in one xact under `auto_create = true` is a no-op
  CH-side
- `cargo test --workspace --lib` stays green. New unit tests cover
  `type_bridge` (Tier 1/2/3 mappings + nullability + default
  rendering), the diff emitter inside `ShadowCatalog`, and the
  DDL applicator's SQL rendering
- New integration test `tests/phase15_ddl_replicates.rs`: spins
  source PG + shadow PG + CH, exercises the §4 + §5 + §6 drills
  against a real CH server (mirrors `bin_stream_e2e`'s harness
  shape from PHASE13 retro)
- Existing `phase8_add_column_replicates_pre_and_post_alter` test
  keeps working. The pre-declared-mapping path it exercises stays
  a tested path via `auto_create = false`; the auto-applied path
  is the new test's responsibility
- Existing TOML configs continue to load without warning;
  upgrade-in-place is a no-op when the operator doesn't add a
  `[namespace.…]` block

## Sequencing

PHASE14 has landed (commit `142681a`), so §3 already has its
prerequisite (`RelAttr.missing_text`). §1 (event channel) & §3
(type bridge) are independent & land in parallel. §2 (applicator)
depends on §1 + §3 + the `TupleObserver` widening for catalog
events. The per-DDL drills land against §2:

1. §4 (ALTER COLUMN) first — smallest test loop, the post-ALTER
   default drill carries PLAN §1's acceptance weight & exercises
   the literal-writer end-to-end
2. §6 (DROP TABLE) second — small mechanical add on top of §1's
   channel + §2's applicator + the `DrainEntry::Catalog` lift
   inside `XactBuffer::commit`'s k-way merge. Closes the PHASE8
   followup. Spill format bump to v2 lands here
3. §5 (CREATE TABLE + namespace + resolver shape) third — adds
   the CREATE-TABLE SQL renderer + lifts `MappingHandle` into
   `watch::Receiver<Arc<ResolvedConfig>>`. Lifts the namespace
   config alongside; PHASE16 plugs the WAL-config + CLI sources
   into the same merge point

Phase closes when the §4 default drill passes end-to-end against a
daemon configured with one `[namespace.…]` block & no per-table
overrides — that's the operator-flow demonstration that walshadow
owns CH-side schema as well as data

Size estimate: ~1300 LOC product (event channel ~150 + applicator
~220 + type bridge ~280 incl. literal writer + CREATE-TABLE renderer
~120 + namespace config + resolver shape ~200 + DROP-TABLE drain
wiring + spill-bump ~130 + `TupleObserver` widening ~80 + worker-task
plumbing ~120) + ~720 LOC tests. The literal writer & the resolver
shape are the new load-bearing surfaces; the rename heuristic +
`DrainEntry::Catalog` ordering against in-flight writes carry over
from the original sizing
