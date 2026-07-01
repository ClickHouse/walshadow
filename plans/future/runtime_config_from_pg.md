# runtime config from source PG

Move walshadow's runtime config out of TOML and into source-PG tables.
Config rows are written via SQL by the same DBA who owns source schema,
replicated through the WAL
stream walshadow's daemon already decodes, and applied atomically at
each row's commit LSN. Post-config xacts route against the
post-config shape under the same barrier fence the catalog
applicator uses for DDL. TOML stays authoritative for bootstrap
(connection params) and as the only config surface when WAL processing
is blocked or the overlay table is absent

**Feature is opt-in via one TOML field.** `[runtime_config] schema =
"…"` names the source-PG schema housing the overlay tables. Empty
string or field omitted means overlay is disabled — daemon runs pure
TOML, never touches `pg_class` lookups for overlay rels, never
classifies their WAL writes. Same flag controls whether the
config_decoder attaches and whether `pg_logical_emit_message` signals
are scanned. One switch turns the whole subsystem off so deployments
without the config schema installed keep working untouched

## Strategy

Eight pieces. Substrate first (§1 + §2), signal channel (§3),
per-table opt-in + backfill (§4), resolver (§5), runtime channel
(§6), bootstrap (§7), failure containment (§8)

### 1. `<schema>.*` config tables

Versioned SQL install script creates config tables under an
operator-named schema (TOML `[runtime_config] schema`, typical value
`walshadow`), run by the DBA who owns source schema. Daemon never writes
them, preserving walshadow's read-only-source posture (see anti-goals).
Five tables:

- `config_global` — single-row, emitter compression/budgets, retry
  knobs, DDL strategy, retention, sampling, **batch size (rows +
  bytes), flush timeout, default CH SETTINGS JSONB applied to every
  INSERT/CREATE TABLE**
- `config_namespace` — per target DB, `auto_create`, engine +
  order_by defaults, `drop_columns` mode, namespace-scoped CH
  SETTINGS overlay
- `config_table` — per qualified relation (text key, not rfn —
  rfn unknown at row-insert time for forward-declared tables),
  `target`, `skip`, `on_drop`, `order_by`, `engine`, table-scoped
  CH SETTINGS overlay, `replicate` (bool, doubles as opt-in
  switch), `initial_load` (bool)
- `config_column` — per (rel, attname): `target` name, `target_type`
  override, **`exclude` flag (column dropped from emitted projection
  + DDL)**
- `config_signal_log` — append-only audit of accepted
  `pg_logical_emit_message` signals (lsn, prefix, payload, outcome).
  Daemon never writes; a PL/pgSQL helper (`<schema>.emit_signal`,
  shipped in the install script) inserts the audit row and calls
  `pg_logical_emit_message` in the same xact, so audit row and message
  share an LSN

Config tables carry a `PRIMARY KEY` (install script owns their DDL, see
above) so DELETE/UPDATE WAL includes the row key for `*Removed` events
under `RI DEFAULT`, matching preflight's post-debe55b key policy, no
`FULL` needed. New config values ride the logged new-tuple image;
unchanged columns come from the §7 seed merged per §6, so decoder never
needs the before-image. `FULL` permitted, not required. config_decoder
resolves each config table's relfilenode by name at attach time and
re-resolves the name→relfilenode binding on `pg_class`-write
invalidations (§2 details why rotation needs a binding refetch, not
WAL self-tracking), so no deterministic-OID mechanism is needed and
deployments need not agree on OIDs. Hard-coded namespace filter
short-circuits CH replication for the overlay schema (replicating
walshadow's own config to CH would be a footgun, not overrideable)

### 2. `config_decoder`

Parallel to `pg_class_decoder`, subscribes to heap-record stream at
same fan-out point, filters on the config-table relfilenodes,
decodes tuple body via existing Tier 1/2 codec stack, emits typed
`ConfigEvent` (`GlobalChanged`, `NamespaceUpserted`,
`NamespaceRemoved`, `TableUpserted`, `TableRemoved`,
`ColumnUpserted`, `ColumnRemoved`). No libpq round-trip per row —
decoder reads tuple bytes off WAL. Cost is one match-on-relfilenode
per heap write, amortised across rels that see writes on operator
timescale, not xact timescale.

**Filter set survives rotation via binding-refetch, not WAL
self-tracking.** TRUNCATE / VACUUM FULL / rewrite rotates a config
table's relfilenode; a filter frozen at attach time then silently
drops every later config write, config appears frozen with no error.
WAL-only tracking can't close this, for two reasons carried straight
from the mirrored `pg_class_decoder`: its harvest folds a rotated
filenode into the tracked set only for system catalogs (`oid <
FIRST_NORMAL_OBJECT_ID`), and config tables sit in a user schema (oid
≥ 16384) so never qualify; and TRUNCATE / VACUUM FULL leave pg_class
cols 1–7 unchanged, so the OID rides the un-logged prefix
(`DecodeOutcome::OidInPrefix`) and the record can't even name which
rel rotated. `XLOG_RELMAP_UPDATE` doesn't help (config tables aren't
mapped catalogs), nor does `seed_from_source` (startup-only). So
config_decoder subscribes to the same coarse `invalidation_epoch`
`CatalogTracker` bumps on any pg_class write and, on bump, re-runs the
name→relfilenode query for the five config rels — the `ShadowCatalog`
resolution path.

**Refetch is of the binding, not the content.** The O(rows) concern
bites only refetching config *content*: re-reading `config_column`
per invalidation is tens of thousands of rows in wide schemas. The
name→relfilenode *binding* is O(config tables) = five rows keyed by
name, re-run only when the epoch bumps (operator timescale); row
bodies still decode from WAL at O(1) per write. So the `ShadowCatalog`
refetch-on-invalidate pattern is adopted for the binding and rejected
for the content — the two are separable, and the cost argument only
ever bit the content

### 3. Signal channel via `pg_logical_emit_message`

Orthogonal to config-row state: imperatives that don't make sense
to store as a row (`flush_now`, `pause_emitter`, `resume_emitter`,
`force_reseed <rfn>`, `drop_slot_at_lsn <X>`, debug toggles).
Daemon's WAL pump already classifies `RmId::LogicalMsg = 21` records
(see `classify`) but discards body; this adds a
`message_decoder` parallel to `config_decoder` that filters on
configurable prefix (TOML `[runtime_config] message_prefix`,
default `walshadow`) and routes payload to a small command parser.
Unknown commands log at WARN and increment
`walshadow_signal_unknown_total{cmd=…}` — never crash.

**Signal source scoping.** `xl_logical_message` carries `dbId`, the
emitting session's `MyDatabaseId` stamped by `LogLogicalMessage`, not
settable from SQL. Physical replication delivers messages from every
database in the cluster, so `message_decoder` filters on `dbId` before
prefix. Global imperatives (`flush_now`, `pause_emitter`,
`drop_slot_at_lsn`, `force_reseed`) accept only from the database named
by TOML `[runtime_config] admin_database`, resolved to a database OID at
attach time by name lookup against `pg_database`, empty meaning the
source database. Point `admin_database` at a locked-down database and the
signal channel inherits PG's database CONNECT privilege as its gate,
`REVOKE CONNECT ON DATABASE <admin_database> FROM PUBLIC` plus GRANT to
operators, enforced daemon-side: a role holding `EXECUTE` on
`pg_logical_emit_message` in an app database emits under that
database's `dbId` and gets dropped. Xact-scoped signals
(`ignore-transaction`) are the exception, they must ride the source
xact carrying the DML so their `dbId` is always the source database and
their blast radius is self-scoped (a caller drops only its own xact).
Reading `dbId` needs the body parse the classifier today discards,
`dbId` precedes prefix in the record so the filter is cheap.

`pg_logical_emit_message(transactional bool, prefix text, content
text)` semantics honored: transactional messages drain at commit
LSN through the same `XactBuffer` ordering as heap rows;
non-transactional messages drain on receipt, used for "do it now"
signals where ordering against in-flight xacts doesn't matter
(maintenance toggles, metric resets). Parser splits the payload on
whitespace, shell-style: first token is the command, remaining
tokens are positional args (`force_reseed 16384`, `drop_slot_at_lsn
0/1A2B3C`). No JSON, no nesting; keeps signals greppable in logs and
typeable by hand at the SQL prompt.

Why messages, not config rows: stored-state config is wrong fit
for "do once" commands. A `flush_now` row would imply persistent
state; toggling it back and forth in a single transaction would be
incoherent. Messages are fire-and-forget at a defined LSN.

#### Xact-scoped drop: `ignore-transaction`

A distinct signal class, neither "act at the message LSN"
(flush/pause) nor a stored config row: a per-xact tag consumed at
that xact's commit drain. Use case: delete rows or drop partitions
on source while keeping them on CH. Wrap the destructive statements
in a transaction that also emits the tag; walshadow discards the
whole xact's CH-bound changes, cursor still advances.

```sql
SELECT pg_logical_emit_message(true, 'walshadow', 'ignore-transaction');
```

Must be transactional. PG forces xid allocation only for
transactional messages (`LogicalMessageInsert`, PG
`src/backend/replication/logical/message.c`), so a non-transactional
message carries no xid to key the drop on and delivers regardless of
the xact's fate. Transactional rides the same xact; PG stamps the
record's `xact_id` with the emitting (sub)xid. Payload is the single
command token `ignore-transaction`, no args; whitespace-split parser,
no JSON.

**Mechanism reuses abort.** Every xact already buffers per-xid in
`XactBuffer`; commit drains to CH via `drain_committed`, abort
discards via `abort`. `ignore-transaction` is "take the abort path
at commit, but still advance the cursor". Pieces:

- **Decode.** Realizes §3's `message_decoder` for this xact-scoped
  class, co-located in the buffering decoder sink rather than a
  standalone task so the poison flag lands before the same worker's
  reorder step processes the commit. `RmId::LogicalMsg` today only
  feeds the classify counter, body discarded. Parse
  `xl_logical_message` from the
  record's main_data, header size via the offsetof-equivalent
  (`SizeOfLogicalMessage` includes the pad after `bool
  transactional`; field-sum under-shoots), then `message` = prefix
  (`prefix_size`, trailing NUL included) ++ payload
  (`message_size`). Accept on transactional + prefix match + dbId ==
  source db. Home is `BufferingDecoderSink::on_record` (sees every
  record, runs before the reorder coordinator on the same worker via
  `DecoderXactPair`), branch ahead of the `route != ToDecoder` gate
  like the TRUNCATE special-case. Pure byte parse, no catalog lock,
  no replay gate
- **Poison flag on `XactState`, not a side set.**
  `XactBuffer::mark_ignore(xid)` lazily inserts the state and flips
  `ignore`. Storing on the state buys subxact/savepoint semantics
  for free: message in a rolled-back subxact drops with that
  subxact's `abort` (matches PG never delivering it), message in top
  or a committed subxact rides into the `states` collected at
  commit. Lazy insert covers the message-only and message-first xact
- **Drop at commit.** `drain_committed` (and legacy `commit`), after
  collecting states, if any `ignore`: unlink spill, discard heaps +
  catalog_events, return an empty `DrainedXact` at commit_lsn. That
  routes through the existing empty-commit branch, which registers a
  rows=0 seq so the contiguous ack watermark passes commit_lsn, slot
  recycles, nothing reaches CH

**Shadow untouched, only CH suppressed.** This makes
drop-partition-on-source-keep-on-CH correct. Catalog/DDL records
replay on shadow independently of the xact buffer
(`Route::ToShadow`), so shadow stays schema-consistent even for the
ignored xact, required because the decoder needs shadow's post-DDL
catalog for later xacts. Heap tuples and the CH DDL applicator's
`SchemaEvent`s both live in the same `XactState`, so dropping the
state drops both: a `DROP PARTITION` updates shadow's catalog but
issues no DROP/ALTER against CH. Dropped DELETEs leave rows on CH at
their last `_lsn`, consistent with the ReplacingMergeTree
convergence model

**Replay-safe without dedup.** Effect scoped to one xact, so restart
+ WAL replay from before the commit re-sees the message inside the
same xact and re-poisons identically. No last-signal-LSN checkpoint
(contrast `drop_slot_at_lsn` under Open questions); keying on the
xact rather than an LSN is the reason

**Not a `DrainEntry`.** Doesn't ride the `DrainEntry::Signal`
variant (§6) that transactional pause/resume use, it mutates buffer
state the commit drain already reads. Ordering within the xact is
irrelevant, the flag is read at commit not applied at the message
LSN, emit it first or last

**Blast radius self-scoped.** A caller can only drop replication of
the xact it writes, lower risk than the global signals under Open
questions ("Signal abuse"); gate at SQL permission on the function. Natural
generalization: `ignore-relation <oid>` / `ignore-changes-for <qname>`
filters only some rels out of the drained set, surgical when the
xact also carries changes to keep

### 4. Per-table opt-in and initial-load path

`config_table` row carries three distinct intents collapsed into one
relation: (a) override mapping for a table already in replication
scope, (b) forward-declare configuration for a table that doesn't
yet exist, (c) opt an existing-but-unreplicated table into scope
plus initial-load it. Resolver inspects `replicate` + `initial_load`
+ catalog state to dispatch:

| row state | rfn known? | table empty? | action |
|---|---|---|---|
| `replicate=t, initial_load=f` | yes | n/a | inclusion-list add; WAL-driven from current LSN, no backfill |
| `replicate=t, initial_load=t` | yes | yes | mark streaming, no backfill needed |
| `replicate=t, initial_load=t` | yes | no | enqueue backfill (see below) |
| `replicate=t` | no (forward-decl) | n/a | hold row, materialize when CREATE TABLE for matching qualname arrives via catalog applicator |
| `replicate=f` | yes | n/a | inclusion-list remove; mid-stream exclusion drains in-flight rows then halts further emission |

Keyed on **qualified name** (`namespace.relname`), not rfn. Resolver
maintains a `pending_decl: HashMap<QualifiedName, ConfigTable>`
populated from rows whose target rfn doesn't exist. Catalog
applicator notifies resolver on each new rel; resolver pops matching
pending entry and registers the rfn↔config binding. Stale entries
(rel never created, or dropped and recreated under different
namespace) tick `walshadow_pending_decl_rels{qname=…}` for ops
visibility

**Backfill path (snapshot-free, convergence via WAL replay).** No
`pg_export_snapshot`, no `SET TRANSACTION SNAPSHOT`, no logical slot.
Correctness rests on two walshadow-specific properties generic logical
initial sync can't assume: the CH sink is order-independent LWW
(`ReplacingMergeTree(_lsn)` keeps the max-`_lsn` version per key
regardless of arrival order), and the `XactBuffer` buffers user-heap
changes per-xid *inclusion-agnostically* (mapping resolved at
commit-drain via `pipeline::lookup_mapping`, not at buffer insert). So
COPY output and WAL output can interleave freely, and an xact
in-flight when the table enters scope is never lost. When initial-load
is required for an existing non-empty table, daemon allocates a
per-table backfiller task:

1. **Opt-in fences the boundary, no pin.** The `config_table` row
   commits at LSN `S` (its `commit_lsn`). From `S` on, the rfn has a
   mapping, so its heap writes route to CH normally; the only writes
   absent from the WAL side are xacts that *already committed before
   `S`* — xacts in-flight at `S` had their rfn writes buffered
   (inclusion-agnostic) and route at their own commit once the mapping
   exists. **Apply everything from `S`; discard nothing.** No per-rfn
   gate, no held queue — the rfn streams continuously from `S`
2. **COPY out, no snapshot coordination.** After the opt-in applies,
   issue `COPY (SELECT … FROM <qname>) TO STDOUT (FORMAT BINARY)` on
   the sidecar connection. A lone `COPY(SELECT)` runs under its own
   statement snapshot `P` (READ COMMITTED suffices, internally
   consistent), and `P ≥ S` because it is issued after the opt-in
   commit is durable — so COPY sees every xact that committed at or
   before `S`, exactly the ones the WAL side dropped. Requires COPY run
   on the node walshadow streams WAL from, so COPY visibility ≥ the
   stream position at `S`. Stream rows through the same projection +
   target_type stack WAL-driven rows use (single code path, single set
   of bugs), buffer in spill dir under memory pressure, INSERT to CH
   under the same SETTINGS overlay with `_lsn = S` for every row
3. **Convergence, not a cut.** COPY rows carry `_lsn = S`; every
   post-opt-in mutation rides its real `commit_lsn > S`. Order-
   independent dedup resolves per key: an update/delete during backfill
   (`commit_lsn > S`) beats the COPY baseline, an untouched row keeps
   its COPY value, a row seen by both COPY and WAL dedups to the WAL
   copy. Backfill merely seeds rows the pre-`S` WAL never carried; the
   steady-state LWW convergence does the rest. Write amplification is
   the cost: rows mutated in `(S, P]` are written twice, absorbed by
   RMT merges, bounded by the backfill window's write volume
4. **Completion is observability, not correctness.** No boundary to
   release. After COPY EOF, read `pg_current_wal_lsn()` → `P_hi`, an
   upper bound on `P` (needs no atomicity — nothing is discarded on
   it). Report rfn `backfill_state="converged"` once WAL apply passes
   `P_hi`; beyond that every source row is in CH or superseded. Daemon
   does NOT write `initial_load` back to source (no two-way sync);
   operator reads completion from metrics or status endpoint

**Coupling to document.** This backfill's correctness for xacts
in-flight at `S` depends on inclusion-agnostic buffering. If walshadow
ever moves inclusion filtering to buffer-insert time (a memory
optimization dropping unmapped rels early), backfill must regain a
drain step — quiesce or wait out xacts in-flight at `S` before COPY —
to avoid losing their writes.

Resume after daemon crash mid-backfill is a plain re-COPY: state
persists `S` per rfn; on restart re-issue COPY at `_lsn = S`
(unchanged). Dedup makes it idempotent — rows WAL already advanced keep
their higher `_lsn`, un-mutated rows re-seed identically — so no CH
truncate and no fresh snapshot. Chunked COPY by primary-key range is
then trivial: each chunk is an independent statement snapshot at the
same `_lsn = S`, since WAL fills any inter-chunk gap. The
`initial_load=chunked:<col>` variant loses its snapshot-sharing hazard
and could land in v1

### 5. Precedence resolver

`config_resolver` merges three layers, highest wins:

1. **CLI flag** explicit on command line. Each live-reloadable knob is an
   `Option<T>` on `CliOverrides` (`src/config.rs`); `Some` means the operator
   set the flag, so it wins over TOML and survives SIGHUP, `None` defers to
   TOML. clap yields `None` for an absent optional flag (no default), so
   default-vs-explicit falls out of the `Option`, no `value_source` probe
2. **`<schema>.config_*` row** from source PG, applied at row's
   commit LSN
3. **TOML** loaded at boot, reload on SIGHUP

CLI on top so operator yank into recovery via flag cannot be stomped
by stale config rows scrolling in via WAL replay. TOML at bottom is
bootstrap fallback. The CLI + TOML layers and the
`watch::Receiver<Arc<ResolvedConfig>>` substrate already exist
([../config.md](../config.md)); subscribers (mapping refresher, ch_ddl
applicator) consume it unchanged and this work inserts the WAL-driven
layer at `ConfigResolver::resolve`'s merge point (layer 2 above). SIGHUP
already republishes the merged snapshot; §6 adds republish on every
applied `ConfigEvent`, §3 on signal-driven mutations (rare — most
signals don't change resolved config, they just trigger actions)

### 6. Runtime channel via xact_buffer

Extend drain entry enum:

```rust
enum DrainEntry {
    Tuple(CommittedTuple),
    Catalog(SchemaEvent),
    Config(ConfigEvent),     // §6
    Signal(SignalEvent),     // §3, transactional variant only
}
```

`DrainEntry::Config` events ride the same `ordered_events` interleave
and barrier apply as `DrainEntry::Catalog`: `drain_committed` merges
them into the heap stream by LSN, `run_barrier` applies each at its
position after fencing earlier data durable (`barrier_fence`:
wait-placed, seal, wait-durable). Per-row writes later in the same xact
see post-config mapping because the apply mutates the routing state the
decode pool reads (`MappingHandle`) **synchronously inside the barrier
apply, before `run_barrier` dispatches the trailing data segment** —
the same discipline DDL uses writing `ShadowCatalog` + bumping
`invalidation_epoch` before its trailing heaps dispatch. No xact-start
snapshot; `pipeline::lookup_mapping` reads the live `MappingHandle`, and
the fenced write is what makes "live" mean post-config for those rows.

**Not via the `watch`→refresher hop.** `spawn_mapping_refresher` swaps
`MappingHandle` off a separate task reacting to `watch` republishes,
unordered against `run_barrier`. If WAL config apply only republished
and left the swap to the refresher, `run_barrier` would dispatch the
trailing data segment the instant apply returns; the decode worker
would `lookup_mapping` against the pre-config map, miss, and drop the
row (`continue`, `unsupported_relations`++) — a silent permanent loss,
since the row committed once and nothing re-emits it, so the Mapping-add
drill fails, not merely lags. So WAL config apply writes `MappingHandle`
directly under the fence; `watch` republish stays the mechanism only for
the barrier-free contexts — boot seed and SIGHUP — plus the DDL
applicator's own `DdlConfig` refresh and per-key metrics. Shape-changing
keys (`target_type`, `exclude`) additionally bump `invalidation_epoch`
inside the same fence so the decode pool's `RelCache` flushes and
`TablePlan` rebuilds; a pure mapping add/opt-in needs no epoch bump
(unmapped-rel misses are never cached), only the `MappingHandle` write.

Ordering invariant: config row writes preceding heap writes in WAL
position apply before those heap writes drain. Non-transactional signals
bypass `XactBuffer` and dispatch immediately on decode

### 7. Bootstrap seeding

Insert config-seed step between catalog seed and pump start:

```
TOML parse → if [runtime_config].schema empty: skip overlay,
  pump start with TOML-only resolver
TOML parse → source attach + schema/version check → catalog seed →
  config seed (SELECT * FROM <schema>.config_*) → pump start
```

Five `SELECT *` through the existing source sidecar libpq connection
(config tables live on source, not shadow), populate resolver's initial
snapshot, then pump starts. Post-seed writes arrive via §6's drain
entry and merge against seeded baseline. Only libpq path config layer
takes — once pump runs, WAL is the only source. Failure modes: TOML
names schema but schema absent (refuse to start — explicit opt-in
implies operator expects overlay; silent fallback hides
config-not-applied bugs), schema named but tables empty (TOML defaults,
no-op for greenfield), schema field empty or missing (TOML-only mode,
schema check skipped entirely, no overlay machinery instantiated),
config version mismatch (refuse to start, surfaces in pre-flight via
`<schema>.config_meta`)

### 8. Failure containment & TOML fallback

**Regime A: WAL pump alive, config row malformed.** Resolver
validates on merge and rejects offending row. Bad row stays in
source PG (operator's problem), previous resolved value stays in
effect, error metric ticks
(`walshadow_config_rejections_total{kind=…}`), tracing surfaces at
WARN. Daemon does not crash, does not pause pump, does not abandon
other config keys. Validation runs at resolver merge time, not
decoder time

**Regime B: WAL pump blocked, config rows unreachable.** Pre-flight
failing, slot dropped, version skew. Daemon falls back to TOML + CLI
only because overlay freshness cannot be guaranteed. Resolver tracks
`config_freshness_lsn`; if `now() - last_resolver_apply >
config_staleness_max` (default 5min), alarm fires and resolver flags
itself degraded. New SIGHUPs still apply TOML layer; overlay freezes
at last-known state, not zeroed. Pump recovery re-applies overlay as
WAL replay catches up. Hard guarantee: TOML configures connection to
source/shadow and everything else when overlay isn't fresh. Overlay
is strictly additive on top of working TOML

## Precedence

CLI > `<schema>.config_*` row > TOML. Resolver records LSN of every
apply. Per-resolved-key Prom metric carries source label
(`source="cli|wal|toml"`) so operator can answer "why is auto_create
false for public?"

## Knob breadth

Inspired by peerdb's `dynamicconf.go` — same shape of operator
control surface, very different transport. Representative rows
across config tables:

| key | table | type | notes |
|---|---|---|---|
| `auto_create` | namespace | bool | landing on first matching CREATE |
| `engine` / `order_by` | table, namespace | text | shape change; fake-invalidates rfn so TablePlan rebuilds via existing drain gate |
| `target` | table | text | rerouting live table requires reseed; resolver rejects mid-stream change for an rfn already streaming |
| `replicate` | table | bool | opt-in / opt-out switch; combines with `initial_load` to dispatch backfill |
| `initial_load` | table | bool | backfiller task spawned per non-empty rfn |
| `target_type` | column | text | drives CAST in projection; validated against CH types at merge; fake-invalidates rfn |
| `exclude` | column | bool | column dropped from projection + future DDL; fake-invalidates rfn |
| `ch_settings` | global, namespace, table | jsonb | applied to INSERT/CREATE; merged narrow-wins |
| `batch_max_rows` | global | int | emitter flush trigger |
| `batch_max_bytes` | global | int | emitter flush trigger |
| `flush_timeout_ms` | global | int | idle-flush deadline |
| `drop_table_strategy` | global, namespace | enum | drop, warn, retain |
| `compression` | global | enum | per-INSERT compression header |
| `retry_max_attempts` | global | int | CH client retry budget |
| `sample_rate` | (TOML only) | float | row-drop sampling for debug |
| `signal_prefix` | (TOML only) | text | which `pg_logical_emit_message` prefix to scan |
| `admin_database` | (TOML only) | text | which database's signals the daemon honors for global imperatives; empty = source db |

Shape-changing keys (`target_type`, `exclude`, `engine`, `order_by`)
bump the catalog generation counter for the affected rfn, so the
existing barrier fence flushes in-flight rows then rebuilds
`TablePlan`. Non-shape keys republish through `watch::Receiver` and
take effect on the next subscriber-side snapshot read with no rfn
drain. Reroutes that can't be done safely mid-stream (e.g. `target`
rename on a streaming rfn) are rejected at merge with an explanatory
metric, not silently applied

## What stays (anti-goals)

- TOML stays valid. Existing `[ch]`, `[table.…]`, `[namespace.…]`
  blocks keep working unchanged
- Connection params stay TOML/CLI. `[ch] host/port/database/user/password`
  for CH; `--source-conninfo` and `--shadow-conninfo` for PG. Cannot
  live in source PG because they describe how to reach source PG.
  Bootstrap fixed point
- **Overlay enable + schema name + signal prefix + admin database
  stay TOML.** Without these, no overlay rels exist to read, and the
  admin-database gate can't bootstrap from a value the untrusted
  signal channel supplies. Putting any of them in the overlay itself
  is a chicken-and-egg
- Spill paths, retention dir, metrics bind stay CLI flags
- `bootstrap_mode` and friends stay boot-time CLI. By the time
  config table is readable, bootstrap is done
- DDL on `<schema>.config_*` itself is DBA-run via a versioned
  migration script that bumps `<schema>.config_meta`. Auto-DDL
  applicator filters the overlay namespace out of CH replication so
  these ALTERs never reach the target
- No config replication to CH. Hard-coded namespace filter, not
  overrideable
- No two-way sync. Daemon never writes to `<schema>.config_*`.
  Source PG is single writer; daemon is single reader. Preserves
  "walshadow never writes to source" invariant

## Open questions

- **Config row LSN attribution.** Today's barrier fence is global,
  not keyed; a per-`(rfn, generation)` gate is new machinery. Config
  flip changing `column_mapping.target_type` for `public.orders`
  needs to drain through same gate. Cleanest: bump catalog generation
  counter for affected rfns on config apply, so the fence flushes and
  rebuilds `TablePlan`. One fake-invalidate per config event
  mentioning a relation, acceptable since config events are rare.
  Same gate applies to `exclude` flip — projection shape change
  needs full rfn drain before new shape takes effect
- **Conflicting writes during failover.** Source PG fails over to
  replica with stale config. Daemon following new primary sees old
  config and rolls back. Mitigation: resolver records LSN of every
  apply; row with LSN lower than `config_freshness_lsn` is logged
  and ignored. Real fix is operator running `pg_dump` of
  `walshadow.config_*` from old primary into new before failover —
  daemon doesn't try to outsmart DBA
- **Schema evolution of config tables themselves.** install script at
  version K declares config schema; daemon binary at K' may expect newer
  column. Strategy: grow additively, NULL means daemon's default
  applies; install script declares min-daemon version in
  `<schema>.config_meta`. Mismatch surfaces in pre-flight, not at
  first row read
- **High-throughput config writes.** Ops script writing 10k
  config_column rows in one xact could stall drain on resolver
  merges. In-memory merge should be µs per event but test matrix
  pins worst case. Bound: if single drain processes > N config
  events, batch resolver republish so subscribers see one merged
  snapshot
- **CLI override discoverability.** With three layers, operator
  querying "why is X set to Y?" needs a tool. Natural follow-up:
  `walshadow-stream config explain <key>` subcommand. Minimum here:
  Prom metric per resolved key with source label
- **Source privileges.** Config install needs `CREATE` on the database
  and ownership of the config tables, no superuser. Deployment lacking
  those sets `[runtime_config] schema = ""` for TOML-only. Document in
  deployment guide
- **Signal abuse.** `pg_logical_emit_message` has no in-backend
  privilege check and ships with default EXECUTE to PUBLIC (PG 18.4
  `logicalfuncs.c`), so the real bar is "any role that can connect",
  not "`walshadow` schema write". Primary gate is daemon-side `dbId`
  scoping (§3 signal source scoping): point `admin_database` at a
  database with `REVOKE CONNECT ... FROM PUBLIC`, and the global
  imperatives (`drop_slot_at_lsn`, `force_reseed`, pause/flush) become
  reachable only by roles that can connect there, unspoofable since
  `dbId` is stamped from `MyDatabaseId`. Secondary, source-side:
  `REVOKE EXECUTE ON FUNCTION pg_logical_emit_message(...) FROM PUBLIC`
  on both text and bytea overloads plus GRANT to a signaler role. dbId
  scoping is preferred because it survives function-ACL drift and the
  emitting role never reaches WAL for the daemon to check. Payload
  signing (trailing signed-by-key token, TOML verification key) stays
  as optional defense-in-depth; with the admin-database gate it's no
  longer required for v1
- **Signal replay.** A signal at LSN N processed once, then daemon
  restarts and replays WAL from < N. Idempotent commands (flush,
  pause, resume) tolerate replay; one-shot commands
  (`drop_slot_at_lsn`) need dedup. Mitigation: persist
  last-processed signal LSN alongside emitter checkpoint; skip
  signals at LSN ≤ checkpoint on replay
- **Backfill vs DDL.** Backfill of `app.orders` in progress (COPY
  projecting the shape as of `P`); source runs `ALTER TABLE app.orders
  DROP COLUMN notes` mid-COPY. No snapshot to preserve now, but COPY's
  projection shape must reconcile with the post-ALTER CH table.
  Additive DDL (`ADD COLUMN`) is tolerated: COPY omits the new column,
  it fills default/NULL, and post-DDL WAL rows converge it. Destructive
  or type-changing DDL (`DROP COLUMN`, type alter) makes COPY's
  projection reference a column CH no longer has — restart COPY against
  the post-DDL shape at a new `S'`. Cheap now that re-COPY needs no
  snapshot and is idempotent at `_lsn = S'`; catalog applicator signals
  the backfiller on a shape-changing `SchemaEvent` for a backfilling
  rfn, no defer-and-stall
- **Forward-decl pollution.** Operator inserts `config_table` rows
  for typo'd qualnames (`app.ordres`). Pending forever, harmless
  but noisy. Mitigation: TTL on `pending_decl` entries
  (default 30 days), tick a metric, log at WARN on expiry; row
  stays, daemon stops watching

## Dependencies

- `ResolvedConfig` + `watch::Receiver` substrate — **landed**, see
  [../config.md](../config.md). CLI > TOML merge, `ConfigResolver` +
  `watch` channel, SIGHUP republish, and the mapping-refresher + DDL
  applicator subscribers are in place. This work adds the WAL overlay
  layer at `ConfigResolver::resolve`'s merge point (between CLI and TOML)
  and populates `ResolvedConfig::columns`
- `DrainEntry::Catalog` must close. `DrainEntry::Config` extends the
  same enum and reuses the drain loop's ordering invariants
- Cross-link: see `../emitter.md` and `../shadow.md` for
  `ShadowCatalog` resolution paths the config_decoder mirrors

Lands strictly after `DrainEntry::Catalog` closes (resolver substrate
already closed). §1 (config schema + install script) is independent and
can land in parallel since it touches only a new SQL install script.
Per-key migration of TOML knobs to overlay-resolvable is incremental:
emitter compression/budgets first (smallest blast radius), then
namespace/table/column mapping, then DDL strategy flags

## Acceptance drills

- **Disabled by default.** TOML omits `[runtime_config].schema` or
  sets it to `""`. Daemon never queries source for overlay rels,
  never attaches `config_decoder` or `message_decoder`.
- **Greenfield seed.** TOML names a schema, config schema installed,
  all config tables empty. Behaviour identical to TOML-only with same
  values
- **Namespace flip.** Daemon running with TOML `auto_create = false`.
  Operator runs `INSERT INTO <schema>.config_namespace
  (namespace_name, target_database, auto_create) VALUES ('public',
  'default', true)` on source. Subsequent `CREATE TABLE
  public.events(...)` materialises on CH in one round-trip, matching
  namespace-mapping drill but with config arriving via WAL
- **Mapping-add.** Source-side `INSERT INTO <schema>.config_table
  ... ; INSERT INTO <schema>.config_column ...` declares new
  mapping in single xact. Same xact inserts rows into now-mapped
  table. CH receives rows under post-config mapping. Confirms
  within-xact ordering invariant
- **Column exclude.** Set `config_column.exclude = true` for
  `public.orders.notes`. Subsequent rows arrive on CH without the
  column; existing CH column either retained as NULLable or DDL'd
  to drop per `drop_columns` mode. Re-clearing exclude restores
  emission, projection rebuilds after in-flight rfn drain
- **Target-type override.** Source column is `numeric(38,0)`,
  default mapping picks `Decimal(38,0)`. Operator sets
  `config_column.target_type = 'Int128'`. Resolver validates
  representability, rfn drain gate fires, post-config rows arrive
  cast to `Int128`. Resolver rejects `target_type = 'Float32'`
  (precision loss) at merge, emits warning, leaves prior value
- **CH settings passthrough.** Set
  `config_table.ch_settings = '{"max_insert_threads":4}'` for one
  table. Inserts for that table carry the SETTINGS clause; other
  tables unaffected. Global default merges with table-scoped under
  narrow-wins
- **Batch tunables.** `config_global.batch_max_rows = 1000`,
  `flush_timeout_ms = 250`. Emitter flushes at smaller of the two.
  Bump to 100k, observe larger batches and lower flush rate.
  Confirms emitter picks up resolved snapshot mid-pipeline
- **Signal: flush_now.** Source runs
  `pg_logical_emit_message(false, 'walshadow', 'flush_now')`
  during idle period. Emitter flushes within one decode-tick, no
  xact required. Non-transactional path
- **Signal: transactional pause.**
  `pg_logical_emit_message(true, 'walshadow', 'pause_emitter')`
  followed by rows in same xact. Rows commit on source, daemon
  pauses at message LSN, never emits the trailing rows until
  `resume_emitter` signal arrives. Confirms transactional ordering
- **Signal: ignore-transaction.** Source runs a DELETE (or DROP
  PARTITION) plus `pg_logical_emit_message(true, 'walshadow',
  'ignore-transaction')` in one xact. Xact commits on source, shadow
  replays any catalog change, CH receives nothing for the xact,
  `emitter_ack_lsn` still advances past commit_lsn. Variant: tag
  emitted inside a rolled-back savepoint leaves the surrounding xact
  replicating normally
- **Opt-in empty table.** Pre-existing empty `app.events` not in
  scope. Operator inserts `config_table (qname, replicate=true,
  initial_load=true)`. Backfiller sees zero rows, no COPY needed,
  per-rfn state flips to `Streaming` immediately. Subsequent
  inserts on source land on CH
- **Opt-in non-empty table.** Pre-existing `app.orders` with 10M
  rows. Operator inserts `config_table (..., initial_load=true)`.
  Daemon streams orders' WAL from opt-in LSN `S` and concurrently
  COPYs at `_lsn = S`; updates/deletes committed during the COPY win
  over the baseline by `commit_lsn > S`. Source state == CH state once
  WAL apply passes `P_hi`. Variant: mutate a row mid-backfill and
  assert CH reflects the mutation, not the COPY baseline. Other
  tables' replication unaffected during backfill
- **Forward-decl.** Operator inserts `config_table (qname =
  "app.new_table", replicate=true)` for a table that doesn't exist.
  Resolver parks row in pending_decl. Source runs `CREATE TABLE
  app.new_table (...)`. Catalog applicator notifies resolver;
  pending row resolves; subsequent inserts land on CH under
  declared config. Confirms forward-declaration loop
- **Opt-out mid-stream.** `app.orders` actively replicating.
  Operator updates `config_table.replicate = false`. In-flight
  rows drain to CH, no further emission. CH target retained per
  `on_drop` policy. Confirms graceful exit
- **TOML fallback.** Source has populated rows with `auto_create =
  true`. WAL pump artificially blocked (drop publication mid-run).
  Resolver flips to degraded after `config_staleness_max`. Operator
  SIGHUPs TOML flipping `auto_create = false`; subsequent CREATE
  TABLE no longer mirrors. Pump unblock restores overlay; new CREATE
  mirrors again under WAL-driven setting. Confirms TOML doesn't get
  stuck off when overlay reconnects
- **Validation rejection.** Insert config_column row with
  `target_type = '!@#$'`. Resolver rejects, metric ticks, daemon
  stays up, other mappings unaffected. UPDATE to valid type; next
  WAL apply picks up corrected value
- **Precedence.** CLI `--drop-table-strategy=warn` +
  `walshadow.config_global` row `drop_table_strategy = 'drop'` +
  TOML `retain`. Resolver applies `warn`. Remove CLI flag (next
  restart) → resolver applies `drop`. Truncate config_global row →
  resolver applies `retain`
