# runtime config from source PG

Move walshadow's runtime config out of TOML and into source-PG tables
owned by walshadow's PG extension. Config rows are written via SQL by
the same DBA who owns source schema, replicated through the WAL
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
without ext installed (managed PG, etc.) keep working untouched

## Strategy

Eight pieces. Substrate first (§1 + §2), signal channel (§3),
per-table opt-in + backfill (§4), resolver (§5), runtime channel
(§6), bootstrap (§7), failure containment (§8)

### 1. `<schema>.*` config tables

Extend pgext with config tables under an operator-named schema (TOML
`[runtime_config] schema`, typical value `walshadow`). Five tables:

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
  Daemon never writes; pgext function writes synchronously as part
  of the same xact that emits the message, so audit row and message
  share an LSN

`REPLICA IDENTITY FULL` on every config table — without before-image
UPDATE WAL omits old values and decoder cannot resolve the changed
columns. Table OIDs deterministic across deployments via
`<schema>.ext_oid_anchors` (PG 18 fixed OIDs; PG 17 discovers by
name and caches in `ShadowCatalog`). Hard-coded namespace filter
short-circuits CH replication for the overlay schema (replicating
walshadow's own config to CH would be a footgun, not overrideable)

### 2. `config_decoder`

Parallel to `pg_class_decoder`, subscribes to heap-record stream at
same fan-out point, filters on the config-table relfilenodes
resolved at attach time, decodes tuple body via existing Tier 1/2
codec stack, emits typed `ConfigEvent` (`GlobalChanged`,
`NamespaceUpserted`, `NamespaceRemoved`, `TableUpserted`,
`TableRemoved`, `ColumnUpserted`, `ColumnRemoved`). No libpq
round-trip per row — decoder reads tuple bytes off WAL. Cost is one
match-on-relfilenode per heap write, amortised across rels that
see writes on operator timescale, not xact timescale.
Refetch-on-invalidate (the `ShadowCatalog` pattern) is rejected
because config_column can carry tens of thousands of rows in wide
schemas; decoder-side parsing is O(1) per write vs O(rows) for
refetch

### 3. Signal channel via `pg_logical_emit_message`

Orthogonal to config-row state: imperatives that don't make sense
to store as a row (`flush_now`, `pause_emitter`, `resume_emitter`,
`force_reseed:<rfn>`, `drop_slot_at_lsn:<X>`, debug toggles).
Daemon's WAL pump already classifies `RmId::LogicalMsg = 21` records
(see `src/classify.rs:105`) but discards body; this adds a
`message_decoder` parallel to `config_decoder` that filters on
configurable prefix (TOML `[runtime_config] message_prefix`,
default `walshadow`) and routes payload to a small command parser.
Unknown commands log at WARN and increment
`walshadow_signal_unknown_total{cmd=…}` — never crash.

`pg_logical_emit_message(transactional bool, prefix text, content
text)` semantics honored: transactional messages drain at commit
LSN through the same `XactBuffer` ordering as heap rows;
non-transactional messages drain on receipt, used for "do it now"
signals where ordering against in-flight xacts doesn't matter
(maintenance toggles, metric resets). Parser accepts JSON payload
with `{"cmd": "...", "args": {...}}` shape; refuses bare text
(forces explicit shape so future commands stay parseable).

Why messages, not config rows: stored-state config is wrong fit
for "do once" commands. A `flush_now` row would imply persistent
state; toggling it back and forth in a single transaction would be
incoherent. Messages are fire-and-forget at a defined LSN.

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

**Backfill path.** When initial-load required for an existing
non-empty table, daemon allocates a per-table backfiller task:

1. **Snapshot LSN pin.** Issue `pg_export_snapshot()` on dedicated
   libpq connection, record exported snapshot id + the
   `pg_current_wal_lsn` at export. WAL pump continues consuming
   other rels' writes normally — backfill runs alongside, not in
   place of, streaming. Snapshot pin keeps source's xmin from
   advancing past needed visibility for the duration; risk is bloat
   if backfill is slow, surfaces via existing `walshadow_replication_backlog`
   plus a dedicated `walshadow_backfill_snapshot_age_seconds` gauge
2. **COPY out.** `COPY (SELECT … FROM <qname>) TO STDOUT (FORMAT
   BINARY)` against source under the snapshot. Stream rows through
   the same projection + target_type stack that WAL-driven rows use
   (single code path, single set of bugs), buffer in spill dir if
   memory pressure, INSERT to CH target under same SETTINGS overlay
3. **Gate streaming on backfill LSN.** Per-rfn gate (new machinery;
   today's barrier fence is global) set to
   `Backfilling{snapshot_lsn: N}`. Heap WAL writes for that
   rfn at LSN ≥ N queue in `XactBuffer` but do NOT drain to CH
   until backfill completes. WAL writes at LSN < N are discarded
   (already in COPY output)
4. **Completion.** On COPY EOF, daemon flips per-rfn state to
   `Streaming`, drains queued WAL writes in order, observability
   reflects `backfill_state="done"`. Daemon does NOT update the
   `initial_load` field in source PG (no two-way sync); operator
   reads completion from metrics or status endpoint

Resume after daemon crash mid-backfill: per-table backfiller state
persists to spill dir alongside snapshot LSN. On restart, resolver
checks per-rfn state; backfill resumes via fresh
`pg_export_snapshot()` from current LSN (the in-progress snapshot
is lost with the connection — PG can't share snapshots across
sessions reliably for our purposes). COPY restarts from row 0,
prior partial output discarded by truncating CH target if `replicate
+ initial_load=auto` semantics imply atomic-or-nothing. Alternative
for huge tables: chunked COPY by primary key range with checkpoint
per range — out of scope for v1, surface as
`initial_load=chunked:<col>` future variant

### 5. Precedence resolver

`config_resolver` merges three layers, highest wins:

1. **CLI flag** explicit on command line (clap's
   `ArgMatches::value_source` distinguishes default from explicit)
2. **`<schema>.config_*` row** from source PG, applied at row's
   commit LSN
3. **TOML** loaded at boot, reload on SIGHUP

CLI on top so operator yank into recovery via flag cannot be stomped
by stale config rows scrolling in via WAL replay. TOML at bottom is
bootstrap fallback. Subscribers (emitter, ch_ddl applicator, metrics
endpoint) keep consuming the
`watch::Receiver<Arc<ResolvedConfig>>` substrate unchanged; this work
plugs WAL-driven + CLI layers into resolver's merge point. SIGHUP
republishes merged snapshot, §6 republishes on every applied
`ConfigEvent`, §3 republishes on signal-driven mutations (rare —
most signals don't change resolved config, they just trigger
actions)

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

`XactBuffer::commit`'s drain loop processes config events through
resolver, which republishes merged snapshot. Per-row writes later in
same xact see post-config snapshot because emitter snapshots
`*watch_rx.borrow()` at xact-start, not row-start — matches PG's
read-committed semantics. Ordering invariant: config row writes
preceding heap writes in WAL position apply before those heap writes
drain. Non-transactional signals bypass `XactBuffer` and dispatch
immediately on decode

### 7. Bootstrap seeding

Insert config-seed step between catalog seed and pump start:

```
TOML parse → if [runtime_config].schema empty: skip overlay,
  pump start with TOML-only resolver
TOML parse → shadow attach + ext check → catalog seed →
  config seed (SELECT * FROM <schema>.config_*) → pump start
```

Five `SELECT *` through existing shadow libpq connection, populate
resolver's initial snapshot, then pump starts. Post-seed writes
arrive via §6's drain entry and merge against seeded baseline. Only
libpq path config layer takes — once pump runs, WAL is the only
source. Failure modes: TOML names schema but ext absent (refuse to
start — explicit opt-in implies operator expects overlay; silent
fallback hides config-not-applied bugs), schema named but tables
empty (TOML defaults, no-op for greenfield), schema field empty or
missing (TOML-only mode, ext check skipped entirely, no overlay
machinery instantiated), ext version mismatch (refuse to start,
surfaces in pre-flight via `<schema>.ext_meta`)

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
- **Overlay enable + schema name + signal prefix stay TOML.**
  Without these, no overlay rels exist to read. Putting any of them
  in the overlay itself is a chicken-and-egg
- Spill paths, retention dir, metrics bind stay CLI flags
- `bootstrap_mode` and friends stay boot-time CLI. By the time
  config table is readable, bootstrap is done
- DDL on `<schema>.config_*` itself rides ext version-bump path,
  not source-side `ALTER TABLE`. Auto-DDL applicator filters against
  the overlay namespace
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
- **Schema evolution of config tables themselves.** ext at version
  K declares config schema; daemon binary at K' may expect newer
  column. Strategy: grow additively, NULL means daemon's default
  applies; install script declares min-daemon version in
  `walshadow.ext_meta`. Mismatch surfaces in pre-flight, not at
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
- **Source PG without superuser.** Ext install requires superuser
  on source (oracle already needed for `walshadow_decode_disk`).
  Managed PG forbidding C extensions cannot install ext and
  therefore cannot use overlay; falls back to TOML-only cleanly.
  Operator sets `[runtime_config] schema = ""` to match the
  reality. Document in deployment guide
- **Signal abuse.** Anyone with `pg_logical_emit_message` privilege
  (lower bar than `walshadow` schema write) can send commands.
  Mitigation: command parser checks signed-by-key field in payload
  when TOML provides a verification key, otherwise restricts to
  read-only commands (metric reset, debug toggles). Destructive
  signals (`drop_slot_at_lsn`, `force_reseed`) require key. Out of
  scope for v1 — initial cut accepts any command from the
  configured prefix and lets DBA gate at SQL-permission layer
- **Signal replay.** A signal at LSN N processed once, then daemon
  restarts and replays WAL from < N. Idempotent commands (flush,
  pause, resume) tolerate replay; one-shot commands
  (`drop_slot_at_lsn`) need dedup. Mitigation: persist
  last-processed signal LSN alongside emitter checkpoint; skip
  signals at LSN ≤ checkpoint on replay
- **Backfill vs DDL race.** Backfill of `app.orders` in progress at
  snapshot LSN N. Source runs `ALTER TABLE app.orders DROP COLUMN
  notes` at LSN N+M. WAL stream sees DDL and would propagate to CH
  before backfill finishes. Catalog applicator must learn that rfn
  is `Backfilling` and either (a) defer DDL until backfill drains,
  or (b) restart backfill against post-DDL snapshot. Option (a)
  preserves snapshot consistency but can stall arbitrarily long
  for huge tables; option (b) wastes work. Default to (a) with a
  bounded wait, then fall through to (b) on timeout
- **Forward-decl pollution.** Operator inserts `config_table` rows
  for typo'd qualnames (`app.ordres`). Pending forever, harmless
  but noisy. Mitigation: TTL on `pending_decl` entries
  (default 30 days), tick a metric, log at WARN on expiry; row
  stays, daemon stops watching

## Dependencies

- `ResolvedConfig` + `watch::Receiver` substrate must close. Single
  TOML producer is the integration point three-layer merge replaces.
  Stub-only state today
- `DrainEntry::Catalog` must close. `DrainEntry::Config` extends the
  same enum and reuses the drain loop's ordering invariants
- Cross-link: see `../emitter.md` and `../shadow.md` for substrate
  partial state and `ShadowCatalog` resolution paths the
  config_decoder mirrors

Lands strictly after resolver + DrainEntry::Catalog close. §1 (ext
schema + install script) is independent and can land in parallel
since it touches only `pgext/`. Per-key migration of TOML knobs to
overlay-resolvable is incremental: emitter compression/budgets first
(smallest blast radius), then namespace/table/column mapping, then
DDL strategy flags

## Acceptance drills

- **Disabled by default.** TOML omits `[runtime_config].schema` or
  sets it to `""`. Daemon never queries source for overlay rels,
  never attaches `config_decoder` or `message_decoder`.
- **Greenfield seed.** TOML names a schema, ext installed, all
  config tables empty. Behaviour identical to TOML-only with same
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
  `pg_logical_emit_message(false, 'walshadow', '{"cmd":"flush_now"}')`
  during idle period. Emitter flushes within one decode-tick, no
  xact required. Non-transactional path
- **Signal: transactional pause.**
  `pg_logical_emit_message(true, 'walshadow', '{"cmd":"pause_emitter"}')`
  followed by rows in same xact. Rows commit on source, daemon
  pauses at message LSN, never emits the trailing rows until
  `resume_emitter` signal arrives. Confirms transactional ordering
- **Opt-in empty table.** Pre-existing empty `app.events` not in
  scope. Operator inserts `config_table (qname, replicate=true,
  initial_load=true)`. Backfiller sees zero rows, no COPY needed,
  per-rfn state flips to `Streaming` immediately. Subsequent
  inserts on source land on CH
- **Opt-in non-empty table.** Pre-existing `app.orders` with 10M
  rows. Operator inserts `config_table (..., initial_load=true)`.
  Daemon pins snapshot, backfills via COPY, queues concurrent WAL
  writes for orders behind backfill gate, drains them in order on
  COPY completion. Source row count == CH row count once gate
  releases. Other tables' replication unaffected during backfill
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
