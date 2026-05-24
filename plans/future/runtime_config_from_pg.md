# runtime config from source PG

Move walshadow's runtime config out of TOML and into source-PG tables
owned by walshadow's PG extension. Config rows are written via SQL by
the same DBA who owns source schema, replicated through the WAL
stream walshadow's daemon already decodes, and applied atomically at
each row's commit LSN. Post-config xacts route against the
post-config shape under the same `await_ready` gate the catalog
applicator uses for DDL. TOML stays authoritative for bootstrap
(connection params) and as the only config surface when WAL processing
is blocked or the overlay table is absent

## Why deferred

Namespace-mapping work (CREATE TABLE / namespace config /
watch-resolver substrate) only partially shipped. The
`watch::Receiver<Arc<ResolvedConfig>>` channel landed with a single
TOML producer but the three-layer merge resolver and
`DrainEntry::Catalog` plumbing are not yet in place. Wiring CLI +
WAL-driven layers into a resolver that doesn't exist outside its stub
form, and a drain pipeline that still treats catalog events as
second-class, means stacking config-overlay correctness on top of an
incomplete substrate

## Why source PG, not shadow or separate DB

Three viable hosts for config tables:

| host | pro | con |
|---|---|---|
| **source PG** | single source of truth shared with DBA; writes flow through WAL so daemon picks them up via same pump as data; SQL is the DBA's native surface | walshadow-specific tables pollute source namespace; requires walshadow PG ext installed source-side |
| shadow PG | already walshadow's host, ext already installed | shadow is reset on bootstrap; no DBA-facing surface; config writes don't ride the WAL pump (shadow's WAL consumed differently) |
| separate config DB | clean separation | bootstrap problem doubled (two conninfos); another connection to babysit; no atomic-with-data semantic |

Source PG wins on atomic-with-data. Config row flipping
`auto_create = false` for namespace at LSN N is guaranteed to take
effect before any post-N xact in that namespace. Ordering is
impossible if config lives anywhere else. Cost is one schema
(`walshadow.*`) that ext already half-owns from oracle install

## Strategy

Six pieces. Substrate first (§1 + §2), resolver (§3), runtime
channel (§4), bootstrap (§5), failure containment (§6)

### 1. `walshadow.*` config tables

Extend pgext with four tables under the `walshadow` schema:
`config_global` (single-row, emitter compression/budgets, retry
knobs, DDL strategy, retention, sampling), `config_namespace` (per
target DB, auto_create, engine + order_by defaults, drop_columns
mode), `config_table` (per qualified relation, target, skip, on_drop,
order_by, engine), `config_column` (per (rel, attname), target name
+ target type)

`REPLICA IDENTITY FULL` on every config table — without before-image
UPDATE WAL omits old values and decoder cannot resolve the changed
columns. Table OIDs deterministic across deployments via
`walshadow.ext_oid_anchors` (PG 18 fixed OIDs; PG 17 discovers by
name and caches in `ShadowCatalog`). Hard-coded namespace filter
short-circuits CH replication for `walshadow.*` (replicating
walshadow's own config to CH would be a footgun, not overrideable)

### 2. `config_decoder`

Parallel to `pg_class_decoder`, subscribes to heap-record stream at
same fan-out point, filters on the four config-table relfilenodes
resolved at attach time, decodes tuple body via existing Tier 1/2
codec stack, emits typed `ConfigEvent` (`GlobalChanged`,
`NamespaceUpserted`, `NamespaceRemoved`, `TableUpserted`,
`TableRemoved`, `ColumnUpserted`, `ColumnRemoved`). No libpq
round-trip per row — decoder reads tuple bytes off WAL. Cost is one
match-on-relfilenode per heap write, amortised across four rels
which see writes on operator timescale, not xact timescale.
Refetch-on-invalidate (the `ShadowCatalog` pattern) is rejected
because config_column can carry tens of thousands of rows in wide
schemas; decoder-side parsing is O(1) per write vs O(rows) for
refetch

### 3. Precedence resolver

`config_resolver` merges three layers, highest wins:

1. **CLI flag** explicit on command line (clap's
   `ArgMatches::value_source` distinguishes default from explicit)
2. **`walshadow.config_*` row** from source PG, applied at row's
   commit LSN
3. **TOML** loaded at boot, reload on SIGHUP

CLI on top so operator yank into recovery via flag cannot be stomped
by stale config rows scrolling in via WAL replay. TOML at bottom is
bootstrap fallback. Subscribers (emitter, ch_ddl applicator, metrics
endpoint) keep consuming the
`watch::Receiver<Arc<ResolvedConfig>>` substrate unchanged; this work
plugs WAL-driven + CLI layers into resolver's merge point. SIGHUP
republishes merged snapshot, and §4 republishes on every applied
`ConfigEvent`

### 4. Runtime channel via xact_buffer

Extend drain entry enum:

```rust
enum DrainEntry {
    Tuple(CommittedTuple),
    Catalog(SchemaEvent),
    Config(ConfigEvent),     // §4
}
```

`XactBuffer::commit`'s drain loop processes config events through
resolver, which republishes merged snapshot. Per-row writes later in
same xact see post-config snapshot because emitter snapshots
`*watch_rx.borrow()` at xact-start, not row-start — matches PG's
read-committed semantics. Ordering invariant: config row writes
preceding heap writes in WAL position apply before those heap writes
drain

### 5. Bootstrap seeding

Insert config-seed step between catalog seed and pump start:

```
TOML parse → shadow attach + ext check → catalog seed →
config seed (SELECT * FROM walshadow.config_*) → pump start
```

Four `SELECT *` through existing shadow libpq connection, populate
resolver's initial snapshot, then pump starts. Post-seed writes
arrive via §4's drain entry and merge against seeded baseline. Only
libpq path config layer takes — once pump runs, WAL is the only
source. Failure modes: ext absent (WARN, TOML-only), tables empty
(TOML defaults, no-op for greenfield), ext version mismatch (refuse
to start, surfaces in pre-flight via `walshadow.ext_meta`)

### 6. Failure containment & TOML fallback

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

CLI > `walshadow.config_*` row > TOML. Resolver records LSN of every
apply. Per-resolved-key Prom metric carries source label
(`source="cli|wal|toml"`) so operator can answer "why is auto_create
false for public?"

## What stays (anti-goals)

- TOML stays valid. Existing `[ch]`, `[table.…]`, `[namespace.…]`
  blocks keep working unchanged
- Connection params stay TOML/CLI. `[ch] host/port/database/user/password`
  for CH; `--source-conninfo` and `--shadow-conninfo` for PG. Cannot
  live in source PG because they describe how to reach source PG.
  Bootstrap fixed point
- Spill paths, retention dir, metrics bind stay CLI flags
- `bootstrap_mode` and friends stay boot-time CLI. By the time
  config table is readable, bootstrap is done
- DDL on `walshadow.config_*` itself rides ext version-bump path,
  not source-side `ALTER TABLE`. Auto-DDL applicator filters against
  `walshadow` namespace
- No config replication to CH. Hard-coded namespace filter, not
  overrideable
- No two-way sync. Daemon never writes to `walshadow.config_*`.
  Source PG is single writer; daemon is single reader. Preserves
  "walshadow never writes to source" invariant

## Open questions

- **Config row LSN attribution.** `await_ready(rfn, generation)` keys
  on `(rfn, generation)`. Config flip changing
  `column_mapping.target_type` for `public.orders` needs to drain
  through same gate. Cleanest: bump catalog generation counter for
  affected rfns on config apply, so emitter's gate flushes and
  rebuilds `TablePlan`. One fake-invalidate per config event
  mentioning a relation, acceptable since config events are rare
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
  Document in deployment guide

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

- **Greenfield seed.** Daemon boots with ext installed but all
  config tables empty. Behaviour identical to pre-overlay
  deployment with same TOML. Zero-regression for ops who don't opt in
- **Namespace flip.** Daemon running with TOML `auto_create = false`.
  Operator runs `INSERT INTO walshadow.config_namespace
  (namespace_name, target_database, auto_create) VALUES ('public',
  'default', true)` on source. Subsequent `CREATE TABLE
  public.events(...)` materialises on CH in one round-trip, matching
  namespace-mapping drill but with config arriving via WAL
- **Mapping-add.** Source-side `INSERT INTO walshadow.config_table
  ... ; INSERT INTO walshadow.config_column ...` declares new
  mapping in single xact. Same xact inserts rows into now-mapped
  table. CH receives rows under post-config mapping. Confirms
  within-xact ordering invariant
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
