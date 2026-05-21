# PHASE16 — drive runtime config from upstream postgres

[PHASE15](PHASE15.md) widens the daemon's authority over CH-side
schema: source DDL on a tracked relation echoes to CH automatically
via `ShadowCatalog`'s `SchemaEvent` channel plus a `ch_ddl`
applicator gated on the emitter through `await_ready`. Mapping
config still rides static TOML
([`src/ch_emitter.rs:106-365`](../src/ch_emitter.rs)) + SIGHUP
reload ([`src/bin/stream.rs:1060-1104`](../src/bin/stream.rs)) per
[PHASE10](PHASE10.md); [`namespace.<ns>`](PHASE15.md#5-create-table--namespace-tracking)
blocks are a file edit + SIGHUP away from taking effect

That split has a sharp seam. Operators editing TOML on the
walshadow host must coordinate with DBAs writing source-side DDL
on a different host, against a different deployment surface. Two
groups, two state stores, no single audit trail. SIGHUP race
windows live one xact wide between "DBA committed `CREATE TABLE`"
and "operator's TOML edit reached walshadow's filesystem"

Goal: walshadow's runtime config is a queryable set of rows in
**source postgres**, written via SQL, replicated through the same
WAL stream the daemon already decodes. Config row writes apply
atomically at the row's commit LSN, so post-config xacts route
against the post-config shape with the same correctness guarantees
PHASE15 §2's `await_ready` already provides for DDL. TOML stays
authoritative for bootstrap, & as the only config surface when WAL
processing is blocked or the source's `walshadow_config` table is
absent

## Why source postgres, not shadow

Three viable hosts for the config table:

| host | pro | con |
|---|---|---|
| **source PG** | single source of truth shared with DBA, writes flow through WAL so daemon picks them up via the same pump it uses for data, ops uses SQL they already know | source carries walshadow-specific tables, namespace pollution, requires the walshadow PG extension to be installed source-side |
| shadow PG | already walshadow's host, ext already installed | shadow is reset on bootstrap, no DBA-facing surface, config writes wouldn't ride the WAL pump (shadow's WAL is consumed differently) |
| separate config DB | clean separation | bootstrap problem doubled (two conninfos), another connection to babysit, no atomic-with-data semantic |

Source PG wins on the atomic-with-data property: a config row that
flips `auto_create = false` for a namespace at LSN N is guaranteed
to take effect before any post-N xact in the same namespace. That
ordering is impossible if config lives anywhere else. Cost of
shared tablespace pollution is one schema (`walshadow.*`) the ext
already half-owns from [PHASE9](PHASE9.md)

## Strategy

Six pieces. §1 + §2 are the substrate (table schema in the
walshadow ext + config decoder); §3 is the precedence resolver;
§4 + §5 are the runtime channel + xact-buffer wiring; §6 covers
failure containment

### 1. `walshadow.*` config tables

Extend the walshadow PG extension ([`pgext/`](../pgext)) with four
config tables, all owned by the `walshadow` schema the ext already
creates for `walshadow_decode_disk`:

```sql
CREATE TABLE walshadow.config_global (
    only_row    bool   PRIMARY KEY DEFAULT true CHECK (only_row),
    -- emitter runtime
    compression text   CHECK (compression IN ('none','lz4','zstd')),
    row_budget  int,
    byte_budget bigint,
    -- retry knobs
    retry_max_attempts     int,
    retry_initial_backoff  interval,
    retry_max_backoff      interval,
    -- ddl applicator (PHASE15)
    drop_table_strategy    text CHECK (drop_table_strategy IN ('retain','drop','warn')),
    -- runtime tunables
    validate_sampling      int,
    retention_bytes        bigint
);

CREATE TABLE walshadow.config_namespace (
    namespace_name    text PRIMARY KEY,
    target_database   text NOT NULL,
    auto_create       bool NOT NULL DEFAULT false,
    engine_default    text,         -- e.g. 'ReplacingMergeTree(_lsn)'
    order_by_default  text,         -- e.g. '(_lsn)'
    drop_columns      text CHECK (drop_columns IN ('stop_encoding','drop'))
);

CREATE TABLE walshadow.config_table (
    qualified_name  text PRIMARY KEY,   -- 'public.orders'
    target          text NOT NULL,      -- 'default.orders'
    skip            bool NOT NULL DEFAULT false,
    on_drop         text CHECK (on_drop IN ('retain','drop','warn')),
    order_by        text,
    engine          text
);

CREATE TABLE walshadow.config_column (
    qualified_name  text NOT NULL,
    src_attname     text NOT NULL,
    target_name     text NOT NULL,
    target_type     text NOT NULL,
    PRIMARY KEY (qualified_name, src_attname)
);
```

Three properties matter:

- **`REPLICA IDENTITY FULL`** on every config table, set in the
  ext install script. Without it, UPDATE WAL records omit the
  before-image & the decoder cannot resolve which columns
  changed. PHASE10's pre-flight validator already enforces FULL
  globally; this just pins the requirement to the config tables
  specifically
- **Table OIDs are deterministic** across deployments via
  `walshadow.ext_oid_anchors` (PG 18+ allows fixed OIDs on
  extension objects; on PG 17 the daemon discovers them by name at
  attach time & caches the resolved oids in its
  [`ShadowCatalog`](../src/shadow_catalog.rs)). Pinning is
  required because the config decoder dispatches on relfilenode,
  not relname
- **No CH replication**. The config_table mapping resolver
  short-circuits the `walshadow.*` namespace: writes to these
  tables produce config events, not CH inserts. Implemented as a
  hard-coded namespace filter in the decoder sink, not a TOML
  exclusion (operators cannot disable the filter; replicating
  walshadow's own config to CH would be a footgun)

### 2. `config_decoder`

New module `src/config_decoder.rs`, parallel to
[`src/pg_class_decoder.rs`](../src/pg_class_decoder.rs). Subscribes
to the heap-record stream at the same fan-out point
([`src/heap_decoder.rs`](../src/heap_decoder.rs) — exact wire-up
TBD against PHASE13's per-relation routing), filters on the four
config-table relfilenodes resolved at attach time, decodes the
tuple body via the same Tier 1/2 codec stack the user-data path
uses, & emits `ConfigEvent`:

```rust
pub enum ConfigEvent {
    GlobalChanged(GlobalMapping),
    NamespaceUpserted(NamespaceMapping),
    NamespaceRemoved(String),
    TableUpserted(TableMapping),
    TableRemoved(String),
    ColumnUpserted { key: (String, String), mapping: ColumnMapping },
    ColumnRemoved(String, String),
}
```

Payload types reuse PHASE15 §5's `GlobalMapping` / `NamespaceMapping`
+ existing code's `TableMapping` / `ColumnMapping`. The decoder
produces partial mappings (only fields the upserted row touches);
the resolver §3 merges them against the TOML baseline

Decoder owns no PG connection: it reads tuple bytes off the WAL
record & resolves to typed values via the existing
[`heap_decoder`](../src/heap_decoder.rs) machinery. No libpq
round-trip per row. Cost is one extra
match-on-relfilenode per heap write, amortised across the four
config rels (which see writes in the operator timescale, not the
xact timescale)

Why decoder-side & not "re-query the table on every change"? The
config tables can carry tens of thousands of `config_column` rows
in a wide schema. A refetch-on-invalidate pattern (the
[`ShadowCatalog`](../src/shadow_catalog.rs) approach) costs an
O(rows) libpq round-trip per config write; decoder-side parsing is
O(1) per write. Matters more here than for `pg_class` because
config writes can ride the same hot xacts as user-data writes
when ops scripts batch them

### 3. Precedence resolver

`src/config_resolver.rs`. PHASE15 §5 lands the
`watch::Receiver<Arc<ResolvedConfig>>` substrate ([struct shape](PHASE15.md#5-create-table--namespace-tracking))
with a single TOML producer; PHASE16 replaces that single producer
with a three-layer merge. Shape is unchanged from PHASE15's
declaration; the per-column overlay (`columns: HashMap<(String,
String), ColumnMapping>`) already exists as the sink for
`[namespace.<ns>] type_overrides` & gains `walshadow.config_column`
rows as a higher-precedence source at the WAL layer

Precedence, highest wins:

1. **CLI flag** explicit on the command line (clap's
   `ArgMatches::value_source` distinguishes default from explicit)
2. **`walshadow.config_*` row** from source PG, applied at the
   row's commit LSN
3. **TOML** loaded at boot, reload on SIGHUP

CLI on top is intentional. An operator yanking the daemon into a
recovery configuration via flag must not have their flag stomped
by a stale config row that scrolls in from WAL replay. TOML at the
bottom is the bootstrap fallback (§6)

Subscribers (emitter, ch_ddl applicator, metrics endpoint) consume
the same `watch::Receiver<Arc<ResolvedConfig>>` PHASE15 §5
introduced. PHASE16 plugs the WAL-driven & CLI layers into the
resolver's merge point without touching subscriber shapes; SIGHUP
republishes the merged snapshot as before, & §4 republishes on
every applied `ConfigEvent`

### 4. Runtime channel via xact_buffer

PHASE15 §6 lifts the xact buffer's drain queue to
[`DrainEntry::{Tuple, Catalog}`](PHASE15.md#6-drop-table) so DDL
events order against in-flight writes. PHASE16 extends the same
enum:

```rust
enum DrainEntry {
    Tuple(CommittedTuple),
    Catalog(SchemaEvent),       // PHASE15 §6
    Config(ConfigEvent),        // PHASE16 §4
}
```

`XactBuffer::commit`'s drain loop processes config events through
the resolver, which republishes the merged snapshot on the
`watch` channel. Per-row writes that follow inside the same drain
(later WAL position, same xact) see the post-config snapshot
because the emitter snapshots `*watch_rx.borrow()` at xact-start,
not row-start — config flips inside an xact apply to the **next**
xact, matching PG's read-committed semantics

For config changes that gate DDL (e.g. a `NamespaceUpserted` with
`auto_create = true` flipping on for `public`), the resolver
republishes before the next catalog event in the same drain
iterates. PHASE15 §2's `await_ready` gate already serialises DDL
against INSERTs; the resolver's snapshot is a `borrow_and_update`
away

Ordering invariant: within a single source xact, config row
writes that precede heap writes in WAL position apply before
those heap writes are drained. Source xact `BEGIN; UPDATE
walshadow.config_namespace SET auto_create = true WHERE
namespace_name = 'public'; CREATE TABLE public.events (...); END;`
results in the CREATE materialising on CH because the
NamespaceUpserted fires earlier in the same drain loop

### 5. Bootstrap seeding

Daemon attach sequence currently runs TOML parse →
`ShadowCatalog::seed_from_source` → WAL pump start
([`src/bin/stream.rs:567`](../src/bin/stream.rs) area). PHASE16
inserts a config seed step between catalog seed & pump start:

```text
TOML parse                  -- boot config baseline
  ↓
shadow attach + ext check   -- verify walshadow ext installed
  ↓
catalog seed                -- ShadowCatalog::seed_from_source
  ↓
config seed  ← NEW          -- SELECT * FROM walshadow.config_*
  ↓
pump start
```

Config seed issues four `SELECT *` queries through the existing
shadow libpq connection, populates the resolver's initial
snapshot, then the pump starts. Any post-seed config row writes
arrive via §4's drain entry & merge against the seeded baseline.
This is the only libpq path the config layer takes; once the pump
is running, WAL is the only source

Failure modes during seed:

- **Extension absent**: walshadow.config_* tables don't exist.
  Daemon logs WARN, uses TOML-only config. Behavior identical to
  pre-PHASE16 deployment
- **Extension present but config_global row missing**: tables
  exist, are empty. Resolver uses TOML defaults + per-table CLI
  flags. No-op for greenfield deployments
- **Extension version mismatch**: ext at v1 (no config tables)
  vs walshadow expecting v2 (with config tables). `walshadow.ext_version` 
  column on `walshadow.ext_meta` (a single-row meta table the ext
  install script populates) — daemon refuses to start if its
  expected min-version is unmet, surfaces error in pre-flight
  output

### 6. Failure containment & TOML fallback

User's framing constraint: "TOML still useful if WAL processing is
blocked on misconfig." Two failure regimes to keep separate:

**Regime A: WAL pump alive, config row is malformed**

A `config_namespace` row with `auto_create = true` references a
namespace that doesn't exist. A `config_column.target_type` parses
as garbage. A `config_table.target` collides with another row's
target. The resolver validates on merge & rejects the offending
row: the bad row stays in source PG (operator's problem to fix),
the previous-resolved value for that key stays in effect, an
error metric ticks (`walshadow_config_rejections_total{kind=…}`),
& tracing surfaces the rejection at WARN. The daemon does not
crash, does not pause the pump, does not abandon other config
keys

Validation runs at resolver merge time, not decoder time. The
decoder produces typed `ConfigEvent`s freely; the resolver is the
single point of "does this make sense in context"

**Regime B: WAL pump blocked, config rows unreachable**

PHASE10's pre-flight is failing (REPLICA IDENTITY FULL missing,
slot dropped, version skew). PHASE13's catalog gate is wedged on
shadow PG. Source PG is up but walshadow cannot consume its WAL.
In this regime, the daemon falls back to **TOML + CLI only**, no
`walshadow.config_*` overlay, because the overlay's freshness
cannot be guaranteed. Operator can still hand-tune mapping via
TOML + SIGHUP & ship to recovery destinations

Switch-over is automatic: the resolver tracks a
`config_freshness_lsn` (the highest LSN at which a config event
applied). If `now() - last_resolver_apply > config_staleness_max`
(default 5min), an alarm fires & the resolver flags itself
"degraded": new SIGHUPs still apply the TOML layer, the
walshadow_config overlay freezes at its last-known state but is
not zeroed out. Pump recovery (PHASE10/13 path) re-applies the
overlay automatically as WAL replay catches up

The hard guarantee: **TOML configures the daemon's connection to
source/shadow**, & TOML configures everything when source-driven
overlay isn't fresh. The walshadow_config overlay is strictly
additive on top of a working TOML

## What stays (anti-goals)

- **TOML stays valid**. Existing `[ch]`, `[table.…]`, & PHASE15's
  `[namespace.…]` blocks keep working unchanged. A deployment that
  doesn't install the walshadow ext or doesn't populate
  `walshadow.config_*` sees zero behavioral difference
- **Connection params stay TOML/CLI**. `[ch] host/port/database/user/password`
  for CH, `--source-conninfo` & `--shadow-conninfo` for PG. These
  cannot live in source PG because they describe how to reach
  source PG. Bootstrap fixed point
- **Spill paths, retention dir, metrics bind**. Daemon-host
  infrastructure stays in CLI flags. Source PG has no business
  knowing about walshadow's filesystem layout
- **`bootstrap_mode` & friends**. Boot-time CLI flags. By the time
  the config table is readable, bootstrap is done
- **DDL on `walshadow.config_*` itself**. Schema migrations to the
  config tables ride the walshadow PG extension's version-bump
  path, not source-side `ALTER TABLE`. PHASE15's auto-DDL applicator
  is filtered against the `walshadow` namespace so config-table
  schema changes don't try to mirror to CH
- **Config write authorisation**. PG's GRANT/REVOKE is the
  authority. If a DBA wants only specific roles to write
  `walshadow.config_*`, they GRANT on the tables. Daemon does not
  re-validate row provenance
- **Config row removal semantics**. DELETE on `walshadow.config_table`
  reverts to "no per-table mapping for this qualified name" — the
  namespace-level config (if `auto_create = true`) takes over,
  identical to "operator removed `[table.…]` from TOML & SIGHUP'd".
  No retroactive un-do of past CH state; config drives forward only
- **No config replication to CH**. The hard-coded namespace
  filter in §1 is not overrideable
- **Two-way sync**. Daemon does not write back to `walshadow.config_*`.
  Source PG is the single writer; daemon is the single reader.
  Read-only on source for the daemon — keeps the
  "walshadow never writes to source" invariant the project has
  held since PHASE3

## Open questions

- **Config row LSN attribution**. PHASE15 §2's `await_ready(rfn,
  generation)` keys on a `(rfn, generation)` pair. A config flip
  that changes a `column_mapping.target_type` for `public.orders`
  needs to drain through the same gate. Cleanest answer: bump the
  catalog generation counter for affected rfns on config apply, so
  the emitter's gate flushes & rebuilds the `TablePlan`. Cost is
  one fake-invalidate per config event that mentions a relation;
  acceptable since config events are rare
- **Conflicting writes during failover**. Source PG fails over to
  a replica that has stale `walshadow.config_*` data (e.g. replica
  was behind on logical apply). Daemon following the new primary
  sees the old config & rolls back. Mitigation: the resolver
  records the LSN of every apply, & a config row with an `LSN`
  lower than the resolver's `config_freshness_lsn` is logged &
  ignored. Real fix is the operator running `pg_dump` of
  `walshadow.config_*` from the old primary into the new one
  before failover; daemon doesn't try to be smarter than the DBA
- **Schema evolution of the config tables themselves**.
  walshadow ext at version K declares config schema; the daemon
  binary at version K' may expect a newer column. Strategy:
  config tables grow columns additively, NULL = "daemon's default
  applies", & the ext install script declares the min-daemon
  version it supports in `walshadow.ext_meta`. Mismatch surfaces
  in pre-flight, not at first row read
- **High-throughput config writes**. An ops script that writes
  10k `config_column` rows in one xact stalls the drain loop on
  resolver merges. Resolver merge is in-memory & should be µs per
  event, but the test matrix should pin the worst case. Bound:
  if a single drain processes > N config events, batch the
  resolver republish so subscribers see one merged snapshot
- **CLI override discoverability**. With three precedence levels,
  an operator querying "why is `auto_create` false for public?"
  needs a tool. Out of scope for PHASE16's binary additions but a
  `walshadow-stream config explain <key>` subcommand is a natural
  PHASE17 follow-up. Minimum for PHASE16: a Prom metric per
  resolved key with source label (`source="cli|wal|toml"`)
- **Source PG without superuser**. The walshadow ext install
  requires superuser on source PG (PHASE9 already needed this for
  `walshadow_decode_disk` C-language function registration).
  Managed PG that forbids C extensions cannot install the ext &
  therefore cannot use PHASE16's overlay; falls back to TOML-only
  cleanly. Documenting this in the deployment guide is sufficient
- **Config events ordering vs schema events**. A single source
  xact can issue `BEGIN; ALTER TABLE foo ADD COLUMN c text;
  INSERT INTO walshadow.config_column VALUES ('public.foo', 'c',
  'c', 'String'); END;`. WAL position of the config write follows
  the catalog write; drain sees `SchemaEvent::Changed` first (new
  column) then `ConfigEvent::ColumnUpserted` (explicit type
  pinning). PHASE15 §3 type-bridge defaults `text → String`
  anyway, so the config event is a no-op in practice, but the
  general principle (config events apply after the catalog
  events they refer to, by WAL ordering) is the invariant. Verify
  via integration test

## Acceptance

- **Greenfield seed drill**. Daemon boots with walshadow ext
  installed but all `walshadow.config_*` tables empty. Behaviour
  is identical to a pre-PHASE16 deployment with the same TOML.
  Confirms zero-regression for ops who don't opt into the overlay
- **Namespace flip drill**. Daemon running with TOML `auto_create
  = false`. Operator runs `INSERT INTO walshadow.config_namespace
  (namespace_name, target_database, auto_create) VALUES ('public',
  'default', true);` on source PG. A subsequent `CREATE TABLE
  public.events(...)` on source materialises on CH inside one
  round-trip, matching the PHASE15 §5 acceptance drill but with
  config arriving via WAL
- **Mapping-add drill**. Source-side `INSERT INTO
  walshadow.config_table ... ; INSERT INTO walshadow.config_column
  ...` declares a new mapping inside a single xact. The same xact
  inserts rows into the now-mapped table. CH dest receives the
  rows under the post-config mapping. Confirms the within-xact
  ordering invariant
- **TOML fallback drill**. Source PG has populated config rows
  flagging `auto_create = true`. WAL pump artificially blocked
  (e.g. drop the publication mid-run). Resolver flips to degraded
  after `config_staleness_max`. Operator SIGHUPs a TOML edit that
  flips `auto_create = false` for the same namespace; subsequent
  source-side CREATE TABLE no longer mirrors (because the WAL is
  blocked & the TOML now says no). Pump unblock restores the
  walshadow_config overlay, & a new CREATE mirrors again under the
  WAL-driven setting. Confirms TOML doesn't get "stuck off" when
  the overlay reconnects
- **Validation rejection drill**. Insert a `config_column` row
  with `target_type = '!@#$'`. Resolver rejects, metric ticks,
  daemon stays up, other mappings unaffected. Operator UPDATEs
  the bad row to a valid type, & the next WAL apply picks up the
  corrected value
- **Precedence drill**. CLI `--drop-table-strategy=warn` + a
  `walshadow.config_global` row setting `drop_table_strategy =
  'drop'` + TOML setting it to `retain`. Resolver applies `warn`
  (CLI wins). Remove the CLI flag (next daemon restart) → resolver
  applies `drop` (WAL overlay wins). Truncate the config_global
  row → resolver applies `retain` (TOML)
- `cargo test --workspace --lib` stays green. Unit tests cover
  `config_decoder` heap-tuple parsing per table, `config_resolver`
  precedence + validation + degraded mode transitions, & the
  watch-channel republish behaviour
- New integration test `tests/phase16_wal_driven_config.rs`:
  spins source PG with walshadow ext installed, exercises the
  greenfield, namespace flip, mapping-add, TOML fallback, &
  validation drills against a real CH server (mirrors
  `phase15_ddl_replicates`'s harness shape from PHASE15)
- Existing PHASE10 SIGHUP test keeps working — the TOML layer is
  unchanged in its semantics, just narrowed in scope when the
  overlay is fresh
- Existing TOML configs continue to load without warning. Operator
  who never installs the walshadow ext sees no PHASE16-related
  log output beyond a single boot-time INFO "config overlay
  disabled (ext absent)"

## Sequencing

§1 (ext schema additions) is independent & lands first; the SQL
install script lives in [`pgext/`](../pgext) & doesn't touch
walshadow's rust code. §2 (config_decoder) depends on §1 & on
PHASE5's heap_decoder being stable; can land in parallel with
PHASE15 §1's event channel because both decoders attach at the
same fan-out point but on different relfilenodes. §3 (resolver)
needs PHASE15 §5's `watch::Receiver<Arc<ResolvedConfig>>` substrate
already in place; §4 (drain wiring) needs PHASE15 §6's
`DrainEntry::Catalog`. PHASE16 lands strictly after PHASE15
closes §5 + §6. §5 (bootstrap seeding) + §6 (failure containment) land
together — they share the freshness-tracking + degraded-mode
state

1. §1 ext schema + install script
2. §2 decoder + §3 resolver scaffold (resolver stubbed at first;
   merge logic lands incrementally per config_* table)
3. §4 drain wiring + watch channel republish path
4. §5 seed path + §6 failure containment
5. Per-key migration of TOML knobs to overlay-resolvable: emitter
   compression/budgets first (smallest blast radius), then
   namespace/table/column mapping, then DDL strategy flags. Each
   step independently shippable

Phase closes when the namespace-flip drill + mapping-add drill
both pass end-to-end against a daemon configured with a minimal
TOML (connection params only) + walshadow.config_* overlay. That's
the demonstration that ops drives walshadow's CDC topology via SQL
against source PG, with TOML reduced to its bootstrap role

Size estimate: ~850 LOC product (ext schema + install script ~120,
config_decoder ~280, config_resolver ~320, drain wiring + watch
~80, bootstrap + failure containment ~150) + ~620 LOC tests.
Smallest load-bearing decision: which config keys move first (the
incremental migration list in step 5 above). Largest: the
degraded-mode state machine in §6 — staleness threshold, alarm
shape, & the recovery-from-degraded path
