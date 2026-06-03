# emitter

CH-side ingest: per-relation buffer-then-seal INSERT pump, DDL
applicator, PG → CH type bridge. Three modules — `src/ch_emitter.rs`,
`src/ch_ddl.rs`,
`src/type_bridge.rs`. Consumed through `TupleObserver` so xact-buffer
drain feeds it verbatim

## Purpose

Translate committed-xact tuple streams from xact-buffer's k-way-merge
into ClickHouse Native blocks buffered per table and sealed as complete
INSERTs.
DDL applicator runs in lockstep on sibling CH connection, consuming
`SchemaEvent`s off `ShadowCatalog::subscribe` and reshaping CH tables
to track source PG catalog deltas. Emitter ack-LSN feeds cursor file so
restart resumes from highest commit-record LSN known durable on CH

## Two CH connections

![emitter](../architecture/emitter.svg)

Per-replica wiring is two TCP sockets, both built off same
`EmitterConfig` `(host, port, user, password, database)`:

- `Emitter::client` — steady-state INSERT pump. One open INSERT at a
  time (CH protocol limit), single-table-at-a-time `send_query` +
  `send_data` loop. Lazy: no wire activity until first row lands
- `DdlApplicator::client` — DDL writer. `send_query` + drain to
  `EndOfStream` per ALTER / CREATE / DROP. Stays idle until a
  `SchemaEvent` arrives

Two connections, not one, because CH's `Client` is
single-query-at-a-time. An in-flight INSERT flush (hot path) would
block any ALTER that needed to ride same wire. Surgical flush on the
affected relation lands its buffered rows before the DDL; other tables'
buffered rows stay untouched across the DDL

## Hold-open buffer shape

Wire shape pivoted from `one-INSERT-per-table-per-xact` to rows held in
per-table `TableEncoder` buffers across xacts, each flush sealing one
*complete* INSERT. No INSERT is held open on the wire between flushes:
`open_wire` issues `send_query`, `close_current_wire` ships the block +
`send_data(None)` + drains `EndOfStream`, and `flush_table` runs the two
back-to-back. `Emitter::wire_open_key` carries the transiently-open
table; `open_wire(key)` no-ops only within one flush's open→close window

The buffer is cleared only after `EndOfStream` (in
`close_current_wire`). A mid-flush disconnect rolls the whole
in-progress INSERT back on CH, so the rows stay buffered and the retry
replays them — `ReplacingMergeTree(_lsn)` collapses any replay dupes.
The earlier shape left rows shipped-but-unsealed on an open wire, which
a disconnect would have lost

`flush_timeout = 0` keeps legacy seal-per-xact behaviour
(`emitter_ack_lsn` tracks `drain_lsn` exactly). Non-zero `flush_timeout`
lets pgbench-shaped 4-table xacts coalesce into one MergeTree part per
window instead of one per xact. Latency cap is configured timeout from
first-buffered-row-of-window

Deadline timer arms when the first row of a fresh window buffers
(`on_xact_end_with_lsn` sets `flush_deadline = now + flush_timeout`;
`open_wire` re-arms defensively). Idle ticks call
`flush_if_deadline_tripped` via `TupleObserver::on_idle` so the last
burst before traffic stops doesn't sit past the deadline

## BlockBuilder per relation

`TableEncoder` owns one `Vec<ColumnBuf>` per destination column, mapped
+ synthetic. Built lazily on first row via `TablePlan::build` off
descriptor + mapping; cached in `Emitter::tables` keyed on source
`<namespace>.<relname>`. Encoder is column-major: each column
accumulates into its own slab, `BlockBuilder` borrows into all slabs at
flush time, `send_data` ships, then `clear()` zeros lengths while
keeping allocation

`ColumnBuf` variants:

| variant | shape | source CH kind |
|---|---|---|
| `Fixed { width, bytes }` | packed LE | non-null fixed-width (Int*, Float*, Decimal*, FixedString, DateTime64, Enum) |
| `String { offsets, data }` | varlen + cumulative offsets | non-null String |
| `NullableFixed { width, null_map, inner }` | dense fixed + null-bitmap | Nullable(fixed) |
| `NullableString { offsets, data, null_map }` | varlen + null-bitmap | Nullable(String) |

Width comes from `clickhouse-c-rs`'s `chc_type_elem_size`, not a
walshadow-side type table, so `FixedString(N)`, `DateTime64(p)`,
`Decimal*(p,s)`, `Enum8` etc resolve without walshadow mirroring
upstream surface. `elem_size == 0` means varlen; only varlen shape
today is `String`, anything else dies cleanly at `append`

Flush triggers (each seals a *complete* INSERT — open → block →
`EndOfStream` → clear):

- `enc.rows >= config.row_budget` (default 65536) — `tripped` branch in
  `Emitter::route`
- `enc.approx_bytes >= config.byte_budget` (default 1 MiB)
- xact end (legacy mode), deadline trip, schema event, TRUNCATE,
  shutdown

Compression: feature-gated through walshadow's own `lz4` / `zstd`
features which forward to `clickhouse-c-rs`. `CompressionChoice::Lz4`
is default; `build_codec` returns `EmitterError::CompressionUnsupported`
when variant's feature is off. CH wire default is LZ4 so default build
matches CH's own posture

## Type bridge

`type_bridge::map(att, pk_member) -> ResolvedColumn` maps one
`RelAttr` to CH type expression plus optional `DEFAULT <expr>`.
`pk_member = true` strips `Nullable(_)` wrap because CH refuses
`Nullable` in `ORDER BY`. Matrix is hard-coded in `base_type_for`:

| PG | CH |
|---|---|
| bool | Bool |
| "char" / int2/4/8 | Int8/16/32/64 |
| oid | UInt32 |
| float4/8 | Float32/64 |
| numeric(p,s), 1 ≤ p ≤ 76 | Decimal(p,s); else String |
| text / varchar(n) / bpchar(n) / name / bytea | String |
| date | Date32 |
| time | Time64(6) |
| timetz / interval | String (text form) |
| timestamp(p) / timestamptz(p) | DateTime64(p, 'UTC'), p ≤ 6 |
| uuid | UUID |
| inet / cidr / json / jsonb | String |
| array / unknown | String fallback |

`numeric` needs `1 ≤ p ≤ 76` for `Decimal`; `p = 0`, scale outside
`0 ≤ s ≤ p`, or unconstrained `numeric` (which can carry NaN/±Inf) fall
back to `String`. Into a `Decimal` column `encode_value` ships the value
as a scaled little-endian two's-complement integer (`value * 10^scale`,
U256 arithmetic spanning Decimal128/256 widths); NaN/±Inf into a
`Decimal` column is unrepresentable and errors with `UnsupportedValue`
(map that column to `String` to keep them). A `String`-mapped `numeric`
still ships lossless text including NaN/Inf

`time` → `Time64(6)` ships raw microseconds-since-midnight LE. CH 25.x
gates `Time64` behind `enable_time_time64_type=1`; the dest server's
profile must enable it or auto-create / insert on `time` columns fails.
`timetz` → `String` renders via `codecs::timetz_to_text`, preserving the
UTC offset the old fixed encoding silently dropped

Default expressions reconstruct from `RelAttr.missing_text` (fast-path
`attmissingval[1]` PG plants on `ALTER TABLE ADD COLUMN ... DEFAULT k`).
`render_default` routes through
`heap_decoder::missing_value_for(att) -> ColumnValue`, then
`column_value_to_sql_literal` emits CH literal — booleans land as
`true`/`false`, ints unquoted, strings single-quoted with `'` escaping,
timestamps as `toDateTime64('...', 6, 'UTC')`. Unbridged shapes return
`None` so `ALTER TABLE ADD COLUMN` lands without a `DEFAULT` clause;
CH applies its own zero-init

### Synthetic columns

Every destination table carries four trailing synthetic columns,
non-nullable by construction, encoded in `TableEncoder::new`:

| column | type | purpose |
|---|---|---|
| `_lsn` | `UInt64` | source commit-record LSN. `ReplacingMergeTree(_lsn)` keys dedup on this so restart-and-replay window collapses re-emitted rows to latest LSN per PK |
| `_xid` | `UInt32` | source xid. Lets analytic queries group all rows from one xact, recover xact boundary CH lost when emitter serialised across tables |
| `_op` | `Enum8('insert'=1,'update'=2,'delete'=3)` | row-op classification. CH-side `WHERE _op != 3` is the cheap "live rows" filter; HOT_UPDATE collapses to UPDATE (code 2), PG-internal distinction doesn't reach CH |
| `_commit_ts` | `DateTime64(6, 'UTC')` | xact commit timestamp, shifted from PG's 2000-01-01 epoch to Unix via `DATETIME64_PG_EPOCH_US` |

`_lsn` is dedup key because emitter ack lags actual CH durability by up
to one flush window. On restart cursor's `emitter_ack_lsn` rewinds to
last close-acked LSN; everything between that and now-stale buffered
rows re-emits, `ReplacingMergeTree(_lsn)` resolves duplicates
server-side without walshadow having to track which rows already landed

## Mapping config

`EmitterConfig::tables` parses from TOML `[table."<src>"]` blocks:

```toml
[table."public.foo"]
target = "default.foo"
columns = [
  { attnum = 1, target = "id",   type = "UInt64" },
  { attnum = 2, target = "name", type = "Nullable(String)" },
]
```

`MappingHandle = Arc<tokio::sync::RwLock<HashMap<String, TableMapping>>>`
is live handle emitter consults per row. Handle is cloneable; daemon's
SIGHUP task swaps whole inner `HashMap` between xacts. Cached
`TableEncoder`s in `Emitter::tables` keep their old `TablePlan` until
next route call rebuilds off fresh mapping

### NamespaceMapping (partial)

Per-source-namespace defaults block, `[namespace."public"]`. Three
fields wired today:

```rust
pub struct NamespaceMapping {
    pub target_database: Option<String>,
    pub auto_create: bool,
    pub drop_table_strategy: Option<String>,
}
```

`auto_create = true` lets `DdlApplicator::apply_added` run
`CREATE TABLE IF NOT EXISTS` on first sight of a relation in the
namespace and auto-derive a `TableMapping` via
`derive_columns_for_mapping`. Per-table TOML still wins when both are
configured for the same relation

`target_database` and `drop_table_strategy` resolve per-namespace
through `DdlConfig::{target_database_for, drop_strategy_for}`: the
applicator carries the namespace map and falls back to the global
`target_database` / `drop_table_strategy` when a namespace has no
override. The per-namespace `target_database` drives both the CREATE
and the derived row-routing mapping, so rows and DDL land in the same
database

## NOT yet landed for namespace mapping

`auto_create`, `target_database`, and `drop_table_strategy` ship; the
richer namespace surface the plan called for is still missing:

- `ResolvedConfig` struct: design called for one pre-materialised value
  carrying `tables`, `namespaces`, and a
  `columns: HashMap<(String, String), ColumnMapping>` type-override
  table. Today no such type; mapping lives in
  `Arc<RwLock<HashMap<String, TableMapping>>>` and namespace defaults
  live separately on `EmitterConfig::namespaces`
- `watch::Receiver<Arc<ResolvedConfig>>` emitter wiring:
  runtime-config-from-PG path wants emitter to consume watch stream so
  config changes propagate without SIGHUP. Today's reload channel is
  `RwLock` swap kicked by SIGHUP
- `NamespaceMapping.order_by_default`: `render_create_table` hard-codes
  `ORDER BY (_lsn)` fallback when no PK exists
- `NamespaceMapping.engine_default`: `render_create_table` hard-codes
  `ENGINE = ReplacingMergeTree(_lsn)`. Plan wanted per-namespace
  override (e.g., `MergeTree`, `CollapsingMergeTree`)
- `NamespaceMapping.type_overrides`: plan wanted per-column type
  overrides keyed on `(namespace, src_attname)`. Today only path is
  per-table TOML

See [future/runtime_config_from_pg.md](future/runtime_config_from_pg.md)
— pg-driven config substrate depends on this resolver shape

## DdlApplicator

`ch_ddl.rs::DdlApplicator` consumes `SchemaEvent` off
`ShadowCatalog::subscribe` (`mpsc::UnboundedReceiver`, not bounded —
plan said bounded, landed code uses unbounded so a stalled applicator
never back-pressures catalog producer). Apply table:

| `SchemaEvent` | CH SQL |
|---|---|
| `Added { desc }` | `CREATE TABLE IF NOT EXISTS` (in the namespace's `target_database`, else global default) when namespace `auto_create = true` and no pre-pinned mapping. Auto-derives `TableMapping` against that same database post-success so next `route` call ships rows against the new table |
| `Changed { diff }` | `ALTER TABLE … RENAME COLUMN` first (so position-match diffs don't trip into drop+add), then `ALTER TABLE … ADD COLUMN IF NOT EXISTS` per added attnum, then `ALTER TABLE … DROP COLUMN IF EXISTS` per dropped attnum |
| `Changed.type_changes` | rejected, logged, `stats.type_changes_rejected += n`. Operator handles via manual CH migration |
| `Dropped { qualified_name }` | gated on the namespace's `DropTableStrategy` (`drop_strategy_for`, else global): `Retain` (default) skips silently, `Warn` skips at WARN, `Drop` runs `DROP TABLE IF EXISTS` |

`render_create_table` builds CREATE off descriptor: attributes through
`type_bridge::map`, PK columns first in `ORDER BY` (else `_lsn`
fallback), engine pinned to `ReplacingMergeTree(_lsn)`. Synthetic
columns appended after mapped columns, same shape as `TablePlan::build`

`apply_changed` also mutates live `MappingHandle` via
`mutate_mapping_for_diff`: renames update `target_name` in place (when
operator's TOML used old source name), drops strip `ColumnMapping`,
adds push new entry derived through `type_bridge::map`. Operator-pinned
overrides survive: only `src_attnum`-matching entries the applicator
could have produced get touched

### Baseline seeding (the `Added`-vs-`Changed` discriminator)

Whether a relation's first post-start descriptor fetch surfaces as
`Added` or `Changed` keys on whether `ShadowCatalog::prev_known` already
holds its oid. `prev_known` is the *baseline ledger* (last source shape
CH and source agreed on), not the descriptor cache — cold at every boot,
never reconstructed on a miss. Left cold, a pinned table that sees no DML
before its first `ALTER` records the post-ALTER shape as `Added`, which
`apply_added` skips for pinned dests (operator-managed CH) → CH stays a
column behind.

`ShadowCatalog::seed_baseline(qualified_names)` warms `prev_known` for
every pinned relation before `subscribe()` so the cache never decides the
branch: `bin/stream.rs` calls it after preflight / before
`START_REPLICATION` over `cfg.tables.keys()` (the inproc harness mirrors
it before its own `subscribe`). Pre-subscribe, `send_event` is a no-op,
so seeding emits nothing and does zero CH work. The first post-boot
`ALTER` then diffs the evolved descriptor against the seeded boot shape →
`Changed` → the `apply_changed` path above runs the CH ALTER.

Seeds the *full source* descriptor, never the mapping: a pinned subset's
unmapped columns sit in the baseline and read as "operator-excluded", so
a later `ALTER` adds only genuinely-new columns, never re-adds an
excluded one. Auto-create tables need no seeding — their first-touch
`Added` → `CREATE TABLE` already records a baseline. Open: boot-time
drift (column added while the daemon is down folds silently into the
seeded baseline) — see `plans/future/pinned_ddl_baseline.md` "Deferred".

### await_ready gate

Coordination with INSERT pump is synchronous, not channel-based.
`Emitter::dispatch_schema_event` flushes + closes wire on affected key,
drops `tables[key]`, then calls `applicator.apply` (diagram, top of
DDL column). Surgical close (this table only) keeps other tables' open
INSERTs intact, important for pgbench's 4-table-per-xact shape where
closing-all would break cross-INSERT pipeline

## TRUNCATE path

`HeapOp::Truncate` in `Emitter::route`:

1. Flush any pending rows for relation through `flush_table` (so prior
   INSERTs in same xact land before truncate)
2. `close_current_wire` — drops open INSERT if any
3. Remove relation's `TableEncoder` from `tables`
4. `send_query("TRUNCATE TABLE <dest>")` on emitter's client
5. Drain to `EndOfStream` / `Exception`
6. Bump `stats.truncates_emitted`

`RESTART_SEQS` flag is ignored — sequence state isn't replicated.
PG's `TRUNCATE … RESTART IDENTITY` arrives as same `HeapOp::Truncate`
with no flag distinction at emitter layer; bit lives on PG xlog record
but doesn't propagate through `DecodedHeap`

## Foreign-DB row skip

Physical replication ships the whole cluster's WAL, so `route` sees
heap records for relations in other databases. `relation_at` rejects
those with `CatalogError::ForeignDatabase` (filenode resolved to a
`db_node` that's neither the shadow DB nor a shared catalog — see
[shadow.md](shadow.md)). Emitter treats it like an unmapped relation:
no append, no poison, bump `stats.foreign_db_rows_skipped`, return
`Ok(())` so the ack advances past it

## Read-time defaults integration

PG's fast-path `ALTER TABLE ADD COLUMN … DEFAULT k` plants
`attmissingval[1]` instead of rewriting heap. `RelAttr.missing_text`
carries typoutput text; resolution tiers:

- Tier 1 (immediate): bool / int / float / numeric / text — decoder
  resolves at parse time via `heap_decoder::missing_value_for(att)`,
  emitter sees fully-decoded `ColumnValue`
- Tier 2 (typmod-aware): timestamp / timestamptz / date — decoder
  resolves with typmod, emitter sees concrete `ColumnValue`
- Tier 3 (oracle): unsupported / array / domain types — decoder emits
  `ColumnValue::PgPending { raw, type_oid }`. Oracle extension
  (separate PG-side process) resolves at emit time; falls through to
  raw bytes when oracle absent

`encode_value` in emitter handles `PgPending` by shipping `raw` as
String — no error, no stat bump, operators handle post-process via
PG-side tooling. See [decoder.md](decoder.md) for tier classification +
[oracle.md](oracle.md) for extension protocol

## Ack-LSN tracking

`TupleObserver::on_xact_end(&mut self, commit_lsn: u64) -> Result<u64, …>`
returns highest LSN known durable on CH. Two values move through
emitter:

- `pending_max_commit_lsn`: highest `commit_lsn` of any row buffered in
  a `TableEncoder` but not yet sealed by a completed INSERT
  (`EndOfStream`). Bumped per tuple in `route`, reset to 0 once every
  buffer has drained. Nothing is ever shipped-but-unsealed, so this
  tracks buffered rows alone
- `last_durable_commit_lsn`: monotonic horizon. Promoted from
  `pending_max_commit_lsn` only inside `close_all_open_inserts`
  (deadline trip or legacy per-xact close) or when an empty xact
  arrives with no rows pending. Promotion is safe precisely because
  every buffer is drained first — the horizon never passes a
  still-buffered row

Hold-open mode means `last_durable_commit_lsn` lags `drain_lsn` until
the deadline trips — `emitter_ack_lsn` in cursor file reflects that lag

`idle_ack_ceiling(lsn)` exposes that horizon to the idle path: when
nothing is buffered (no open wire, every encoder empty) it returns
`lsn` (trailing non-commit WAL is shippable-free, safe to ack fully),
else it caps at `last_durable_commit_lsn` so a quiescent-tick nudge
can't ack rows still buffered. `on_idle` separately returns the LSN a
deadline-triggered close just made durable, which the xact buffer folds
into `emitter_ack_lsn` via `note_idle_durable`. See [ops.md](ops.md)
for cursor + recovery contract; `cursor.rs` writes value to disk on
every observer ack and replay starts from
`min(shadow_replay_lsn, emitter_ack_lsn)`

## Bootstrap-time emitter

Transitional emitter spun up by `backfill_bootstrap.rs` for initial
COPY-FROM drain. No `DdlApplicator` attached (bootstrap descriptor set
is frozen at snapshot time), no SIGHUP wiring, no held-open behaviour.
Force-closed at end of bootstrap via `flush_open_inserts`; steady-state
emitter then opens fresh connections for streaming. See
[bootstrap.md](bootstrap.md)

## Retry behaviour

Bounded retry on every public `Emitter::*` method. `is_retryable`
classifies `EmitterError::{Io, Client, ServerException}` as transient
(network / CH-server / clickhouse-c protocol); `Config`, `Type`,
`Catalog`, `UnsupportedValue` stay fatal because they encode bugs in
daemon or mapping that retry would loop forever on

Wrapper functions (`route_with_retry`, `on_xact_end_with_retry`,
`flush_if_deadline_tripped_with_retry`, `flush_open_inserts_with_retry`)
loop up to `RetryConfig::max_attempts` with exponential backoff capped
at `max_backoff`, calling `Emitter::reconnect` between attempts.
`reconnect` opens fresh `TcpStream`, builds new `Client`, hot-swaps
`self.client`, clears `wire_open_key`. Per-table accumulator state in
`self.tables` survives so a CH bounce mid-xact lets surviving buffered
rows flush through new connection on retry

Budget expiry kills daemon — `route_with_retry` returns last `Err`,
worker poisons stream, daemon exits, cursor file resumes on restart.
See [future/ch_bounce_recovery.md](future/ch_bounce_recovery.md) for
deeper "re-emit from spill" story (segment-buffered replay across
extended CH outages) not yet shipped

DDL retry is currently a no-op: `dispatch_schema_event_with_retry`
calls through without retry — DDL errors poison stream so operator
sees them directly. Runtime-config-from-PG work may add bounded
reconnect for DDL connection

## Cross-links

- [xact.md](xact.md) — `XactBuffer::commit` k-way-merges
  `CommittedTuple` + `SchemaEvent` in source-LSN order, drains into
  emitter via `TupleObserver`
- [shadow.md](shadow.md) — `ShadowCatalog::subscribe` produces
  `SchemaEvent` stream; catalog snapshot drives descriptors
  `TablePlan::build` reads
- [decoder.md](decoder.md) — `HeapDecoder` produces `ColumnValue` /
  `CommittedTuple`. Read-time defaults tier-classify here
- [ops.md](ops.md) — `cursor.rs` writes `emitter_ack_lsn` to on-disk
  cursor file; restart resumes from
  `min(shadow_replay_lsn, emitter_ack_lsn)`
- [safety.md](safety.md) — `clickhouse-c-rs` unsafe surface
  (`BlockBuilder` borrows into `ColumnBuf` slabs, `PosixIo` owns fd,
  `Client` lifetime invariants)
- [bootstrap.md](bootstrap.md) — transitional bootstrap emitter wiring,
  force-close handshake
- [oracle.md](oracle.md) — Tier 3 default resolution via PG-side
  extension, `PgPending` routing
- [future/runtime_config_from_pg.md](future/runtime_config_from_pg.md)
  — runtime-config substrate; `ResolvedConfig` + `watch::Receiver`
  shape partial namespace-mapping work needs to land first
- [future/ch_bounce_recovery.md](future/ch_bounce_recovery.md) —
  spill-buffered re-emit for extended CH outages
