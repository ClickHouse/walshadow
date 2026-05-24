# emitter

CH-side ingest surface: per-relation held-open INSERT pump, DDL
applicator, PG → CH type bridge. Lives in three modules,
`src/ch_emitter.rs`, `src/ch_ddl.rs`, `src/type_bridge.rs`. Consumed
through the `TupleObserver` trait so the xact-buffer drain feeds it
verbatim without wiring detours.

## Purpose

Translate committed-xact tuple streams from the xact-buffer's
k-way-merge into ClickHouse Native blocks landing on per-table
held-open INSERTs. DDL applicator runs in lockstep on a sibling CH
connection, consuming `SchemaEvent`s off `ShadowCatalog::subscribe`
and reshaping CH tables to track source PG catalog deltas. The
emitter ack-LSN feeds the cursor file so a restart resumes from the
highest commit-record LSN known durable on CH.

## Two CH connections

Per-replica wiring is two TCP sockets, both built off the same
`EmitterConfig` `(host, port, user, password, database)`:

- `Emitter::client` — steady-state INSERT pump. One open INSERT at a
  time (CH protocol limit), single-table-at-a-time `send_query` +
  `send_data` loop. Lazy: no wire activity until first row lands.
- `DdlApplicator::client` — DDL writer. `send_query` + drain to
  `EndOfStream` per ALTER / CREATE / DROP. Stays idle until a
  `SchemaEvent` arrives.

Two connections, not one, because CH's `Client` is
single-query-at-a-time. Holding an INSERT open across xacts (the
hot path) would block any ALTER that needed to ride the same wire.
Surgical close on the affected relation gates the DDL behind any
buffered rows for that table; other tables' open INSERTs stay live
across the DDL.

## Held-open INSERT shape

Wire shape pivoted from `one-INSERT-per-table-per-xact` to
`one-INSERT-per-table held across xacts`:

- `send_query("INSERT INTO <dest> (...) FORMAT Native")` once per
  table per flush window. `Emitter::wire_open_key` carries the
  currently-open table; `open_wire(key)` is a no-op when the key
  matches, else closes the prior wire (one `send_data(None)` + drain
  to `EndOfStream`) before opening the new one.
- `send_data(Some(&block))` per flush of that table's per-relation
  encoder. Flushes fire on `row_budget` / `byte_budget` trip inside
  `route`, or on deadline trip inside `close_all_open_inserts`.
- `send_data(None)` + drain only on `flush_timeout` deadline, on
  table-switch, on schema event for the affected relation, on
  TRUNCATE, or on `on_close` (shutdown).

`flush_timeout = 0` keeps the legacy close-per-xact behaviour
(`emitter_ack_lsn` tracks `drain_lsn` exactly). Non-zero
`flush_timeout` lets pgbench-shaped 4-table xacts coalesce into one
MergeTree part per window instead of one per xact. Latency cap is
the configured timeout from first-row-of-window.

Deadline timer starts when the first row of a fresh INSERT lands
(`open_wire` sets `flush_deadline = now + flush_timeout`). Idle
ticks call `flush_if_deadline_tripped` via `TupleObserver::on_idle`
so the last burst before traffic stops doesn't sit past the
deadline.

## BlockBuilder per relation

`TableEncoder` owns one `Vec<ColumnBuf>` per destination column,
mapped + synthetic. Built lazily on first row via `TablePlan::build`
off the descriptor + mapping; cached in `Emitter::tables` keyed on
source `<namespace>.<relname>`. The encoder is column-major: each
column accumulates into its own slab, the `BlockBuilder` borrows
into all slabs at flush time, `send_data` ships, then `clear()` zeros
the lengths while keeping the allocation.

`ColumnBuf` variants:

| variant | shape | source CH kind |
|---|---|---|
| `Fixed { width, bytes }` | packed LE | non-null fixed-width (Int*, Float*, Decimal*, FixedString, DateTime64, Enum) |
| `String { offsets, data }` | varlen + cumulative offsets | non-null String |
| `NullableFixed { width, null_map, inner }` | dense fixed + null-bitmap | Nullable(fixed) |
| `NullableString { offsets, data, null_map }` | varlen + null-bitmap | Nullable(String) |

Width comes from `clickhouse-c-rs`'s `chc_type_elem_size`, not a
walshadow-side type table, so `FixedString(N)`, `DateTime64(p)`,
`Decimal*(p,s)`, `Enum8` etc. resolve without walshadow mirroring
the upstream surface. `elem_size == 0` means varlen; the only varlen
shape today is `String`, anything else dies cleanly at `append`.

Flush triggers (`tripped` branch in `Emitter::route`):

- `enc.rows >= config.row_budget` (default 65536)
- `enc.approx_bytes >= config.byte_budget` (default 1 MiB)
- xact end (legacy mode), deadline trip, schema event, TRUNCATE,
  shutdown

Compression: feature-gated through walshadow's own `lz4` / `zstd`
features which forward to `clickhouse-c-rs`. `CompressionChoice::Lz4`
is the default; `build_codec` returns `EmitterError::CompressionUnsupported`
when the variant's feature is off. CH wire default is LZ4 so the
default build matches CH's own posture.

## Type bridge

`type_bridge::map(att, pk_member) -> ResolvedColumn` maps one
`RelAttr` to its CH type expression plus optional `DEFAULT <expr>`.
`pk_member = true` strips the `Nullable(_)` wrap because CH refuses
`Nullable` in `ORDER BY`. The matrix is hard-coded in `base_type_for`:

| PG | CH |
|---|---|
| bool | Bool |
| "char" / int2/4/8 | Int8/16/32/64 |
| oid | UInt32 |
| float4/8 | Float32/64 |
| numeric(p,s), p ≤ 76 | Decimal(p,s); else String |
| text / varchar(n) / bpchar(n) / name / bytea | String |
| date | Date32 |
| time / timetz / interval | String (text form) |
| timestamp(p) / timestamptz(p) | DateTime64(p, 'UTC'), p ≤ 6 |
| uuid | UUID |
| inet / cidr / json / jsonb | String |
| array / unknown | String fallback |

Default expressions reconstruct from `RelAttr.missing_text` (the
fast-path `attmissingval[1]` PG plants on `ALTER TABLE ADD COLUMN
... DEFAULT k`). `render_default` routes through
`heap_decoder::missing_value_for(att) -> ColumnValue`, then
`column_value_to_sql_literal` emits the CH literal — booleans land
as `true`/`false`, ints unquoted, strings single-quoted with `'`
escaping, timestamps as `toDateTime64('...', 6, 'UTC')`. Unbridged
shapes return `None` so the `ALTER TABLE ADD COLUMN` lands without
a `DEFAULT` clause; CH applies its own zero-init.

### Synthetic columns

Every destination table carries four trailing synthetic columns,
non-nullable by construction, encoded in `TableEncoder::new`:

| column | type | purpose |
|---|---|---|
| `_lsn` | `UInt64` | source commit-record LSN. `ReplacingMergeTree(_lsn)` keys dedup on this so a restart-and-replay window collapses re-emitted rows to the latest LSN per PK |
| `_xid` | `UInt32` | source xid. Lets analytic queries group all rows from one xact, recover the xact boundary CH lost when the emitter serialised across tables |
| `_op` | `Enum8('insert'=1,'update'=2,'delete'=3)` | row-op classification. CH-side `WHERE _op != 3` is the cheap "live rows" filter; HOT_UPDATE collapses to UPDATE (code 2), the PG-internal distinction doesn't reach CH |
| `_commit_ts` | `DateTime64(6, 'UTC')` | xact commit timestamp, shifted from PG's 2000-01-01 epoch to Unix via `DATETIME64_PG_EPOCH_US` |

`_lsn` is the dedup key because the emitter ack lags actual CH
durability by up to one flush window. On restart the cursor's
`emitter_ack_lsn` rewinds to the last close-acked LSN; everything
between that and the now-stale buffered rows re-emits, and
`ReplacingMergeTree(_lsn)` resolves duplicates server-side without
walshadow having to track which rows already landed.

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
is the live handle the emitter consults per row. The handle is
cloneable; daemon's SIGHUP task swaps the whole inner `HashMap`
between xacts. Cached `TableEncoder`s in `Emitter::tables` keep
their old `TablePlan` until the next route call rebuilds off the
fresh mapping.

### NamespaceMapping (partial)

Per-source-namespace defaults block, `[namespace."public"]`. Today's
shipped surface is one field only:

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
`derive_columns_for_mapping`. Per-table TOML still wins when both
are configured for the same relation.

## What's NOT yet landed for namespace mapping

Plan called for a richer namespace surface; only the auto_create
sliver shipped. Missing:

- `ResolvedConfig` struct: design called for one pre-materialised
  value carrying `tables`, `namespaces`, and a
  `columns: HashMap<(String, String), ColumnMapping>` type-override
  table. Today there's no such type; mapping lives in
  `Arc<RwLock<HashMap<String, TableMapping>>>` and namespace defaults
  live separately on `EmitterConfig::namespaces`.
- `watch::Receiver<Arc<ResolvedConfig>>` emitter wiring: the
  runtime-config-from-PG path wants the emitter to consume a watch
  stream so config changes propagate without SIGHUP. Today's reload
  channel is a `RwLock` swap kicked by SIGHUP.
- `NamespaceMapping.order_by_default`: `render_create_table` hard-codes
  `ORDER BY (_lsn)` fallback when no PK exists. Plan wanted a
  configurable expression.
- `NamespaceMapping.engine_default`: `render_create_table` hard-codes
  `ENGINE = ReplacingMergeTree(_lsn)`. Plan wanted per-namespace
  override (e.g., `MergeTree`, `CollapsingMergeTree`).
- `NamespaceMapping.type_overrides`: plan wanted per-column type
  overrides keyed on `(namespace, src_attname)`. Today the only path
  is per-table TOML.

See [future/runtime_config_from_pg.md](future/runtime_config_from_pg.md) —
pg-driven config substrate depends on this resolver shape.

## DdlApplicator

`ch_ddl.rs::DdlApplicator` consumes `SchemaEvent` off
`ShadowCatalog::subscribe` (an `mpsc::UnboundedReceiver`, not a
bounded channel — plan said bounded, landed code uses unbounded so a
stalled applicator never back-pressures the catalog producer). Apply
table:

| `SchemaEvent` | CH SQL |
|---|---|
| `Added { desc }` | `CREATE TABLE IF NOT EXISTS` when namespace `auto_create = true` and no pre-pinned mapping. Auto-derives a `TableMapping` post-success so the next `route` call ships rows against the new table |
| `Changed { diff }` | `ALTER TABLE … RENAME COLUMN` first (so position-match diffs don't trip into drop+add), then `ALTER TABLE … ADD COLUMN IF NOT EXISTS` per added attnum, then `ALTER TABLE … DROP COLUMN IF EXISTS` per dropped attnum |
| `Changed.type_changes` | rejected, logged, `stats.type_changes_rejected += n`. Operator handles via manual CH migration |
| `Dropped { qualified_name }` | gated on `DropTableStrategy`: `Retain` (default) skips silently, `Warn` skips at WARN, `Drop` runs `DROP TABLE IF EXISTS` |

`render_create_table` builds the CREATE off the descriptor:
attributes through `type_bridge::map`, PK columns first in
`ORDER BY` (else `_lsn` fallback), engine pinned to
`ReplacingMergeTree(_lsn)`. Synthetic columns appended after the
mapped columns, same shape as `TablePlan::build`.

`apply_changed` also mutates the live `MappingHandle` via
`mutate_mapping_for_diff`: renames update `target_name` in place
(when the operator's TOML used the old source name), drops strip
the `ColumnMapping`, adds push a new entry derived through
`type_bridge::map`. Operator-pinned overrides survive: only
`src_attnum`-matching entries the applicator could have produced
get touched.

### await_ready gate

Coordination with the INSERT pump is synchronous, not channel-based.
`Emitter::dispatch_schema_event`:

1. Finds the affected relation key from the event.
2. If `tables[key]` has buffered rows, flushes via `flush_table`
   (opens wire if not open, ships pending block).
3. If `wire_open_key == Some(key)`, closes the wire
   (`send_data(None)` + drain to `EndOfStream` — durable on CH).
4. Drops `tables[key]` so the next `route` rebuilds `TablePlan` off
   the post-DDL descriptor.
5. Calls `applicator.apply(event)` which runs the SQL synchronously.

Surgical close (this table only) keeps other tables' open INSERTs
intact, important for pgbench's 4-table-per-xact shape where
closing-all would break the cross-INSERT pipeline.

## TRUNCATE path

`HeapOp::Truncate` in `Emitter::route`:

1. Flush any pending rows for the relation through `flush_table`
   (so prior INSERTs in the same xact land before the truncate).
2. `close_current_wire` — drops the open INSERT if any.
3. Remove the relation's `TableEncoder` from `tables`.
4. `send_query("TRUNCATE TABLE <dest>")` on the emitter's client.
5. Drain to `EndOfStream` / `Exception`.
6. Bump `stats.truncates_emitted`.

`RESTART_SEQS` flag is ignored — sequence-state replication is
deferred, see [future/sequence_state.md](future/sequence_state.md).
PG's `TRUNCATE … RESTART IDENTITY` arrives as the same `HeapOp::Truncate`
with no flag distinction at the emitter layer; the bit lives on the
PG xlog record but doesn't propagate through `DecodedHeap`.

## Read-time defaults integration

PG's fast-path `ALTER TABLE ADD COLUMN … DEFAULT k` plants
`attmissingval[1]` instead of rewriting the heap. `RelAttr.missing_text`
carries the typoutput text; resolution tiers:

- Tier 1 (immediate): bool / int / float / numeric / text — decoder
  resolves at parse time via `heap_decoder::missing_value_for(att)`,
  emitter sees a fully-decoded `ColumnValue`.
- Tier 2 (typmod-aware): timestamp / timestamptz / date — decoder
  resolves with typmod, emitter sees concrete `ColumnValue`.
- Tier 3 (oracle): unsupported / array / domain types — decoder
  emits `ColumnValue::PgPending { raw, type_oid }`. Oracle extension
  (separate PG-side process) resolves at emit time; falls through to
  raw bytes when oracle absent.

`encode_value` in the emitter handles `PgPending` by shipping
`raw` as a String — no error, no stat bump, operators handle the
post-process via PG-side tooling. See [decoder.md](decoder.md) for
the tier classification + [oracle.md](oracle.md) for the extension
protocol.

## Ack-LSN tracking

`TupleObserver::on_xact_end(&mut self, commit_lsn: u64) -> Result<u64, …>`
returns the highest LSN known durable on CH. Two values move
through the emitter:

- `pending_max_commit_lsn`: highest `commit_lsn` of any row
  currently buffered (in `TableEncoder` memory OR shipped via
  `send_data(Some)` but not yet sealed by `send_data(None)`). Bumped
  per tuple in `route`, reset to 0 on close.
- `last_durable_commit_lsn`: monotonic horizon. Promoted from
  `pending_max_commit_lsn` only inside `close_all_open_inserts`
  (deadline trip or legacy per-xact close) or when an empty xact
  arrives with no rows pending.

Hold-open mode means `last_durable_commit_lsn` lags `drain_lsn`
until the deadline trips — `emitter_ack_lsn` in the cursor file
reflects that lag. See [ops.md](ops.md) for the cursor + recovery
contract; `cursor.rs` writes the value to disk on every observer
ack and replay starts from `min(shadow_replay_lsn, emitter_ack_lsn)`.

## Bootstrap-time emitter

Transitional emitter spun up by `backfill_bootstrap.rs` for the
initial COPY-FROM drain. No `DdlApplicator` attached (the bootstrap
descriptor set is frozen at snapshot time), no SIGHUP wiring, no
held-open behaviour. Force-closed at end of bootstrap via
`flush_open_inserts`; the steady-state emitter then opens fresh
connections for streaming. See [bootstrap.md](bootstrap.md).

## Retry behaviour

Bounded retry on every public `Emitter::*` method.
`is_retryable` classifies `EmitterError::{Io, Client, ServerException}`
as transient (network / CH-server / clickhouse-c protocol);
`Config`, `Type`, `Catalog`, `UnsupportedValue` stay fatal because
they encode bugs in the daemon or mapping that retry would loop
forever on.

Wrapper functions (`route_with_retry`, `on_xact_end_with_retry`,
`flush_if_deadline_tripped_with_retry`, `flush_open_inserts_with_retry`)
loop up to `RetryConfig::max_attempts` with exponential backoff
capped at `max_backoff`, calling `Emitter::reconnect` between
attempts. `reconnect` opens a fresh `TcpStream`, builds a new
`Client`, hot-swaps `self.client`, clears `wire_open_key`. The
per-table accumulator state in `self.tables` survives so a CH
bounce mid-xact lets the surviving buffered rows flush through the
new connection on retry.

Budget expiry kills the daemon — `route_with_retry` returns the
last `Err`, the worker poisons the stream, the daemon exits, cursor
file resumes on restart. See
[future/ch_bounce_recovery.md](future/ch_bounce_recovery.md) for the
deeper "re-emit from spill" story (segment-buffered replay across
extended CH outages) not yet shipped.

DDL retry is currently a no-op:
`dispatch_schema_event_with_retry` calls through without retry —
DDL errors poison the stream so the operator sees them directly.
Runtime-config-from-PG work may add bounded reconnect for the DDL
connection.

## Cross-links

- [xact.md](xact.md) — `XactBuffer::commit` k-way-merges
  `CommittedTuple` + `SchemaEvent` in source-LSN order, drains into
  the emitter via `TupleObserver`. Producer for everything the
  emitter consumes.
- [shadow.md](shadow.md) — `ShadowCatalog::subscribe` produces the
  `SchemaEvent` stream. Catalog snapshot drives the descriptors
  `TablePlan::build` reads.
- [decoder.md](decoder.md) — `HeapDecoder` produces `ColumnValue` /
  `CommittedTuple`. Read-time defaults tier-classify here.
- [ops.md](ops.md) — `cursor.rs` writes `emitter_ack_lsn` to the
  on-disk cursor file; restart resumes from
  `min(shadow_replay_lsn, emitter_ack_lsn)`.
- [safety.md](safety.md) — `clickhouse-c-rs` unsafe surface
  (`BlockBuilder` borrows into `ColumnBuf` slabs, `PosixIo` owns the
  fd, `Client` lifetime invariants).
- [bootstrap.md](bootstrap.md) — transitional bootstrap emitter
  wiring, force-close handshake.
- [oracle.md](oracle.md) — Tier 3 default resolution via PG-side
  extension, `PgPending` routing.
- [future/runtime_config_from_pg.md](future/runtime_config_from_pg.md)
  — runtime-config substrate; `ResolvedConfig` + `watch::Receiver`
  shape the partial namespace-mapping work needs to land first.
- [future/sequence_state.md](future/sequence_state.md) — TRUNCATE
  RESTART_SEQS + nextval replication, deferred.
- [future/ch_bounce_recovery.md](future/ch_bounce_recovery.md) —
  spill-buffered re-emit for extended CH outages, beyond the bounded
  retry budget.
