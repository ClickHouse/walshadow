# runtime config resolver

Layered config resolver: merge operator config layers into one
pre-materialised [`ResolvedConfig`](../src/config.rs) and publish it on a
`watch` channel subscribers snapshot from. Three layers, highest wins:
**CLI flag > `<schema>.config_*` PG row > TOML**.

The PG-row layer is the source-PG overlay: config rows a DBA writes via SQL
into operator-owned tables on source PG, replicated through the WAL stream the
daemon already decodes, and applied at each row's commit LSN under the same
barrier fence the catalog applicator uses for DDL. TOML stays authoritative for
bootstrap (connection params) and is the only surface when the overlay is
disabled. CLI on top so an operator's recovery flag can't be stomped by a stale
config row scrolling in via WAL replay.

## Opt-in

The overlay is off unless `[runtime_config] schema = "…"` names the source-PG
schema housing the config tables (typical value `walshadow`). Empty string or
field omitted: overlay disabled, daemon runs pure TOML+CLI, never queries source
for overlay rows, never classifies their WAL writes. One switch turns the whole
subsystem off, so deployments without the schema installed keep working
untouched.

## What resolves live vs boot-only

`ResolvedConfig` carries every knob that reloads without a restart:

- `tables` — per-relation destination mapping, keyed `"<namespace>.<relname>"`
- `namespaces` — per-namespace defaults (`auto_create`, `target_database`,
  `drop_table_strategy`)
- `columns` — per-`(namespace.relname, source attname)` type override,
  populated from the `config_column` overlay and WAL-tracked. The emitted
  projection does not consume it; that wiring (source-attname→attnum resolution
  plus a `TablePlan` rebuild) lives in
  [future/runtime_config_from_pg.md](future/runtime_config_from_pg.md)
- `drop_table_strategy` — global DROP fallback; per-namespace overrides it
- `row_budget`, `byte_budget`, `flush_timeout` — emitter batch-seal triggers,
  read live by the batcher per seal decision (ticker re-armed on change)
- `compression` — per-INSERT wire codec, read live by the inserter, which
  rebuilds its codec + reconnects on change
- `retry_max_attempts` — CH client retry budget, read live by the inserter

The last five live on `ResolvedConfig` rather than boot-only on
[`EmitterConfig`](../src/ch_emitter.rs) so a `config_global` row can retune
batching / compression / retry mid-stream. The pipeline spawns from
`EmitterConfig`, so `bin/stream.rs` reconciles these five back onto the boot
`EmitterConfig` from the seeded resolved snapshot before spawn — boot state
matches steady state.

Bootstrap fixed points stay on `EmitterConfig`, boot-only, never republished:
connection params (`[ch] host/port/user/password/database/secure`), toast store,
`soft_delete`. These describe how to reach CH or wire into pipeline stages at
spawn; a live swap would mean reconnecting or rebuilding the pipeline.
`target_database` and `soft_delete` thread into the DDL applicator at
construction and carry across refreshes unchanged.

## `<schema>.config_*` tables

DBA runs [`sql/runtime_config_install.sql`](../sql/runtime_config_install.sql)
(schema name via psql var `walshadow_schema`, default `walshadow`). The daemon
NEVER writes these tables — single writer is source PG, single reader is the
daemon — preserving walshadow's read-only-source posture. Four tables:

- `config_global` — singleton (`id smallint PK CHECK (id = 1)`): `row_budget`,
  `byte_budget`, `flush_timeout_ms`, `compression`, `retry_max_attempts`,
  `drop_table_strategy`
- `config_namespace` — key `namespace`: `target_database`, `auto_create`,
  `drop_table_strategy`
- `config_table` — key `(namespace, relname)`: `target` (`"<db>.<table>"`),
  `replicate`, `initial_load` (`none`, `copy`, `base_backup`, `object_store`).
  Text key, not relfilenode, rfn is unknown at row-insert time for
  forward-declared tables
- `config_column` — key `(namespace, relname, attname)`: `target_type`

Every column is nullable and NULL means "daemon default / TOML applies", so the
schema grows additively: a newer daemon reading an older install still works.
TOML `[table.*]` blocks take the same mode strings; omitting the key there
matches SQL NULL. All four carry `REPLICA IDENTITY FULL`, see decode below.

## Decode + interpret

No `config_decoder` task, no relfilenode filter. Config-table writes ride the
normal heap-decode path and are intercepted in
[`BufferingDecoderSink::on_record`](../src/xact_buffer.rs) after decode, before
routing to CH: a write is a config write when its resolved descriptor has
`namespace_name == <schema>` and `ConfigTableKind::from_relname(name)` matches
one of the four tables. Detection by resolved qualified name is **rotation-proof
for free** — TRUNCATE / VACUUM FULL / rewrite rotates the relfilenode but the
decode path re-resolves every descriptor, so the name still matches, with no
frozen filter to refetch. Config writes never reach CH (the implicit namespace
filter that keeps walshadow's own config out of the target).

[`runtime_config::interpret`](../src/runtime_config.rs) turns each config tuple
into a typed `ConfigEvent` (`GlobalUpserted`/`GlobalCleared`,
`Namespace{Upserted,Removed}`, `Table{Upserted,Removed}`,
`Column{Upserted,Removed}`). At walshadow's `wal_level=logical` floor PG logs the
new tuple whole (prefix/suffix compression is off for logically-logged
relations), so INSERT and UPDATE already carry every column; `interpret`
reconstructs each from the record alone (INSERT/UPDATE from the new tuple,
DELETE from the old image for the row key) with no dependency on prior in-daemon
state and no before-image lookup. `REPLICA IDENTITY FULL` guarantees the DELETE
old image carries those key columns regardless of the table's primary-key shape,
at negligible WAL cost (operator-scale writes, tiny rows). Events carry whole
typed rows; values are validated late, at resolver merge, not here.

## Apply at commit LSN

```rust
enum DrainEntry {
    Catalog(SchemaEvent),
    Config(ConfigEvent),
}
```

`DrainEntry::Config` events ride the same `ordered_events` interleave and barrier
apply as `DrainEntry::Catalog`: interpreted events stamp `(xid, source_lsn)` and
merge into the heap stream by LSN, so a config row preceding heap writes in WAL
position applies before those writes drain. Two apply sites share the enum: the
serial `commit()` path in `xact_buffer.rs` and the live pipeline's `run_barrier`
in [`pipeline/reorder.rs`](../src/pipeline/reorder.rs).

`ConfigResolver::apply_config_event` mutates the overlay, **writes the live
`MappingHandle` synchronously under the barrier fence**, bumps
`invalidation_epoch` for shape-changing (`Column*`) events, then republishes.
The fenced map write is what makes trailing rows in the applying xact route
against the post-config mapping — the same discipline DDL uses writing
`ShadowCatalog` + bumping the epoch before its trailing heaps dispatch. Routing
through the async `watch`→refresher hop instead would let `run_barrier` dispatch
the trailing segment before the swap took effect, and the decode worker would
miss the mapping and silently drop the row. So WAL config apply writes the map
directly; `watch` republish stays the mechanism only for the barrier-free
contexts (boot seed, SIGHUP) and the DDL applicator's own `DdlConfig` refresh.

## Boot seed

`bin/stream.rs::seed_runtime_config` runs between catalog seed and pump start,
when the overlay is enabled: four `SELECT`s through the source sidecar libpq
connection populate a `ConfigOverlay`, handed to `ConfigResolver::seed_overlay`,
then the pump starts and WAL becomes the only config source. TOML
`initial_load` for pinned mappings dispatches after this seed
([add_table.md](add_table.md) covers the backup-sourced modes), so SQL
inclusion/exclusion rows win. The `config_global` read doubles as the install
probe: a missing table errors there, so a schema
named in TOML but not installed refuses to start rather than silently no-op
(explicit opt-in). A present-but-empty `config_global` is fine — greenfield
falls through to TOML defaults, behaviour identical to TOML-only.

## Resolver

`ConfigResolver` owns the `watch::Sender` and, behind one lock, the two mutable
merge inputs (TOML `base`, PG `overlay`) so an apply is atomic against a
concurrent SIGHUP:

- `new(base, cli, toml_path, mapping, invalidation_epoch)` builds the initial
  (overlay-empty) snapshot and returns the shared resolver plus a seeded receiver
- `resolve(base, overlay, cli)` merges one snapshot: TOML base, then the overlay,
  then explicit CLI on top. Rebuilt whole each time, so a snapshot never tears
- `seed_overlay(overlay)` replaces the overlay wholesale (boot seed) + republishes
- `apply_config_event(event)` is the live WAL path above
- `reload()` (SIGHUP) re-reads TOML, re-merges overlay + CLI, publishes.
  Connection params in the reloaded file are ignored; read/parse errors leave the
  last snapshot in effect (no send on failure)

CLI overrides are `Option<T>`: `Some` wins over overlay + TOML and survives
reload, `None` defers. Two flags: `--drop-table-strategy` and
`--ch-flush-timeout-ms`.

## Failure containment (Regime A)

WAL pump alive, a config value malformed: the resolver validates at merge and
rejects the offending value, keeping the pre-overlay value in effect. Never
crashes, never pauses the pump, never abandons other keys. Per-field:
`drop_table_strategy` via `DropTableStrategy::parse`; `row_budget`/`byte_budget`
via `usize` conversion + `> 0`; `flush_timeout_ms`/`retry_max_attempts` via
unsigned conversion; `compression` via `CompressionChoice::parse` **then
`build_codec`**, so a codec unsupported at compile time (e.g. zstd with the
feature off) is rejected at merge, never surfaced as a fatal when the inserter
reconnects. Rejections increment a counter (`ConfigResolver::rejections`) and log
at WARN. Validation runs at merge, not at decode.

`config_table.target` overrides the destination only of a table already mapped by
TOML (which carries the column projection); a row for an unmapped table would
need column auto-derivation, so it is skipped with a WARN rather than emitting a
column-less INSERT.

## Subscribers

Consumers snapshot the receiver; the overlay feeds only the resolver merge
point, not the consumer set. Four consumers:

- **Routing map refresher** (`spawn_mapping_refresher`, `bin/stream.rs`) — on
  each republish full-swaps the live `MappingHandle` the decode pool reads. (The
  WAL apply path writes this handle directly under the fence; the refresher
  covers the barrier-free republishes)
- **DDL applicator** ([`ch_ddl::DdlApplicator`](../src/ch_ddl.rs)) — folds a
  republished snapshot into its `DdlConfig` (namespaces + drop strategy) via
  `refresh_config` at the top of each `apply`
- **Batcher** ([`pipeline/batcher.rs`](../src/pipeline/batcher.rs)) — reads
  `row_budget`/`byte_budget`/`flush_timeout` off the watch per seal decision;
  re-arms the idle-flush ticker when `flush_timeout` changes
- **Inserter** ([`pipeline/inserter.rs`](../src/pipeline/inserter.rs)) — reads
  `retry_max_attempts` per attempt loop and `compression` at each batch boundary,
  reconnecting when the codec changes

## SIGHUP

`spawn_sighup_handler` holds the resolver and calls `reload()` on each signal. No
resolver (metrics-only run, no `--ch-config`) makes it a no-op tap. Install
failure drops the resolver, so `has_changed` returns `Err` and subscribers freeze
at the boot snapshot — reload disabled, config still serves.

## Known limitation

Republish full-swaps the operator `tables`, dropping mappings the DDL applicator
auto-derived on `auto_create`. An auto-created table loses its routing entry on
the next reload until a fresh `Added` re-derives it. Mapping lifecycle is owned
by the per-table opt-in work
([future/runtime_config_from_pg.md](future/runtime_config_from_pg.md)).

## Deferred

Source-PG-driven work that builds on this resolver lives in
[future/runtime_config_from_pg.md](future/runtime_config_from_pg.md).

## Acceptance drills

- **Disabled by default.** TOML omits `[runtime_config].schema` or sets `""`.
  Daemon never queries source for overlay rows, never classifies config writes
- **Greenfield seed.** Schema installed, all config tables empty. Behaviour
  identical to TOML-only with the same values
- **Not installed.** `[runtime_config].schema` names a schema that isn't
  installed. `seed_runtime_config` errors on the `config_global` probe; daemon
  refuses to start
- **Namespace flip.** TOML `auto_create = false`; operator inserts
  `config_namespace ('public', 'default', true)`. Subsequent
  `CREATE TABLE public.events(...)` materialises on CH
- **Mapping-add ordering.** Single xact `INSERT config_table` then rows into the
  now-mapped table. CH receives the rows under the post-config target, proving
  the within-xact fenced-map write
- **Batch tunables live.** `config_global.row_budget = 1000`,
  `flush_timeout_ms = 250`; emitter flushes at the smaller trigger. Bump to 100k
  and observe larger batches — batcher picks up the resolved snapshot mid-pipeline
- **Precedence.** CLI `--drop-table-strategy=warn` + `config_global` row `drop`
  + TOML `retain` resolves to `warn`. Drop the CLI flag → `drop`. Truncate the
  row → `retain`
- **Validation rejection.** Insert `config_global.compression = 'brotli'` (or a
  negative budget). Resolver rejects, `rejections` ticks, daemon stays up, other
  keys unaffected; UPDATE to a valid value and the next apply picks it up

## Cross-links

- [emitter.md](emitter.md) — `MappingHandle`, `NamespaceMapping`, `TablePlan`;
  the `ResolvedConfig` shape
- [shadow.md](shadow.md) — `ShadowCatalog::subscribe` feeds the DDL applicator
  that refreshes from the resolver
- [future/runtime_config_from_pg.md](future/runtime_config_from_pg.md) — the
  source-PG-driven work that builds on this resolver
