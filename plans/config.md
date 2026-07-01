# runtime config resolver

Layered config resolver: merge operator config layers into one
pre-materialised [`ResolvedConfig`](../src/config.rs) and publish it on a
`watch` channel subscribers snapshot from. Prerequisite the
source-PG overlay ([future/runtime_config_from_pg.md](future/runtime_config_from_pg.md))
plugs into — that plan's `watch::Receiver<Arc<ResolvedConfig>>` dependency
closes here.

Precedence, highest wins: **CLI flag > TOML**. The overlay layer
(config rows replicated through WAL) slots between CLI and TOML later,
at `ConfigResolver::resolve`'s merge point.

## What resolves live vs boot-only

`ResolvedConfig` carries only operator config that reloads without a
restart:

- `tables` — per-relation destination mapping, keyed `"<namespace>.<relname>"`
- `namespaces` — per-namespace defaults (`auto_create`, `target_database`,
  `drop_table_strategy`)
- `columns` — per-`(namespace, source attname)` type override. Empty in
  TOML+CLI mode; reserved hook the overlay's `config_column` layer
  populates, nothing reads it yet
- `drop_table_strategy` — global DROP fallback; per-namespace overrides it

Bootstrap fixed points stay on [`EmitterConfig`](../src/ch_emitter.rs),
boot-only, never republished: connection params (`[ch] host/port/user/
password/database/secure`), compression, row/byte budgets,
`flush_timeout`, retry knobs, toast store, `soft_delete`. These describe
how to reach CH or are wired into pipeline stages at spawn; a live swap
would mean reconnecting or rebuilding the pipeline. `target_database`
(`[ch] database`) and `soft_delete` thread into the DDL applicator at
construction and carry across refreshes unchanged.

## Resolver

`ConfigResolver` owns the `watch::Sender` and the layers it merges:

- `new(base, cli, toml_path)` builds the initial snapshot (TOML base, CLI
  on top) and returns the resolver plus a receiver seeded with it, so the
  daemon seeds dependent runtime state before any reload
- `resolve(base, cli)` merges one snapshot: clone TOML mapping/namespace/
  drop-strategy, then apply explicit CLI overrides. Rebuilt whole each
  time, so a snapshot never tears mid-field
- `reload()` (SIGHUP) re-reads the TOML file, re-merges CLI on top,
  publishes. Connection params in the reloaded file are ignored.
  Read/parse errors leave the last snapshot in effect (no send on
  failure) — the daemon never loses config to a bad edit

CLI overrides are `Option<T>`: `Some` means the operator set the flag
explicitly, so it wins over TOML and survives reload; `None` defers to
TOML. Today one knob: `--drop-table-strategy` (`retain`/`drop`/`warn`).

## Subscribers

Two independent consumers snapshot the receiver; adding the overlay layer
touches neither — it only feeds the resolver's merge point:

- **Routing map refresher** (`spawn_mapping_refresher`, `bin/stream.rs`) —
  on each republish, full-swaps the live `MappingHandle`
  (`Arc<RwLock<HashMap>>`) the decode pool reads per row. Full swap
  matches the boot seed
- **DDL applicator** ([`ch_ddl::DdlApplicator`](../src/ch_ddl.rs)) — holds
  a receiver, folds a republished snapshot into its `DdlConfig`
  (namespaces + drop strategy) via `refresh_config` at the top of each
  `apply`. DDL is rare, so per-apply refresh is free. This is what makes
  SIGHUP retarget namespace `auto_create` / `target_database` / drop
  strategy without a restart — the earlier SIGHUP reloaded only `tables`

The decode pool keeps its fast `RwLock` read rather than borrowing the
watch value per row; the refresher bridges watch → map. Per-xact-boundary
config snapshotting (the plan's §6 read-committed semantics) is only
needed once config applies at a row's commit LSN, which is overlay-layer
work.

## SIGHUP

`spawn_sighup_handler` holds the resolver and calls `reload()` on each
signal. No resolver (metrics-only run, no `--ch-config`) makes it a
no-op tap. SIGHUP install failure drops the resolver, so `has_changed`
returns `Err` and subscribers freeze at the boot snapshot — reload
disabled, config still serves.

## Deferred (overlay layer)

Everything source-PG-driven stays in
[future/runtime_config_from_pg.md](future/runtime_config_from_pg.md):
`<schema>.config_*` tables + install script, `config_decoder`,
`message_decoder` + signal channel, per-table opt-in + backfill,
`DrainEntry::Config` applying config at a row's commit LSN, the WAL layer
of precedence, TOML-fallback degraded mode, and populating
`ResolvedConfig::columns`. The per-resolved-key Prom metric with a
`source` label (`cli`/`toml`) is not wired yet.

## Known limitation

Republish full-swaps the operator `tables`, dropping mappings the DDL
applicator auto-derived on `auto_create` (matches the earlier SIGHUP
behaviour). An auto-created table loses its routing entry on the next
reload until a fresh `Added` re-derives it. Overlay-layer work revisits
mapping lifecycle.

## Cross-links

- [emitter.md](emitter.md) — `MappingHandle`, `NamespaceMapping`,
  `TablePlan`; the `ResolvedConfig` shape this closed
- [shadow.md](shadow.md) — `ShadowCatalog::subscribe` feeds the DDL
  applicator that now refreshes from the resolver
- [future/runtime_config_from_pg.md](future/runtime_config_from_pg.md) —
  overlay that plugs into the resolver merge point
