//! Layered runtime config resolver. Merges operator config layers into a
//! single pre-materialised [`ResolvedConfig`] and publishes it on a
//! `watch` channel subscribers snapshot from.
//!
//! Precedence, highest wins: **CLI flag > `<schema>.config_*` PG row > TOML**.
//! The PG-row layer (the runtime-config overlay,
//! [plans/future/runtime_config_from_pg.md]) is typed in-memory state
//! ([`crate::runtime_config::ConfigOverlay`]) seeded at boot from source PG and
//! mutated live by [`ConfigResolver::apply_config_event`] as config-table WAL
//! writes drain at their commit LSN. `resolve` is the single merge point.
//!
//! Connection params (`[ch] host/port/...`) and TOAST stay boot-only fixed
//! points on [`EmitterConfig`]. Everything the operator tunes lives on
//! `ResolvedConfig` and reloads live: per-relation mapping, per-namespace
//! defaults, drop-table strategy, and the emitter batch/compression/retry
//! knobs (read live by the batcher + inserter off the watch channel).
//!
//! **Storage: in-memory.** The overlay is a derived cache — re-seeded from PG
//! then caught up by WAL replay on restart — so it holds no checkpoint. The
//! resolver rebuilds `ResolvedConfig` whole per apply, so a subscriber snapshot
//! never tears.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::{Mutex, watch};

use clickhouse_c::{Allocator, TypeAst};

use crate::ch_ddl::{DropTableStrategy, derive_columns_for_mapping, fold_diff_into_mapping};
use crate::ch_emitter::{
    CompressionChoice, EmitterConfig, EmitterError, MappingHandle, NamespaceMapping, TableMapping,
    TableTarget,
};
use crate::runtime_config::{ConfigEvent, ConfigOverlay, TableRow};
use crate::shadow_catalog::{RelDescriptor, RelName, SchemaDiff};

/// Pre-materialised resolved config, snapshotted by subscribers via
/// `watch::Receiver<Arc<ResolvedConfig>>`. Rebuilt whole on every reload,
/// so a snapshot is internally consistent — no per-field tearing.
#[derive(Debug, Clone)]
pub struct ResolvedConfig {
    /// Per-relation destination mapping
    pub tables: HashMap<RelName, TableMapping>,
    /// Per-namespace defaults keyed on PG schema name
    pub namespaces: HashMap<String, NamespaceMapping>,
    /// Per-column CH-type override from the `config_column` overlay, keyed
    /// rel → source attname → CH type expression.
    /// Type strings are parse-validated at merge (Regime A: malformed
    /// rejected, prior value kept). Consumed by `TablePlan::build`, which
    /// resolves attname→attnum against the descriptor at hand and swaps the
    /// column's encode type when the override is wire-compatible.
    pub columns: HashMap<RelName, HashMap<String, String>>,
    /// Global DROP TABLE strategy fallback (`retain` / `drop` / `warn`);
    /// per-namespace `NamespaceMapping::drop_table_strategy` overrides it
    pub drop_table_strategy: String,
    /// Emitter batch-seal row trigger (live: batcher reads per seal decision)
    pub row_budget: usize,
    /// Emitter batch-seal byte trigger (live)
    pub byte_budget: usize,
    /// Hold-open / idle flush deadline (live)
    pub flush_timeout: Duration,
    /// Per-INSERT wire compression (live: inserter rebuilds its codec on change)
    pub compression: CompressionChoice,
    /// CH client retry budget (live: inserter reads per attempt loop)
    pub retry_max_attempts: u32,
}

impl Default for ResolvedConfig {
    fn default() -> Self {
        // Derive from an all-defaults config so budgets are the real defaults,
        // never a 0-budget footgun. Runtime never uses this — the watch channel
        // is seeded from `resolve` — but keeps the type `Default`.
        ConfigResolver::resolve(
            &EmitterConfig::default(),
            &ConfigOverlay::default(),
            &CliOverrides::default(),
            &OptInState::default(),
            &HashMap::new(),
        )
        .0
    }
}

/// CLI-layer overrides. `Some` means the operator set the flag explicitly
/// on the command line, so it wins over the overlay + TOML; `None` defers.
/// clap yields `None` for an absent optional flag, so default-vs-explicit
/// falls out of the `Option` with no `value_source` probe. Only knobs with a
/// CLI flag today live here.
#[derive(Debug, Clone, Default)]
pub struct CliOverrides {
    pub drop_table_strategy: Option<String>,
    pub flush_timeout: Option<Duration>,
}

/// Per-table opt-in state derived from `config_table.replicate`, kept
/// alongside the merge inputs. Distinct from the overlay because building an
/// opt-in `TableMapping` needs the source `RelDescriptor` (catalog state the
/// pure [`ConfigResolver::resolve`] merge has no access to); the coordinator /
/// boot path resolves the descriptor and calls
/// [`ConfigResolver::materialize_opt_in`].
///
/// Opt-in mappings live here, not in `base.tables`, so the `MappingHandle`
/// full-swap in [`ConfigResolver::republish`] keeps them — fixing, for opt-in
/// rels, the clobber the base [config.md] "Known limitation" describes.
#[derive(Debug, Clone, Default)]
struct OptInState {
    /// Descriptor-derived mappings for tables opted in via `replicate=true`.
    /// Overlaid onto `resolved.tables`.
    mappings: HashMap<RelName, TableMapping>,
    /// Applicator-derived runtime mappings: `auto_create` CREATEs and ALTER
    /// diff folds ([`ConfigResolver::apply_schema_diff`]). Overlaid onto
    /// `resolved.tables` under `mappings`, so opt-in wins. Living here (not
    /// only in the live handle) is what makes them survive the republish
    /// full-swap — closing the auto-create half of the [config.md] "Known
    /// limitation" clobber.
    derived: HashMap<RelName, TableMapping>,
    /// Tables opted out via `replicate=false` / `TableRemoved`; removed from
    /// `resolved.tables` even when TOML-mapped.
    excluded: HashSet<RelName>,
    /// `replicate=true` rows whose rel isn't known yet (forward-declared),
    /// materialised when the matching `CREATE TABLE` arrives.
    pending_decl: HashMap<RelName, TableRow>,
}

/// The mutable merge inputs, behind one lock so an apply is atomic against a
/// concurrent SIGHUP reload.
struct MergeInputs {
    /// Last-parsed TOML config (layer 3). Replaced by [`ConfigResolver::reload`].
    base: EmitterConfig,
    /// Source-PG overlay (layer 2). Seeded at boot, mutated per WAL config event.
    overlay: ConfigOverlay,
    /// Per-table opt-in state (§per-table opt-in). Merged into `resolved.tables`.
    opt_in: OptInState,
}

/// Owns the `watch::Sender` and the layers it merges. Shared (`Arc`) between
/// the SIGHUP task (calls [`reload`](Self::reload)) and the WAL apply path
/// (calls [`apply_config_event`](Self::apply_config_event)).
pub struct ConfigResolver {
    /// `--ch-config`; `None` disables reload (nothing to re-read)
    toml_path: Option<PathBuf>,
    cli: CliOverrides,
    inner: Mutex<MergeInputs>,
    tx: watch::Sender<Arc<ResolvedConfig>>,
    /// Live routing map shared with the decode pool. A WAL config apply writes
    /// it synchronously under the barrier fence (plan §6) so trailing rows in
    /// the applying xact route against the post-config mapping, not waiting on
    /// the async watch refresher.
    mapping: MappingHandle,
    /// Decode-pool cache generation. Bumped inside a shape-changing apply and
    /// on every inclusion add/remove so the decode pool re-resolves the
    /// `rfn→mapping` entry it caches.
    invalidation_epoch: Arc<AtomicU64>,
    /// Count of overlay values currently rejected at merge (Regime A).
    rejections: AtomicU64,
    /// Forward-declared opt-in rels awaiting their `CREATE TABLE` (gauge).
    pending_decl: AtomicU64,
    /// Cumulative `replicate=true` materialisations / `replicate=false`
    /// exclusions applied.
    opt_in_total: AtomicU64,
    opt_out_total: AtomicU64,
}

impl ConfigResolver {
    /// Build from the boot-parsed [`EmitterConfig`] plus the CLI overlay.
    /// Returns the shared resolver and a receiver seeded with the initial
    /// (overlay-empty) snapshot; call [`seed_overlay`](Self::seed_overlay)
    /// before pump start to fold in the source-PG rows.
    pub fn new(
        base: &EmitterConfig,
        cli: CliOverrides,
        toml_path: Option<PathBuf>,
        mapping: MappingHandle,
        invalidation_epoch: Arc<AtomicU64>,
    ) -> (Arc<Self>, watch::Receiver<Arc<ResolvedConfig>>) {
        let overlay = ConfigOverlay::default();
        let opt_in = OptInState::default();
        let (initial, _) = Self::resolve(base, &overlay, &cli, &opt_in, &HashMap::new());
        let (tx, rx) = watch::channel(Arc::new(initial));
        let this = Arc::new(Self {
            toml_path,
            cli,
            inner: Mutex::new(MergeInputs {
                base: base.clone(),
                overlay,
                opt_in,
            }),
            tx,
            mapping,
            invalidation_epoch,
            rejections: AtomicU64::new(0),
            pending_decl: AtomicU64::new(0),
            opt_in_total: AtomicU64::new(0),
            opt_out_total: AtomicU64::new(0),
        });
        (this, rx)
    }

    /// Another receiver on the same channel.
    pub fn subscribe(&self) -> watch::Receiver<Arc<ResolvedConfig>> {
        self.tx.subscribe()
    }

    /// Overlay values currently rejected at merge.
    pub fn rejections(&self) -> u64 {
        self.rejections.load(Ordering::Relaxed)
    }

    /// Forward-declared opt-in rels awaiting their `CREATE TABLE`.
    pub fn pending_decl_count(&self) -> u64 {
        self.pending_decl.load(Ordering::Relaxed)
    }

    /// Cumulative `replicate=true` materialisations applied.
    pub fn opt_in_total(&self) -> u64 {
        self.opt_in_total.load(Ordering::Relaxed)
    }

    /// Cumulative `replicate=false` / `TableRemoved` exclusions applied.
    pub fn opt_out_total(&self) -> u64 {
        self.opt_out_total.load(Ordering::Relaxed)
    }

    /// Replace the overlay wholesale (boot `SELECT *` seed, §7) and republish.
    pub async fn seed_overlay(&self, overlay: ConfigOverlay) {
        let mut inner = self.inner.lock().await;
        inner.overlay = overlay;
        self.republish(&inner).await;
    }

    /// Apply one WAL-driven config event at its commit LSN (§6). Mutates the
    /// overlay, writes the routing map under the fence, bumps the cache
    /// generation for shape-changing events, then republishes. Called from the
    /// reorder coordinator's barrier apply, so it runs after earlier data in
    /// the xact is durable and before the trailing segment dispatches.
    pub async fn apply_config_event(&self, event: ConfigEvent) {
        let shape_change = matches!(
            event,
            ConfigEvent::ColumnUpserted { .. } | ConfigEvent::ColumnRemoved { .. }
        );
        let mut inner = self.inner.lock().await;
        inner.overlay.apply(event);
        if shape_change {
            self.invalidation_epoch.fetch_add(1, Ordering::Release);
        }
        self.republish(&inner).await;
    }

    /// Bring `desc` into scope (`replicate=true`, rel known): derive a mapping
    /// from the descriptor, store it so it survives republish, drop any
    /// pending / excluded state, bump the cache epoch, republish. Idempotent —
    /// a re-apply overwrites with an identical mapping. The caller must ensure
    /// the CH table exists first (see `DdlApplicator::ensure_ch_table`).
    pub async fn materialize_opt_in(
        &self,
        desc: &RelDescriptor,
        db_override: Option<String>,
        table_override: Option<String>,
    ) {
        let mut inner = self.inner.lock().await;
        let rel = desc.rel_name.clone();
        let target = TableTarget {
            database: db_override.unwrap_or_else(|| Self::target_db_for(&inner, &rel.namespace)),
            table: table_override.unwrap_or_else(|| rel.name.to_string()),
        };
        let columns = derive_columns_for_mapping(desc);
        inner
            .opt_in
            .mappings
            .insert(rel.clone(), TableMapping { target, columns });
        inner.opt_in.derived.remove(&rel);
        inner.opt_in.excluded.remove(&rel);
        inner.opt_in.pending_decl.remove(&rel);
        self.pending_decl
            .store(inner.opt_in.pending_decl.len() as u64, Ordering::Relaxed);
        self.opt_in_total.fetch_add(1, Ordering::Relaxed);
        self.invalidation_epoch.fetch_add(1, Ordering::Release);
        self.republish(&inner).await;
    }

    /// Take a rel out of scope (`replicate=false` / `TableRemoved`): drop its
    /// mapping + any pending decl, record the exclusion so republish keeps it
    /// out even when TOML-mapped, bump the cache epoch, republish. In-flight
    /// rows already dispatched still drain; further rows drop at
    /// `lookup_mapping`.
    pub async fn exclude_table(&self, rel: &RelName) {
        let mut inner = self.inner.lock().await;
        inner.opt_in.mappings.remove(rel);
        inner.opt_in.derived.remove(rel);
        inner.opt_in.pending_decl.remove(rel);
        inner.opt_in.excluded.insert(rel.clone());
        self.pending_decl
            .store(inner.opt_in.pending_decl.len() as u64, Ordering::Relaxed);
        self.opt_out_total.fetch_add(1, Ordering::Relaxed);
        self.invalidation_epoch.fetch_add(1, Ordering::Release);
        self.republish(&inner).await;
    }

    /// Park a `replicate=true` row whose rel isn't known yet; materialised when
    /// the matching `CREATE TABLE` arrives (see [`Self::take_pending_decl`]).
    /// No routing change, so no republish.
    pub async fn park_pending_decl(&self, rel: RelName, row: TableRow) {
        let mut inner = self.inner.lock().await;
        inner.opt_in.pending_decl.insert(rel, row);
        self.pending_decl
            .store(inner.opt_in.pending_decl.len() as u64, Ordering::Relaxed);
    }

    /// Remove and return a parked forward-declaration, if any.
    pub async fn take_pending_decl(&self, rel: &RelName) -> Option<TableRow> {
        let mut inner = self.inner.lock().await;
        let row = inner.opt_in.pending_decl.remove(rel);
        self.pending_decl
            .store(inner.opt_in.pending_decl.len() as u64, Ordering::Relaxed);
        row
    }

    /// Whether the operator opted this rel out (`replicate=false`). Read by
    /// `DdlApplicator::apply_added` so an excluded rel skips auto-create.
    pub async fn is_excluded(&self, rel: &RelName) -> bool {
        self.inner.lock().await.opt_in.excluded.contains(rel)
    }

    /// Record an applicator-derived mapping (`auto_create` CREATE TABLE) so
    /// it survives republish, write the fenced routing map, republish. Runs
    /// inside the reorder barrier like the DDL that produced it.
    pub async fn register_derived_mapping(&self, rel: &RelName, mapping: TableMapping) {
        let mut inner = self.inner.lock().await;
        inner.opt_in.derived.insert(rel.clone(), mapping);
        self.republish(&inner).await;
    }

    /// Forget a runtime-derived mapping (source DROP TABLE under
    /// strategy=Drop) so a future `Added` re-derives columns. TOML-pinned
    /// mappings are untouched — republish rebuilds them from `base`, and
    /// `DdlApplicator::apply_added` re-creates their dest on a source
    /// re-create. An overlay `replicate=true` row re-parks as a
    /// forward-declaration so a re-create re-materialises the opt-in
    /// against the fresh descriptor.
    pub async fn forget_derived_mapping(&self, rel: &RelName) {
        let mut inner = self.inner.lock().await;
        inner.opt_in.derived.remove(rel);
        inner.opt_in.mappings.remove(rel);
        if let Some(row) = inner.overlay.tables.get(rel)
            && row.replicate == Some(true)
        {
            let row = row.clone();
            inner.opt_in.pending_decl.insert(rel.clone(), row);
            self.pending_decl
                .store(inner.opt_in.pending_decl.len() as u64, Ordering::Relaxed);
        }
        self.republish(&inner).await;
    }

    /// Fold an ALTER diff into the layer owning the rel's mapping so the
    /// auto-extension survives republish: opt-in / derived fold in place; a
    /// TOML-owned mapping folds copy-on-write into `derived` (which shadows
    /// `base` at resolve, so a SIGHUP TOML re-read can't revert the fold).
    /// Unmapped or excluded rels no-op without republish, mirroring
    /// `mutate_mapping_for_diff`'s early return.
    pub async fn apply_schema_diff(&self, new: &RelDescriptor, diff: &SchemaDiff) {
        let mut inner = self.inner.lock().await;
        let rel = &new.rel_name;
        if inner.opt_in.excluded.contains(rel) {
            return;
        }
        if let Some(m) = inner.opt_in.mappings.get_mut(rel) {
            fold_diff_into_mapping(m, new, diff);
        } else if let Some(m) = inner.opt_in.derived.get_mut(rel) {
            fold_diff_into_mapping(m, new, diff);
        } else if let Some(base) = inner.base.tables.get(rel) {
            let mut m = base.clone();
            fold_diff_into_mapping(&mut m, new, diff);
            inner.opt_in.derived.insert(rel.clone(), m);
        } else {
            return;
        }
        self.republish(&inner).await;
    }

    /// CH target database for a namespace: per-namespace override (overlay then
    /// TOML) else the global `[ch] database`. Mirrors
    /// [`crate::ch_ddl::DdlConfig::target_database_for`].
    fn target_db_for(inner: &MergeInputs, namespace: &str) -> String {
        inner
            .overlay
            .namespaces
            .get(namespace)
            .and_then(|r| r.target_database.clone())
            .or_else(|| {
                inner
                    .base
                    .namespaces
                    .get(namespace)
                    .and_then(|m| m.target_database.clone())
            })
            .unwrap_or_else(|| inner.base.database.clone())
    }

    /// Rebuild the resolved snapshot, write the fenced routing map, publish.
    async fn republish(&self, inner: &MergeInputs) {
        let prev = self.tx.borrow().clone();
        let (resolved, rejections) = Self::resolve(
            &inner.base,
            &inner.overlay,
            &self.cli,
            &inner.opt_in,
            &prev.columns,
        );
        self.rejections.store(rejections, Ordering::Relaxed);
        *self.mapping.write().await = resolved.tables.clone();
        // Decode workers cache rfn→(descriptor, mapping) per epoch, cache
        // hits skip the mapping read; bump after every swap so no worker
        // routes against the pre-publish map
        self.invalidation_epoch.fetch_add(1, Ordering::Release);
        // Err only when every receiver dropped (daemon tearing down); ignore
        let _ = self.tx.send(Arc::new(resolved));
    }

    /// Merge one snapshot: TOML base, then the PG overlay, then explicit CLI
    /// overrides on top. Returns the resolved config and the count of overlay
    /// values rejected as malformed (kept at the pre-overlay value, logged at
    /// WARN — Regime A: a bad row never crashes or freezes the pump).
    /// `prev_columns` is the last published snapshot's column overrides: a
    /// `target_type` that fails to parse falls back to its entry there, so a
    /// malformed update can't revert an already-accepted encode type.
    fn resolve(
        base: &EmitterConfig,
        overlay: &ConfigOverlay,
        cli: &CliOverrides,
        opt_in: &OptInState,
        prev_columns: &HashMap<RelName, HashMap<String, String>>,
    ) -> (ResolvedConfig, u64) {
        let mut rejections = 0u64;
        let mut rc = ResolvedConfig {
            tables: base.tables.clone(),
            namespaces: base.namespaces.clone(),
            columns: HashMap::new(),
            drop_table_strategy: base.drop_table_strategy.clone(),
            row_budget: base.row_budget,
            byte_budget: base.byte_budget,
            flush_timeout: base.flush_timeout,
            compression: base.compression,
            retry_max_attempts: base.retry.max_attempts,
        };

        // Runtime-derived layers (before the overlay target loop so a
        // `config_table` target row finds its mapping here). Both carry the
        // descriptor-derived projection a bare target row lacks; derived
        // first so an explicit opt-in wins over an auto-create.
        for (rel, mapping) in &opt_in.derived {
            rc.tables.insert(rel.clone(), mapping.clone());
        }
        for (rel, mapping) in &opt_in.mappings {
            rc.tables.insert(rel.clone(), mapping.clone());
        }

        // Layer 2: source-PG overlay.
        if let Some(g) = &overlay.global {
            if let Some(v) = &g.drop_table_strategy {
                if DropTableStrategy::parse(v).is_ok() {
                    rc.drop_table_strategy = v.clone();
                } else {
                    rejections += 1;
                    tracing::warn!(target: "walshadow::config", value = %v, "config_global.drop_table_strategy rejected");
                }
            }
            if let Some(v) = g.row_budget {
                match usize::try_from(v) {
                    Ok(u) if u > 0 => rc.row_budget = u,
                    _ => {
                        rejections += 1;
                        tracing::warn!(target: "walshadow::config", value = v, "config_global.row_budget rejected");
                    }
                }
            }
            if let Some(v) = g.byte_budget {
                match usize::try_from(v) {
                    Ok(u) if u > 0 => rc.byte_budget = u,
                    _ => {
                        rejections += 1;
                        tracing::warn!(target: "walshadow::config", value = v, "config_global.byte_budget rejected");
                    }
                }
            }
            if let Some(v) = g.flush_timeout_ms {
                match u64::try_from(v) {
                    Ok(ms) => rc.flush_timeout = Duration::from_millis(ms),
                    Err(_) => {
                        rejections += 1;
                        tracing::warn!(target: "walshadow::config", value = v, "config_global.flush_timeout_ms rejected");
                    }
                }
            }
            if let Some(v) = g.retry_max_attempts {
                match u32::try_from(v) {
                    Ok(n) => rc.retry_max_attempts = n,
                    Err(_) => {
                        rejections += 1;
                        tracing::warn!(target: "walshadow::config", value = v, "config_global.retry_max_attempts rejected");
                    }
                }
            }
            if let Some(v) = &g.compression {
                // Validate via build_codec so an unsupported-at-compile-time
                // codec (e.g. zstd with the feature off) is rejected here, never
                // surfaced as a fatal when the inserter reconnects.
                match CompressionChoice::parse(v).and_then(|c| c.build_codec().map(|_| c)) {
                    Ok(c) => rc.compression = c,
                    Err(_) => {
                        rejections += 1;
                        tracing::warn!(target: "walshadow::config", value = %v, "config_global.compression rejected");
                    }
                }
            }
        }

        for (ns, row) in &overlay.namespaces {
            let entry = rc.namespaces.entry(ns.clone()).or_default();
            if let Some(v) = &row.target_database {
                entry.target_database = Some(v.clone());
            }
            if let Some(v) = row.auto_create {
                entry.auto_create = v;
            }
            if let Some(v) = &row.drop_table_strategy {
                if DropTableStrategy::parse(v).is_ok() {
                    entry.drop_table_strategy = Some(v.clone());
                } else {
                    rejections += 1;
                    tracing::warn!(target: "walshadow::config", namespace = %ns, value = %v, "config_namespace.drop_table_strategy rejected");
                }
            }
        }

        for (rel, row) in &overlay.tables {
            // `target_database`/`target_table` override the destination of a
            // table already mapped by TOML or opted in above (both carry the
            // column projection). A `config_table` row that only sets a target
            // for an unmapped table can't be routed without a projection —
            // `replicate=true` is the way to bring such a table into scope.
            // NULL = that part unchanged.
            if row.target_database.is_some() || row.target_table.is_some() {
                match rc.tables.get_mut(rel) {
                    Some(m) => {
                        if let Some(db) = &row.target_database {
                            m.target.database = db.clone();
                        }
                        if let Some(t) = &row.target_table {
                            m.target.table = t.clone();
                        }
                    }
                    None => tracing::warn!(
                        target: "walshadow::config",
                        qname = %rel,
                        "config_table target ignored: no mapping (set replicate=true to opt-in)",
                    ),
                }
            }
        }

        for ((rel, attname), row) in &overlay.columns {
            if let Some(ty) = &row.target_type {
                // Parse-validate here so a malformed type never reaches a
                // TablePlan build (whose error would poison the batcher).
                // Wire-shape compatibility needs the descriptor, so that
                // check (with fallback) runs at plan build instead.
                if TypeAst::parse(ty, Allocator::stdlib()).is_ok() {
                    rc.columns
                        .entry(rel.clone())
                        .or_default()
                        .insert(attname.clone(), ty.clone());
                } else {
                    rejections += 1;
                    // Bad update keeps last accepted override (prev snapshot);
                    // overlay mirrors PG rows so retention can't live there
                    let prior = prev_columns.get(rel).and_then(|m| m.get(attname));
                    if let Some(prior) = prior {
                        rc.columns
                            .entry(rel.clone())
                            .or_default()
                            .insert(attname.clone(), prior.clone());
                    }
                    tracing::warn!(target: "walshadow::config", qname = %rel, attname = %attname, value = %ty, kept_prior = prior.is_some(), "config_column.target_type rejected: unparseable CH type");
                }
            }
        }

        // Opt-out (last, so exclusion wins over any TOML/overlay mapping):
        // a `replicate=false` rel leaves the routing map, so `lookup_mapping`
        // returns None and the decode pool drops its rows mid-stream.
        for rel in &opt_in.excluded {
            rc.tables.remove(rel);
        }

        // Layer 1: CLI (top). Survives SIGHUP + stale overlay rows.
        if let Some(v) = &cli.drop_table_strategy {
            rc.drop_table_strategy = v.clone();
        }
        if let Some(d) = cli.flush_timeout {
            rc.flush_timeout = d;
        }

        (rc, rejections)
    }

    /// Re-read TOML (SIGHUP), re-merge with overlay + CLI, publish. Connection
    /// params in the reloaded file are ignored — boot-only. Parse / read
    /// errors surface to the caller and leave the last snapshot in effect
    /// (watch retains it; no send on failure).
    pub async fn reload(&self) -> Result<(), EmitterError> {
        let Some(path) = &self.toml_path else {
            return Ok(());
        };
        let toml = tokio::fs::read_to_string(path).await?;
        let base = EmitterConfig::from_toml_str(&toml)?;
        let mut inner = self.inner.lock().await;
        inner.base = base;
        self.republish(&inner).await;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::runtime_config::{GlobalRow, NamespaceRow, TableRow};

    fn base_with(drop_strategy: &str) -> EmitterConfig {
        EmitterConfig::from_toml_str(&format!(
            "[ch]\ndrop_table_strategy = \"{drop_strategy}\"\n"
        ))
        .unwrap()
    }

    fn dummy_handles() -> (MappingHandle, Arc<AtomicU64>) {
        (
            Arc::new(tokio::sync::RwLock::new(HashMap::new())),
            Arc::new(AtomicU64::new(0)),
        )
    }

    #[test]
    fn cli_beats_overlay_beats_toml() {
        let base = base_with("retain");
        let overlay = ConfigOverlay {
            global: Some(GlobalRow {
                drop_table_strategy: Some("drop".into()),
                ..Default::default()
            }),
            ..Default::default()
        };
        // Overlay beats TOML.
        let (r, _) = ConfigResolver::resolve(
            &base,
            &overlay,
            &CliOverrides::default(),
            &OptInState::default(),
            &HashMap::new(),
        );
        assert_eq!(r.drop_table_strategy, "drop");
        // CLI beats overlay.
        let cli = CliOverrides {
            drop_table_strategy: Some("warn".into()),
            ..Default::default()
        };
        let (r, _) = ConfigResolver::resolve(
            &base,
            &overlay,
            &cli,
            &OptInState::default(),
            &HashMap::new(),
        );
        assert_eq!(r.drop_table_strategy, "warn");
    }

    #[test]
    fn toml_wins_when_overlay_and_cli_absent() {
        let base = base_with("drop");
        let (r, _) = ConfigResolver::resolve(
            &base,
            &ConfigOverlay::default(),
            &CliOverrides::default(),
            &OptInState::default(),
            &HashMap::new(),
        );
        assert_eq!(r.drop_table_strategy, "drop");
    }

    #[test]
    fn overlay_promotes_emitter_knobs() {
        let base = base_with("retain");
        // `none` is codec-feature-independent, unlike lz4/zstd; it also differs
        // from the Lz4 default so the override is observable.
        let overlay = ConfigOverlay {
            global: Some(GlobalRow {
                row_budget: Some(1000),
                flush_timeout_ms: Some(250),
                compression: Some("none".into()),
                retry_max_attempts: Some(9),
                ..Default::default()
            }),
            ..Default::default()
        };
        let (r, rej) = ConfigResolver::resolve(
            &base,
            &overlay,
            &CliOverrides::default(),
            &OptInState::default(),
            &HashMap::new(),
        );
        assert_eq!(rej, 0);
        assert_eq!(r.row_budget, 1000);
        assert_eq!(r.flush_timeout, Duration::from_millis(250));
        assert_eq!(r.compression, CompressionChoice::None);
        assert_eq!(r.retry_max_attempts, 9);
    }

    #[test]
    fn malformed_overlay_value_rejected_keeps_prior() {
        let base = base_with("retain");
        let overlay = ConfigOverlay {
            global: Some(GlobalRow {
                drop_table_strategy: Some("nonsense".into()),
                compression: Some("brotli".into()),
                row_budget: Some(-5),
                ..Default::default()
            }),
            ..Default::default()
        };
        let (r, rej) = ConfigResolver::resolve(
            &base,
            &overlay,
            &CliOverrides::default(),
            &OptInState::default(),
            &HashMap::new(),
        );
        assert_eq!(rej, 3);
        // Prior (TOML/base) values survive each rejection.
        assert_eq!(r.drop_table_strategy, "retain");
        assert_eq!(r.compression, base.compression);
        assert_eq!(r.row_budget, base.row_budget);
    }

    #[test]
    fn overlay_namespace_and_table_merge() {
        // config_table overrides the target of a TOML-mapped table (which
        // carries the column projection); the columns survive the override.
        let base = EmitterConfig::from_toml_str(
            "[ch]\ndrop_table_strategy = \"retain\"\n\
             [table.public.events]\ntarget_database = \"old\"\n\
             columns = [{ attnum = 1, target = \"id\", type = \"Int32\" }]\n",
        )
        .unwrap();
        let mut overlay = ConfigOverlay::default();
        overlay.namespaces.insert(
            "public".into(),
            NamespaceRow {
                auto_create: Some(true),
                target_database: Some("default".into()),
                drop_table_strategy: None,
            },
        );
        overlay.tables.insert(
            RelName::new("public", "events"),
            TableRow {
                target_database: Some("default".into()),
                ..Default::default()
            },
        );
        let (r, _) = ConfigResolver::resolve(
            &base,
            &overlay,
            &CliOverrides::default(),
            &OptInState::default(),
            &HashMap::new(),
        );
        let ns = r.namespaces.get("public").unwrap();
        assert!(ns.auto_create);
        assert_eq!(ns.target_database.as_deref(), Some("default"));
        let t = r.tables.get(&RelName::new("public", "events")).unwrap();
        assert_eq!(t.target, TableTarget::new("default", "events"));
        assert_eq!(
            t.columns.len(),
            1,
            "TOML columns preserved through override"
        );
    }

    fn rel_desc(namespace: &str, name: &str) -> RelDescriptor {
        use crate::shadow_catalog::{RelAttr, ReplIdent};
        use walrus::pg::walparser::RelFileNode;
        RelDescriptor {
            rfn: RelFileNode {
                spc_node: 1663,
                db_node: 5,
                rel_node: 30000,
            },
            oid: 30000,
            namespace_oid: 2200,
            rel_name: RelName::new(namespace, name),
            kind: 'r',
            persistence: 'p',
            replident: ReplIdent::Default { pk_attnums: None },
            attributes: vec![RelAttr {
                attnum: 1,
                name: "id".into(),
                type_oid: 23, // int4, bridges cleanly
                typmod: -1,
                not_null: true,
                dropped: false,
                type_name: "int4".into(),
                type_byval: true,
                type_len: 4,
                type_align: 'i',
                type_storage: 'p',
                missing_text: None,
            }],
        }
    }

    #[test]
    fn opt_in_mapping_overlays_and_exclusion_removes() {
        // An opt-in mapping lands in resolved.tables with no TOML entry; an
        // excluded qname is dropped even when TOML-mapped.
        let base = EmitterConfig::from_toml_str(
            "[ch]\n\
             [table.public.keep]\ntarget_database = \"old\"\n\
             columns = [{ attnum = 1, target = \"id\", type = \"Int32\" }]\n",
        )
        .unwrap();
        let mut opt_in = OptInState::default();
        opt_in.mappings.insert(
            RelName::new("public", "events"),
            TableMapping {
                target: TableTarget::new("default", "events"),
                columns: Vec::new(),
            },
        );
        opt_in.excluded.insert(RelName::new("public", "keep"));
        let (r, _) = ConfigResolver::resolve(
            &base,
            &ConfigOverlay::default(),
            &CliOverrides::default(),
            &opt_in,
            &HashMap::new(),
        );
        assert!(
            r.tables.contains_key(&RelName::new("public", "events")),
            "opt-in included"
        );
        assert!(
            !r.tables.contains_key(&RelName::new("public", "keep")),
            "excluded rel dropped even when TOML-mapped"
        );
    }

    #[tokio::test]
    async fn materialize_opt_in_derives_maps_and_bumps_epoch() {
        let base = base_with("retain");
        let (mapping, epoch) = dummy_handles();
        let (resolver, mut rx) = ConfigResolver::new(
            &base,
            CliOverrides::default(),
            None,
            mapping.clone(),
            epoch.clone(),
        );
        resolver
            .materialize_opt_in(&rel_desc("public", "events"), None, None)
            .await;
        assert!(rx.changed().await.is_ok());
        let snap = rx.borrow_and_update();
        let rel = RelName::new("public", "events");
        let t = snap.tables.get(&rel).expect("mapping present");
        assert_eq!(t.target.table, "events", "target derived from descriptor");
        assert!(!t.columns.is_empty(), "columns derived from descriptor");
        // Fenced routing map written + cache epoch bumped for the decode pool.
        assert!(mapping.read().await.contains_key(&rel));
        assert!(epoch.load(Ordering::Relaxed) >= 1);
        assert_eq!(resolver.opt_in_total(), 1);
    }

    #[tokio::test]
    async fn exclude_table_removes_mapping() {
        let base = EmitterConfig::from_toml_str(
            "[ch]\n[table.public.events]\n\
             columns = [{ attnum = 1, target = \"id\", type = \"Int32\" }]\n",
        )
        .unwrap();
        let (mapping, epoch) = dummy_handles();
        let (resolver, mut rx) =
            ConfigResolver::new(&base, CliOverrides::default(), None, mapping.clone(), epoch);
        let rel = RelName::new("public", "events");
        assert!(rx.borrow().tables.contains_key(&rel));
        resolver.exclude_table(&rel).await;
        assert!(rx.changed().await.is_ok());
        assert!(
            !rx.borrow_and_update().tables.contains_key(&rel),
            "opt-out drops the mapping"
        );
        assert!(!mapping.read().await.contains_key(&rel));
        assert_eq!(resolver.opt_out_total(), 1);
    }

    #[tokio::test]
    async fn derived_mapping_survives_republish() {
        let base = base_with("retain");
        let (mapping, epoch) = dummy_handles();
        let (resolver, mut rx) =
            ConfigResolver::new(&base, CliOverrides::default(), None, mapping.clone(), epoch);
        let rel = RelName::new("public", "auto");
        resolver
            .register_derived_mapping(
                &rel,
                TableMapping {
                    target: TableTarget::new("default", "auto"),
                    columns: Vec::new(),
                },
            )
            .await;
        assert!(rx.changed().await.is_ok());
        assert!(rx.borrow_and_update().tables.contains_key(&rel));
        assert!(mapping.read().await.contains_key(&rel));
        // An unrelated overlay apply full-swaps the handle; the derived
        // mapping must survive (the [config.md] "Known limitation" clobber)
        resolver
            .apply_config_event(ConfigEvent::GlobalCleared)
            .await;
        assert!(rx.changed().await.is_ok());
        assert!(rx.borrow_and_update().tables.contains_key(&rel));
        assert!(mapping.read().await.contains_key(&rel));
        // Source DROP TABLE under strategy=Drop forgets it everywhere
        resolver.forget_derived_mapping(&rel).await;
        assert!(!mapping.read().await.contains_key(&rel));
        assert!(rx.changed().await.is_ok());
        assert!(!rx.borrow_and_update().tables.contains_key(&rel));
    }

    #[tokio::test]
    async fn forget_reparks_opt_in_row_as_pending_decl() {
        let base = base_with("drop");
        let (mapping, epoch) = dummy_handles();
        let (resolver, _rx) =
            ConfigResolver::new(&base, CliOverrides::default(), None, mapping.clone(), epoch);
        let rel = RelName::new("public", "events");
        resolver
            .apply_config_event(ConfigEvent::TableUpserted {
                rel: rel.clone(),
                row: TableRow {
                    replicate: Some(true),
                    ..Default::default()
                },
            })
            .await;
        resolver
            .materialize_opt_in(&rel_desc("public", "events"), None, None)
            .await;
        assert!(mapping.read().await.contains_key(&rel));
        // Source DROP under strategy=Drop: mapping forgotten, opt-in row
        // re-parked so the next CREATE re-materialises it
        resolver.forget_derived_mapping(&rel).await;
        assert!(!mapping.read().await.contains_key(&rel));
        assert_eq!(resolver.pending_decl_count(), 1);
        let row = resolver.take_pending_decl(&rel).await.expect("re-parked");
        assert_eq!(row.replicate, Some(true));
    }

    #[tokio::test]
    async fn schema_diff_fold_survives_republish() {
        use crate::shadow_catalog::RelAttr;
        // TOML-owned mapping: the fold lands copy-on-write in the derived
        // layer, so neither a config apply nor a SIGHUP re-merge reverts it
        let base = EmitterConfig::from_toml_str(
            "[ch]\n[table.public.events]\n\
             columns = [{ attnum = 1, target = \"id\", type = \"Int32\" }]\n",
        )
        .unwrap();
        let (mapping, epoch) = dummy_handles();
        let (resolver, _rx) =
            ConfigResolver::new(&base, CliOverrides::default(), None, mapping.clone(), epoch);
        let mut desc = rel_desc("public", "events");
        desc.attributes.push(RelAttr {
            attnum: 2,
            name: "note".into(),
            type_oid: 25, // text
            typmod: -1,
            not_null: false,
            dropped: false,
            type_name: "text".into(),
            type_byval: false,
            type_len: -1,
            type_align: 'i',
            type_storage: 'x',
            missing_text: None,
        });
        let diff = SchemaDiff {
            added_columns: vec![desc.attributes[1].clone()],
            dropped_columns: vec![],
            renamed_columns: vec![],
            type_changes: vec![],
        };
        resolver.apply_schema_diff(&desc, &diff).await;
        let has_note = |m: &HashMap<RelName, TableMapping>| {
            m.get(&RelName::new("public", "events"))
                .is_some_and(|t| t.columns.iter().any(|c| c.src_attnum == 2))
        };
        assert!(has_note(&*mapping.read().await), "fold reaches the handle");
        resolver
            .apply_config_event(ConfigEvent::GlobalCleared)
            .await;
        assert!(
            has_note(&*mapping.read().await),
            "fold survives the republish full-swap"
        );
    }

    #[test]
    fn column_override_validated_at_merge() {
        use crate::runtime_config::ColumnRow;
        let base = base_with("retain");
        let mut overlay = ConfigOverlay::default();
        overlay.columns.insert(
            (RelName::new("public", "t"), "amount".into()),
            ColumnRow {
                target_type: Some("Int128".into()),
            },
        );
        overlay.columns.insert(
            (RelName::new("public", "t"), "bad".into()),
            ColumnRow {
                target_type: Some("NotAType(".into()),
            },
        );
        let (r, rej) = ConfigResolver::resolve(
            &base,
            &overlay,
            &CliOverrides::default(),
            &OptInState::default(),
            &HashMap::new(),
        );
        assert_eq!(rej, 1, "unparseable type rejected");
        let t = r
            .columns
            .get(&RelName::new("public", "t"))
            .expect("table entry");
        assert_eq!(t.get("amount").map(String::as_str), Some("Int128"));
        assert!(!t.contains_key("bad"));
    }

    #[test]
    fn column_override_invalid_update_keeps_last_accepted() {
        use crate::runtime_config::ColumnRow;
        let base = base_with("retain");
        let key = (RelName::new("public", "t"), "amount".to_owned());
        let mut overlay = ConfigOverlay::default();
        overlay.columns.insert(
            key.clone(),
            ColumnRow {
                target_type: Some("Decimal(38, 2)".into()),
            },
        );
        let (first, rej) = ConfigResolver::resolve(
            &base,
            &overlay,
            &CliOverrides::default(),
            &OptInState::default(),
            &HashMap::new(),
        );
        assert_eq!(rej, 0);
        // Malformed update replaces the overlay row wholesale; merge keeps
        // the accepted value off the previous snapshot
        overlay.columns.insert(
            key,
            ColumnRow {
                target_type: Some("NotAType(".into()),
            },
        );
        let (second, rej) = ConfigResolver::resolve(
            &base,
            &overlay,
            &CliOverrides::default(),
            &OptInState::default(),
            &first.columns,
        );
        assert_eq!(rej, 1);
        let amount = |r: &ResolvedConfig| {
            r.columns
                .get(&RelName::new("public", "t"))
                .and_then(|t| t.get("amount"))
                .cloned()
        };
        assert_eq!(amount(&second).as_deref(), Some("Decimal(38, 2)"));
        // Retention carries forward while the bad row stays in the overlay
        let (third, rej) = ConfigResolver::resolve(
            &base,
            &overlay,
            &CliOverrides::default(),
            &OptInState::default(),
            &second.columns,
        );
        assert_eq!(rej, 1);
        assert_eq!(amount(&third).as_deref(), Some("Decimal(38, 2)"));
    }

    #[tokio::test]
    async fn column_override_survives_malformed_update_until_removed() {
        use crate::runtime_config::ColumnRow;
        let base = base_with("retain");
        let (mapping, epoch) = dummy_handles();
        let (resolver, mut rx) =
            ConfigResolver::new(&base, CliOverrides::default(), None, mapping, epoch);
        let upsert = |ty: &str| ConfigEvent::ColumnUpserted {
            rel: RelName::new("public", "t"),
            attname: "amount".into(),
            row: ColumnRow {
                target_type: Some(ty.into()),
            },
        };
        resolver.apply_config_event(upsert("Decimal(38, 2)")).await;
        assert!(rx.changed().await.is_ok());
        assert_eq!(
            rx.borrow_and_update().columns[&RelName::new("public", "t")]["amount"],
            "Decimal(38, 2)"
        );
        resolver.apply_config_event(upsert("NotAType(")).await;
        assert!(rx.changed().await.is_ok());
        assert_eq!(
            rx.borrow_and_update().columns[&RelName::new("public", "t")]["amount"],
            "Decimal(38, 2)",
            "malformed update keeps last accepted override"
        );
        assert_eq!(resolver.rejections(), 1);
        // Explicit DELETE clears; retention applies to bad updates only
        resolver
            .apply_config_event(ConfigEvent::ColumnRemoved {
                rel: RelName::new("public", "t"),
                attname: "amount".into(),
            })
            .await;
        assert!(rx.changed().await.is_ok());
        assert!(
            !rx.borrow_and_update()
                .columns
                .contains_key(&RelName::new("public", "t"))
        );
        assert_eq!(resolver.rejections(), 0, "gauge clears with the bad row");
    }

    #[tokio::test]
    async fn pending_decl_parks_and_takes() {
        let base = base_with("retain");
        let (mapping, epoch) = dummy_handles();
        let (resolver, _rx) =
            ConfigResolver::new(&base, CliOverrides::default(), None, mapping, epoch);
        let rel = RelName::new("app", "later");
        resolver
            .park_pending_decl(rel.clone(), TableRow::default())
            .await;
        assert_eq!(resolver.pending_decl_count(), 1);
        assert!(resolver.take_pending_decl(&rel).await.is_some());
        assert_eq!(resolver.pending_decl_count(), 0);
        assert!(resolver.take_pending_decl(&rel).await.is_none());
    }

    #[tokio::test]
    async fn seed_and_apply_republish() {
        let base = base_with("retain");
        let (mapping, epoch) = dummy_handles();
        let (resolver, mut rx) =
            ConfigResolver::new(&base, CliOverrides::default(), None, mapping, epoch);
        assert_eq!(rx.borrow().drop_table_strategy, "retain");

        let overlay = ConfigOverlay {
            global: Some(GlobalRow {
                drop_table_strategy: Some("drop".into()),
                ..Default::default()
            }),
            ..Default::default()
        };
        resolver.seed_overlay(overlay).await;
        assert!(rx.changed().await.is_ok());
        assert_eq!(rx.borrow_and_update().drop_table_strategy, "drop");

        resolver
            .apply_config_event(ConfigEvent::GlobalCleared)
            .await;
        assert!(rx.changed().await.is_ok());
        assert_eq!(rx.borrow_and_update().drop_table_strategy, "retain");
    }

    #[tokio::test]
    async fn reload_without_path_is_noop() {
        let base = base_with("retain");
        let (mapping, epoch) = dummy_handles();
        let (resolver, rx) =
            ConfigResolver::new(&base, CliOverrides::default(), None, mapping, epoch);
        resolver.reload().await.unwrap();
        assert_eq!(rx.borrow().drop_table_strategy, "retain");
    }
}
