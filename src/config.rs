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

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use tokio::sync::{Mutex, watch};

use crate::ch_ddl::DropTableStrategy;
use crate::ch_emitter::{
    ColumnMapping, CompressionChoice, EmitterConfig, EmitterError, MappingHandle, NamespaceMapping,
    TableMapping,
};
use crate::runtime_config::{ConfigEvent, ConfigOverlay};

/// Pre-materialised resolved config, snapshotted by subscribers via
/// `watch::Receiver<Arc<ResolvedConfig>>`. Rebuilt whole on every reload,
/// so a snapshot is internally consistent — no per-field tearing.
#[derive(Debug, Clone)]
pub struct ResolvedConfig {
    /// Per-relation destination mapping keyed on `"<namespace>.<relname>"`
    pub tables: HashMap<String, TableMapping>,
    /// Per-namespace defaults keyed on PG schema name
    pub namespaces: HashMap<String, NamespaceMapping>,
    /// Per-column override keyed on `("<namespace>.<relname>", source attname)`,
    /// populated from the `config_column` overlay. Captured + WAL-tracked;
    /// wiring it into the emitted projection (needs source-attname→attnum
    /// resolution + a TablePlan rebuild) is a follow-up, so nothing reads it
    /// yet — same status as before the overlay landed.
    pub columns: HashMap<(String, String), ColumnMapping>,
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

/// The mutable merge inputs, behind one lock so an apply is atomic against a
/// concurrent SIGHUP reload.
struct MergeInputs {
    /// Last-parsed TOML config (layer 3). Replaced by [`ConfigResolver::reload`].
    base: EmitterConfig,
    /// Source-PG overlay (layer 2). Seeded at boot, mutated per WAL config event.
    overlay: ConfigOverlay,
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
    /// Decode-pool cache generation. Bumped inside a shape-changing apply so a
    /// future `config_column` reader rebuilds `TablePlan`.
    invalidation_epoch: Arc<AtomicU64>,
    /// Count of overlay values currently rejected at merge (Regime A).
    rejections: AtomicU64,
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
        let (initial, _) = Self::resolve(base, &overlay, &cli);
        let (tx, rx) = watch::channel(Arc::new(initial));
        let this = Arc::new(Self {
            toml_path,
            cli,
            inner: Mutex::new(MergeInputs {
                base: base.clone(),
                overlay,
            }),
            tx,
            mapping,
            invalidation_epoch,
            rejections: AtomicU64::new(0),
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

    /// Rebuild the resolved snapshot, write the fenced routing map, publish.
    async fn republish(&self, inner: &MergeInputs) {
        let (resolved, rejections) = Self::resolve(&inner.base, &inner.overlay, &self.cli);
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
    fn resolve(
        base: &EmitterConfig,
        overlay: &ConfigOverlay,
        cli: &CliOverrides,
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

        for (qname, row) in &overlay.tables {
            // `target` overrides the destination of a table already mapped by
            // TOML (which carries the column projection). A config_table row for
            // an unmapped table would need column auto-derivation (plan §4
            // opt-in/forward-decl, not wired), so skip it rather than emit a
            // column-less INSERT. `target` NULL = no reroute.
            if let Some(target) = &row.target {
                match rc.tables.get_mut(qname) {
                    Some(m) => m.target = target.clone(),
                    None => tracing::warn!(
                        target: "walshadow::config",
                        qname,
                        "config_table.target ignored: no TOML mapping (forward-declared opt-in is a follow-up)",
                    ),
                }
            }
        }

        for ((qname, attname), row) in &overlay.columns {
            if let Some(ty) = &row.target_type {
                rc.columns.insert(
                    (qname.clone(), attname.clone()),
                    ColumnMapping {
                        src_attnum: 0,
                        target_name: attname.clone(),
                        target_type: ty.clone(),
                    },
                );
            }
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
        let (r, _) = ConfigResolver::resolve(&base, &overlay, &CliOverrides::default());
        assert_eq!(r.drop_table_strategy, "drop");
        // CLI beats overlay.
        let cli = CliOverrides {
            drop_table_strategy: Some("warn".into()),
            ..Default::default()
        };
        let (r, _) = ConfigResolver::resolve(&base, &overlay, &cli);
        assert_eq!(r.drop_table_strategy, "warn");
    }

    #[test]
    fn toml_wins_when_overlay_and_cli_absent() {
        let base = base_with("drop");
        let (r, _) =
            ConfigResolver::resolve(&base, &ConfigOverlay::default(), &CliOverrides::default());
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
        let (r, rej) = ConfigResolver::resolve(&base, &overlay, &CliOverrides::default());
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
        let (r, rej) = ConfigResolver::resolve(&base, &overlay, &CliOverrides::default());
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
             [table.\"public.events\"]\ntarget = \"old.events\"\n\
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
            "public.events".into(),
            TableRow {
                target: Some("default.events".into()),
            },
        );
        let (r, _) = ConfigResolver::resolve(&base, &overlay, &CliOverrides::default());
        let ns = r.namespaces.get("public").unwrap();
        assert!(ns.auto_create);
        assert_eq!(ns.target_database.as_deref(), Some("default"));
        let t = r.tables.get("public.events").unwrap();
        assert_eq!(t.target, "default.events");
        assert_eq!(
            t.columns.len(),
            1,
            "TOML columns preserved through override"
        );
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
