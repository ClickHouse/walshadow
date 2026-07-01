//! Layered runtime config resolver. Merges operator config layers into a
//! single pre-materialised [`ResolvedConfig`] and publishes it on a
//! `watch` channel subscribers snapshot from.
//!
//! Precedence, highest wins: **CLI flag > TOML**. A source-PG overlay
//! layer (config rows replicated through WAL) slots between CLI and TOML
//! later; see [plans/config.md](../plans/config.md) for the landed
//! surface and
//! [plans/future/runtime_config_from_pg.md](../plans/future/runtime_config_from_pg.md)
//! for the overlay that plugs into the resolver's `resolve` merge point.
//!
//! Connection params (`[ch] host/port/...`), budgets, compression, retry,
//! toast, and `flush_timeout` are bootstrap fixed points: they stay on
//! [`EmitterConfig`] and are boot-only, never republished. `ResolvedConfig`
//! carries only the operator config that reloads live: per-relation
//! mapping, per-namespace defaults, and the global drop-table strategy.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::watch;

use crate::ch_emitter::{
    ColumnMapping, EmitterConfig, EmitterError, NamespaceMapping, TableMapping,
};

/// Pre-materialised resolved config, snapshotted by subscribers via
/// `watch::Receiver<Arc<ResolvedConfig>>`. Rebuilt whole on every reload,
/// so a snapshot is internally consistent — no per-field tearing.
#[derive(Debug, Clone, Default)]
pub struct ResolvedConfig {
    /// Per-relation destination mapping keyed on `"<namespace>.<relname>"`
    pub tables: HashMap<String, TableMapping>,
    /// Per-namespace defaults keyed on PG schema name
    pub namespaces: HashMap<String, NamespaceMapping>,
    /// Per-column type override keyed on `(namespace, source attname)`.
    /// Empty in TOML+CLI mode — reserved hook the PG overlay's
    /// `config_column` layer populates; nothing reads it yet.
    pub columns: HashMap<(String, String), ColumnMapping>,
    /// Global DROP TABLE strategy fallback (`retain` / `drop` / `warn`);
    /// per-namespace `NamespaceMapping::drop_table_strategy` overrides it
    pub drop_table_strategy: String,
}

/// CLI-layer overrides. `Some` means the operator set the flag explicitly
/// on the command line, so it wins over the TOML value; `None` defers to
/// TOML. Only knobs that reload live belong here — boot-only knobs apply
/// straight to [`EmitterConfig`] at startup.
#[derive(Debug, Clone, Default)]
pub struct CliOverrides {
    pub drop_table_strategy: Option<String>,
}

/// Owns the `watch::Sender` and the layers it merges. Held by the SIGHUP
/// task, which calls [`ConfigResolver::reload`] to re-read TOML and
/// republish. Subscribers take receivers via [`ConfigResolver::subscribe`]
/// (or the one returned by [`ConfigResolver::new`]).
pub struct ConfigResolver {
    /// `--ch-config`; `None` disables reload (nothing to re-read)
    toml_path: Option<PathBuf>,
    cli: CliOverrides,
    tx: watch::Sender<Arc<ResolvedConfig>>,
}

impl ConfigResolver {
    /// Build from the boot-parsed [`EmitterConfig`] plus the CLI overlay.
    /// Returns the resolver and a receiver seeded with the initial
    /// snapshot so callers can seed dependent runtime state before any
    /// reload fires.
    pub fn new(
        base: &EmitterConfig,
        cli: CliOverrides,
        toml_path: Option<PathBuf>,
    ) -> (Self, watch::Receiver<Arc<ResolvedConfig>>) {
        let initial = Arc::new(Self::resolve(base, &cli));
        let (tx, rx) = watch::channel(initial);
        (Self { toml_path, cli, tx }, rx)
    }

    /// Another receiver on the same channel.
    pub fn subscribe(&self) -> watch::Receiver<Arc<ResolvedConfig>> {
        self.tx.subscribe()
    }

    /// Merge one snapshot: TOML base, then explicit CLI overrides on top.
    /// The overlay layer inserts between these two once it lands.
    fn resolve(base: &EmitterConfig, cli: &CliOverrides) -> ResolvedConfig {
        let mut rc = ResolvedConfig {
            tables: base.tables.clone(),
            namespaces: base.namespaces.clone(),
            columns: HashMap::new(),
            drop_table_strategy: base.drop_table_strategy.clone(),
        };
        if let Some(v) = &cli.drop_table_strategy {
            rc.drop_table_strategy = v.clone();
        }
        rc
    }

    /// Re-read TOML (SIGHUP), re-merge with CLI on top, publish. Connection
    /// params in the reloaded file are ignored — boot-only. Parse / read
    /// errors surface to the caller and leave the last snapshot in effect
    /// (watch retains it; no send on failure).
    pub async fn reload(&self) -> Result<(), EmitterError> {
        let Some(path) = &self.toml_path else {
            return Ok(());
        };
        let toml = tokio::fs::read_to_string(path).await?;
        let base = EmitterConfig::from_toml_str(&toml)?;
        let resolved = Arc::new(Self::resolve(&base, &self.cli));
        // Err only when every receiver dropped (daemon tearing down); ignore
        let _ = self.tx.send(resolved);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base_with(drop_strategy: &str) -> EmitterConfig {
        EmitterConfig::from_toml_str(&format!(
            "[ch]\ndrop_table_strategy = \"{drop_strategy}\"\n"
        ))
        .unwrap()
    }

    #[test]
    fn cli_drop_strategy_beats_toml() {
        let base = base_with("retain");
        let cli = CliOverrides {
            drop_table_strategy: Some("warn".into()),
        };
        let resolved = ConfigResolver::resolve(&base, &cli);
        assert_eq!(resolved.drop_table_strategy, "warn");
    }

    #[test]
    fn toml_wins_when_cli_absent() {
        let base = base_with("drop");
        let resolved = ConfigResolver::resolve(&base, &CliOverrides::default());
        assert_eq!(resolved.drop_table_strategy, "drop");
    }

    #[tokio::test]
    async fn reload_without_path_is_noop() {
        let base = base_with("retain");
        let (resolver, rx) = ConfigResolver::new(&base, CliOverrides::default(), None);
        resolver.reload().await.unwrap();
        assert_eq!(rx.borrow().drop_table_strategy, "retain");
    }
}
