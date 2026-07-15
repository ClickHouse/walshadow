//! Per-table opt-in dispatch shared by the live WAL apply path (the reorder
//! coordinator, [`crate::emit::pipeline::reorder`]) and the boot seed
//! (`stream` binary's `seed_runtime_config` follow-up). A `config_table` row's
//! `replicate` flag brings a rel into or out of replication scope; the actual
//! work — resolve the descriptor, create the CH table, register the mapping —
//! needs the catalog + a CH client the [`crate::config::ConfigResolver`] lacks,
//! so it lives here rather than on the resolver.
//!
//! Boot must re-run this for seeded `replicate=true` rows: on a normal restart
//! the resume cursor is already past the config row's commit LSN, so WAL replay
//! never re-delivers it — the seed is the only chance to re-materialise the
//! opt-in mapping (the CH table itself persists across restarts).
//!
//! `initial_load` on a non-empty rel hands off to the
//! [`crate::backfill::copy_backfill::CopyBackfiller`]: `'copy'` issues a snapshot-free
//! COPY of pre-opt-in rows at `_lsn = S` (the opt-in LSN); `'base_backup'` /
//! `'object_store'` coalesce into a backup-sourced page-walk pass
//! ([`crate::backfill::backup_backfill`], plans/add_table.md). All
//! converge with the WAL stream via `ReplacingMergeTree(_lsn)` dedup, and the
//! backfiller's ledger dedups restarts. `None` (backfiller not wired) streams
//! from the opt-in LSN only; so do unknown mode strings (validate-late, never
//! crash).

use std::sync::Arc;

use async_trait::async_trait;
use tokio::sync::Mutex;

use crate::catalog::shadow_catalog::ShadowCatalog;
use crate::ch::EmitterError;
use crate::config::ConfigResolver;
use crate::emit::ch_ddl::DdlApplicator;
use crate::runtime_config::{InitialLoadMode, TableRow};
use crate::schema::{RelDescriptor, RelName};

#[async_trait]
pub trait Backfiller: Send + Sync {
    async fn note_opt_in(
        self: Arc<Self>,
        desc: Arc<RelDescriptor>,
        mode: InitialLoadMode,
        opt_in_lsn: u64,
    );

    async fn note_opt_out(&self, rel: &RelName);
}

/// Dispatch one `config_table` row's inclusion intent. `opt_in_lsn` is the
/// backfill boundary `S`: the row's commit LSN on the live path, the WAL
/// resume LSN on the boot seed (both satisfy COPY-snapshot ≥ `S`).
///
/// - `replicate=true`, rel known → create the CH table + register a
///   descriptor-derived mapping (+ backfill per `initial_load` mode).
/// - `replicate=true`, rel unknown → park a forward-declaration, materialised
///   by [`materialize_pending_on_added`] when its `CREATE TABLE` arrives.
/// - `replicate=false` → exclude (mid-stream drain).
/// - `replicate=None` → no scope change (legacy `target`-override rows, handled
///   by the resolver's overlay merge).
#[allow(clippy::too_many_arguments)]
pub async fn apply_table_opt_in(
    resolver: &ConfigResolver,
    applicator: &mut DdlApplicator,
    catalog: &Arc<Mutex<ShadowCatalog>>,
    backfiller: Option<&Arc<dyn Backfiller>>,
    rel: &RelName,
    row: &TableRow,
    opt_in_lsn: u64,
) -> Result<(), EmitterError> {
    match row.replicate {
        Some(true) => {
            let desc = catalog
                .lock()
                .await
                .descriptor_by_name(rel)
                .await
                .map_err(|error| EmitterError::Catalog(error.to_string()))?;
            match desc {
                Some(desc) => {
                    opt_in_known(resolver, applicator, backfiller, &desc, row, opt_in_lsn).await?
                }
                None => {
                    tracing::warn!(
                        target: "walshadow::config",
                        qname = %rel,
                        "config_table.replicate=true for unknown rel; parked as forward-declaration",
                    );
                    resolver.park_pending_decl(rel.clone(), row.clone()).await;
                }
            }
        }
        Some(false) => {
            resolver.exclude_table(rel).await;
            if let Some(b) = backfiller {
                b.note_opt_out(rel).await;
            }
        }
        None => {}
    }
    Ok(())
}

/// When a `CREATE TABLE` lands, materialise a parked forward-declaration for
/// that qname (no-op otherwise). Runs from the catalog-event apply, inside the
/// same barrier fence, so trailing rows in the creating xact route.
///
/// No backfill: the rel was born after the declaration, so nothing pre-dates
/// its WAL coverage — any xact that can see the table commits after the
/// `CREATE`, and its rows were buffered inclusion-agnostically.
pub async fn materialize_pending_on_added(
    resolver: &ConfigResolver,
    applicator: &mut DdlApplicator,
    desc: &Arc<RelDescriptor>,
) -> Result<(), EmitterError> {
    if let Some(row) = resolver.take_pending_decl(&desc.rel_name).await {
        if row.initial_load.is_some() {
            tracing::info!(
                target: "walshadow::config",
                qname = %desc.rel_name,
                "forward-declared opt-in: initial_load unnecessary (rel born after the declaration)",
            );
        }
        opt_in_known(resolver, applicator, None, desc, &row, 0).await?;
    }
    Ok(())
}

/// Create the CH table (idempotent) then register the opt-in mapping. Skips the
/// mapping if the shape can't be bridged so no rows route to a missing table.
async fn opt_in_known(
    resolver: &ConfigResolver,
    applicator: &mut DdlApplicator,
    backfiller: Option<&Arc<dyn Backfiller>>,
    desc: &Arc<RelDescriptor>,
    row: &TableRow,
    opt_in_lsn: u64,
) -> Result<(), EmitterError> {
    if !applicator.ensure_ch_table(desc).await? {
        return Ok(());
    }
    resolver
        .materialize_opt_in(desc, row.target_database.clone(), row.target_table.clone())
        .await;
    if let Some(mode) = row.initial_load.as_deref() {
        match InitialLoadMode::parse(mode) {
            Some(InitialLoadMode::None) => {}
            Some(parsed) => match backfiller {
                Some(b) => {
                    b.clone()
                        .note_opt_in(desc.clone(), parsed, opt_in_lsn)
                        .await
                }
                None => tracing::info!(
                    target: "walshadow::config",
                    qname = %desc.rel_name,
                    mode,
                    "initial_load requested but no backfiller wired; streaming from opt-in LSN only",
                ),
            },
            None => tracing::warn!(
                target: "walshadow::config",
                qname = %desc.rel_name,
                mode,
                "unknown initial_load mode; streaming from opt-in LSN only",
            ),
        }
    }
    Ok(())
}
