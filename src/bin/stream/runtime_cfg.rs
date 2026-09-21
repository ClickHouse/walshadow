//! Runtime config: seed the resolver overlay from source PG's config tables,
//! then apply each republished snapshot on SIGHUP reload.

use std::sync::Arc;

use ahash::HashSet;
use anyhow::Context;
use tokio::sync::{Mutex, watch};
use walshadow::config::{ConfigResolver, ResolvedConfig};
use walshadow::mapping::MappingHandle;
use walshadow::pg::quote_ident;
use walshadow::runtime_config::InitialLoadMode;
use walshadow::schema::RelName;
use walshadow::shadow_catalog::ShadowCatalog;

pub(crate) fn spawn_sighup_reload(
    mut sig: tokio::signal::unix::Signal,
    reloader: Arc<walshadow::control::Reloader>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while sig.recv().await.is_some() {
            tracing::info!(target: "walshadow", "SIGHUP — live reload");
            match reloader.reload().await {
                Err(e) => {
                    tracing::warn!(target: "walshadow", error = %format!("{e:#}"), "reload failed")
                }
                Ok(unfollowed) if !unfollowed.is_empty() => tracing::warn!(
                    target: "walshadow::config",
                    databases = %unfollowed.join(","),
                    "config names databases this process does not follow; \
                     shadow registers bridge workers at startup, so restart to add them",
                ),
                Ok(_) => {}
            }
        }
    })
}

/// Seed the resolver overlay from source PG's `<schema>.config_*` tables via
/// the sidecar libpq connection (plan §7). Refuses (Err → daemon exits) when
/// the schema is named but not installed, or the install is newer than this
/// daemon understands — explicit opt-in should not silently no-op.
pub(crate) async fn seed_runtime_config(
    client: &tokio_postgres::Client,
    schema: &str,
    resolver: &ConfigResolver,
) -> anyhow::Result<Vec<(RelName, walshadow::runtime_config::TableRow)>> {
    use walshadow::runtime_config::{ColumnRow, ConfigOverlay, GlobalRow, NamespaceRow, TableRow};
    let s = quote_ident(schema);
    let mut overlay = ConfigOverlay::default();

    // The config_global read doubles as the install probe: a missing table
    // errors here, so a schema named but not installed refuses to start rather
    // than silently no-op (explicit opt-in). config_global is the singleton, so
    // 0 rows (greenfield) is fine — all TOML defaults then apply.
    if let Some(row) = client
        .query_opt(
            &format!(
                "SELECT row_budget, byte_budget, flush_timeout_ms, compression, \
                 retry_max_attempts, drop_table_strategy FROM {s}.config_global WHERE id = 1"
            ),
            &[],
        )
        .await
        .with_context(|| {
            format!(
                "runtime_config schema {schema:?} not installed (config_global unreadable); \
                 set [runtime_config] schema = \"\" to disable the overlay"
            )
        })?
    {
        overlay.global = Some(GlobalRow {
            row_budget: row.get("row_budget"),
            byte_budget: row.get("byte_budget"),
            flush_timeout_ms: row.get("flush_timeout_ms"),
            compression: row.get("compression"),
            retry_max_attempts: row
                .get::<_, Option<i32>>("retry_max_attempts")
                .map(i64::from),
            drop_table_strategy: row.get("drop_table_strategy"),
        });
    }

    for row in client
        .query(
            &format!(
                "SELECT namespace, target_database, auto_create, drop_table_strategy \
                 FROM {s}.config_namespace"
            ),
            &[],
        )
        .await
        .context("read config_namespace")?
    {
        let namespace: String = row.get("namespace");
        overlay.namespaces.insert(
            namespace,
            NamespaceRow {
                target_database: row.get("target_database"),
                auto_create: row.get("auto_create"),
                drop_table_strategy: row.get("drop_table_strategy"),
            },
        );
    }

    // `SELECT *` + `try_get` for the post-v1 columns so a newer daemon reads an
    // older install (missing `replicate`/`initial_load`) without a hard error —
    // the additive-schema promise. Re-running the install adds the columns.
    for row in client
        .query(&format!("SELECT * FROM {s}.config_table"), &[])
        .await
        .context("read config_table")?
    {
        let namespace: String = row.get("namespace");
        let relname: String = row.get("relname");
        overlay.tables.insert(
            RelName::new(&namespace, &relname),
            TableRow {
                target_database: row.try_get("target_database").ok().flatten(),
                target_table: row.try_get("target_table").ok().flatten(),
                replicate: row.try_get("replicate").ok().flatten(),
                initial_load: row.try_get("initial_load").ok().flatten(),
                order_by: row.try_get("order_by").ok().flatten(),
                primary_key: row.try_get("primary_key").ok().flatten(),
                system: walshadow::mapping::SystemColumnNames {
                    lsn: row.try_get("lsn").ok().flatten(),
                    xid: row.try_get("xid").ok().flatten(),
                    commit_ts: row.try_get("commit_ts").ok().flatten(),
                    is_deleted: row.try_get("is_deleted").ok().flatten(),
                },
                match_kind: row.try_get("match").ok().flatten(),
            },
        );
    }

    for row in client
        .query(
            &format!(
                "SELECT namespace, relname, attname, match, target_type FROM {s}.config_column"
            ),
            &[],
        )
        .await
        .context("read config_column")?
    {
        let namespace: String = row.get("namespace");
        let relname: String = row.get("relname");
        let attname: String = row.get("attname");
        overlay.columns.insert(
            (RelName::new(&namespace, &relname), attname),
            ColumnRow {
                target_type: row.try_get("target_type").ok().flatten(),
                match_kind: row.try_get("match").ok().flatten(),
            },
        );
    }

    let (has_global, n_ns, n_tbl, n_col) = (
        overlay.global.is_some(),
        overlay.namespaces.len(),
        overlay.tables.len(),
        overlay.columns.len(),
    );
    // Snapshot table rows for the boot opt-in dispatch: on restart the resume
    // cursor is past these rows' commit LSN, so WAL replay won't re-deliver
    // them — the seed is the only chance to re-materialise their scope.
    let table_rows: Vec<(RelName, TableRow)> = overlay
        .tables
        .iter()
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    resolver.seed_overlay(overlay).await;
    tracing::info!(
        target: "walshadow::config",
        schema,
        global = has_global,
        namespaces = n_ns,
        tables = n_tbl,
        columns = n_col,
        "runtime config overlay seeded from source PG",
    );
    Ok(table_rows)
}

pub(crate) async fn apply_toml_initial_loads(
    catalog: &Arc<Mutex<ShadowCatalog>>,
    backfiller: Option<&Arc<walshadow::copy_backfill::CopyBackfiller>>,
    table_initial_loads: &ahash::HashMap<RelName, String>,
    active_tables: &HashSet<RelName>,
    sql_scoped_tables: &HashSet<RelName>,
    raw_start: u64,
) -> anyhow::Result<()> {
    for (rel, mode) in table_initial_loads {
        if !active_tables.contains(rel) || sql_scoped_tables.contains(rel) {
            continue;
        }
        match mode.parse() {
            Ok(InitialLoadMode::None) => {}
            Ok(parsed) => {
                let desc = catalog.lock().await.descriptor_by_name(rel).await?;
                let Some(desc) = desc else {
                    tracing::warn!(
                        target: "walshadow::config",
                        qname = %rel,
                        "TOML initial_load ignored: source rel unknown",
                    );
                    continue;
                };
                match backfiller {
                    Some(b) => b.note_opt_in(&desc, parsed, raw_start).await,
                    None => tracing::info!(
                        target: "walshadow::config",
                        qname = %rel,
                        mode,
                        "TOML initial_load requested but no backfiller wired; streaming from start LSN only",
                    ),
                }
            }
            Err(_) => tracing::warn!(
                target: "walshadow::config",
                qname = %rel,
                mode,
                "unknown TOML initial_load mode; streaming from start LSN only",
            ),
        }
    }
    Ok(())
}

/// Applies each republished [`ResolvedConfig`] snapshot to the live routing
/// map. Full swap of the operator mapping, matching the boot seed; runs
/// until the resolver's sender drops (SIGHUP disabled or daemon teardown).
pub(crate) fn spawn_mapping_refresher(
    mut config_rx: watch::Receiver<Arc<ResolvedConfig>>,
    mapping: MappingHandle,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        // Boot value already seeded into `mapping`; react to republishes.
        while config_rx.changed().await.is_ok() {
            let tables = config_rx.borrow_and_update().tables.clone();
            mapping.publish(Arc::new(tables)).await;
            tracing::info!(
                target: "walshadow::config",
                "routing map refreshed from resolved config",
            );
        }
    })
}
