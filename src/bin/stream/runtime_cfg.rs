//! Runtime config: seed the resolver overlay from source PG's config tables,
//! then apply each republished snapshot on SIGHUP reload.

use std::sync::Arc;

use anyhow::Context;
use foldhash::HashSet;
use tokio::sync::{Mutex, watch};
use tokio_util::sync::CancellationToken;
use walshadow::config::ResolvedConfig;
use walshadow::mapping::{InitialLoadMode, MappingHandle};
use walshadow::pg::quote_ident;
use walshadow::schema::RelName;
use walshadow::shadow_catalog::ShadowCatalog;

/// Reload on each `sig` item, returning once signals end
pub(crate) async fn sighup_reload(
    mut sig: impl futures::Stream<Item = ()> + Unpin,
    reloader: Arc<walshadow::control::Reloader>,
) {
    use futures::StreamExt;
    while sig.next().await.is_some() {
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
}

/// First SIGINT/SIGTERM cancels the returned token, second exits at once
pub(crate) fn spawn_shutdown_signals() -> anyhow::Result<CancellationToken> {
    use tokio::signal::unix::{SignalKind, signal};
    let mut term = signal(SignalKind::terminate()).context("install SIGTERM handler")?;
    let mut int = signal(SignalKind::interrupt()).context("install SIGINT handler")?;
    let token = CancellationToken::new();
    let cancel = token.clone();
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = term.recv() => {}
                _ = int.recv() => {}
            }
            if cancel.is_cancelled() {
                tracing::warn!(target: "walshadow", "second signal, exiting without drain");
                std::process::exit(1);
            }
            tracing::info!(target: "walshadow", "signal, shutting down");
            cancel.cancel();
        }
    });
    Ok(token)
}

/// Run `fut` unless shutdown is signalled first. Bailing mid-startup skips
/// the final manifest write, which restart tolerates like a crash
pub(crate) async fn or_signal<T>(
    shutdown: &CancellationToken,
    fut: impl Future<Output = anyhow::Result<T>>,
) -> anyhow::Result<T> {
    tokio::select! {
        biased;
        () = shutdown.cancelled() => anyhow::bail!("signal"),
        out = fut => out,
    }
}

/// Load the resolver's boot overlay from source PG's `<schema>.config_*`
/// tables via the sidecar libpq connection (plan §7). Refuses (Err → daemon
/// exits) when the schema is named but not installed, or the install is newer
/// than this daemon understands — explicit opt-in should not silently no-op.
pub(crate) async fn load_runtime_config(
    client: &tokio_postgres::Client,
    schema: &str,
) -> anyhow::Result<(
    walshadow::runtime_config::ConfigOverlay,
    Vec<(RelName, walshadow::runtime_config::TableRow)>,
)> {
    use walshadow::runtime_config::{ColumnRow, ConfigOverlay, GlobalRow, NamespaceRow, TableRow};
    let s = quote_ident(schema);
    let mut overlay = ConfigOverlay::default();

    // The config_global read doubles as the install probe: a missing table
    // errors here, so a schema named but not installed refuses to start rather
    // than silently no-op (explicit opt-in). config_global is the singleton, so
    // 0 rows (greenfield) is fine — all TOML defaults then apply.
    let missing = format!(
        "runtime_config schema {schema:?} not installed (config_global unreadable); \
         set [runtime_config] schema = \"\" to disable the overlay"
    );
    if let Some(row) = client
        .query_opt(
            &format!("SELECT * FROM {s}.config_global WHERE id = 1"),
            &[],
        )
        .await
        .context(missing)?
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
            detach_strategy: row.try_get("detach_strategy").ok().flatten(),
        });
    }

    for row in client
        .query(&format!("SELECT * FROM {s}.config_namespace"), &[])
        .await
        .context("read config_namespace")?
    {
        let namespace: String = row.get("namespace");
        overlay.namespaces.insert(
            namespace,
            NamespaceRow {
                target_database: row.get("target_database"),
                auto_create: row.get("auto_create"),
                auto_create_name: row.get("auto_create_name"),
                drop_table_strategy: row.get("drop_table_strategy"),
                detach_strategy: row.try_get("detach_strategy").ok().flatten(),
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
                engine: row.try_get("engine").ok().flatten(),
                partition_by: row.try_get("partition_by").ok().flatten(),
                route: row.try_get("route").ok().flatten(),
                system: walshadow::mapping::SystemColumnNames {
                    lsn: row.try_get("lsn").ok().flatten(),
                    xid: row.try_get("xid").ok().flatten(),
                    commit_ts: row.try_get("commit_ts").ok().flatten(),
                    is_deleted: row.try_get("is_deleted").ok().flatten(),
                    leaf: row.try_get("leaf").ok().flatten(),
                },
                match_kind: row.try_get("match").ok().flatten(),
            },
        );
    }

    for row in client
        .query(&format!("SELECT * FROM {s}.config_column"), &[])
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
                nan: row.try_get("nan").ok().flatten(),
                pos_inf: row.try_get("pos_inf").ok().flatten(),
                neg_inf: row.try_get("neg_inf").ok().flatten(),
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
    tracing::info!(
        target: "walshadow::config",
        schema,
        global = has_global,
        namespaces = n_ns,
        tables = n_tbl,
        columns = n_col,
        "runtime config overlay seeded from source PG",
    );
    Ok((overlay, table_rows))
}

pub(crate) async fn apply_toml_initial_loads(
    catalog: &Arc<Mutex<ShadowCatalog>>,
    backfiller: &Arc<walshadow::copy_backfill::CopyBackfiller>,
    table_initial_loads: &foldhash::HashMap<RelName, InitialLoadMode>,
    active_tables: &HashSet<RelName>,
    sql_scoped_tables: &HashSet<RelName>,
    raw_start: u64,
) -> anyhow::Result<()> {
    let wanted = table_initial_loads.iter().filter(|(rel, mode)| {
        **mode != InitialLoadMode::None
            && active_tables.contains(*rel)
            && !sql_scoped_tables.contains(*rel)
    });
    for (rel, &mode) in wanted {
        let desc = catalog.lock().await.descriptor_by_name(rel).await?;
        let Some(desc) = desc else {
            tracing::warn!(
                target: "walshadow::config",
                qname = %rel,
                "TOML initial_load ignored: source rel unknown",
            );
            continue;
        };
        backfiller.note_opt_in(&desc, mode, raw_start).await;
    }
    Ok(())
}

/// Applies each republished [`ResolvedConfig`] snapshot to the live routing
/// map. Full swap of the operator mapping, matching the boot seed; runs
/// until the resolver's sender drops (daemon teardown).
pub(crate) async fn refresh_mapping(
    mut config_rx: watch::Receiver<Arc<ResolvedConfig>>,
    mapping: MappingHandle,
) {
    // Boot value already seeded into `mapping`; react to republishes.
    while config_rx.changed().await.is_ok() {
        let tables = config_rx.borrow_and_update().tables.clone();
        mapping.publish(Arc::new(walshadow::mapping::shared(tables)));
        tracing::info!(
            target: "walshadow::config",
            "routing map refreshed from resolved config",
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn sighup_reload_runs_per_signal_and_returns_once_they_end() {
        use tracing::instrument::WithSubscriber;
        use tracing_subscriber::layer::SubscriberExt;
        let messages = crate::housekeeping::tests::Messages::default();
        let reloader = Arc::new(walshadow::control::Reloader::default());
        sighup_reload(futures::stream::iter([(), ()]), reloader)
            .with_subscriber(tracing_subscriber::registry().with(messages.clone()))
            .await;
        assert_eq!(messages.count("SIGHUP — live reload"), 2);
    }

    #[tokio::test]
    async fn refresh_mapping_republishes_until_sender_drops() {
        let (tx, rx) = watch::channel(Arc::new(ResolvedConfig::default()));
        let mapping = walshadow::mapping::mapping_handle(Default::default());
        let before = mapping.snapshot();
        let task = tokio::spawn(refresh_mapping(rx, mapping.clone()));
        tx.send(Arc::new(ResolvedConfig::default())).unwrap();
        drop(tx);
        task.await.unwrap();
        assert!(!mapping.unmoved(&before));
    }
}
