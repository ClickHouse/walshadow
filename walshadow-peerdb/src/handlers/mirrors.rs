use std::collections::BTreeMap;

use serde_json::{Value, json};
use toml::{Table, Value as TomlValue};

use crate::control::parse_tables;
use crate::error::GrpcError;
use crate::handlers::{
    check_dest_identifier, flow_status_from_status, parse_body, rows_synced_from_status,
    split_identifier, warn_flow_ignored, warn_mapping_ignored,
};
use crate::model::{
    CDCBatch, CDCMirrorStatus, CreateCDCFlowRequest, CreateCDCFlowResponse, FlowStateChangeRequest,
    FlowStatus, ListMirrorsItem, MirrorStatusRequest, MirrorStatusResponse, TableMapping,
};
use crate::pb::{now_unix, timestamp_rfc3339};
use crate::response::Json;
use crate::routes::App;
use crate::state::{MirrorRecord, Role, ShimState, TableRef};
use crate::warn::warn_ignored;

/// Resolve source/destination peer names against the registry
fn resolve_peers(state: &ShimState, source: &str, dest: &str) -> Result<(), GrpcError> {
    for (name, role) in [(source, Role::Source), (dest, Role::Dest)] {
        let record = state
            .peers
            .get(name)
            .ok_or_else(|| GrpcError::not_found(format!("peer {name} not found")))?;
        if record.role != role {
            return Err(GrpcError::invalid(format!(
                "peer {name} has type {}, expected a {} peer",
                record.db_type,
                match role {
                    Role::Source => "source Postgres",
                    Role::Dest => "destination ClickHouse",
                }
            )));
        }
    }
    Ok(())
}

/// Validate one mapping, splitting the source identifier into a
/// (namespace, relname) pair for the opt-in set
fn opt_in_table(m: &TableMapping) -> Result<TableRef, GrpcError> {
    let (namespace, relname) = split_identifier(&m.source_table_identifier)?;
    check_dest_identifier(m)?;
    warn_mapping_ignored(m);
    Ok(TableRef {
        namespace: namespace.into(),
        relname: relname.into(),
    })
}

fn reject_unsupported_modes(cfg: &crate::model::FlowConnectionConfigs) -> Result<(), GrpcError> {
    // faking success here would make callers believe a load ran
    if cfg.resync {
        return Err(GrpcError::unimplemented("resync unsupported"));
    }
    if cfg.initial_snapshot_only {
        return Err(GrpcError::unimplemented(
            "initialSnapshotOnly unsupported: walshadow is CDC-only",
        ));
    }
    Ok(())
}

async fn stream_status(app: &App) -> Result<Table, GrpcError> {
    app.control
        .call("status", &Table::new())
        .await
        .map_err(GrpcError::from)
}

/// `[table.<ns>.<rel>] replicate = true` blocks under a `table` root; empty
/// when no tables (the daemon reads a present block as opted-in)
fn tables_fragment(tables: &[TableRef]) -> Table {
    let mut by_ns: BTreeMap<&str, Table> = BTreeMap::new();
    for t in tables {
        let mut block = Table::new();
        block.insert("replicate".into(), true.into());
        by_ns
            .entry(&t.namespace)
            .or_default()
            .insert(t.relname.clone(), TomlValue::Table(block));
    }
    let mut root = Table::new();
    if !by_ns.is_empty() {
        let mut table = Table::new();
        for (ns, rels) in by_ns {
            table.insert(ns.into(), TomlValue::Table(rels));
        }
        root.insert("table".into(), TomlValue::Table(table));
    }
    root
}

fn set_paused(root: &mut Table, paused: bool) {
    let mut stream = Table::new();
    stream.insert("paused".into(), paused.into());
    root.insert("stream".into(), TomlValue::Table(stream));
}

/// Reconcile the opt-in set: opt in `desired` (idempotent) then remove
/// `previous` entries no longer wanted. Applying before unsetting keeps a
/// still-wanted table selected at every step, so a live stream never drops
async fn reconcile_tables(
    app: &App,
    desired: &[TableRef],
    previous: &[TableRef],
) -> Result<(), GrpcError> {
    let add = tables_fragment(desired);
    if !add.is_empty() {
        app.control
            .call("apply", &add)
            .await
            .map_err(GrpcError::from)?;
    }
    let removed: Vec<&TableRef> = previous.iter().filter(|p| !desired.contains(p)).collect();
    if !removed.is_empty() {
        let mut by_ns: BTreeMap<&str, Table> = BTreeMap::new();
        for t in &removed {
            by_ns
                .entry(&t.namespace)
                .or_default()
                .insert(t.relname.clone(), TomlValue::String(String::new()));
        }
        let mut table = Table::new();
        for (ns, rels) in by_ns {
            table.insert(ns.into(), TomlValue::Table(rels));
        }
        let mut mask = Table::new();
        mask.insert("table".into(), TomlValue::Table(table));
        app.control
            .call("unset", &mask)
            .await
            .map_err(GrpcError::from)?;
    }
    Ok(())
}

/// Drop the whole shim-owned `[table]` section from the fragment
async fn clear_tables(app: &App) -> Result<(), GrpcError> {
    let mut mask = Table::new();
    mask.insert("table".into(), TomlValue::String(String::new()));
    app.control
        .call("unset", &mask)
        .await
        .map_err(GrpcError::from)
        .map(|_| ())
}

async fn apply_paused(app: &App, paused: bool) -> Result<(), GrpcError> {
    let mut root = Table::new();
    set_paused(&mut root, paused);
    app.control
        .call("apply", &root)
        .await
        .map_err(GrpcError::from)
        .map(|_| ())
}

pub async fn validate_cdc(app: &App, v: Value) -> Result<Json<Value>, GrpcError> {
    let req: CreateCDCFlowRequest = parse_body(v)?;
    let Some(cfg) = req.connection_configs else {
        return Err(GrpcError::invalid("connectionConfigs required"));
    };
    reject_unsupported_modes(&cfg)?;
    let state = app.store.get().await;
    resolve_peers(&state, &cfg.source_name, &cfg.destination_name)?;
    let mut wanted = Vec::new();
    for m in &cfg.table_mappings {
        wanted.push(opt_in_table(m)?);
    }
    // `tables` connects to the (already-applied) source, so reachability and
    // table existence validate together; the protocol has no destination probe
    let body = app
        .control
        .call("tables", &Table::new())
        .await
        .map_err(|e| match e {
            crate::control::ControlError::Daemon(m) => {
                GrpcError::invalid(format!("source validation: {m}"))
            }
            other => other.into(),
        })?;
    let known = parse_tables(&body);
    for t in &wanted {
        if !known
            .iter()
            .any(|k| k.namespace == t.namespace && k.relname == t.relname)
        {
            return Err(GrpcError::invalid(format!(
                "table {}.{} not found on source",
                t.namespace, t.relname
            )));
        }
    }
    Ok(Json(json!({})))
}

pub async fn create_cdc(app: &App, v: Value) -> Result<Json<CreateCDCFlowResponse>, GrpcError> {
    let req: CreateCDCFlowRequest = parse_body(v.clone())?;
    let Some(cfg) = req.connection_configs else {
        return Err(GrpcError::invalid("connectionConfigs required"));
    };
    if cfg.flow_job_name.is_empty() {
        return Err(GrpcError::invalid("flowJobName required"));
    }

    let state = app.store.get().await;
    if let Some(existing) = &state.mirror {
        if existing.name == cfg.flow_job_name && req.attach_to_existing {
            return Ok(Json(CreateCDCFlowResponse {
                workflow_id: existing.workflow_id.clone(),
            }));
        }
        return Err(GrpcError::already_exists(format!(
            "mirror {} already exists; mirror cardinality is one per deployment",
            existing.name
        )));
    }

    reject_unsupported_modes(&cfg)?;
    resolve_peers(&state, &cfg.source_name, &cfg.destination_name)?;
    if cfg.table_mappings.is_empty() {
        return Err(GrpcError::invalid("tableMappings required"));
    }
    warn_flow_ignored(&cfg);
    let mut tables = Vec::new();
    for m in &cfg.table_mappings {
        tables.push(opt_in_table(m)?);
    }

    // opt in the tables and unpause in one apply, so the stream never runs
    // over an empty selection nor sits selected-but-paused between reloads
    let mut frag = tables_fragment(&tables);
    set_paused(&mut frag, false);
    app.control
        .call("apply", &frag)
        .await
        .map_err(GrpcError::from)?;

    let raw_config = v
        .get("connectionConfigs")
        .or_else(|| v.get("connection_configs"))
        .cloned()
        .unwrap_or(Value::Null);
    let record = MirrorRecord {
        name: cfg.flow_job_name.clone(),
        workflow_id: cfg.flow_job_name.clone(),
        source_name: cfg.source_name.clone(),
        destination_name: cfg.destination_name.clone(),
        tables,
        do_initial_snapshot: cfg.do_initial_snapshot,
        created_at_unix: now_unix(),
        config: raw_config,
    };
    app.store
        .update(|s| {
            s.terminated.retain(|n| n != &record.name);
            s.mirror = Some(record);
        })
        .await
        .map_err(|e| GrpcError::internal(format!("persist shim state: {e:#}")))?;
    Ok(Json(CreateCDCFlowResponse {
        workflow_id: cfg.flow_job_name,
    }))
}

pub async fn state_change(app: &App, v: Value) -> Result<Json<Value>, GrpcError> {
    let req: FlowStateChangeRequest = parse_body(v)?;
    let state = app.store.get().await;
    let mirror = match &state.mirror {
        Some(m) if m.name == req.flow_job_name => m.clone(),
        _ if state.terminated.contains(&req.flow_job_name)
            && req.requested_flow_state == FlowStatus::Terminated =>
        {
            return Ok(Json(json!({})));
        }
        _ => {
            return Err(GrpcError::not_found(format!(
                "mirror {} not found",
                req.flow_job_name
            )));
        }
    };

    if req.drop_mirror_stats {
        warn_ignored("dropMirrorStats", "shim keeps no batch history to drop");
    }

    if let Some(update) = req
        .flow_config_update
        .as_ref()
        .and_then(|u| u.cdc_flow_config_update.as_ref())
    {
        let mut tables = mirror.tables.clone();
        for m in &update.additional_tables {
            let t = opt_in_table(m)?;
            if !tables.contains(&t) {
                tables.push(t);
            }
        }
        for m in &update.removed_tables {
            let (namespace, relname) = split_identifier(&m.source_table_identifier)?;
            tables.retain(|t| !(t.namespace == namespace && t.relname == relname));
        }
        if update.batch_size != 0 || update.idle_timeout != 0 || update.number_of_syncs != 0 {
            warn_ignored(
                "cdcFlowConfigUpdate.batching",
                "batching governed by walshadow emitter budgets",
            );
        }
        if !update.updated_env.is_empty() {
            warn_ignored(
                "cdcFlowConfigUpdate.updatedEnv",
                "per-flow env not forwarded",
            );
        }
        if update.snapshot_num_rows_per_partition != 0
            || update.snapshot_num_partitions_override != 0
            || update.snapshot_max_parallel_workers != 0
            || update.snapshot_num_tables_in_parallel != 0
        {
            warn_ignored(
                "cdcFlowConfigUpdate.snapshotKnobs",
                "backfill knobs have no walshadow counterpart",
            );
        }
        if update.skip_initial_snapshot_for_table_additions {
            warn_ignored(
                "skipInitialSnapshotForTableAdditions",
                "walshadow always backfills newly opted-in tables",
            );
        }
        reconcile_tables(app, &tables, &mirror.tables).await?;
        app.store
            .update(|s| {
                if let Some(m) = &mut s.mirror {
                    m.tables = tables;
                }
            })
            .await
            .map_err(|e| GrpcError::internal(format!("persist shim state: {e:#}")))?;
    }

    match req.requested_flow_state {
        FlowStatus::Paused => {
            apply_paused(app, true).await?;
        }
        FlowStatus::Running => {
            apply_paused(app, false).await?;
        }
        FlowStatus::Terminated => {
            if !req.skip_destination_drop {
                // control never drops destination tables; terminate behaves
                // as skipDestinationDrop = true always
                warn_ignored(
                    "skipDestinationDrop=false",
                    "destination tables are never dropped on terminate",
                );
            }
            // pausing then clearing the opt-in set is idempotent: a
            // pause-then-terminate re-pauses without error
            apply_paused(app, true).await?;
            clear_tables(app).await?;
            app.store
                .update(|s| {
                    s.mirror = None;
                    if !s.terminated.contains(&mirror.name) {
                        s.terminated.push(mirror.name.clone());
                    }
                })
                .await
                .map_err(|e| GrpcError::internal(format!("persist shim state: {e:#}")))?;
        }
        // STATUS_UNKNOWN carries a pure config update
        FlowStatus::Unknown => {}
        other => {
            return Err(GrpcError::invalid(format!(
                "requestedFlowState {} unsupported",
                other.as_str()
            )));
        }
    }
    Ok(Json(json!({})))
}

/// One coarse synthetic batch from the rows-synced counter, enough for UI
/// rendering; no per-batch history exists shim-side
fn synth_batches(rows: i64, created_at_unix: i64) -> Vec<CDCBatch> {
    if rows == 0 {
        return Vec::new();
    }
    vec![CDCBatch {
        start_lsn: 0,
        end_lsn: 0,
        num_rows: rows,
        start_time: timestamp_rfc3339(created_at_unix),
        end_time: timestamp_rfc3339(now_unix()),
        batch_id: 1,
    }]
}

pub async fn mirror_status(app: &App, v: Value) -> Result<Json<MirrorStatusResponse>, GrpcError> {
    let req: MirrorStatusRequest = parse_body(v)?;
    let state = app.store.get().await;
    let Some(mirror) = state
        .mirror
        .as_ref()
        .filter(|m| m.name == req.flow_job_name)
    else {
        if state.terminated.contains(&req.flow_job_name) {
            return Ok(Json(MirrorStatusResponse {
                flow_job_name: req.flow_job_name,
                cdc_status: CDCMirrorStatus {
                    config: json!({}),
                    snapshot_status: json!({"clones": []}),
                    cdc_batches: Vec::new(),
                    source_type: "POSTGRES",
                    destination_type: "CLICKHOUSE",
                    rows_synced: 0,
                },
                current_flow_state: FlowStatus::Terminated,
                created_at: String::new(),
            }));
        }
        return Err(GrpcError::not_found(format!(
            "mirror {} not found",
            req.flow_job_name
        )));
    };
    let status = stream_status(app).await?;
    let rows = rows_synced_from_status(&status);
    Ok(Json(MirrorStatusResponse {
        flow_job_name: mirror.name.clone(),
        cdc_status: CDCMirrorStatus {
            config: mirror.config.clone(),
            snapshot_status: json!({"clones": []}),
            cdc_batches: if req.exclude_batches {
                Vec::new()
            } else {
                synth_batches(rows, mirror.created_at_unix)
            },
            source_type: "POSTGRES",
            destination_type: "CLICKHOUSE",
            rows_synced: rows,
        },
        current_flow_state: flow_status_from_status(&status),
        created_at: timestamp_rfc3339(mirror.created_at_unix),
    }))
}

pub async fn list_mirrors(app: &App) -> Result<Json<Value>, GrpcError> {
    let state = app.store.get().await;
    let Some(mirror) = &state.mirror else {
        return Ok(Json(json!({"mirrors": []})));
    };
    let status = match stream_status(app).await {
        Ok(s) => flow_status_from_status(&s),
        // list should render even with control down
        Err(_) => FlowStatus::Unknown,
    };
    let item = ListMirrorsItem {
        id: 1,
        workflow_id: mirror.workflow_id.clone(),
        name: mirror.name.clone(),
        source_name: mirror.source_name.clone(),
        source_type: "POSTGRES",
        destination_name: mirror.destination_name.clone(),
        destination_type: "CLICKHOUSE",
        created_at: (mirror.created_at_unix as f64) * 1000.0,
        is_cdc: true,
        status,
    };
    Ok(Json(json!({"mirrors": [item]})))
}

pub async fn list_mirror_names(app: &App) -> Json<Value> {
    let state = app.store.get().await;
    let names: Vec<&str> = state.mirror.iter().map(|m| m.name.as_str()).collect();
    Json(json!({"names": names}))
}

/// Rows counter for the stats endpoints; unknown mirror name → zero rather
/// than an error so UI panels render
async fn mirror_rows(app: &App, flow_job_name: &str) -> (i64, i64) {
    let state = app.store.get().await;
    let Some(mirror) = state.mirror.as_ref().filter(|m| m.name == flow_job_name) else {
        return (0, 0);
    };
    let rows = match stream_status(app).await {
        Ok(s) => rows_synced_from_status(&s),
        Err(_) => 0,
    };
    (rows, mirror.created_at_unix)
}

pub async fn cdc_batches_get(app: &App, flow_job_name: String) -> Json<Value> {
    let (rows, created) = mirror_rows(app, &flow_job_name).await;
    let batches = synth_batches(rows, created);
    Json(json!({"cdcBatches": batches, "total": batches.len(), "page": 1}))
}

pub async fn cdc_batches_post(app: &App, v: Value) -> Json<Value> {
    let req: crate::model::GetCDCBatchesRequest = parse_body(v).unwrap_or_default();
    let (rows, created) = mirror_rows(app, &req.flow_job_name).await;
    let batches = synth_batches(rows, created);
    Json(json!({"cdcBatches": batches, "total": batches.len(), "page": 1}))
}

pub async fn cdc_graph(app: &App, v: Value) -> Json<Value> {
    let req: crate::model::GraphRequest = parse_body(v).unwrap_or_default();
    let (rows, _) = mirror_rows(app, &req.flow_job_name).await;
    Json(json!({"data": [], "totalRows": rows.to_string()}))
}

pub async fn table_total_counts(app: &App, flow_job_name: String) -> Json<Value> {
    let (rows, _) = mirror_rows(app, &flow_job_name).await;
    Json(json!({
        "totalData": {"totalCount": rows.to_string()},
        "tablesData": [],
    }))
}

pub async fn total_rows_synced(app: &App, flow_job_name: String) -> Json<Value> {
    let (rows, _) = mirror_rows(app, &flow_job_name).await;
    Json(json!({
        "totalCountCDC": rows.to_string(),
        "totalCountInitialLoad": "0",
        "totalCount": rows.to_string(),
    }))
}

pub async fn initial_load_summary() -> Json<Value> {
    Json(json!({"tableSummaries": []}))
}

pub async fn mirror_logs(v: Value) -> Json<Value> {
    let req: crate::model::ListMirrorLogsRequest = parse_body(v).unwrap_or_default();
    Json(json!({"errors": [], "total": 0, "page": req.page}))
}
