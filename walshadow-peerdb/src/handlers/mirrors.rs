use std::collections::HashMap;

use serde_json::{Value, json};

use crate::control::{ControlError, parse_kv_body, parse_tables_body, positional};
use crate::error::GrpcError;
use crate::handlers::{
    check_dest_identifier, flow_status_from_kv, parse_body, rows_synced_from_kv, split_identifier,
    warn_flow_ignored, warn_mapping_ignored,
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

async fn stream_status_kv(app: &App) -> Result<HashMap<String, String>, GrpcError> {
    let body = app
        .control
        .call(&["stream".into(), "status".into()])
        .await
        .map_err(GrpcError::from)?;
    Ok(parse_kv_body(&body))
}

async fn select_tables(app: &App, tables: &[TableRef]) -> Result<(), GrpcError> {
    let mut parts = vec!["tables".into(), "select".into()];
    for t in tables {
        parts.push(positional(&format!("{}.{}", t.namespace, t.relname))?);
    }
    if tables.is_empty() {
        parts = vec!["tables".into(), "clear".into()];
    }
    app.control
        .call(&parts)
        .await
        .map_err(GrpcError::from)
        .map(|_| ())
}

fn map_start_err(e: ControlError) -> GrpcError {
    match e {
        ControlError::Daemon(m) if m.contains("not set") => GrpcError::failed_precondition(m),
        other => other.into(),
    }
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
    app.control
        .call(&["source".into(), "test".into()])
        .await
        .map_err(|e| match e {
            ControlError::Daemon(m) => GrpcError::invalid(format!("source validation: {m}")),
            other => other.into(),
        })?;
    app.control
        .call(&["dest".into(), "test".into()])
        .await
        .map_err(|e| match e {
            ControlError::Daemon(m) => GrpcError::invalid(format!("destination validation: {m}")),
            other => other.into(),
        })?;
    let body = app
        .control
        .call(&["tables".into(), "list".into()])
        .await
        .map_err(GrpcError::from)?;
    let known = parse_tables_body(&body);
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

    select_tables(app, &tables).await?;
    app.control
        .call(&["stream".into(), "start".into()])
        .await
        .map_err(map_start_err)?;

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
        select_tables(app, &tables).await?;
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
            app.control
                .call(&["stream".into(), "stop".into()])
                .await
                .map_err(GrpcError::from)?;
        }
        FlowStatus::Running => {
            app.control
                .call(&["stream".into(), "start".into()])
                .await
                .map_err(map_start_err)?;
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
            // already-stopped is the common path (pause, then terminate)
            if let Err(e) = app.control.call(&["stream".into(), "stop".into()]).await {
                match e {
                    ControlError::Daemon(msg) => {
                        tracing::info!(error = %msg, "stream stop during terminate");
                    }
                    other => return Err(other.into()),
                }
            }
            app.control
                .call(&["tables".into(), "clear".into()])
                .await
                .map_err(GrpcError::from)?;
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
    let kvs = stream_status_kv(app).await?;
    let rows = rows_synced_from_kv(&kvs);
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
        current_flow_state: flow_status_from_kv(&kvs),
        created_at: timestamp_rfc3339(mirror.created_at_unix),
    }))
}

pub async fn list_mirrors(app: &App) -> Result<Json<Value>, GrpcError> {
    let state = app.store.get().await;
    let Some(mirror) = &state.mirror else {
        return Ok(Json(json!({"mirrors": []})));
    };
    let status = match stream_status_kv(app).await {
        Ok(kvs) => flow_status_from_kv(&kvs),
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
    let rows = match stream_status_kv(app).await {
        Ok(kvs) => rows_synced_from_kv(&kvs),
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
