//! Accept-&-ignore surface: success-shaped empty bodies so PeerDB callers
//! proceed, plus version/instance introspection and the qrep reject

use hyper::Uri;
use serde_json::{Value, json};

use crate::error::GrpcError;
use crate::response::Json;
use crate::routes::App;

pub async fn version(app: &App) -> Json<Value> {
    Json(json!({"version": app.version}))
}

pub async fn instance_info(app: &App) -> Json<Value> {
    // ready == control socket answers
    let status = match app.control.call("status", &toml::Table::new()).await {
        Ok(_) => "INSTANCE_STATUS_READY",
        Err(_) => "INSTANCE_STATUS_UNKNOWN",
    };
    Json(json!({"status": status}))
}

/// no qrep engine; faking success would make callers believe a load ran
pub async fn qrep_create() -> GrpcError {
    GrpcError::unimplemented("qrep flows unsupported: walshadow is CDC-only")
}

pub async fn alert_configs_get() -> Json<Value> {
    Json(json!({"configs": []}))
}

pub async fn alert_config_post() -> Json<Value> {
    Json(json!({"id": 0}))
}

pub async fn alert_config_delete() -> Json<Value> {
    Json(json!({}))
}

pub async fn dynamic_settings_get() -> Json<Value> {
    Json(json!({"settings": []}))
}

pub async fn dynamic_setting_post() -> Json<Value> {
    Json(json!({}))
}

pub async fn scripts_get() -> Json<Value> {
    Json(json!({"scripts": []}))
}

pub async fn script_post() -> Json<Value> {
    Json(json!({"id": 0}))
}

pub async fn script_delete() -> Json<Value> {
    Json(json!({}))
}

pub async fn flow_tags_post(v: Value) -> Json<Value> {
    let name = v
        .get("flowName")
        .or_else(|| v.get("flow_name"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    Json(json!({"flowName": name}))
}

pub async fn flow_tags_get(flow_name: String) -> Json<Value> {
    Json(json!({"flowName": flow_name, "tags": []}))
}

pub async fn maintenance_post() -> Json<Value> {
    Json(json!({"workflowId": "", "runId": ""}))
}

pub async fn maintenance_status() -> Json<Value> {
    Json(json!({
        "maintenanceRunning": false,
        "phase": "MAINTENANCE_PHASE_UNKNOWN",
        "pendingActivities": [],
    }))
}

pub async fn skip_snapshot_wait() -> Json<Value> {
    Json(json!({"signalSent": false, "message": "walshadow has no snapshot wait"}))
}

pub async fn sequences_reset() -> Json<Value> {
    Json(json!({"ok": true, "errorMessage": ""}))
}

pub async fn cancel_table_addition(v: Value) -> Json<Value> {
    let name = v
        .get("flowJobName")
        .or_else(|| v.get("flow_job_name"))
        .and_then(Value::as_str)
        .unwrap_or_default();
    Json(json!({"flowJobName": name, "tablesAfterCancellation": [], "runId": ""}))
}

pub async fn unimplemented_fallback(uri: &Uri) -> GrpcError {
    GrpcError::unimplemented(format!(
        "{} not implemented by walshadow-peerdb",
        uri.path()
    ))
}
