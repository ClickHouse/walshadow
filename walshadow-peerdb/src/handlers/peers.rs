use serde_json::{Value, json};
use toml::{Table, Value as TomlValue};

use crate::control::parse_tables;
use crate::error::GrpcError;
use crate::handlers::{
    dest_fragment, parse_body, source_fragment, warn_ch_ignored, warn_pg_ignored,
};
use crate::model::{
    ColumnsItem, CreatePeerRequest, CreatePeerResponse, DbType, DropPeerRequest, PeerActivityQuery,
    PeerListItem, SchemaTablesQuery, SlotInfo, TableColumnsQuery, TableResponse,
    ValidatePeerRequest, ValidatePeerResponse, redact,
};
use crate::pb::now_unix;
use crate::response::Json;
use crate::routes::App;
use crate::state::{PeerRecord, Role};

fn peer_failed(message: impl Into<String>) -> Json<CreatePeerResponse> {
    Json(CreatePeerResponse {
        status: "FAILED",
        message: message.into(),
    })
}

/// Role + `apply` fragment from a submitted peer; rejects unsupported types
fn peer_role_and_fragment(
    peer: &crate::model::Peer,
) -> Result<(Role, &'static str, Table), GrpcError> {
    match (
        &peer.db_type,
        &peer.postgres_config,
        &peer.clickhouse_config,
    ) {
        (DbType::Postgres, Some(cfg), _) => {
            if cfg.host.is_empty() {
                return Err(GrpcError::invalid("postgresConfig.host required"));
            }
            warn_pg_ignored(cfg);
            Ok((Role::Source, "POSTGRES", source_fragment(cfg)))
        }
        (DbType::Clickhouse, _, Some(cfg)) => {
            if cfg.host.is_empty() {
                return Err(GrpcError::invalid("clickhouseConfig.host required"));
            }
            warn_ch_ignored(cfg);
            Ok((Role::Dest, "CLICKHOUSE", dest_fragment(cfg)))
        }
        (t, _, _) => Err(GrpcError::unimplemented(format!(
            "peer type {} unsupported; walshadow mirrors Postgres → ClickHouse",
            t.as_str()
        ))),
    }
}

pub async fn create_peer(app: &App, v: Value) -> Result<Json<CreatePeerResponse>, GrpcError> {
    let req: CreatePeerRequest = parse_body(v.clone())?;
    let Some(peer) = req.peer else {
        return Ok(peer_failed("peer required"));
    };
    if peer.name.is_empty() {
        return Ok(peer_failed("peer name required"));
    }
    let (role, db_type, fragment) = peer_role_and_fragment(&peer)?;

    let state = app.store.get().await;
    if let Some(existing) = state.peers.get(&peer.name) {
        if existing.role != role {
            return Ok(peer_failed(format!(
                "peer {} already exists with type {}",
                peer.name, existing.db_type
            )));
        }
        if !req.allow_update {
            return Err(GrpcError::already_exists(format!(
                "peer {} already exists",
                peer.name
            )));
        }
    } else if let Some((held_by, _)) = state.peer_by_role(role) {
        // one control daemon drives one streamer: a single source and a
        // single destination slot
        return Ok(peer_failed(format!(
            "{} slot already held by peer {held_by}; one walshadow deployment per pipe",
            match role {
                Role::Source => "source",
                Role::Dest => "destination",
            }
        )));
    }

    app.control
        .call("apply", &fragment)
        .await
        .map_err(GrpcError::from)?;
    let raw_peer = v.get("peer").cloned().unwrap_or(Value::Null);
    app.store
        .update(|s| {
            s.peers.insert(
                peer.name.clone(),
                PeerRecord {
                    db_type: db_type.into(),
                    role,
                    config: raw_peer,
                    created_at_unix: now_unix(),
                },
            );
        })
        .await
        .map_err(|e| GrpcError::internal(format!("persist shim state: {e:#}")))?;
    Ok(Json(CreatePeerResponse {
        status: "CREATED",
        message: String::new(),
    }))
}

/// The TOML protocol has no non-persisting connection probe (`apply` would
/// mutate and reload the daemon), so validation is structural: the request
/// must carry a supported peer type with a host. Connectivity surfaces when
/// the config is applied by `create_peer`, or on `mirrors/cdc/validate`,
/// which lists source tables over the live socket
pub async fn validate_peer(v: Value) -> Result<Json<ValidatePeerResponse>, GrpcError> {
    let req: ValidatePeerRequest = parse_body(v)?;
    let Some(peer) = req.peer else {
        return Err(GrpcError::invalid("peer required"));
    };
    let invalid = match (
        &peer.db_type,
        &peer.postgres_config,
        &peer.clickhouse_config,
    ) {
        (DbType::Postgres, Some(cfg), _) if cfg.host.is_empty() => {
            Some("postgresConfig.host required".into())
        }
        (DbType::Postgres, Some(_), _) => None,
        (DbType::Clickhouse, _, Some(cfg)) if cfg.host.is_empty() => {
            Some("clickhouseConfig.host required".into())
        }
        (DbType::Clickhouse, _, Some(_)) => None,
        (t, _, _) => Some(format!("peer type {} unsupported", t.as_str())),
    };
    Ok(Json(match invalid {
        None => ValidatePeerResponse {
            status: "VALID",
            message: String::new(),
        },
        Some(message) => ValidatePeerResponse {
            status: "INVALID",
            message,
        },
    }))
}

pub async fn drop_peer(app: &App, v: Value) -> Result<Json<Value>, GrpcError> {
    let req: DropPeerRequest = parse_body(v)?;
    let state = app.store.get().await;
    if !state.peers.contains_key(&req.peer_name) {
        return Err(GrpcError::not_found(format!(
            "peer {} not found",
            req.peer_name
        )));
    }
    if let Some(m) = &state.mirror
        && (m.source_name == req.peer_name || m.destination_name == req.peer_name)
    {
        return Err(GrpcError::failed_precondition(format!(
            "peer {} is referenced by mirror {}",
            req.peer_name, m.name
        )));
    }
    app.store
        .update(|s| {
            s.peers.remove(&req.peer_name);
        })
        .await
        .map_err(|e| GrpcError::internal(format!("persist shim state: {e:#}")))?;
    Ok(Json(json!({})))
}

pub async fn list_peers(app: &App) -> Json<Value> {
    let state = app.store.get().await;
    let items: Vec<_> = state
        .peers
        .iter()
        .map(|(name, p)| {
            serde_json::to_value(PeerListItem {
                name,
                db_type: &p.db_type,
            })
            .unwrap_or_default()
        })
        .collect();
    let by_role = |role: Role| -> Vec<Value> {
        state
            .peers
            .iter()
            .filter(|(_, p)| p.role == role)
            .map(|(name, p)| {
                serde_json::to_value(PeerListItem {
                    name,
                    db_type: &p.db_type,
                })
                .unwrap_or_default()
            })
            .collect()
    };
    Json(json!({
        "items": items,
        "sourceItems": by_role(Role::Source),
        "destinationItems": by_role(Role::Dest),
    }))
}

fn get_peer(state: &crate::state::ShimState, name: &str) -> Result<PeerRecord, GrpcError> {
    state
        .peers
        .get(name)
        .cloned()
        .ok_or_else(|| GrpcError::not_found(format!("peer {name} not found")))
}

pub async fn peer_info(app: &App, peer_name: String) -> Result<Json<Value>, GrpcError> {
    let state = app.store.get().await;
    let record = get_peer(&state, &peer_name)?;
    let mut peer = record.config;
    redact(&mut peer);
    Ok(Json(json!({"peer": peer, "version": ""})))
}

pub async fn peer_type(app: &App, peer_name: String) -> Result<Json<Value>, GrpcError> {
    let state = app.store.get().await;
    let record = get_peer(&state, &peer_name)?;
    Ok(Json(json!({"peerType": record.db_type})))
}

/// Source peer must exist and hold the source role before introspection
async fn require_source(app: &App, peer_name: &str) -> Result<(), GrpcError> {
    let state = app.store.get().await;
    let record = get_peer(&state, peer_name)?;
    if record.role != Role::Source {
        return Err(GrpcError::invalid(format!(
            "peer {peer_name} is not a source Postgres peer"
        )));
    }
    Ok(())
}

pub async fn schemas(app: &App, q: PeerActivityQuery) -> Result<Json<Value>, GrpcError> {
    require_source(app, &q.peer_name).await?;
    let body = app
        .control
        .call("schemas", &Table::new())
        .await
        .map_err(GrpcError::from)?;
    let schemas: Vec<&str> = body
        .get("schemas")
        .and_then(TomlValue::as_array)
        .map(|a| a.iter().filter_map(TomlValue::as_str).collect())
        .unwrap_or_default();
    Ok(Json(json!({"schemas": schemas})))
}

/// `tables`, scoped to one namespace when asked; the daemon filters
async fn list_tables(
    app: &App,
    namespace: Option<&str>,
) -> Result<Vec<crate::control::TableRow>, GrpcError> {
    let mut req = Table::new();
    if let Some(ns) = namespace {
        req.insert("namespace".into(), ns.into());
    }
    let body = app
        .control
        .call("tables", &req)
        .await
        .map_err(GrpcError::from)?;
    Ok(parse_tables(&body))
}

pub async fn tables_in_schema(app: &App, q: SchemaTablesQuery) -> Result<Json<Value>, GrpcError> {
    require_source(app, &q.peer_name).await?;
    let rows = list_tables(app, Some(&q.schema_name)).await?;
    let tables: Vec<_> = rows
        .into_iter()
        .map(|t| TableResponse {
            table_name: t.relname,
            can_mirror: true,
            table_size: String::new(),
            is_replica_identity_full: t.replica_identity_full,
        })
        .collect();
    Ok(Json(json!({"tables": tables})))
}

pub async fn all_tables(app: &App, q: PeerActivityQuery) -> Result<Json<Value>, GrpcError> {
    require_source(app, &q.peer_name).await?;
    let rows = list_tables(app, None).await?;
    let tables: Vec<String> = rows
        .into_iter()
        .map(|t| format!("{}.{}", t.namespace, t.relname))
        .collect();
    Ok(Json(json!({"tables": tables})))
}

pub async fn columns(app: &App, q: TableColumnsQuery) -> Result<Json<Value>, GrpcError> {
    require_source(app, &q.peer_name).await?;
    let mut req = Table::new();
    req.insert("namespace".into(), q.schema_name.clone().into());
    req.insert("relname".into(), q.table_name.clone().into());
    let body = app
        .control
        .call("columns", &req)
        .await
        .map_err(GrpcError::from)?;
    // `columns` carries name/type/notnull; the protocol exposes no per-column
    // key membership, so is_key / is_replica_identity stay false
    let columns: Vec<_> = body
        .get("columns")
        .and_then(TomlValue::as_array)
        .map(|arr| {
            arr.iter()
                .filter_map(|v| {
                    let t = v.as_table()?;
                    Some(ColumnsItem {
                        name: t.get("name").and_then(TomlValue::as_str)?.to_string(),
                        column_type: t
                            .get("type")
                            .and_then(TomlValue::as_str)
                            .unwrap_or_default()
                            .to_string(),
                        is_key: false,
                        qkind: String::new(),
                        is_replica_identity: false,
                    })
                })
                .collect()
        })
        .unwrap_or_default();
    Ok(Json(json!({"columns": columns})))
}

pub async fn slots(app: &App, peer_name: String) -> Result<Json<Value>, GrpcError> {
    require_source(app, &peer_name).await?;
    let status = app
        .control
        .call("status", &Table::new())
        .await
        .map_err(GrpcError::from)?;
    let lag_bytes = status
        .get("lag_bytes")
        .and_then(TomlValue::as_integer)
        .unwrap_or(0) as f32;
    // physical slot presented in logical-slot clothing
    let slot = SlotInfo {
        slot_name: "walshadow".into(),
        redo_lsn: String::new(),
        restart_lsn: String::new(),
        active: !status
            .get("paused")
            .and_then(TomlValue::as_bool)
            .unwrap_or(true),
        lag_in_mb: lag_bytes / (1024.0 * 1024.0),
        confirmed_flush_lsn: String::new(),
        wal_status: "reserved".into(),
    };
    Ok(Json(json!({"slotData": [slot]})))
}

pub async fn stats(app: &App, peer_name: String) -> Result<Json<Value>, GrpcError> {
    require_source(app, &peer_name).await?;
    Ok(Json(json!({"statData": []})))
}

/// walshadow consumes physical WAL; publications don't exist in the model
pub async fn publications() -> Json<Value> {
    Json(json!({"publicationNames": []}))
}

/// Serving empty disables UI type pickers, the safe start
pub async fn all_type_conversions() -> Json<Value> {
    Json(json!({"conversions": []}))
}

pub async fn slot_lag_history() -> Json<Value> {
    Json(json!({"data": []}))
}
