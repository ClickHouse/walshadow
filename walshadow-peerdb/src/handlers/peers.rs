use serde_json::{Value, json};

use crate::control::{ControlError, parse_tables_body, positional};
use crate::error::GrpcError;
use crate::handlers::{
    dest_set_parts, dest_test_parts, parse_body, source_set_parts, source_test_parts,
    warn_ch_ignored, warn_pg_ignored,
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

/// Role + control line from a submitted peer; rejects unsupported types
fn peer_role_and_set(
    peer: &crate::model::Peer,
) -> Result<(Role, &'static str, Vec<String>), GrpcError> {
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
            Ok((Role::Source, "POSTGRES", source_set_parts(cfg)?))
        }
        (DbType::Clickhouse, _, Some(cfg)) => {
            if cfg.host.is_empty() {
                return Err(GrpcError::invalid("clickhouseConfig.host required"));
            }
            warn_ch_ignored(cfg);
            Ok((Role::Dest, "CLICKHOUSE", dest_set_parts(cfg)?))
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
    let (role, db_type, set_parts) = peer_role_and_set(&peer)?;

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
        .call(&set_parts)
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

pub async fn validate_peer(app: &App, v: Value) -> Result<Json<ValidatePeerResponse>, GrpcError> {
    let req: ValidatePeerRequest = parse_body(v)?;
    let Some(peer) = req.peer else {
        return Err(GrpcError::invalid("peer required"));
    };
    let test_parts = match (
        &peer.db_type,
        &peer.postgres_config,
        &peer.clickhouse_config,
    ) {
        (DbType::Postgres, Some(cfg), _) => source_test_parts(cfg)?,
        (DbType::Clickhouse, _, Some(cfg)) => dest_test_parts(cfg)?,
        (t, _, _) => {
            return Ok(Json(ValidatePeerResponse {
                status: "INVALID",
                message: format!("peer type {} unsupported", t.as_str()),
            }));
        }
    };
    match app.control.call(&test_parts).await {
        Ok(_) => Ok(Json(ValidatePeerResponse {
            status: "VALID",
            message: String::new(),
        })),
        Err(ControlError::Daemon(msg)) => Ok(Json(ValidatePeerResponse {
            status: "INVALID",
            message: msg,
        })),
        Err(e) => Err(e.into()),
    }
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
    let schemas = match app.control.call(&["schemas".into(), "list".into()]).await {
        Ok(body) => body.lines().map(str::to_string).collect::<Vec<_>>(),
        // pre-extension daemon: derive namespaces from the table list
        Err(ControlError::UnknownCommand(_)) => {
            let body = app
                .control
                .call(&["tables".into(), "list".into()])
                .await
                .map_err(GrpcError::from)?;
            let mut namespaces: Vec<String> = parse_tables_body(&body)
                .into_iter()
                .map(|t| t.namespace)
                .collect();
            namespaces.sort();
            namespaces.dedup();
            namespaces
        }
        Err(e) => return Err(e.into()),
    };
    Ok(Json(json!({"schemas": schemas})))
}

/// `tables list`, filtered to one namespace when asked. The scoped
/// `tables list <ns>` extension is a payload-size optimization only; a
/// pre-extension daemon ignores the positional and returns everything,
/// so the namespace filter always applies shim-side
async fn list_tables(
    app: &App,
    namespace: Option<&str>,
) -> Result<Vec<crate::control::TableRow>, GrpcError> {
    let mut parts = vec!["tables".into(), "list".into()];
    if let Some(ns) = namespace {
        parts.push(positional(ns)?);
    }
    let body = app.control.call(&parts).await.map_err(GrpcError::from)?;
    let mut rows = parse_tables_body(&body);
    if let Some(ns) = namespace {
        rows.retain(|t| t.namespace == ns);
    }
    Ok(rows)
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
    // `columns list <ns> <rel>` is a control-protocol extension; expected
    // payload one column per line: name\ttype[\tkey]
    let body = app
        .control
        .call(&[
            "columns".into(),
            "list".into(),
            positional(&q.schema_name)?,
            positional(&q.table_name)?,
        ])
        .await
        .map_err(GrpcError::from)?;
    let columns: Vec<_> = body
        .lines()
        .filter_map(|l| {
            let mut cols = l.split('\t');
            let name = cols.next()?.to_string();
            let column_type = cols.next().unwrap_or("").to_string();
            let is_key = cols.next() == Some("key");
            Some(ColumnsItem {
                name,
                column_type,
                is_key,
                qkind: String::new(),
                is_replica_identity: is_key,
            })
        })
        .collect();
    Ok(Json(json!({"columns": columns})))
}

pub async fn slots(app: &App, peer_name: String) -> Result<Json<Value>, GrpcError> {
    require_source(app, &peer_name).await?;
    let body = app
        .control
        .call(&["stream".into(), "status".into()])
        .await
        .map_err(GrpcError::from)?;
    let kvs = crate::control::parse_kv_body(&body);
    let lag_bytes: f32 = kvs
        .get("lag_bytes")
        .and_then(|v| v.parse().ok())
        .unwrap_or(0.0);
    // physical slot presented in logical-slot clothing
    let slot = SlotInfo {
        slot_name: "walshadow".into(),
        redo_lsn: String::new(),
        restart_lsn: String::new(),
        active: kvs.get("state").map(String::as_str) == Some("running"),
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
