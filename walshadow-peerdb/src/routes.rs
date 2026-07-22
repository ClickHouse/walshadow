use std::sync::Arc;

use http_body_util::{BodyExt, Limited};
use hyper::body::{Body, Bytes};
use hyper::{Request, Response, Uri};
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::auth::require_auth;
use crate::control::ControlClient;
use crate::error::GrpcError;
use crate::handlers::{mirrors, misc, peers};
use crate::response::IntoResponse;
use crate::state::Store;
use crate::stats::StatsHistory;

pub struct App {
    pub control: ControlClient,
    pub store: Store,
    /// PEERDB_PASSWORD-style shared secret; unauthenticated when None
    pub password: Option<String>,
    pub version: String,
    /// Sampled rows-synced history backing the sync-history graph
    pub stats: Arc<StatsHistory>,
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;
type Full = http_body_util::Full<Bytes>;

/// axum's default request body cap carried over
const BODY_LIMIT: usize = 2 * 1024 * 1024;

pub async fn handle<B>(app: &App, req: Request<B>) -> Response<Full>
where
    B: Body,
    B::Error: Into<BoxError>,
{
    route(app, req).await.into_response()
}

async fn route<B>(app: &App, req: Request<B>) -> Result<Response<Full>, GrpcError>
where
    B: Body,
    B::Error: Into<BoxError>,
{
    let (parts, body) = req.into_parts();
    require_auth(app.password.as_deref(), &parts.headers)?;
    let path = parts.uri.path();
    Ok(match (parts.method.as_str(), path) {
        // mapped: drive the control socket
        ("POST", "/v1/peers/create") => peers::create_peer(app, json_body(body).await?)
            .await
            .into_response(),
        ("POST", "/v1/peers/validate") => peers::validate_peer(json_body(body).await?)
            .await
            .into_response(),
        ("POST", "/v1/peers/drop") => peers::drop_peer(app, json_body(body).await?)
            .await
            .into_response(),
        ("POST", "/v1/mirrors/cdc/validate") => mirrors::validate_cdc(app, json_body(body).await?)
            .await
            .into_response(),
        ("POST", "/v1/flows/cdc/create") => mirrors::create_cdc(app, json_body(body).await?)
            .await
            .into_response(),
        ("POST", "/v1/mirrors/state_change") => mirrors::state_change(app, json_body(body).await?)
            .await
            .into_response(),
        ("POST", "/v1/mirrors/status") => mirrors::mirror_status(app, json_body(body).await?)
            .await
            .into_response(),
        ("GET", "/v1/mirrors/list") => mirrors::list_mirrors(app).await.into_response(),
        ("GET", "/v1/mirrors/names") => mirrors::list_mirror_names(app).await.into_response(),
        // served from shim/control state
        ("GET", "/v1/peers/list") => peers::list_peers(app).await.into_response(),
        ("GET", "/v1/peers/schemas") => peers::schemas(app, query(&parts.uri)?)
            .await
            .into_response(),
        ("GET", "/v1/peers/tables") => peers::tables_in_schema(app, query(&parts.uri)?)
            .await
            .into_response(),
        ("GET", "/v1/peers/tables/all") => peers::all_tables(app, query(&parts.uri)?)
            .await
            .into_response(),
        ("GET", "/v1/peers/columns") => peers::columns(app, query(&parts.uri)?)
            .await
            .into_response(),
        ("GET", "/v1/peers/columns/all_type_conversions") => {
            peers::all_type_conversions().await.into_response()
        }
        ("POST", "/v1/mirrors/cdc/batches") => {
            mirrors::cdc_batches_post(app, json_body(body).await?)
                .await
                .into_response()
        }
        ("POST", "/v1/mirrors/cdc/graph") => mirrors::cdc_graph(app, json_body(body).await?)
            .await
            .into_response(),
        ("POST", "/v1/mirrors/logs") => mirrors::mirror_logs(json_body(body).await?)
            .await
            .into_response(),
        ("GET", "/v1/version") => misc::version(app).await.into_response(),
        ("GET", "/v1/instance/info") => misc::instance_info(app).await.into_response(),
        // accept & ignore
        ("GET", "/v1/peers/publications") => peers::publications().await.into_response(),
        ("POST", "/v1/peers/slots/lag_history") => peers::slot_lag_history().await.into_response(),
        ("GET", "/v1/alerts/config") => misc::alert_configs_get().await.into_response(),
        ("POST", "/v1/alerts/config") => misc::alert_config_post().await.into_response(),
        ("GET", "/v1/dynamic_settings") => misc::dynamic_settings_get().await.into_response(),
        ("POST", "/v1/dynamic_settings") => misc::dynamic_setting_post().await.into_response(),
        ("POST", "/v1/scripts") => misc::script_post().await.into_response(),
        ("POST", "/v1/flows/tags") => misc::flow_tags_post(json_body(body).await?)
            .await
            .into_response(),
        ("POST", "/v1/instance/maintenance") => misc::maintenance_post().await.into_response(),
        ("GET", "/v1/instance/maintenance/status") => {
            misc::maintenance_status().await.into_response()
        }
        ("POST", "/v1/instance/maintenance/skip-snapshot-wait") => {
            misc::skip_snapshot_wait().await.into_response()
        }
        ("POST", "/v1/mirrors/sequences/reset") => misc::sequences_reset().await.into_response(),
        ("POST", "/v1/flows/cdc/cancel_table_addition") => {
            misc::cancel_table_addition(json_body(body).await?)
                .await
                .into_response()
        }
        // reject
        ("POST", "/v1/flows/qrep/create") => misc::qrep_create().await.into_response(),
        (method, path) => {
            if method == "GET"
                && let Some(peer_name) = param(path, "/v1/peers/info/")
            {
                peers::peer_info(app, peer_name).await.into_response()
            } else if method == "GET"
                && let Some(peer_name) = param(path, "/v1/peers/type/")
            {
                peers::peer_type(app, peer_name).await.into_response()
            } else if method == "GET"
                && let Some(peer_name) = param(path, "/v1/peers/slots/")
            {
                peers::slots(app, peer_name).await.into_response()
            } else if method == "GET"
                && let Some(peer_name) = param(path, "/v1/peers/stats/")
            {
                peers::stats(app, peer_name).await.into_response()
            } else if method == "GET"
                && let Some(flow) = param(path, "/v1/mirrors/cdc/batches/")
            {
                mirrors::cdc_batches_get(app, flow).await.into_response()
            } else if method == "GET"
                && let Some(flow) = param(path, "/v1/mirrors/cdc/table_total_counts/")
            {
                mirrors::table_total_counts(app, flow).await.into_response()
            } else if method == "GET"
                && let Some(flow) = param(path, "/v1/mirrors/total_rows_synced/")
            {
                mirrors::total_rows_synced(app, flow).await.into_response()
            } else if method == "GET" && param(path, "/v1/mirrors/cdc/initial_load/").is_some() {
                mirrors::initial_load_summary().await.into_response()
            } else if method == "GET"
                && let Some(flow_name) = param(path, "/v1/flows/tags/")
            {
                misc::flow_tags_get(flow_name).await.into_response()
            } else if method == "DELETE" && param(path, "/v1/alerts/config/").is_some() {
                misc::alert_config_delete().await.into_response()
            } else if method == "GET" && param(path, "/v1/scripts/").is_some() {
                misc::scripts_get().await.into_response()
            } else if method == "DELETE" && param(path, "/v1/scripts/").is_some() {
                misc::script_delete().await.into_response()
            } else {
                misc::unimplemented_fallback(&parts.uri)
                    .await
                    .into_response()
            }
        }
    })
}

async fn json_body<B>(body: B) -> Result<Value, GrpcError>
where
    B: Body,
    B::Error: Into<BoxError>,
{
    let bytes = Limited::new(body, BODY_LIMIT)
        .collect()
        .await
        .map_err(|e| GrpcError::invalid(format!("malformed request body: {e}")))?
        .to_bytes();
    serde_json::from_slice(&bytes)
        .map_err(|e| GrpcError::invalid(format!("malformed request body: {e}")))
}

fn query<T: DeserializeOwned>(uri: &Uri) -> Result<T, GrpcError> {
    serde_urlencoded::from_str(uri.query().unwrap_or(""))
        .map_err(|e| GrpcError::invalid(format!("malformed query string: {e}")))
}

/// Trailing single-segment path param, percent-decoded (axum Path semantics)
fn param(path: &str, prefix: &str) -> Option<String> {
    let rest = path.strip_prefix(prefix)?;
    (!rest.is_empty() && !rest.contains('/')).then(|| percent_decode(rest))
}

fn percent_decode(s: &str) -> String {
    let b = s.as_bytes();
    let hex = |i: usize| {
        b.get(i)
            .and_then(|&c| char::from(c).to_digit(16))
            .map(|d| d as u8)
    };
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%'
            && let (Some(hi), Some(lo)) = (hex(i + 1), hex(i + 2))
        {
            out.push(hi << 4 | lo);
            i += 3;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8(out).unwrap_or_else(|e| String::from_utf8_lossy(&e.into_bytes()).into_owned())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_params() {
        assert_eq!(
            param("/v1/peers/info/pg", "/v1/peers/info/"),
            Some("pg".into())
        );
        assert_eq!(
            param("/v1/peers/info/a%20b", "/v1/peers/info/"),
            Some("a b".into())
        );
        assert_eq!(param("/v1/peers/info/", "/v1/peers/info/"), None);
        assert_eq!(param("/v1/peers/info/a/b", "/v1/peers/info/"), None);
        assert_eq!(param("/v1/other", "/v1/peers/info/"), None);
    }

    #[test]
    fn percent_decoding() {
        assert_eq!(percent_decode("plain"), "plain");
        assert_eq!(percent_decode("a%2Fb%3f"), "a/b?");
        // stray % passes through
        assert_eq!(percent_decode("100%"), "100%");
        assert_eq!(percent_decode("%zz"), "%zz");
    }
}
