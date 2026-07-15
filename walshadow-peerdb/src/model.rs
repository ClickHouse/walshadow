//! Hand-written serde structs for the consumed subset of PeerDB's
//! `route.proto`/`peers.proto`/`flow.proto` (grpc-gateway JSON encoding).
//! Deserialization is tolerant: unknown fields ignored, missing fields
//! defaulted, enums accepted as names or numbers, snake_case accepted
//! alongside lowerCamelCase — matches proto3 semantics so PeerDB clients
//! evolve without lockstep shim releases

use std::collections::HashMap;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;

use crate::pb::{EnumToken, enum_name_or_number, i64_str};

fn flex_u64<'de, D: Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum Raw {
        Num(u64),
        Str(String),
    }
    match Raw::deserialize(d)? {
        Raw::Num(n) => Ok(n),
        Raw::Str(s) => s.parse().map_err(serde::de::Error::custom),
    }
}

fn flex_i64<'de, D: Deserializer<'de>>(d: D) -> Result<i64, D::Error> {
    i64_str::deserialize(d)
}

// ---------------------------------------------------------------- enums

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum FlowStatus {
    #[default]
    Unknown,
    Running,
    Paused,
    Pausing,
    Setup,
    Snapshot,
    Terminating,
    Terminated,
    Completed,
    Resync,
    Failed,
    Modifying,
}

impl FlowStatus {
    pub fn as_str(self) -> &'static str {
        match self {
            FlowStatus::Unknown => "STATUS_UNKNOWN",
            FlowStatus::Running => "STATUS_RUNNING",
            FlowStatus::Paused => "STATUS_PAUSED",
            FlowStatus::Pausing => "STATUS_PAUSING",
            FlowStatus::Setup => "STATUS_SETUP",
            FlowStatus::Snapshot => "STATUS_SNAPSHOT",
            FlowStatus::Terminating => "STATUS_TERMINATING",
            FlowStatus::Terminated => "STATUS_TERMINATED",
            FlowStatus::Completed => "STATUS_COMPLETED",
            FlowStatus::Resync => "STATUS_RESYNC",
            FlowStatus::Failed => "STATUS_FAILED",
            FlowStatus::Modifying => "STATUS_MODIFYING",
        }
    }

    fn from_token(t: &EnumToken) -> Self {
        let all = [
            FlowStatus::Unknown,
            FlowStatus::Running,
            FlowStatus::Paused,
            FlowStatus::Pausing,
            FlowStatus::Setup,
            FlowStatus::Snapshot,
            FlowStatus::Terminating,
            FlowStatus::Terminated,
            FlowStatus::Completed,
            FlowStatus::Resync,
            FlowStatus::Failed,
            FlowStatus::Modifying,
        ];
        match t {
            EnumToken::Name(s) => all
                .into_iter()
                .find(|v| v.as_str() == s)
                .unwrap_or_default(),
            EnumToken::Number(n) => usize::try_from(*n)
                .ok()
                .and_then(|i| all.get(i).copied())
                .unwrap_or_default(),
        }
    }
}

impl Serialize for FlowStatus {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.as_str())
    }
}

impl<'de> Deserialize<'de> for FlowStatus {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Ok(enum_name_or_number(d)?
            .map(|t| FlowStatus::from_token(&t))
            .unwrap_or_default())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DbType {
    Postgres,
    Clickhouse,
    Other(String),
}

impl Default for DbType {
    fn default() -> Self {
        DbType::Other("UNSPECIFIED".into())
    }
}

impl DbType {
    pub fn as_str(&self) -> &str {
        match self {
            DbType::Postgres => "POSTGRES",
            DbType::Clickhouse => "CLICKHOUSE",
            DbType::Other(s) => s,
        }
    }
}

impl<'de> Deserialize<'de> for DbType {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        Ok(match enum_name_or_number(d)? {
            None => DbType::default(),
            Some(EnumToken::Name(s)) => match s.as_str() {
                "POSTGRES" => DbType::Postgres,
                "CLICKHOUSE" => DbType::Clickhouse,
                _ => DbType::Other(s),
            },
            Some(EnumToken::Number(3)) => DbType::Postgres,
            Some(EnumToken::Number(8)) => DbType::Clickhouse,
            Some(EnumToken::Number(n)) => DbType::Other(n.to_string()),
        })
    }
}

// -------------------------------------------------------- peer requests

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct PostgresConfig {
    pub host: String,
    pub port: u32,
    pub user: String,
    pub password: String,
    pub database: String,
    #[serde(alias = "require_tls")]
    pub require_tls: bool,
    #[serde(alias = "disable_tls")]
    pub disable_tls: Option<bool>,
    #[serde(alias = "tls_host")]
    pub tls_host: String,
    #[serde(alias = "metadata_schema")]
    pub metadata_schema: Option<String>,
    #[serde(alias = "ssh_config")]
    pub ssh_config: Option<Value>,
    #[serde(alias = "root_ca")]
    pub root_ca: Option<String>,
    #[serde(alias = "auth_type")]
    pub auth_type: Option<Value>,
    #[serde(alias = "aws_auth")]
    pub aws_auth: Option<Value>,
    #[serde(alias = "skip_cert_verification")]
    pub skip_cert_verification: bool,
}

impl PostgresConfig {
    /// disable_tls / require_tls fold into libpq sslmode; walshadow-control
    /// takes sslmode verbatim
    pub fn sslmode(&self) -> &'static str {
        if self.disable_tls == Some(true) {
            "disable"
        } else if self.require_tls {
            "require"
        } else {
            "prefer"
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ClickhouseConfig {
    pub host: String,
    pub port: u32,
    pub user: String,
    pub password: String,
    pub database: String,
    #[serde(alias = "s3_path")]
    pub s3_path: String,
    #[serde(alias = "access_key_id")]
    pub access_key_id: String,
    #[serde(alias = "secret_access_key")]
    pub secret_access_key: String,
    pub region: String,
    #[serde(alias = "disable_tls")]
    pub disable_tls: bool,
    pub endpoint: Option<String>,
    pub certificate: Option<String>,
    #[serde(alias = "private_key")]
    pub private_key: Option<String>,
    #[serde(alias = "root_ca")]
    pub root_ca: Option<String>,
    #[serde(alias = "tls_host")]
    pub tls_host: String,
    pub s3: Option<Value>,
    pub cluster: String,
    pub replicated: bool,
    #[serde(alias = "tls_certificate_directory")]
    pub tls_certificate_directory: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct Peer {
    pub name: String,
    #[serde(rename = "type")]
    pub db_type: DbType,
    #[serde(alias = "postgres_config")]
    pub postgres_config: Option<PostgresConfig>,
    #[serde(alias = "clickhouse_config")]
    pub clickhouse_config: Option<ClickhouseConfig>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct CreatePeerRequest {
    pub peer: Option<Peer>,
    #[serde(alias = "allow_update")]
    pub allow_update: bool,
    #[serde(alias = "disable_validation")]
    pub disable_validation: bool,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct ValidatePeerRequest {
    pub peer: Option<Peer>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct DropPeerRequest {
    #[serde(alias = "peer_name")]
    pub peer_name: String,
}

// ------------------------------------------------------- flow requests

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct TableMapping {
    #[serde(alias = "source_table_identifier")]
    pub source_table_identifier: String,
    #[serde(alias = "destination_table_identifier")]
    pub destination_table_identifier: String,
    #[serde(alias = "partition_key")]
    pub partition_key: String,
    pub exclude: Vec<String>,
    pub columns: Vec<Value>,
    #[serde(deserialize_with = "enum_name_or_number")]
    pub engine: Option<EnumToken>,
    #[serde(alias = "sharding_key")]
    pub sharding_key: String,
    #[serde(alias = "policy_name")]
    pub policy_name: String,
    #[serde(alias = "partition_by_expr")]
    pub partition_by_expr: String,
}

impl TableMapping {
    /// TableEngine 0 = CH_ENGINE_REPLACING_MERGE_TREE, walshadow's native
    /// destination shape; anything else is a divergence worth a WARN
    pub fn engine_is_default(&self) -> bool {
        match &self.engine {
            None => true,
            Some(EnumToken::Number(n)) => *n == 0,
            Some(EnumToken::Name(s)) => s == "CH_ENGINE_REPLACING_MERGE_TREE",
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct FlowConnectionConfigs {
    #[serde(alias = "flow_job_name")]
    pub flow_job_name: String,
    #[serde(alias = "table_mappings")]
    pub table_mappings: Vec<TableMapping>,
    #[serde(alias = "max_batch_size")]
    pub max_batch_size: u32,
    #[serde(alias = "idle_timeout_seconds", deserialize_with = "flex_u64")]
    pub idle_timeout_seconds: u64,
    #[serde(alias = "cdc_staging_path")]
    pub cdc_staging_path: String,
    #[serde(alias = "publication_name")]
    pub publication_name: String,
    #[serde(alias = "replication_slot_name")]
    pub replication_slot_name: String,
    #[serde(alias = "do_initial_snapshot")]
    pub do_initial_snapshot: bool,
    #[serde(alias = "snapshot_num_rows_per_partition")]
    pub snapshot_num_rows_per_partition: u32,
    #[serde(alias = "snapshot_num_partitions_override")]
    pub snapshot_num_partitions_override: u32,
    #[serde(alias = "snapshot_staging_path")]
    pub snapshot_staging_path: String,
    #[serde(alias = "snapshot_max_parallel_workers")]
    pub snapshot_max_parallel_workers: u32,
    #[serde(alias = "snapshot_num_tables_in_parallel")]
    pub snapshot_num_tables_in_parallel: u32,
    pub resync: bool,
    #[serde(alias = "initial_snapshot_only")]
    pub initial_snapshot_only: bool,
    #[serde(alias = "soft_delete_col_name")]
    pub soft_delete_col_name: String,
    #[serde(alias = "synced_at_col_name")]
    pub synced_at_col_name: String,
    pub script: String,
    #[serde(deserialize_with = "enum_name_or_number")]
    pub system: Option<EnumToken>,
    #[serde(alias = "source_name")]
    pub source_name: String,
    #[serde(alias = "destination_name")]
    pub destination_name: String,
    pub env: HashMap<String, String>,
    pub version: u32,
    pub flags: Vec<String>,
    #[serde(alias = "skip_validation")]
    pub skip_validation: Option<bool>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct CreateCDCFlowRequest {
    #[serde(alias = "connection_configs")]
    pub connection_configs: Option<FlowConnectionConfigs>,
    #[serde(alias = "attach_to_existing")]
    pub attach_to_existing: bool,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct CdcFlowConfigUpdate {
    #[serde(alias = "additional_tables")]
    pub additional_tables: Vec<TableMapping>,
    #[serde(alias = "removed_tables")]
    pub removed_tables: Vec<TableMapping>,
    #[serde(alias = "batch_size")]
    pub batch_size: u32,
    #[serde(alias = "idle_timeout", deserialize_with = "flex_u64")]
    pub idle_timeout: u64,
    #[serde(alias = "number_of_syncs")]
    pub number_of_syncs: i32,
    #[serde(alias = "updated_env")]
    pub updated_env: HashMap<String, String>,
    #[serde(alias = "snapshot_num_rows_per_partition")]
    pub snapshot_num_rows_per_partition: u32,
    #[serde(alias = "snapshot_num_partitions_override")]
    pub snapshot_num_partitions_override: u32,
    #[serde(alias = "snapshot_max_parallel_workers")]
    pub snapshot_max_parallel_workers: u32,
    #[serde(alias = "snapshot_num_tables_in_parallel")]
    pub snapshot_num_tables_in_parallel: u32,
    #[serde(alias = "skip_initial_snapshot_for_table_additions")]
    pub skip_initial_snapshot_for_table_additions: bool,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct FlowConfigUpdate {
    #[serde(alias = "cdc_flow_config_update")]
    pub cdc_flow_config_update: Option<CdcFlowConfigUpdate>,
    #[serde(alias = "qrep_flow_config_update")]
    pub qrep_flow_config_update: Option<Value>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct FlowStateChangeRequest {
    #[serde(alias = "flow_job_name")]
    pub flow_job_name: String,
    #[serde(alias = "requested_flow_state")]
    pub requested_flow_state: FlowStatus,
    #[serde(alias = "flow_config_update")]
    pub flow_config_update: Option<FlowConfigUpdate>,
    #[serde(alias = "drop_mirror_stats")]
    pub drop_mirror_stats: bool,
    #[serde(alias = "skip_destination_drop")]
    pub skip_destination_drop: bool,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct MirrorStatusRequest {
    #[serde(alias = "flow_job_name")]
    pub flow_job_name: String,
    #[serde(alias = "include_flow_info")]
    pub include_flow_info: bool,
    #[serde(alias = "exclude_batches")]
    pub exclude_batches: bool,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct GetCDCBatchesRequest {
    #[serde(alias = "flow_job_name")]
    pub flow_job_name: String,
    pub limit: u32,
    pub ascending: bool,
    #[serde(alias = "before_id", deserialize_with = "flex_i64")]
    pub before_id: i64,
    #[serde(alias = "after_id", deserialize_with = "flex_i64")]
    pub after_id: i64,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct GraphRequest {
    #[serde(alias = "flow_job_name")]
    pub flow_job_name: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default, rename_all = "camelCase")]
pub struct ListMirrorLogsRequest {
    #[serde(alias = "flow_job_name")]
    pub flow_job_name: String,
    pub level: String,
    pub page: i32,
    #[serde(alias = "num_per_page")]
    pub num_per_page: i32,
}

// -------------------------------------------------------- query params

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct PeerActivityQuery {
    #[serde(alias = "peerName")]
    pub peer_name: String,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct SchemaTablesQuery {
    #[serde(alias = "peerName")]
    pub peer_name: String,
    #[serde(alias = "schemaName")]
    pub schema_name: String,
    #[serde(alias = "cdcEnabled")]
    pub cdc_enabled: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(default)]
pub struct TableColumnsQuery {
    #[serde(alias = "peerName")]
    pub peer_name: String,
    #[serde(alias = "schemaName")]
    pub schema_name: String,
    #[serde(alias = "tableName")]
    pub table_name: String,
}

// ----------------------------------------------------------- responses

#[derive(Serialize)]
pub struct CreatePeerResponse {
    pub status: &'static str,
    pub message: String,
}

#[derive(Serialize)]
pub struct ValidatePeerResponse {
    pub status: &'static str,
    pub message: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateCDCFlowResponse {
    pub workflow_id: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ListMirrorsItem {
    #[serde(with = "i64_str")]
    pub id: i64,
    pub workflow_id: String,
    pub name: String,
    pub source_name: String,
    pub source_type: &'static str,
    pub destination_name: String,
    pub destination_type: &'static str,
    /// epoch milliseconds; proto double, PeerDB fills `UnixMilli()`
    pub created_at: f64,
    pub is_cdc: bool,
    pub status: FlowStatus,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CDCBatch {
    #[serde(with = "i64_str")]
    pub start_lsn: i64,
    #[serde(with = "i64_str")]
    pub end_lsn: i64,
    #[serde(with = "i64_str")]
    pub num_rows: i64,
    pub start_time: String,
    pub end_time: String,
    #[serde(with = "i64_str")]
    pub batch_id: i64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CDCMirrorStatus {
    pub config: Value,
    pub snapshot_status: Value,
    pub cdc_batches: Vec<CDCBatch>,
    pub source_type: &'static str,
    pub destination_type: &'static str,
    #[serde(with = "i64_str")]
    pub rows_synced: i64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MirrorStatusResponse {
    pub flow_job_name: String,
    pub cdc_status: CDCMirrorStatus,
    pub current_flow_state: FlowStatus,
    pub created_at: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct TableResponse {
    pub table_name: String,
    pub can_mirror: bool,
    pub table_size: String,
    pub is_replica_identity_full: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ColumnsItem {
    pub name: String,
    #[serde(rename = "type")]
    pub column_type: String,
    pub is_key: bool,
    pub qkind: String,
    pub is_replica_identity: bool,
}

/// LSN fields carry proto names like `redo_lSN`, whose protojson name is
/// `redoLSN`; spelled out per field rather than trusting rename_all
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SlotInfo {
    pub slot_name: String,
    #[serde(rename = "redoLSN")]
    pub redo_lsn: String,
    #[serde(rename = "restartLSN")]
    pub restart_lsn: String,
    pub active: bool,
    pub lag_in_mb: f32,
    #[serde(rename = "confirmedFlushLSN")]
    pub confirmed_flush_lsn: String,
    pub wal_status: String,
}

#[derive(Serialize)]
pub struct PeerListItem<'a> {
    pub name: &'a str,
    #[serde(rename = "type")]
    pub db_type: &'a str,
}

/// Mask `peerdb_redacted` string fields anywhere in a stored peer config;
/// PeerDB masks with literal `********`
pub fn redact(value: &mut Value) {
    const REDACTED: &[&str] = &[
        "password",
        "rootCa",
        "root_ca",
        "accessKeyId",
        "access_key_id",
        "secretAccessKey",
        "secret_access_key",
        "certificate",
        "privateKey",
        "private_key",
        "subscriptionId",
        "subscription_id",
        "apiKey",
        "api_key",
    ];
    match value {
        Value::Object(map) => {
            for (k, v) in map.iter_mut() {
                if REDACTED.contains(&k.as_str()) {
                    if let Value::String(s) = v
                        && !s.is_empty()
                    {
                        *s = "********".into();
                    }
                } else {
                    redact(v);
                }
            }
        }
        Value::Array(items) => items.iter_mut().for_each(redact),
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tolerant_decode_defaults_and_unknowns() {
        let req: CreateCDCFlowRequest = serde_json::from_str(
            r#"{
                "connectionConfigs": {
                    "flow_job_name": "m1",
                    "tableMappings": [
                        {"sourceTableIdentifier": "public.users", "futureKnob": 7}
                    ],
                    "sourceName": "pg",
                    "destinationName": "ch",
                    "idleTimeoutSeconds": "60"
                },
                "someFutureField": {"nested": true}
            }"#,
        )
        .unwrap();
        let cfg = req.connection_configs.unwrap();
        assert_eq!(cfg.flow_job_name, "m1");
        assert_eq!(cfg.idle_timeout_seconds, 60);
        assert_eq!(
            cfg.table_mappings[0].source_table_identifier,
            "public.users"
        );
        assert!(!cfg.do_initial_snapshot);
        assert!(!req.attach_to_existing);
    }

    #[test]
    fn enums_accept_names_and_numbers() {
        let by_name: FlowStatus = serde_json::from_str("\"STATUS_PAUSED\"").unwrap();
        assert_eq!(by_name, FlowStatus::Paused);
        let by_number: FlowStatus = serde_json::from_str("2").unwrap();
        assert_eq!(by_number, FlowStatus::Paused);
        let unknown: FlowStatus = serde_json::from_str("\"STATUS_FROM_THE_FUTURE\"").unwrap();
        assert_eq!(unknown, FlowStatus::Unknown);
        assert_eq!(
            serde_json::to_string(&FlowStatus::Running).unwrap(),
            "\"STATUS_RUNNING\""
        );

        let pg: DbType = serde_json::from_str("3").unwrap();
        assert_eq!(pg, DbType::Postgres);
        let ch: DbType = serde_json::from_str("\"CLICKHOUSE\"").unwrap();
        assert_eq!(ch, DbType::Clickhouse);
    }

    #[test]
    fn peer_decode_both_casings() {
        let p: Peer = serde_json::from_str(
            r#"{"name": "pg", "type": "POSTGRES",
                "postgres_config": {"host": "db", "port": 5432, "requireTls": true}}"#,
        )
        .unwrap();
        let cfg = p.postgres_config.unwrap();
        assert_eq!(cfg.host, "db");
        assert_eq!(cfg.sslmode(), "require");
    }

    #[test]
    fn redact_masks_nested_secrets() {
        let mut v = serde_json::json!({
            "postgresConfig": {"host": "db", "password": "hunter2", "rootCa": ""}
        });
        redact(&mut v);
        assert_eq!(v["postgresConfig"]["password"], "********");
        assert_eq!(v["postgresConfig"]["rootCa"], "");
        assert_eq!(v["postgresConfig"]["host"], "db");
    }
}
