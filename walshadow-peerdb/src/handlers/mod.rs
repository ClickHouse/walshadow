pub mod mirrors;
pub mod misc;
pub mod peers;

use serde::de::DeserializeOwned;
use serde_json::Value as JsonValue;
use toml::{Table, Value};

use crate::error::GrpcError;
use crate::model::{ClickhouseConfig, FlowStatus, PostgresConfig, TableMapping};
use crate::warn::warn_ignored;

pub fn parse_body<T: DeserializeOwned>(v: JsonValue) -> Result<T, GrpcError> {
    serde_json::from_value(v).map_err(|e| GrpcError::invalid(format!("malformed request: {e}")))
}

/// Wrap a section table under its config key, ready to `apply`
fn section(key: &str, table: Table) -> Table {
    let mut root = Table::new();
    root.insert(key.into(), Value::Table(table));
    root
}

/// `[source]` apply fragment for a submitted Postgres peer config
pub fn source_fragment(cfg: &PostgresConfig) -> Table {
    let port = if cfg.port == 0 { 5432 } else { cfg.port };
    let mut src = Table::new();
    src.insert("host".into(), cfg.host.clone().into());
    src.insert("port".into(), i64::from(port).into());
    src.insert("dbname".into(), cfg.database.clone().into());
    src.insert("user".into(), cfg.user.clone().into());
    src.insert("password".into(), cfg.password.clone().into());
    src.insert("sslmode".into(), cfg.sslmode().into());
    section("source", src)
}

/// `[ch]` apply fragment for a submitted ClickHouse peer config
pub fn dest_fragment(cfg: &ClickhouseConfig) -> Table {
    let port = if cfg.port == 0 { 9000 } else { cfg.port };
    let mut ch = Table::new();
    ch.insert("host".into(), cfg.host.clone().into());
    ch.insert("port".into(), i64::from(port).into());
    ch.insert("database".into(), cfg.database.clone().into());
    ch.insert("user".into(), cfg.user.clone().into());
    ch.insert("password".into(), cfg.password.clone().into());
    ch.insert("secure".into(), (!cfg.disable_tls).into());
    section("ch", ch)
}

pub fn warn_pg_ignored(cfg: &PostgresConfig) {
    if cfg
        .metadata_schema
        .as_deref()
        .is_some_and(|s| !s.is_empty())
    {
        warn_ignored(
            "postgresConfig.metadataSchema",
            "walshadow keeps no catalog metadata schema on source",
        );
    }
    if cfg.ssh_config.is_some() {
        warn_ignored("postgresConfig.sshConfig", "no SSH tunnel support");
    }
    if cfg.root_ca.as_deref().is_some_and(|s| !s.is_empty()) {
        warn_ignored(
            "postgresConfig.rootCa",
            "custom CA not forwarded to control",
        );
    }
    if !cfg.tls_host.is_empty() {
        warn_ignored("postgresConfig.tlsHost", "tls host override not forwarded");
    }
    if cfg.skip_cert_verification {
        warn_ignored(
            "postgresConfig.skipCertVerification",
            "not forwarded; sslmode covers verification level",
        );
    }
    if cfg.aws_auth.is_some() {
        warn_ignored(
            "postgresConfig.awsAuth",
            "IAM auth unsupported, password auth only",
        );
    }
    if cfg
        .auth_type
        .as_ref()
        .is_some_and(|v| *v != 0 && *v != "POSTGRES_PASSWORD")
    {
        warn_ignored("postgresConfig.authType", "password auth only");
    }
}

pub fn warn_ch_ignored(cfg: &ClickhouseConfig) {
    if !cfg.s3_path.is_empty()
        || cfg.s3.is_some()
        || !cfg.access_key_id.is_empty()
        || !cfg.secret_access_key.is_empty()
        || !cfg.region.is_empty()
    {
        warn_ignored(
            "clickhouseConfig.s3",
            "walshadow inserts over native protocol, no S3 staging",
        );
    }
    if cfg.endpoint.as_deref().is_some_and(|s| !s.is_empty()) {
        warn_ignored(
            "clickhouseConfig.endpoint",
            "endpoint override not forwarded",
        );
    }
    if cfg.certificate.is_some()
        || cfg.private_key.is_some()
        || cfg.tls_certificate_directory.is_some()
    {
        warn_ignored(
            "clickhouseConfig.clientCert",
            "client certificates not forwarded",
        );
    }
    if cfg.root_ca.as_deref().is_some_and(|s| !s.is_empty()) {
        warn_ignored("clickhouseConfig.rootCa", "custom CA not forwarded");
    }
    if !cfg.tls_host.is_empty() {
        warn_ignored(
            "clickhouseConfig.tlsHost",
            "tls host override not forwarded",
        );
    }
    if !cfg.cluster.is_empty() || cfg.replicated {
        warn_ignored(
            "clickhouseConfig.cluster",
            "cluster/replicated DDL not driven by walshadow",
        );
    }
}

pub fn warn_flow_ignored(cfg: &crate::model::FlowConnectionConfigs) {
    if !cfg.publication_name.is_empty() {
        warn_ignored(
            "publicationName",
            "walshadow consumes physical WAL, publications don't exist in the model",
        );
    }
    if !cfg.replication_slot_name.is_empty() {
        warn_ignored("replicationSlotName", "physical slot managed by walshadow");
    }
    if !cfg.soft_delete_col_name.is_empty() {
        warn_ignored(
            "softDeleteColName",
            "destination shape is walshadow's _lsn convergence model",
        );
    }
    if !cfg.synced_at_col_name.is_empty() {
        warn_ignored(
            "syncedAtColName",
            "destination shape is walshadow's _lsn convergence model",
        );
    }
    if cfg.snapshot_num_rows_per_partition != 0
        || cfg.snapshot_num_partitions_override != 0
        || cfg.snapshot_max_parallel_workers != 0
        || cfg.snapshot_num_tables_in_parallel != 0
        || !cfg.snapshot_staging_path.is_empty()
    {
        warn_ignored(
            "snapshotKnobs",
            "backfill partitioning/parallelism has no walshadow counterpart",
        );
    }
    if !cfg.cdc_staging_path.is_empty() {
        warn_ignored("cdcStagingPath", "no staging path in walshadow");
    }
    if !cfg.env.is_empty() {
        warn_ignored("env", "per-flow env not forwarded");
    }
    if !cfg.script.is_empty() {
        warn_ignored("script", "lua scripting unsupported");
    }
    if cfg.system.as_ref().is_some_and(|v| {
        !matches!(v, crate::pb::EnumToken::Number(0))
            && !matches!(v, crate::pb::EnumToken::Name(n) if n == "Q")
    }) {
        warn_ignored("system", "type system fixed to walshadow's PG→CH map");
    }
    if cfg.max_batch_size != 0 {
        warn_ignored(
            "maxBatchSize",
            "batching governed by walshadow emitter budgets",
        );
    }
    if cfg.idle_timeout_seconds != 0 {
        warn_ignored(
            "idleTimeoutSeconds",
            "batching governed by walshadow emitter budgets",
        );
    }
    if !cfg.do_initial_snapshot {
        warn_ignored(
            "doInitialSnapshot=false",
            "walshadow always backfills newly opted-in tables",
        );
    }
}

pub fn warn_mapping_ignored(m: &TableMapping) {
    if !m.exclude.is_empty() {
        warn_ignored(
            "tableMapping.exclude",
            "column exclusion pends runtime-config column overrides",
        );
    }
    if !m.columns.is_empty() {
        warn_ignored(
            "tableMapping.columns",
            "per-column settings pend runtime-config column overrides",
        );
    }
    if !m.engine_is_default() {
        warn_ignored(
            "tableMapping.engine",
            "destination engine fixed to ReplacingMergeTree",
        );
    }
    if !m.partition_key.is_empty()
        || !m.sharding_key.is_empty()
        || !m.policy_name.is_empty()
        || !m.partition_by_expr.is_empty()
    {
        warn_ignored(
            "tableMapping.partitioning",
            "partition/sharding/policy overrides pend runtime-config table overrides",
        );
    }
}

/// Source identifiers split into (namespace, relname) at ingress; dotted
/// strings exist only at control-line interpolation
pub fn split_identifier(id: &str) -> Result<(&str, &str), GrpcError> {
    id.split_once('.')
        .filter(|(ns, rel)| !ns.is_empty() && !rel.is_empty())
        .ok_or_else(|| {
            GrpcError::invalid(format!(
                "sourceTableIdentifier {id:?} must be namespace.relname"
            ))
        })
}

/// destinationTableIdentifier differing from source naming is rejected
/// until per-table target rename exists in runtime config; bare relname
/// and exact echo both count as matching
pub fn check_dest_identifier(m: &TableMapping) -> Result<(), GrpcError> {
    let src = &m.source_table_identifier;
    let dst = &m.destination_table_identifier;
    if dst.is_empty() || dst == src || Some(dst.as_str()) == src.split_once('.').map(|(_, rel)| rel)
    {
        return Ok(());
    }
    Err(GrpcError::unimplemented(format!(
        "destinationTableIdentifier {dst:?} differs from source {src:?}; per-table rename unsupported"
    )))
}

/// `status` reply → FlowStatus. Paused reflects the config `stream.paused`
/// flag; a pending backfill surfaces as SNAPSHOT. A live daemon always
/// answers running or paused; UNKNOWN is reserved for an unreachable one,
/// which the callers derive from a failed call
pub fn flow_status_from_status(status: &Table) -> FlowStatus {
    if status
        .get("paused")
        .and_then(Value::as_bool)
        .unwrap_or(false)
    {
        FlowStatus::Paused
    } else if status
        .get("backfills_pending")
        .and_then(Value::as_integer)
        .unwrap_or(0)
        > 0
    {
        FlowStatus::Snapshot
    } else {
        FlowStatus::Running
    }
}

pub fn rows_synced_from_status(status: &Table) -> i64 {
    status
        .get("rows_synced")
        .and_then(Value::as_integer)
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifier_split() {
        assert_eq!(
            split_identifier("public.users").unwrap(),
            ("public", "users")
        );
        assert!(split_identifier("users").is_err());
        assert!(split_identifier(".users").is_err());
        assert!(split_identifier("public.").is_err());
    }

    #[test]
    fn dest_identifier_rules() {
        let m = |src: &str, dst: &str| TableMapping {
            source_table_identifier: src.into(),
            destination_table_identifier: dst.into(),
            ..Default::default()
        };
        assert!(check_dest_identifier(&m("public.users", "")).is_ok());
        assert!(check_dest_identifier(&m("public.users", "public.users")).is_ok());
        assert!(check_dest_identifier(&m("public.users", "users")).is_ok());
        assert!(check_dest_identifier(&m("public.users", "renamed")).is_err());
    }

    #[test]
    fn status_mapping() {
        let status = |toml: &str| toml.parse::<Table>().unwrap();
        assert_eq!(
            flow_status_from_status(&status("paused = false")),
            FlowStatus::Running
        );
        assert_eq!(
            flow_status_from_status(&status("paused = false\nbackfills_pending = 2")),
            FlowStatus::Snapshot
        );
        assert_eq!(
            flow_status_from_status(&status("paused = true")),
            FlowStatus::Paused
        );
        // absent paused key degrades to running, not unknown
        assert_eq!(flow_status_from_status(&Table::new()), FlowStatus::Running);
        assert_eq!(rows_synced_from_status(&status("rows_synced = 77")), 77);
        assert_eq!(rows_synced_from_status(&Table::new()), 0);
    }
}
