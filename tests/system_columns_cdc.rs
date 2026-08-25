//! Destination-shape config end-to-end: renamed system columns, no delete
//! marker, operator `ORDER BY` + `PRIMARY KEY`.
//!
//! One drill: namespace `auto_create` renders the `CREATE TABLE`, so the
//! rendered shape and the INSERT contract must agree — a mismatch fails on the
//! first row, not in a unit test's string compare. The DELETE has nowhere to
//! land without a marker column, so CH keeps the row source no longer has.

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use std::sync::Arc;
use std::time::Duration;

use walshadow::mapping::{NamespaceMapping, SystemColumns};
use walshadow::schema::RelName;
use walshadow::table_rules::{MatchKind, TableRule};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn renamed_system_columns_and_operator_keys() {
    if !fx::pg_available() || !fx::pg_basebackup_available() || !fx::clickhouse_available() {
        eprintln!("skip: missing initdb / pg_basebackup / clickhouse");
        return;
    }

    let slot = fx::Ports::alloc();
    let tmp = tempfile::tempdir().unwrap();
    let (
        fx::BootstrappedClusters {
            source,
            shadow,
            shadow_filter_dir,
        },
        shadow_stream_state,
    ) = fx::bootstrap_clusters(
        &tmp,
        "CREATE SCHEMA sc;\n",
        slot.source,
        slot.shadow,
        slot.walsender,
    )
    .await;
    let _src_stop = fx::StopOnDrop { sh: &source };
    let _shd_stop = fx::StopOnDrop { sh: &shadow };

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create db");

    let mut ddl_args = fx::DdlPipelineArgs::default();
    ddl_args.namespaces.insert(
        "sc".into(),
        NamespaceMapping {
            target_database: Some("walshadow_test".into()),
            auto_create: true,
            drop_table_strategy: None,
        },
    );

    let mut pipeline = fx::build_pipeline_with(
        fx::BuildPipelineArgs {
            tmp: &tmp,
            source: &source,
            shadow: &shadow,
            shadow_filter_dir: &shadow_filter_dir,
            shadow_stream_state,
            ch_database: "walshadow_test",
            ch_tcp_port: slot.ch_tcp,
            mappings: vec![],
            app_name: "walshadow-system-columns",
            ddl: Some(ddl_args),
        },
        |cfg| {
            cfg.system_columns = Arc::new(SystemColumns {
                lsn: "_peerdb_version".into(),
                xid: "_x".into(),
                commit_ts: "_peerdb_synced_at".into(),
                is_deleted: None,
            });
            // Reverse of the declared PK, so the key can only come from config
            cfg.table_entries.push((
                RelName::new("sc", "t"),
                MatchKind::Exact,
                TableRule {
                    order_by: Some(vec!["tenant".into(), "id".into()]),
                    primary_key: Some(vec!["tenant".into()]),
                    ..TableRule::default()
                },
            ));
        },
    )
    .await;
    let stats = pipeline.stats.clone();

    let driver = fx::spawn_workload(
        &source,
        vec![
            "CREATE TABLE sc.t (id bigint, tenant bigint, body text, PRIMARY KEY (id, tenant))"
                .into(),
            "INSERT INTO sc.t (id, tenant, body) VALUES (1, 7, 'a'), (2, 7, 'b')".into(),
            "DELETE FROM sc.t WHERE id = 2".into(),
            "SELECT pg_switch_wal()".into(),
        ],
    );

    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(60)).await;
    let _ = driver.join();
    assert!(shipped >= 1, "no segments shipped in 60s");

    let target = pipeline.stream.dispatched_lsn();
    let observed = shadow
        .wait_for_replay(target, Duration::from_secs(30))
        .expect("shadow replay");
    assert!(observed >= target);
    pipeline.shutdown().await.expect("pipeline drains clean");

    let ddl = ch
        .query("SHOW CREATE TABLE walshadow_test.t")
        .expect("show create");
    assert!(ddl.contains("ReplacingMergeTree(_peerdb_version)"), "{ddl}");
    assert!(ddl.contains("ORDER BY (tenant, id)"), "{ddl}");
    assert!(ddl.contains("PRIMARY KEY (tenant)"), "{ddl}");

    let cols = ch
        .query(
            "SELECT arrayStringConcat(groupArray(name), ',') FROM \
             (SELECT name FROM system.columns \
              WHERE database = 'walshadow_test' AND table = 't' AND name LIKE '\\_%' \
              ORDER BY position)",
        )
        .expect("system.columns");
    assert_eq!(cols, "_peerdb_version,_x,_peerdb_synced_at", "{cols}");

    // Source deleted id=2; with no marker column that row has no
    // representation, so it stays on CH and the drop is counted
    assert_eq!(
        source.psql_one("SELECT count(*) FROM sc.t").unwrap(),
        "1",
        "source row deleted"
    );
    let ch_count = ch
        .query("SELECT count() FROM walshadow_test.t FINAL")
        .expect("ch count");
    assert_eq!(ch_count, "2", "delete row dropped, insert retained");
    assert_eq!(
        stats
            .deletes_discarded
            .load(std::sync::atomic::Ordering::Relaxed),
        1,
        "one DELETE discarded",
    );
}
