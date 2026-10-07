//! Tee end-to-end: one source table feeds a `ReplacingMergeTree` primary, a
//! `MergeTree` audit log, and a replica in another database. Rows, deletes
//! and `ADD COLUMN` must reach all three.

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use std::time::Duration;

use walshadow::mapping::{NamespaceMapping, Tee};
use walshadow::schema::RelName;
use walshadow::table_rules::{MatchKind, TableRule};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tee_receives_rows_and_ddl() {
    if !fx::tools::requirements_available() {
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
            auto_create_name: Some(walshadow::mapping::NameTemplate::parse("$table$").unwrap()),
            drop_table_strategy: None,
            initial_load: None,
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
            app_name: "walshadow-tee",
            ddl: Some(ddl_args),
        },
        |cfg| {
            cfg.table_entries.push((
                RelName::new("sc", "t"),
                MatchKind::Exact,
                TableRule {
                    tee: Some(vec![
                        Tee {
                            table: "t_audit".into(),
                            engine: Some("MergeTree".into()),
                            order_by: vec!["id".into(), "_lsn".into()],
                            ..Tee::default()
                        },
                        Tee {
                            database: Some("walshadow_replica".into()),
                            table: "t".into(),
                            ..Tee::default()
                        },
                    ]),
                    ..TableRule::default()
                },
            ));
        },
    )
    .await;

    let driver = fx::spawn_workload(
        &source,
        vec![
            "CREATE TABLE sc.t (id bigint PRIMARY KEY, body text)".into(),
            "INSERT INTO sc.t VALUES (1, 'a'), (2, 'b')".into(),
            "UPDATE sc.t SET body = 'a2' WHERE id = 1".into(),
            "ALTER TABLE sc.t ADD COLUMN note text".into(),
            "INSERT INTO sc.t VALUES (3, 'c', 'n')".into(),
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

    let live = "SELECT arrayStringConcat(groupArray(concat(toString(id), ':', body, ':', \
                ifNull(note, '-'))), ',') FROM (SELECT * FROM {} FINAL ORDER BY id)";
    for table in ["walshadow_test.t", "walshadow_replica.t"] {
        assert_eq!(
            ch.query(&live.replace("{}", table)).expect("live rows"),
            "1:a2:-,3:c:n",
            "{table}"
        );
    }

    let audit = ch
        .query(
            "SELECT arrayStringConcat(groupArray(concat(toString(id), ':', ifNull(body, ''), ':', \
             toString(_is_deleted))), ',') FROM \
             (SELECT * FROM walshadow_test.t_audit ORDER BY _lsn, id)",
        )
        .expect("audit rows");
    assert_eq!(
        audit, "1:a:false,2:b:false,1:a2:false,3:c:false,2::true",
        "every version kept"
    );
    let ddl = ch
        .query("SHOW CREATE TABLE walshadow_test.t_audit")
        .expect("show create");
    assert!(ddl.contains("ENGINE = MergeTree"), "{ddl}");
    assert!(ddl.contains("ORDER BY (id, _lsn)"), "{ddl}");
    assert_eq!(
        ch.query(
            "SELECT count() FROM system.columns WHERE name = 'note' \
             AND (database, table) IN (('walshadow_test', 't'), \
             ('walshadow_test', 't_audit'), ('walshadow_replica', 't'))"
        )
        .expect("note columns"),
        "3",
        "ADD COLUMN reached every target"
    );
}
