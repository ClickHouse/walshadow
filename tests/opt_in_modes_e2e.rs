//! `config_table.initial_load` values the backfiller never runs: an unknown
//! mode streams from the opt-in only, and a forward-declared opt-in needs no
//! load because its relation is born after the declaration

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use std::time::Duration;

const INSTALL_SQL: &str = include_str!("../sql/runtime_config_install.sql");

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unloadable_initial_load_modes_stream_from_opt_in() {
    if !fx::tools::requirements_available() {
        return;
    }
    let slot = fx::Ports::alloc();
    let tmp = tempfile::tempdir().unwrap();
    let schema_sql = format!(
        "{INSTALL_SQL}\n\
         CREATE SCHEMA app;\n\
         CREATE TABLE app.pre (id bigint PRIMARY KEY, body text);\n\
         INSERT INTO app.pre VALUES (1, 'before');\n"
    );
    let (
        fx::BootstrappedClusters {
            source,
            shadow,
            shadow_filter_dir,
        },
        shadow_stream_state,
    ) = fx::bootstrap_clusters(&tmp, &schema_sql, slot.source, slot.shadow, slot.walsender).await;
    let _src_stop = fx::StopOnDrop { sh: &source };
    let _shd_stop = fx::StopOnDrop { sh: &shadow };
    let ch = fx::ChServer::spawn(tempfile::tempdir().unwrap(), slot.ch_tcp, slot.ch_http)
        .expect("spawn ch");
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create db");
    let mut pipeline = fx::build_pipeline(fx::BuildPipelineArgs {
        tmp: &tmp,
        source: &source,
        shadow: &shadow,
        shadow_filter_dir: &shadow_filter_dir,
        shadow_stream_state,
        ch_database: "walshadow_test",
        ch_tcp_port: slot.ch_tcp,
        mappings: vec![],
        app_name: "walshadow-opt-in-modes",
        ddl: Some(fx::DdlPipelineArgs {
            config_schema: Some("walshadow".into()),
            ..Default::default()
        }),
    })
    .await;

    let driver = fx::spawn_workload(
        &source,
        vec![
            "INSERT INTO walshadow.config_table (namespace, relname, replicate, initial_load) \
             VALUES ('app', 'pre', true, 'bogus'), ('app', 'later', true, 'copy')"
                .into(),
            "INSERT INTO app.pre VALUES (2, 'after')".into(),
            "CREATE TABLE app.later (id bigint PRIMARY KEY, body text)".into(),
            "INSERT INTO app.later VALUES (1, 'born')".into(),
            "SELECT pg_switch_wal()".into(),
        ],
    );
    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(45)).await;
    let _ = driver.join();
    assert!(shipped >= 1, "no segments shipped in 45s");
    fx::wait_query(
        &ch,
        "SELECT groupArray(body) FROM (SELECT body FROM walshadow_test.app_pre FINAL ORDER BY id)",
        "['after']",
        "unknown mode skips the load",
    )
    .await;
    fx::wait_query(
        &ch,
        "SELECT groupArray(body) FROM walshadow_test.app_later FINAL",
        "['born']",
        "forward declaration materialises",
    )
    .await;
    pipeline.shutdown().await.expect("pipeline drains clean");
}
