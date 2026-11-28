//! `config_table.initial_load` values the backfiller never runs: `none` and
//! an unknown mode stream from the opt-in only, and a forward-declared opt-in needs no
//! load because its relation is born after the declaration. Opt-ins this
//! process cannot serve leave their tables out of scope

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
         INSERT INTO app.pre VALUES (1, 'before');\n\
         CREATE TABLE app.skip (id bigint PRIMARY KEY, body text);\n\
         INSERT INTO app.skip VALUES (1, 'before');\n"
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
             VALUES ('app', 'pre', true, 'bogus'), ('app', 'later', true, 'copy'), \
             ('app', 'skip', true, 'none')"
                .into(),
            "INSERT INTO app.pre VALUES (2, 'after')".into(),
            "INSERT INTO app.skip VALUES (2, 'after')".into(),
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
        "SELECT groupArray(body) FROM (SELECT body FROM walshadow_test.app_skip FINAL ORDER BY id)",
        "['after']",
        "none skips the load",
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

/// Rows refused at opt-in create no destination: a backup load in a database
/// the cluster backup does not cover, a TOAST heap shadow never holds
/// (unlogged, so no WAL create admits it), and a row without `replicate`
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unservable_opt_ins_stay_out_of_scope() {
    if !fx::tools::requirements_available() {
        return;
    }
    let slot = fx::Ports::alloc();
    let tmp = tempfile::tempdir().unwrap();
    let schema_sql = format!(
        "{INSTALL_SQL}\n\
         CREATE SCHEMA app;\n\
         CREATE TABLE app.pre (id bigint PRIMARY KEY, body text);\n\
         CREATE TABLE app.held (id bigint PRIMARY KEY);\n\
         CREATE UNLOGGED TABLE app.cache (id bigint PRIMARY KEY, body text);\n\
         CREATE TABLE app.plain (id bigint PRIMARY KEY);\n"
    );
    let (
        fx::BootstrappedClusters {
            source,
            shadow,
            shadow_filter_dir,
        },
        shadow_stream_state,
    ) = fx::bootstrap_clusters_with_bridge(
        &tmp,
        &schema_sql,
        slot.source,
        slot.shadow,
        slot.walsender,
    )
    .await;
    let _src_stop = fx::StopOnDrop { sh: &source };
    let _shd_stop = fx::StopOnDrop { sh: &shadow };
    let ch = fx::ChServer::spawn(tempfile::tempdir().unwrap(), slot.ch_tcp, slot.ch_http)
        .expect("spawn ch");
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create db");
    let bridge = walshadow::bridge::connect_with_budget(
        shadow.bridge_socket().expect("shadow bridge configured"),
        1,
        Duration::from_secs(20),
    )
    .await
    .expect("dial shadow bridge");
    let oracle = std::sync::Arc::new(walshadow::oracle::Oracle::new(std::sync::Arc::new(bridge)));
    let mut pipeline = fx::build_pipeline_tuned(
        fx::BuildPipelineArgs {
            tmp: &tmp,
            source: &source,
            shadow: &shadow,
            shadow_filter_dir: &shadow_filter_dir,
            shadow_stream_state,
            ch_database: "walshadow_test",
            ch_tcp_port: slot.ch_tcp,
            mappings: vec![],
            app_name: "walshadow-opt-in-refusals",
            ddl: Some(fx::DdlPipelineArgs {
                config_schema: Some("walshadow".into()),
                non_primary: true,
                ..Default::default()
            }),
        },
        |cfg| {
            cfg.toast.mode = walshadow::emitter_config::ToastMode::Shadow;
            cfg.replicate_all = false;
        },
        Some(oracle),
    )
    .await;

    let driver = fx::spawn_workload(
        &source,
        vec![
            "INSERT INTO walshadow.config_table (namespace, relname, replicate, initial_load) \
             VALUES ('app', 'held', true, 'base_backup'), ('app', 'cache', true, NULL), \
             ('app', 'plain', NULL, NULL), ('app', 'pre', true, NULL)"
                .into(),
            "INSERT INTO app.pre VALUES (1, 'after')".into(),
            "SELECT pg_switch_wal()".into(),
        ],
    );
    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(45)).await;
    let _ = driver.join();
    assert!(shipped >= 1, "no segments shipped in 45s");
    fx::wait_query(
        &ch,
        "SELECT groupArray(body) FROM walshadow_test.app_pre FINAL",
        "['after']",
        "served opt-in streams",
    )
    .await;
    let tables = ch
        .query("SELECT groupArray(name) FROM system.tables WHERE database = 'walshadow_test'")
        .expect("list tables");
    assert_eq!(tables, "['app_pre']");
    pipeline.shutdown().await.expect("pipeline drains clean");
}
