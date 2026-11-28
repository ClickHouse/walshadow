//! ClickHouse failures inside a reorder barrier fail the pipeline, naming
//! the step that failed

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use std::num::NonZeroUsize;
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use walshadow::mapping::{NameTemplate, NamespaceMapping};
use walshadow::toast_retire::RETIRE_LEDGER_FILENAME;

const INSTALL_SQL: &str = include_str!("../sql/runtime_config_install.sql");

/// `app.t` with out-of-line TOAST, auto-created on CH at boot. `other.u`
/// comes into scope only through a config row
const SCHEMA: &str = "CREATE SCHEMA app;\n\
    CREATE TABLE app.t (id bigint PRIMARY KEY, body text);\n\
    ALTER TABLE app.t ALTER body SET STORAGE EXTERNAL;\n\
    CREATE SCHEMA other;\n\
    CREATE TABLE other.u (id bigint PRIMARY KEY);\n";

/// What a sabotage step can break once the pipeline booted
struct Env<'a> {
    ch: &'a fx::ChServer,
    source: &'a fx::ClusterGuard,
    shadow: &'a fx::ClusterGuard,
    spill_dir: &'a Path,
}

/// Boot, `sabotage`, stream `stmts`, return the error they raise
async fn fail_after(app_name: &str, sabotage: impl FnOnce(&Env), stmts: &[&str]) -> String {
    fail_after_tuned(app_name, |_| {}, sabotage, stmts).await.0
}

/// [`fail_after`] with final say over the emitter config, also returning
/// the pipeline's counters
async fn fail_after_tuned(
    app_name: &str,
    tune: impl FnOnce(&mut walshadow::emitter_config::EmitterConfig),
    sabotage: impl FnOnce(&Env),
    stmts: &[&str],
) -> (String, Arc<walshadow::emit::stats::EmitterStats>) {
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
        &format!("{INSTALL_SQL}\n{SCHEMA}"),
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
    let mut ddl = fx::DdlPipelineArgs {
        config_schema: Some("walshadow".into()),
        ..Default::default()
    };
    ddl.namespaces.insert(
        "app".into(),
        NamespaceMapping {
            target_database: Some("walshadow_test".into()),
            auto_create: true,
            auto_create_name: Some(NameTemplate::parse("$table$").unwrap()),
            ..Default::default()
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
            mappings: Vec::new(),
            app_name,
            ddl: Some(ddl),
        },
        |cfg| {
            // Fail on first exception, no retry backoff
            cfg.retry.max_attempts = 0;
            // Mirror puts write through instead of waiting for a barrier
            cfg.toast.put_batch_rows = NonZeroUsize::new(1);
            tune(cfg);
        },
    )
    .await;
    let stats = pipeline.stats.clone();
    sabotage(&Env {
        ch: &ch,
        source: &source,
        shadow: &shadow,
        spill_dir: &tmp.path().join("spill"),
    });

    let mut stmts: Vec<String> = stmts.iter().map(|s| (*s).into()).collect();
    stmts.push("SELECT pg_switch_wal()".into());
    let driver = fx::spawn_workload(&source, stmts);
    let pumped = fx::pump_segments_res(&mut pipeline, 1, Duration::from_secs(45)).await;
    let _ = driver.join();
    let err = match pumped {
        Err(e) => e,
        Ok(_) => pipeline
            .shutdown()
            .await
            .expect_err("barrier against sabotaged CH succeeded"),
    };
    (err, stats)
}

fn drop_dest(env: &Env) {
    env.ch
        .query("DROP TABLE walshadow_test.t")
        .expect("drop dest");
}

/// Mirror name taken by a view: `CREATE TABLE IF NOT EXISTS` no-ops, writes fail
fn view_over_mirror(env: &Env) {
    let mirror = env
        .source
        .psql_one(
            "SELECT 'pg_toast_' || d.oid || '_' || c.reltoastrelid FROM pg_class c, pg_database d \
             WHERE c.oid = 'app.t'::regclass AND d.datname = current_database()",
        )
        .expect("mirror name");
    env.ch
        .query(&format!(
            "CREATE VIEW walshadow_test.{mirror} AS SELECT 1 AS x"
        ))
        .expect("view over mirror");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncate_on_missing_destination_fails() {
    if !fx::tools::requirements_available() {
        return;
    }
    let err = fail_after("walshadow-fault-truncate", drop_dest, &["TRUNCATE app.t"]).await;
    assert!(err.contains("ch truncate"), "{err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn alter_on_missing_destination_fails() {
    if !fx::tools::requirements_available() {
        return;
    }
    let err = fail_after(
        "walshadow-fault-alter",
        drop_dest,
        &["ALTER TABLE app.t ADD COLUMN v int"],
    )
    .await;
    assert!(err.contains("ddl apply"), "{err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn toast_put_into_unwritable_mirror_fails() {
    if !fx::tools::requirements_available() {
        return;
    }
    let err = fail_after(
        "walshadow-fault-toast-put",
        view_over_mirror,
        &["INSERT INTO app.t VALUES (1, repeat('x', 100000))"],
    )
    .await;
    assert!(err.contains("toast store put"), "{err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn truncate_of_unwritable_mirror_fails() {
    if !fx::tools::requirements_available() {
        return;
    }
    let err = fail_after(
        "walshadow-fault-toast-truncate",
        view_over_mirror,
        &["TRUNCATE app.t"],
    )
    .await;
    assert!(err.contains("toast mirror truncate"), "{err}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rewrite_over_unwritable_mirror_fails() {
    if !fx::tools::requirements_available() {
        return;
    }
    let err = fail_after(
        "walshadow-fault-toast-rewrite",
        view_over_mirror,
        &["VACUUM FULL app.t"],
    )
    .await;
    assert!(err.contains("toast rewrite barrier"), "{err}");
}

/// Directory where the retire ledger renames its file in
fn block_retire_ledger(env: &Env) {
    std::fs::create_dir_all(env.spill_dir.join(RETIRE_LEDGER_FILENAME).join("blocker"))
        .expect("block ledger");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn drop_with_unwritable_retire_ledger_fails() {
    if !fx::tools::requirements_available() {
        return;
    }
    let err = fail_after(
        "walshadow-fault-retire-ledger",
        block_retire_ledger,
        &["DROP TABLE app.t"],
    )
    .await;
    assert!(err.contains("toast retire ledger"), "{err}");
}

fn drop_ch_database(env: &Env) {
    env.ch
        .query("DROP DATABASE walshadow_test")
        .expect("drop ch database");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn config_opt_in_without_ch_database_fails() {
    if !fx::tools::requirements_available() {
        return;
    }
    let err = fail_after(
        "walshadow-fault-opt-in",
        drop_ch_database,
        &[
            "INSERT INTO walshadow.config_table (namespace, relname, replicate, initial_load) \
             VALUES ('other', 'u', true, 'none')",
        ],
    )
    .await;
    assert!(err.contains("opt-in: "), "{err}");
}

/// Plan past its in-memory cap spills to a file the disk budget refuses
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn plan_over_disk_budget_fails() {
    if !fx::tools::requirements_available() {
        return;
    }
    let (err, stats) = fail_after_tuned(
        "walshadow-fault-plan-spool",
        |cfg| cfg.plan_disk_max = 4096,
        |_| {},
        &[
            "CREATE TABLE app.w (id int PRIMARY KEY, body text STORAGE PLAIN)",
            "INSERT INTO app.w SELECT g, repeat('x', 8000) FROM generate_series(1, 150) g",
        ],
    )
    .await;
    assert!(err.contains("plan seal: plan spool"), "{err}");
    assert!(err.contains("exceeds disk budget 4096"), "{err}");
    assert_eq!(stats.plan_failures_spool.load(Ordering::Relaxed), 1);
}

/// Opt-in resolves its descriptor through the shadow catalog
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn config_opt_in_without_shadow_fails() {
    if !fx::tools::requirements_available() {
        return;
    }
    let err = fail_after(
        "walshadow-fault-opt-in-shadow",
        |env| env.shadow.stop().expect("stop shadow"),
        &[
            "INSERT INTO walshadow.config_table (namespace, relname, replicate, initial_load) \
             VALUES ('other', 'u', true, 'none')",
        ],
    )
    .await;
    assert!(err.contains("opt-in: catalog: "), "{err}");
}
