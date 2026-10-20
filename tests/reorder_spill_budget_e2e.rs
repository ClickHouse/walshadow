//! Reorder commit under shrunken resource caps: a transaction larger than
//! the xact buffer spills to disk and drains back through the reorder sink,
//! its plan dispatching in drain-slice segments that outgrow a small
//! admission pool. Asserts every row lands, admission waited on the pool,
//! and the commit unlinked its spill file.

#![cfg(target_os = "linux")]

#[path = "common/inproc_harness.rs"]
mod fx;

use std::time::Duration;

use walshadow::budget::MemoryBudget;
use walshadow::mapping::{ColumnMapping, TableTarget};
use walshadow::schema::RelName;

const ROWS: usize = 400;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spilled_commit_waits_on_admission_and_unlinks_spill() {
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
        "CREATE TABLE public.foo (id int PRIMARY KEY, val text);\n",
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
    ch.query(
        "CREATE OR REPLACE TABLE walshadow_test.foo (\
            id Int32,\
            val Nullable(String),\
            _lsn UInt64,\
            _xid UInt32,\
            _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool\
         ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY id",
    )
    .expect("create dest table");

    let mappings = vec![fx::TableMappingSpec {
        source_table: RelName::new("public", "foo"),
        target_table: TableTarget::new("walshadow_test", "foo"),
        columns: vec![
            ColumnMapping {
                src_attnum: 1,
                target_name: "id".into(),
                target_type: "Int32".into(),
                type_pinned: false,
            },
            ColumnMapping {
                src_attnum: 2,
                target_name: "val".into(),
                target_type: "Nullable(String)".into(),
                type_pinned: false,
            },
        ],
    }];

    // ~200 B rows: 40-row segments fit the pool alone, never two at once
    let budget = MemoryBudget::new(16 << 10);
    let mut pipeline = fx::build_pipeline_knobs(
        fx::BuildPipelineArgs {
            tmp: &tmp,
            source: &source,
            shadow: &shadow,
            shadow_filter_dir: &shadow_filter_dir,
            shadow_stream_state,
            ch_database: "walshadow_test",
            ch_tcp_port: slot.ch_tcp,
            mappings,
            app_name: "walshadow-reorder-spill-budget",
            ddl: None,
        },
        |c| {
            c.replicate_all = false;
            c.drain_batch_rows = 40;
            c.decode_chunk_rows = 40;
        },
        fx::PipelineKnobs {
            xact_buffer_max: Some(16 << 10),
            budget: Some(budget.clone()),
        },
    )
    .await;

    let driver = fx::spawn_workload(
        &source,
        vec![
            format!(
                "INSERT INTO public.foo \
                 SELECT g, repeat(md5(g::text), 6) FROM generate_series(1, {ROWS}) g"
            ),
            "SELECT pg_switch_wal()".into(),
        ],
    );
    let shipped = fx::pump_segments(&mut pipeline, 1, Duration::from_secs(20)).await;
    let _ = driver.join();
    assert!(shipped >= 1, "no segment shipped in 20s");

    let spill_evictions = pipeline
        .xact_buffer
        .lock()
        .await
        .stats()
        .spill_evictions_total;
    pipeline.shutdown().await.expect("pipeline drains clean");

    assert!(spill_evictions > 0, "transaction never spilled");
    assert!(
        budget.waits_total() > 0,
        "admission never waited on the pool"
    );
    assert_eq!(budget.resident_bytes(), 0, "every permit released");
    let leftover: Vec<_> = std::fs::read_dir(walshadow::fs::scratch_dir(&tmp.path().join("spill")))
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|n| n.starts_with("xid-"))
        .collect();
    assert!(leftover.is_empty(), "spill files left behind: {leftover:?}");

    let want = source
        .psql_one("SELECT count(*) || ' ' || sum(length(val)) FROM public.foo")
        .expect("source digest");
    let got = ch
        .query(
            "SELECT toString(count()) || ' ' || toString(sum(length(val))) \
             FROM walshadow_test.foo FINAL",
        )
        .expect("ch digest");
    assert_eq!(got, want);
    assert_eq!(want, format!("{ROWS} {}", ROWS * 192));
}
