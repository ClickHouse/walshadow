//! Reuse a TOAST value ID during backup. A hint-bit page image preserves old
//! chunks with versions before backup start, so backup rows read old values

#![cfg(target_os = "linux")]

#[path = "common/bootstrap_ch_fixture.rs"]
mod fx;

use std::io::Write;
use std::process::Command;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use walshadow::mapping::TableTarget;
use walshadow::schema::RelName;

const SCHEMA: &str = "s28";
/// Keep rows inline so new rows append near end of main heap
const FILLER_ROWS: i32 = 120_000;
const MAX_RATE_KIB: &str = "16384";
const LIVE_ID: i32 = 200_000;
/// 32-byte MD5 digests per old value
const DEAD_DIGESTS: u32 = 94;

/// Different chunk counts prevent reassembly; replay supplies replacement row
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reissued_value_id_of_another_size_inside_backup_window_converges() {
    reissued_value_id(160).await;
}

/// Equal sizes return old value; replay must replace it
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reissued_value_id_of_same_size_inside_backup_window_converges() {
    reissued_value_id(DEAD_DIGESTS).await;
}

async fn reissued_value_id(live_digests: u32) {
    if !fx::tools::requirements_available() {
        return;
    }

    let slot = fx::Ports::alloc();
    let tmp = tempfile::tempdir().unwrap();
    let source = fx::start_source(&tmp);
    let _src_stop = fx::StopOnDrop { sh: &source };

    let t = format!("{SCHEMA}.t");
    // EXTERNAL skips compression, so value length determines chunk count
    source
        .apply_schema_dump(&format!(
            "CREATE SCHEMA {SCHEMA};\n\
             CREATE TABLE {t} (id int4 PRIMARY KEY, name text NOT NULL) \
               WITH (autovacuum_enabled = false, toast.autovacuum_enabled = false);\n\
             ALTER TABLE {t} ALTER COLUMN name SET STORAGE EXTERNAL;\n\
             ALTER TABLE {t} REPLICA IDENTITY FULL;\n\
             INSERT INTO {t} SELECT g, repeat('f', 1000) FROM generate_series(1, {FILLER_ROWS}) g;\n\
             INSERT INTO {t} SELECT 0, string_agg(md5(g::text || 'dead'), '') \
               FROM generate_series(1, {DEAD_DIGESTS}) g;\n"
        ))
        .expect("load workload");
    let toast = source
        .psql_one(&format!(
            "SELECT reltoastrelid::regclass::text FROM pg_class \
             WHERE oid = '{t}'::regclass"
        ))
        .expect("toast relation")
        .trim()
        .to_string();
    let dead_id = source
        .psql_one(&format!("SELECT DISTINCT chunk_id FROM {toast}"))
        .expect("dead value id")
        .trim()
        .to_string();
    source
        .apply_schema_dump(&format!("DELETE FROM {t} WHERE id = 0;\nCHECKPOINT;\n"))
        .expect("delete dead generation");

    // Log hint-bit page images and reuse old value ID after vacuum removes chunks
    let data_dir = source.config().data_dir.clone();
    writeln!(
        std::fs::OpenOptions::new()
            .append(true)
            .open(data_dir.join("postgresql.conf"))
            .unwrap(),
        "wal_log_hints = on"
    )
    .unwrap();
    source.stop().expect("stop source");
    let reset = Command::new("pg_resetwal")
        .args(["-o", &dead_id, "-D"])
        .arg(&data_dir)
        .status()
        .expect("run pg_resetwal");
    assert!(reset.success(), "pg_resetwal -o {dead_id} failed");
    source.start().expect("restart source");

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");
    fx::create_ch_dest_table(&ch, "default", "t").expect("create ch table");
    let ch_config_path = tmp.path().join("ch-config.toml");
    fx::write_ch_config_toml(
        &ch_config_path,
        "127.0.0.1",
        slot.ch_tcp,
        "default",
        &RelName::new(SCHEMA, "t"),
        &TableTarget::new("default", "t"),
    )
    .expect("write ch-config");

    let daemon = fx::DaemonRun::prepare(tmp.path(), slot.metrics).expect("daemon layout");
    let child = daemon
        .spawn(
            &source,
            &ch_config_path,
            slot.walsender,
            &[
                "--bootstrap-max-rate-kib",
                MAX_RATE_KIB,
                "--slot",
                "walshadow_toast_reuse",
            ],
        )
        .expect("spawn walshadow-stream");
    let guard = fx::ChildGuard::new(child);

    let result: Result<()> = async {
        fx::wait_for_backup_streaming(&source, Duration::from_secs(60))?;
        // First read after backup start logs page image containing old chunks
        source
            .psql_one(&format!("SELECT count(*) FROM {toast}"))
            .context("hint dead chunks")?;
        // Vacuum only TOAST heap so new rows append near end of main heap
        source
            .apply_schema_dump(&format!("VACUUM {toast};\n"))
            .context("vacuum toast heap")?;
        source
            .psql_one(&format!(
                "INSERT INTO {t} SELECT {LIVE_ID}, string_agg(md5(g::text || 'live'), '') \
                 FROM generate_series(1, {live_digests}) g"
            ))
            .context("insert live generation")?;
        let live_id = source
            .psql_one(&format!("SELECT DISTINCT chunk_id FROM {toast}"))
            .context("live value id")?;
        ensure!(
            live_id.trim() == dead_id,
            "value id not reissued: dead {dead_id}, live {}",
            live_id.trim()
        );
        ensure!(
            fx::backup_in_progress(&source),
            "backup finished before new value was written"
        );

        fx::wait_for_listen(daemon.metrics_addr, Duration::from_secs(120))
            .context("daemon metrics endpoint never came up")?;
        fx::wait_for_ch_value(
            &ch,
            &format!("SELECT count() FROM default.t FINAL WHERE id = {LIVE_ID}"),
            "1",
            Duration::from_secs(60),
        )?;
        fx::wait_ch_matches_source(&ch, &source, &t, "default.t", Duration::from_secs(30))
    }
    .await;

    fx::finish_daemon(guard, &daemon, result);
}
