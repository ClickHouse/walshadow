//! Settle pending updates, deletes, subtransactions, multixact updaters, and
//! external values after bootstrap

#![cfg(target_os = "linux")]

#[path = "common/bootstrap_ch_fixture.rs"]
mod fx;

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use tokio_postgres::Client;
use walshadow::mapping::TableTarget;
use walshadow::schema::RelName;
use walshadow::shadow::Shadow;
use walshadow::source_feed::open_sql_client;

const N_ROWS: i32 = 64;
/// Keep writers open across backup window
const MAX_RATE_KIB: &str = "32768";
const SCHEMA: &str = "s27";

fn wait_for_metric(addr: SocketAddr, name: &str, least: u64, timeout: Duration) -> Result<u64> {
    let start = Instant::now();
    let mut seen = None;
    while start.elapsed() < timeout {
        let body = fx::http_get(addr, "/metrics")?;
        let value = fx::parse_metric(&body, name);
        if value.is_some_and(|v| v >= least) {
            return Ok(value.expect("checked"));
        }
        seen = value;
        std::thread::sleep(Duration::from_millis(200));
    }
    bail!("{name} never reached {least}, last {seen:?}");
}

struct Writer {
    client: Client,
    commit: bool,
}

async fn open_writer(source: &Shadow, tag: &str, commit: bool, sql: &str) -> Writer {
    let client = open_sql_client(&fx::pg_cfg(source, tag))
        .await
        .unwrap_or_else(|e| panic!("{tag} connect: {e}"));
    client
        .batch_execute(&format!("BEGIN; {sql}"))
        .await
        .unwrap_or_else(|e| panic!("{tag} writes before backup redo: {e}"));
    Writer { client, commit }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pending_updates_deletes_subxacts_multixacts_and_external_values_settle() {
    if !fx::tools::requirements_available() {
        return;
    }

    let slot = fx::Ports::alloc();
    let tmp = tempfile::tempdir().unwrap();

    let source = fx::start_source(&tmp);
    let _src_stop = fx::StopOnDrop { sh: &source };
    fx::load_source_workload(&source, SCHEMA, N_ROWS).expect("load source workload");

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

    let t = format!("{SCHEMA}.t");
    // Keep row locks until updaters finish to preserve multixacts
    let lockers = [
        open_writer(
            &source,
            "lock-a",
            true,
            &format!("SELECT * FROM {t} WHERE id = 22 FOR KEY SHARE"),
        )
        .await,
        open_writer(
            &source,
            "lock-b",
            true,
            &format!("SELECT * FROM {t} WHERE id = 23 FOR KEY SHARE"),
        )
        .await,
    ];
    let writers = [
        open_writer(
            &source,
            "upd-commit",
            true,
            &format!("UPDATE {t} SET name = 'upd-'||id WHERE id BETWEEN 1 AND 5"),
        )
        .await,
        open_writer(
            &source,
            "upd-abort",
            false,
            &format!("UPDATE {t} SET name = 'lost-'||id WHERE id BETWEEN 6 AND 10"),
        )
        .await,
        open_writer(
            &source,
            "del-commit",
            true,
            &format!("DELETE FROM {t} WHERE id BETWEEN 11 AND 15"),
        )
        .await,
        open_writer(
            &source,
            "del-abort",
            false,
            &format!("DELETE FROM {t} WHERE id BETWEEN 16 AND 20"),
        )
        .await,
        open_writer(
            &source,
            "subxact",
            true,
            &format!(
                "INSERT INTO {t} SELECT g, 'sub-'||g FROM generate_series(3001, 3005) g; \
                 SAVEPOINT a; \
                 INSERT INTO {t} SELECT g, 'sub-'||g FROM generate_series(3006, 3010) g; \
                 ROLLBACK TO a; \
                 SAVEPOINT b; \
                 UPDATE {t} SET name = 'sub-upd' WHERE id = 21; \
                 RELEASE b"
            ),
        )
        .await,
        open_writer(
            &source,
            "multi-commit",
            true,
            &format!("UPDATE {t} SET name = 'multi-'||id WHERE id = 22"),
        )
        .await,
        open_writer(
            &source,
            "multi-abort",
            false,
            &format!("UPDATE {t} SET name = 'multi-lost' WHERE id = 23"),
        )
        .await,
        // Incompressible enough to stay external after pglz
        open_writer(
            &source,
            "external",
            true,
            &format!(
                "INSERT INTO {t} SELECT 4001, string_agg(md5(g::text || 'x'), '') \
                 FROM generate_series(1, 1000) g; \
                 UPDATE {t} SET name = (SELECT string_agg(md5(g::text), '') \
                   FROM generate_series(1, 1000) g) WHERE id = 24"
            ),
        )
        .await,
    ];
    source
        .apply_schema_dump("CHECKPOINT;\n")
        .expect("checkpoint uncommitted pages");

    let daemon = fx::DaemonRun::prepare(tmp.path(), slot.metrics).expect("daemon layout");
    let child = daemon
        .spawn(
            &source,
            &ch_config_path,
            slot.walsender,
            &[
                "--bootstrap-max-rate-kib",
                MAX_RATE_KIB,
                "--bootstrap-wind-down-secs",
                "0",
                "--slot",
                "walshadow_pending_shapes",
            ],
        )
        .expect("spawn walshadow-stream");
    let guard = fx::ChildGuard::new(child);

    let result: Result<()> = async {
        fx::wait_for_listen(daemon.metrics_addr, Duration::from_secs(120))
            .context("daemon metrics endpoint never came up")?;
        wait_for_metric(
            daemon.metrics_addr,
            "walshadow_pending_rows_total",
            40,
            Duration::from_secs(60),
        )
        .context("pending rows never reached ClickHouse")?;

        for w in writers.iter().chain(&lockers) {
            let end = if w.commit { "COMMIT" } else { "ROLLBACK" };
            w.client.batch_execute(end).await.context(end)?;
        }
        // Write after settlement so replay can advance past it
        source
            .psql_one(&format!("INSERT INTO {t} VALUES (5001, 'tail')"))
            .context("tail insert")?;

        fx::wait_for_ch_value(
            &ch,
            "SELECT count() FROM default.t FINAL WHERE id = 5001",
            "1",
            Duration::from_secs(60),
        )?;
        fx::wait_for_ch_value(
            &ch,
            "EXISTS TABLE default.t__wspending",
            "0",
            Duration::from_secs(60),
        )
        .context("pending table still exists after all rows settled")?;
        fx::wait_ch_matches_source(&ch, &source, &t, "default.t", Duration::from_secs(30))
    }
    .await;

    fx::finish_daemon(guard, &daemon, result);
}
