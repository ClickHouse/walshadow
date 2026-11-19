//! Apply SQL table rules, column overrides, and TOML initial loads at boot
//! Use direct bootstrap with WAL retention enabled

#![cfg(target_os = "linux")]

#[path = "common/bootstrap_ch_fixture.rs"]
mod fx;

use std::os::unix::process::CommandExt;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, ensure};

const INSTALL_SQL: &str = include_str!("../sql/runtime_config_install.sql");
const LIVE: &str = "FINAL WHERE _is_deleted = 0";

fn pinned(rel: &str, initial_load: &str) -> String {
    format!(
        "[table.public.{rel}]\n\
         initial_load = \"{initial_load}\"\n\
         columns = [\n  \
           {{ attnum = 1, target = \"id\", type = \"Int32\" }},\n  \
           {{ attnum = 2, target = \"name\", type = \"String\" }},\n\
         ]\n"
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn boot_seeds_sql_scope_and_toml_initial_loads() {
    if !fx::tools::requirements_available() {
        return;
    }

    let slot = fx::Ports::alloc();
    let tmp = tempfile::tempdir().unwrap();
    let source = fx::start_source(&tmp);
    let _src_stop = fx::StopOnDrop { sh: &source };
    source
        .apply_schema_dump(&format!(
            "{INSTALL_SQL}
             CREATE TABLE public.a (id int PRIMARY KEY, amount numeric);
             CREATE TABLE public.b (id int PRIMARY KEY, name text);
             CREATE TABLE public.p_1 (id int PRIMARY KEY, v text);
             CREATE TABLE public.q (id int PRIMARY KEY);
             INSERT INTO public.a SELECT g, g + 0.25 FROM generate_series(1, 40) g;
             INSERT INTO public.b SELECT g, 'b' || g FROM generate_series(1, 40) g;
             INSERT INTO public.p_1 SELECT g, 'p' || g FROM generate_series(1, 40) g;
             INSERT INTO public.q SELECT g FROM generate_series(1, 40) g;
             INSERT INTO walshadow.config_table (namespace, relname, replicate, initial_load)
               VALUES ('public', 'a', true, 'copy');
             INSERT INTO walshadow.config_table (namespace, relname, match, replicate)
               VALUES ('public', 'p_*', 'glob', true);
             INSERT INTO walshadow.config_column (namespace, relname, attname, target_type)
               VALUES ('public', 'a', 'amount', 'Decimal(38, 2)');
             CHECKPOINT;
             SELECT pg_switch_wal();"
        ))
        .expect("load source workload");

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");
    let ch_config_path = tmp.path().join("ch-config.toml");
    std::fs::write(
        &ch_config_path,
        format!(
            "[ch]\nhost = \"127.0.0.1\"\nport = {}\ndatabase = \"default\"\n\n\
             [stream]\nreplicate_all = false\n\n\
             [runtime_config]\nschema = \"walshadow\"\n\n{}\n{}\n\
             [table.public.q]\ntarget_table = \"q_renamed\"\n",
            slot.ch_tcp,
            pinned("b", "copy"),
            pinned("ghost", "copy"),
        ),
    )
    .unwrap();

    // Keep default WAL retention; DaemonRun normally disables it
    let daemon = fx::DaemonRun::prepare(tmp.path(), slot.metrics).expect("daemon layout");
    let stderr = std::fs::File::create(&daemon.stderr_path).unwrap();
    let child = Command::new(std::env::var("CARGO_BIN_EXE_walshadow-stream").unwrap())
        .args([
            "--host",
            source.config().socket_dir.to_str().unwrap(),
            "--port",
            &source.config().port.to_string(),
            "--user",
            "postgres",
            "--dbname",
            "postgres",
            "--sslmode",
            "disable",
            "--out-dir",
            daemon.filter_dir.to_str().unwrap(),
            "--shadow-socket-dir",
            daemon.shadow_sock.to_str().unwrap(),
            "--shadow-port",
            &fx::PG_SHADOW_PORT.to_string(),
            "--shadow-user",
            "postgres",
            "--bridge-lib-dir",
            fx::pgext_dir().to_str().unwrap(),
            "--spill-dir",
            daemon.spill_dir.to_str().unwrap(),
            "--status-interval",
            "1",
            "--metrics-bind",
            &daemon.metrics_addr.to_string(),
            "--walsender-bind",
            &format!("127.0.0.1:{}", slot.walsender),
            "--ch-config",
            ch_config_path.to_str().unwrap(),
            "--bootstrap-mode",
            "direct",
            "--bootstrap-shadow-data-dir",
            daemon.shadow_data_dir.to_str().unwrap(),
            "--bootstrap-wind-down-secs",
            "0",
            // Skip preflight to test boot handling of missing relations
            "--skip-preflight",
        ])
        .env("RUST_LOG", "warn,walshadow=info")
        .stdout(Stdio::null())
        .stderr(Stdio::from(stderr))
        .process_group(0)
        .spawn()
        .expect("spawn walshadow-stream");
    let mut guard = fx::ChildGuard::new(child);

    let result: Result<()> = async {
        let deadline = Instant::now() + Duration::from_secs(120);
        while !daemon.stderr().contains("walsender connected") {
            let exited = guard.child.as_mut().unwrap().try_wait()?;
            ensure!(exited.is_none(), "daemon exited during boot: {exited:?}");
            ensure!(Instant::now() < deadline, "pump never started");
            std::thread::sleep(Duration::from_millis(100));
        }
        let log = daemon.stderr();
        ensure!(
            log.contains("TOML initial_load ignored: source rel unknown"),
            "missing relation not reported"
        );
        source.apply_schema_dump(
            "INSERT INTO public.a VALUES (100, 7.5);
             INSERT INTO public.b VALUES (100, 'b-live');
             INSERT INTO public.p_1 VALUES (100, 'p-live');
             INSERT INTO public.q VALUES (100);
             SELECT pg_switch_wal();",
        )?;
        for (table, n) in [("public_a", "41"), ("public_b", "41"), ("public_p_1", "1")] {
            fx::wait_for_ch_value(
                &ch,
                &format!("SELECT count() FROM default.{table} {LIVE}"),
                n,
                Duration::from_secs(60),
            )
            .with_context(|| format!("{table} never converged"))?;
        }
        let amount = ch.query(&format!(
            "SELECT toTypeName(amount), amount = 7.5 FROM default.public_a {LIVE} AND id = 100"
        ))?;
        ensure!(
            amount == "Decimal(38, 2)\t1",
            "column override lost: {amount}"
        );
        let q = ch.query(
            "SELECT count() FROM system.tables WHERE database = 'default' \
             AND name IN ('public_q', 'q_renamed')",
        )?;
        ensure!(q == "0", "target-only row brought table into scope");
        Ok(())
    }
    .await;

    fx::finish_daemon(guard, &daemon, result);
}
