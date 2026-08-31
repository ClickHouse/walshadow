//! Greenfield Direct bootstrap type coverage
//!
//! Require installed `walshadow.so`, ClickHouse, and source extensions

#![cfg(target_os = "linux")]

#[path = "common/bootstrap_ch_fixture.rs"]
mod fx;

use std::fs;
use std::net::SocketAddr;
use std::os::unix::process::CommandExt;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Duration;

use anyhow::{Context, Result};
use walshadow::shadow::{Shadow, ShadowConfig};

fn extension_available(name: &str) -> bool {
    let Ok(out) = Command::new("pg_config").arg("--sharedir").output() else {
        return false;
    };
    if !out.status.success() {
        return false;
    }
    let dir = String::from_utf8_lossy(&out.stdout).trim().to_string();
    Path::new(&dir)
        .join(format!("extension/{name}.control"))
        .exists()
}

fn make_source(tmp: &tempfile::TempDir) -> Shadow {
    let mut cfg = ShadowConfig::new(
        tmp.path().join("source-data"),
        tmp.path().join("source-filtered"),
    );
    cfg.port = fx::PG_SOURCE_PORT;
    cfg.socket_dir = tmp.path().join("source-sock");
    cfg.ctl_timeout = Duration::from_secs(60);
    fs::create_dir_all(&cfg.filter_out_dir).unwrap();
    fs::create_dir_all(&cfg.socket_dir).unwrap();
    Shadow::new(cfg)
}

fn load_types_workload(source: &Shadow, has_postgis: bool) -> Result<()> {
    let mut cols = String::from(
        "id int PRIMARY KEY, c_bool bool, c_int2 smallint, c_int4 int, c_int8 bigint, \
         c_float4 real, c_float8 double precision, c_num numeric(10,2), c_num_u numeric, \
         c_uuid uuid, c_text text, c_varchar varchar(20), c_date date, c_ts timestamp, \
         c_inet inet, c_json json, c_jsonb jsonb, c_hstore hstore, c_citext citext, \
         c_enum mood, c_int_arr int4[], c_text_arr text[], c_int8_arr bigint[], c_vector vector(3)",
    );
    let mut names = String::from(
        "id, c_bool, c_int2, c_int4, c_int8, c_float4, c_float8, c_num, c_num_u, c_uuid, \
         c_text, c_varchar, c_date, c_ts, c_inet, c_json, c_jsonb, c_hstore, c_citext, \
         c_enum, c_int_arr, c_text_arr, c_int8_arr, c_vector",
    );
    let mut vals = String::from(
        "1, true, 32000, 123456, 9000000000, 1.5, 2.5, 1234.56, 3.14159, \
         '11111111-1111-1111-1111-111111111111', 'hello', 'vc', '2024-01-15', \
         '2024-01-15 13:45:30', '192.168.1.1', '{\"a\": 1, \"b\": [2,3]}', \
         '{\"k\": 42, \"arr\": [1,2,3]}', 'x=>1, y=>2', 'CaseInsensitive', 'happy', \
         '{1,2,3}', '{a,b,c}', '{100,200}', '[0.1,0.2,0.3]'",
    );
    if has_postgis {
        cols.push_str(", c_geog geography(Point,4326), c_geom geometry(Point,4326)");
        names.push_str(", c_geog, c_geom");
        vals.push_str(", 'SRID=4326;POINT(30.5 50.25)', 'SRID=4326;POINT(1 2)'");
    }

    let mut sql = String::from(
        "CREATE EXTENSION IF NOT EXISTS hstore;\n\
         CREATE EXTENSION IF NOT EXISTS citext;\n\
         CREATE EXTENSION IF NOT EXISTS vector;\n",
    );
    if has_postgis {
        sql.push_str("CREATE EXTENSION IF NOT EXISTS postgis;\n");
    }
    sql.push_str("CREATE TYPE mood AS ENUM ('sad','ok','happy');\n");
    sql.push_str(&format!("CREATE TABLE public.all_types ({cols});\n"));
    sql.push_str("ALTER TABLE public.all_types REPLICA IDENTITY FULL;\n");
    sql.push_str(&format!(
        "INSERT INTO public.all_types ({names}) VALUES ({vals});\n"
    ));
    sql.push_str("CHECKPOINT;\nSELECT pg_switch_wal();\n");
    source
        .apply_schema_dump(&sql)
        .context("apply source schema")
}

fn write_autocreate_config(path: &Path, ch_port: u16) -> Result<()> {
    let body = format!(
        "[ch]\n\
         host = \"127.0.0.1\"\n\
         port = {ch_port}\n\
         database = \"default\"\n\
         compression = \"lz4\"\n\
         \n\
         [table.\"public\".\"all_types\"]\n\
         replicate = true\n\
         initial_load = \"none\"\n"
    );
    fs::write(path, body).context("write ch-config")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn direct_bootstrap_all_types_end_to_end() {
    if !fx::pg_available() || !fx::pg_basebackup_available() || !fx::clickhouse_available() {
        eprintln!("skip: missing initdb / pg_basebackup / clickhouse");
        return;
    }
    for ext in ["hstore", "citext", "vector"] {
        if !extension_available(ext) {
            eprintln!("skip: extension {ext} not installed");
            return;
        }
    }
    let has_postgis = extension_available("postgis");

    let slot = fx::Ports::alloc();
    let tmp = tempfile::tempdir().unwrap();

    let source = make_source(&tmp);
    source.initdb().expect("initdb source");
    source.write_base_conf().expect("source base conf");
    fx::append_source_conf(&source).expect("append source conf");
    source.start().expect("start source");
    let _src_stop = fx::StopOnDrop { sh: &source };
    load_types_workload(&source, has_postgis).expect("load types workload");

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");

    let ch_config_path = tmp.path().join("ch-config.toml");
    write_autocreate_config(&ch_config_path, slot.ch_tcp).expect("write ch-config");

    let bootstrap_shadow_data_dir = tmp.path().join("shadow-data");
    let shadow_sock = tmp.path().join("shadow-sock");
    fs::create_dir_all(&shadow_sock).unwrap();
    let shadow_filter_dir = tmp.path().join("filtered");
    fs::create_dir_all(&shadow_filter_dir).unwrap();
    let spill_dir = tmp.path().join("spill");
    fs::create_dir_all(&spill_dir).unwrap();

    let bin = env!("CARGO_BIN_EXE_walshadow-stream");
    let stderr_path = tmp.path().join("daemon.stderr.log");
    let stderr_file = fs::File::create(&stderr_path).expect("open daemon stderr log");
    let metrics_addr: SocketAddr = format!("127.0.0.1:{}", slot.metrics).parse().unwrap();
    let child = Command::new(bin)
        .args([
            "--host",
            source.config().socket_dir.to_str().unwrap(),
            "--port",
            &fx::PG_SOURCE_PORT.to_string(),
            "--user",
            "postgres",
            "--dbname",
            "postgres",
            "--sslmode",
            "disable",
            "--out-dir",
            shadow_filter_dir.to_str().unwrap(),
            "--shadow-socket-dir",
            shadow_sock.to_str().unwrap(),
            "--shadow-port",
            &fx::PG_SHADOW_PORT.to_string(),
            "--shadow-user",
            "postgres",
            "--shadow-dbname",
            "postgres",
            "--bridge-lib-dir",
            fx::pgext_dir().to_str().unwrap(),
            "--spill-dir",
            spill_dir.to_str().unwrap(),
            "--status-interval",
            "1",
            "--metrics-bind",
            &metrics_addr.to_string(),
            "--walsender-bind",
            &format!("127.0.0.1:{}", slot.walsender),
            "--retention-bytes",
            "0",
            "--ch-config",
            ch_config_path.to_str().unwrap(),
            "--bootstrap-mode",
            "direct",
            "--bootstrap-shadow-data-dir",
            bootstrap_shadow_data_dir.to_str().unwrap(),
            "--bootstrap-shadow-replay-timeout",
            "120",
        ])
        .env("RUST_LOG", "warn,walshadow=info")
        .stdout(Stdio::null())
        .stderr(Stdio::from(stderr_file))
        .process_group(0)
        .spawn()
        .expect("spawn walshadow-stream");
    let guard = fx::ChildGuard::new(child);

    let result = (|| -> Result<()> {
        fx::wait_for_listen(metrics_addr, Duration::from_secs(30))
            .context("daemon metrics endpoint never came up")?;

        let deadline = std::time::Instant::now() + Duration::from_secs(90);
        loop {
            let n = ch
                .query("SELECT count() FROM default.all_types FINAL WHERE _is_deleted = 0")
                .unwrap_or_default();
            if n == "1" {
                break;
            }
            let stderr = fs::read_to_string(&stderr_path).unwrap_or_default();
            if stderr.contains("oracle unavailable") {
                anyhow::bail!("bootstrap oracle degraded (tier-3 would be empty)");
            }
            if std::time::Instant::now() >= deadline {
                anyhow::bail!("bootstrap row never reached CH (got {n:?})");
            }
            std::thread::sleep(Duration::from_millis(250));
        }

        let mut checks: Vec<(&str, String)> = vec![
            ("c_bool", "true".into()),
            ("c_int4", "123456".into()),
            ("c_int8", "9000000000".into()),
            ("c_num", "1234.56".into()),
            ("c_num_u", "3.14159".into()),
            ("c_text", "hello".into()),
            ("c_uuid", "11111111-1111-1111-1111-111111111111".into()),
            ("c_jsonb.k", "42".into()),
            ("c_jsonb.arr[1]", "1".into()),
            ("c_hstore['x']", "1".into()),
            ("c_citext", "CaseInsensitive".into()),
            ("c_enum", "happy".into()),
            ("arrayStringConcat(c_int_arr, ',')", "1,2,3".into()),
            ("arrayStringConcat(c_text_arr, ',')", "a,b,c".into()),
            ("arrayStringConcat(c_int8_arr, ',')", "100,200".into()),
            ("length(c_vector)", "3".into()),
            ("round(c_vector[1], 2)", "0.1".into()),
        ];
        if has_postgis {
            checks.push(("c_geog", "POINT(30.5 50.25)".into()));
            checks.push(("c_geom", "POINT(1 2)".into()));
        }

        let mut fails = Vec::new();
        for (expr, want) in &checks {
            let got = ch
                .query(&format!(
                    "SELECT toString({expr}) FROM default.all_types FINAL WHERE id = 1"
                ))
                .unwrap_or_default();
            if &got != want {
                fails.push(format!("{expr}: got {got:?}, want {want:?}"));
            }
        }
        if !fails.is_empty() {
            anyhow::bail!("column mismatches:\n  {}", fails.join("\n  "));
        }
        Ok(())
    })();

    let _ = guard.into_inner().map(|mut c| {
        let _ = c.kill();
        let _ = c.wait();
    });
    if bootstrap_shadow_data_dir.join("postmaster.pid").exists() {
        let mut shadow_cfg =
            ShadowConfig::new(bootstrap_shadow_data_dir.clone(), shadow_filter_dir.clone());
        shadow_cfg.port = fx::PG_SHADOW_PORT;
        shadow_cfg.socket_dir = shadow_sock.clone();
        shadow_cfg.ctl_timeout = Duration::from_secs(60);
        let _ = Shadow::new(shadow_cfg).stop();
    }

    if let Err(e) = result {
        let stderr = fs::read_to_string(&stderr_path).unwrap_or_default();
        panic!("{e:#}\n--- daemon stderr ---\n{stderr}");
    }
}
