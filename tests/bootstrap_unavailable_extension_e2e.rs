//! Require source extension libraries on bootstrap oracle host, even when unused

#![cfg(target_os = "linux")]

#[path = "common/bootstrap_ch_fixture.rs"]
mod fx;

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Duration;

use anyhow::Result;
use walshadow::backfill::bootstrap_oracle::BootstrapOracle;
use walshadow::shadow::{Shadow, ShadowConfig};

const STUB_C: &str = "\
#include \"postgres.h\"\n\
#include \"fmgr.h\"\n\
PG_MODULE_MAGIC;\n\
PG_FUNCTION_INFO_V1(stub_noop);\n\
Datum stub_noop(PG_FUNCTION_ARGS) { PG_RETURN_INT32(1); }\n";

fn pg_major() -> Option<u32> {
    let out = Command::new("initdb").arg("--version").output().ok()?;
    if !out.status.success() {
        return None;
    }
    String::from_utf8_lossy(&out.stdout)
        .split_whitespace()
        .find_map(|tok| {
            let digits: String = tok.chars().take_while(char::is_ascii_digit).collect();
            digits.parse::<u32>().ok()
        })
}

fn pg_includedir_server() -> Option<String> {
    let out = Command::new("pg_config")
        .arg("--includedir-server")
        .output()
        .ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
}

/// Build a no-op C extension named `stub` under `tmp`. Returns the
/// `extension_control_path` dir and the `dynamic_library_path` dir, or `None`
/// when the toolchain (a C compiler + server headers) is unavailable — same
/// skip discipline the other e2e drills use for missing dependencies.
fn build_stub(tmp: &Path) -> Result<Option<(PathBuf, PathBuf)>> {
    let Some(inc) = pg_includedir_server() else {
        return Ok(None);
    };
    let ctl = tmp.join("ctl/extension");
    let lib = tmp.join("lib");
    fs::create_dir_all(&ctl)?;
    fs::create_dir_all(&lib)?;

    let c_path = tmp.join("stub.c");
    fs::write(&c_path, STUB_C)?;
    let obj = tmp.join("stub.o");
    let so = lib.join("stub.so");
    let cc = std::env::var("CC").unwrap_or_else(|_| "cc".into());

    let compiled = Command::new(&cc)
        .args(["-fPIC", &format!("-I{inc}"), "-c"])
        .arg(&c_path)
        .arg("-o")
        .arg(&obj)
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    if !compiled {
        return Ok(None);
    }
    let linked = Command::new(&cc)
        .arg("-shared")
        .arg(&obj)
        .arg("-o")
        .arg(&so)
        .status()
        .map(|s| s.success())
        .unwrap_or(false);
    if !linked {
        return Ok(None);
    }

    fs::write(
        ctl.join("stub.control"),
        "default_version = '1.0'\nrelocatable = true\n",
    )?;
    // Bare library name (not `$libdir/stub`) so the source resolves it through
    // dynamic_library_path; the oracle's default path never finds it.
    fs::write(
        ctl.join("stub--1.0.sql"),
        "CREATE FUNCTION stub_noop() RETURNS int4 AS 'stub', 'stub_noop' LANGUAGE c;\n",
    )?;
    Ok(Some((tmp.join("ctl"), lib)))
}

#[tokio::test]
async fn bootstrap_requires_source_extension_library() {
    if !fx::tools::pg_available() {
        return;
    }
    // extension_control_path (the no-root way to give only the source a private
    // extension) is PostgreSQL 18+.
    match pg_major() {
        Some(m) if m >= 18 => {}
        _ => {
            eprintln!("skip: needs PostgreSQL 18+ (extension_control_path)");
            return;
        }
    }

    let tmp = tempfile::tempdir().unwrap();
    let (ctl_dir, lib_dir) = match build_stub(tmp.path()).expect("build stub extension") {
        Some(dirs) => dirs,
        None => {
            fx::tools::skip("no C compiler / server headers to build the stub extension");
            return;
        }
    };

    let source = fx::make_source(&tmp);
    source.initdb().expect("initdb source");
    source.write_base_conf().expect("source base conf");
    {
        // PostgreSQL 18+ permits source-only extension paths without root
        let conf = source.config().data_dir.join("postgresql.conf");
        let mut f = fs::OpenOptions::new().append(true).open(&conf).unwrap();
        writeln!(
            f,
            "\nextension_control_path = '$system:{}'",
            ctl_dir.display()
        )
        .unwrap();
        writeln!(f, "dynamic_library_path = '$libdir:{}'", lib_dir.display()).unwrap();
    }
    source.start().expect("start source");
    let _src_stop = fx::StopOnDrop { sh: &source };

    source
        .apply_schema_dump("CREATE EXTENSION stub;")
        .expect("install source extension");

    let base_dir = tmp.path().join("oracle");
    let conninfo = format!(
        "host={} port={} user=postgres dbname=postgres",
        source.config().socket_dir.display(),
        source.config().port,
    );
    let Err(err) = BootstrapOracle::provision(
        base_dir.clone(),
        conninfo,
        None,
        None,
        1,
        Duration::from_secs(10),
    )
    .await
    else {
        panic!("bootstrap accepted missing source extension library");
    };
    let message = format!("{err:#}");
    assert!(
        message.contains("install source extension libraries"),
        "{message}"
    );
    assert!(
        message.contains("could not access file \"stub\""),
        "{message}"
    );

    let oracle = Shadow::new(ShadowConfig::new(base_dir.join("pg"), base_dir));
    assert!(!oracle.is_running().expect("oracle status"));
}
