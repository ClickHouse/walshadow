//! Greenfield Direct bootstrap must not abort on a source extension that is
//! irrelevant to type translation and absent from the bootstrap oracle's host.
//!
//! `pg_dump --binary-upgrade` recreates every source extension's members
//! inline, including C functions that load `$libdir/<ext>`. The throwaway
//! bootstrap oracle runs on walshadow's own host, where such an extension's
//! `.so` need not exist — applying the dump then dies with
//! `could not access file "<ext>"` and takes the whole bootstrap with it, even
//! when no mapped column uses the extension's types.
//!
//! Repro without a second machine: give the SOURCE a private extension via
//! `extension_control_path` + `dynamic_library_path` (PG18+), so it lives in a
//! temp dir the oracle's default paths never see. A `jsonb` column forces the
//! oracle to be provisioned; the extension itself is unused. On the buggy tree
//! the bootstrap aborts and the row never reaches ClickHouse; the fix drops the
//! unavailable extension from the dump and the row lands.

#![cfg(target_os = "linux")]

#[path = "common/bootstrap_ch_fixture.rs"]
mod fx;

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};

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

fn write_autocreate_config(path: &Path, ch_port: u16) -> Result<()> {
    let body = format!(
        "[ch]\n\
         host = \"127.0.0.1\"\n\
         port = {ch_port}\n\
         database = \"default\"\n\
         compression = \"lz4\"\n\
         \n\
         [table.\"public\".\"t\"]\n\
         replicate = true\n"
    );
    fs::write(path, body).context("write ch-config")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bootstrap_survives_extension_absent_from_oracle() {
    if !fx::requirements_available() {
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
            eprintln!("skip: no C compiler / server headers to build the stub extension");
            return;
        }
    };

    let slot = fx::Ports::alloc();

    let source = fx::make_source(&tmp);
    source.initdb().expect("initdb source");
    source.write_base_conf().expect("source base conf");
    fx::append_source_conf(&source).expect("append source conf");
    {
        // Only the source sees `stub`; the daemon's bootstrap oracle uses
        // default paths, so it is absent from the oracle's pg_available_extensions.
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

    // `stub` is irrelevant to translation — no column uses its type. The `jsonb`
    // column is what forces the bootstrap oracle to be provisioned at all.
    source
        .apply_schema_dump(
            "CREATE EXTENSION stub;\n\
             CREATE TABLE public.t (id int PRIMARY KEY, name text, doc jsonb);\n\
             ALTER TABLE public.t REPLICA IDENTITY FULL;\n\
             INSERT INTO public.t VALUES (1, 'hello', '{\"k\": 42}');\n\
             CHECKPOINT;\n\
             SELECT pg_switch_wal();\n",
        )
        .expect("load workload");

    let ch_tmp = tempfile::tempdir().unwrap();
    let ch = fx::ChServer::spawn(ch_tmp, slot.ch_tcp, slot.ch_http).expect("spawn ch");

    let ch_config = tmp.path().join("ch-config.toml");
    write_autocreate_config(&ch_config, slot.ch_tcp).expect("write ch-config");

    let daemon = fx::DaemonRun::prepare(tmp.path(), slot.metrics).expect("prepare daemon");
    let child = daemon
        .spawn(&source, &ch_config, slot.walsender, &[])
        .expect("spawn walshadow-stream");
    let guard = fx::ChildGuard::new(child);

    let result = (|| -> Result<()> {
        let deadline = Instant::now() + Duration::from_secs(90);
        loop {
            let n = ch
                .query("SELECT count() FROM default.t FINAL WHERE _is_deleted = 0")
                .unwrap_or_default();
            if n == "1" {
                break;
            }
            if daemon.stderr().contains("could not access file") {
                anyhow::bail!(
                    "bootstrap aborted on `stub`, an extension it does not need to translate any \
                     mapped column"
                );
            }
            if Instant::now() >= deadline {
                anyhow::bail!("bootstrap row never reached ClickHouse (last count {n:?})");
            }
            std::thread::sleep(Duration::from_millis(250));
        }
        // The oracle-routed jsonb column must still resolve.
        let k = ch
            .query("SELECT toString(doc.k) FROM default.t FINAL WHERE id = 1")
            .context("read jsonb path from CH")?;
        if k != "42" {
            anyhow::bail!("jsonb column not decoded: doc.k = {k:?}");
        }
        Ok(())
    })();

    fx::finish_daemon(guard, &daemon, result);
}
