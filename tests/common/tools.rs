//! Integration test gates. A local run skips a test whose tool or generated
//! fixture is missing; CI sets `WALSHADOW_REQUIRE_TOOLS` so absence fails
//! instead of passing as an empty test

#![allow(dead_code)]

use std::fmt::Display;
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

const REQUIRE: &str = "WALSHADOW_REQUIRE_TOOLS";

/// Enable every callsite so coverage runs evaluate tracing field expressions
#[ctor::ctor(unsafe)]
fn enable_tracing() {
    let _ = tracing::subscriber::set_global_default(tracing_subscriber::registry());
}

/// SIGINT, then SIGKILL if still running after 15 s. Instrumented binaries
/// write their coverage profile only on exit, never under SIGKILL
pub fn stop_gracefully(child: &mut Child) {
    let _ = Command::new("kill")
        .args(["-INT", &child.id().to_string()])
        .status();
    let deadline = Instant::now() + Duration::from_secs(15);
    while matches!(child.try_wait(), Ok(None)) && Instant::now() < deadline {
        std::thread::sleep(Duration::from_millis(100));
    }
    let _ = child.kill();
    let _ = child.wait();
}

/// Report a skip and return `false`, panic when CI requires every tool
pub fn skip(reason: impl Display) -> bool {
    assert!(
        std::env::var_os(REQUIRE).is_none(),
        "{REQUIRE} set: {reason}"
    );
    eprintln!("skip: {reason}");
    false
}

/// `bin arg` exits successfully
pub fn on_path(bin: &str, arg: &str) -> bool {
    Command::new(bin)
        .arg(arg)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
        || skip(format_args!("no {bin} on PATH"))
}

/// Captured WAL fixture exists, `capture.sh` regenerates it
pub fn fixture(path: &Path) -> bool {
    path.exists() || skip(format_args!("no fixture at {}", path.display()))
}

/// `{name}.control` under `pg_config --sharedir`
pub fn extension_installed(name: &str) -> bool {
    Command::new("pg_config")
        .arg("--sharedir")
        .output()
        .ok()
        .filter(|o| o.status.success())
        .is_some_and(|o| {
            let dir = String::from_utf8_lossy(&o.stdout);
            Path::new(dir.trim())
                .join(format!("extension/{name}.control"))
                .exists()
        })
}

pub fn extension(name: &str) -> bool {
    extension_installed(name) || skip(format_args!("extension {name} not installed"))
}

pub fn pg_available() -> bool {
    on_path("initdb", "--version")
}

pub fn pg_basebackup_available() -> bool {
    on_path("pg_basebackup", "--version")
}

pub fn clickhouse_available() -> bool {
    on_path("clickhouse", "--version")
}

/// Source, shadow clone, and ClickHouse server
pub fn requirements_available() -> bool {
    pg_available() && pg_basebackup_available() && clickhouse_available()
}
