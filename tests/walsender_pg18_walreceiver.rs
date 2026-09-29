//! Shadow-stream protocol validation against a real PG18 walreceiver.
//!
//! Spins up:
//! 1. A fresh PG18 cluster via `initdb` in a tempdir (primary source).
//! 2. A second cluster via `pg_basebackup` against the primary (the
//!    walreceiver under test).
//! 3. The wal-rus walsender server bound to a TCP port.
//! 4. Wires the standby's `primary_conninfo` at the walsender.
//!
//! Asserts the standby's walreceiver connects to our walsender,
//! issues `IDENTIFY_SYSTEM`, and reads our cached identity. The
//! standby's startup log surfaces the connection attempt — that's
//! the validation: a real PG18 walreceiver speaks our walsender's
//! protocol and accepts the handshake.
//!
//! Skips locally when `initdb` or `pg_ctl` is absent.

#[path = "common/tools.rs"]
mod tools;

use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio::sync::Mutex;
use walshadow::shadow_stream::{
    ShadowStreamState, WALRECEIVER_PROTOCOL_PIN, WalSenderAddr, spawn_listener,
};

#[tokio::test(flavor = "current_thread")]
async fn pg_walreceiver_connects_and_runs_identify_system() {
    if !tools::pg_available() || !tools::on_path("pg_ctl", "--version") {
        return;
    }

    let tmp = tempfile::tempdir().unwrap();
    let standby_data = tmp.path().join("standby");
    let standby_socket_dir = tmp.path().join("sock");
    let standby_log = tmp.path().join("standby.log");
    std::fs::create_dir_all(&standby_socket_dir).unwrap();

    // Spin up the walsender server first (bootstrap-barrier
    // ordering) so PG's walreceiver doesn't hit
    // wal_retrieve_retry_interval on first start.
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let walsender_port = listener.local_addr().unwrap().port();
    drop(listener); // re-bound by spawn_listener below

    let state = Arc::new(Mutex::new(ShadowStreamState::new(
        1,
        "7340000000000000000".into(),
        0x1234_5678,
        1024 * 1024,
    )));
    let _handle = spawn_listener(
        WalSenderAddr::Tcp(format!("127.0.0.1:{walsender_port}").parse().unwrap()),
        state.clone(),
        Duration::from_millis(100),
    )
    .await
    .expect("bind walsender listener");

    // initdb a fresh standby cluster.
    let initdb_status = std::process::Command::new("initdb")
        .args(["-D", standby_data.to_str().unwrap()])
        .args(["-U", "postgres", "--auth=trust", "--no-instructions"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .unwrap();
    assert!(initdb_status.success(), "initdb failed");

    // Configure standby: standby.signal + minimal postgresql.conf
    // entry pointing at our walsender.
    std::fs::write(standby_data.join("standby.signal"), b"").unwrap();
    let conninfo = format!(
        "host=127.0.0.1 port={walsender_port} user=walshadow application_name=walreceiver_check sslmode=disable"
    );
    let socket_str = standby_socket_dir.to_str().unwrap();
    // Pick a free TCP port for the standby's listener so it doesn't
    // collide with the host's primary PG; bind/release pattern.
    let pg_port = {
        let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let p = l.local_addr().unwrap().port();
        drop(l);
        p
    };
    let conf = format!(
        "\n# walreceiver validation\n\
         primary_conninfo = '{conninfo}'\n\
         hot_standby = on\n\
         wal_level = replica\n\
         listen_addresses = ''\n\
         unix_socket_directories = '{socket_str}'\n\
         port = {pg_port}\n\
         wal_retrieve_retry_interval = 200ms\n",
    );
    use std::io::Write as _;
    let mut conf_file = std::fs::OpenOptions::new()
        .append(true)
        .open(standby_data.join("postgresql.conf"))
        .unwrap();
    conf_file.write_all(conf.as_bytes()).unwrap();
    drop(conf_file);

    // Start PG; pg_ctl -w blocks until startup is signalled or the
    // timeout elapses. Standby startup will fail (the WAL stream we
    // emit isn't bootstrap-consistent) — that's expected. The
    // validation is that the walreceiver connected at all.
    let start = std::process::Command::new("pg_ctl")
        .args(["-D", standby_data.to_str().unwrap()])
        .args(["-l", standby_log.to_str().unwrap()])
        .args(["-w", "-t", "10", "start"])
        .env(WALRECEIVER_PROTOCOL_PIN.0, WALRECEIVER_PROTOCOL_PIN.1)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();
    // Either Ok (PG started — got past walreceiver kickoff) or Err
    // (PG ultimately failed). We expect Err because the synthesized
    // WAL bytes aren't replayable; but the walreceiver attempt
    // surfaces in the log either way.
    let _ = start;

    // Give the walreceiver a moment to actually attempt connection
    // even if pg_ctl returned early.
    tokio::time::sleep(Duration::from_secs(2)).await;

    // Stop PG cleanly (best effort).
    let _ = std::process::Command::new("pg_ctl")
        .args(["-D", standby_data.to_str().unwrap()])
        .args(["-m", "immediate", "-w", "-t", "5", "stop"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status();

    // Connection state on the walsender side: did we accept the
    // walreceiver's TCP connect + startup message?
    let agg = state.lock().await.aggregate();
    eprintln!(
        "walsender saw {} active connections; flush={:?}",
        agg.active_connections, agg.min_flush_lsn,
    );

    // PG's startup log should mention the walreceiver attempting
    // connection.
    let log = std::fs::read_to_string(&standby_log).unwrap_or_default();
    eprintln!(
        "standby log (first 4 KiB):\n{}",
        &log[..log.len().min(4096)]
    );

    // Protocol round-trip proof: PG18's walreceiver issues
    // IDENTIFY_SYSTEM, parses our DataRow, then compares the
    // advertised `systemid` against its own. The walreceiver's log
    // line includes our advertised systemid verbatim — that proves
    // every step of the handshake from StartupMessage through
    // RowDescription / DataRow / CommandComplete worked. (The
    // identifiers will mismatch because the standby came from
    // initdb, not pg_basebackup against us; that's the expected
    // FATAL — we're not synthesising a basebackup-consistent
    // primary, only validating the wire protocol.)
    let advertised_systemid = "7340000000000000000";
    let handshake_succeeded = log.contains(advertised_systemid);
    assert!(
        handshake_succeeded,
        "PG18 walreceiver did not surface our advertised systemid → \
         IDENTIFY_SYSTEM round-trip failed. Log:\n{log}",
    );
}
