//! walshadow CH-Native insert tail over TLS, end to end against a locally
//! spawned `clickhouse server` listening on a secure native port with a
//! self-signed cert.
//!
//! Exercises the production `EmitterConfig { secure: true, .. }` path
//! through the inserter pool: `tail::spawn` → `inserter::spawn_pool` →
//! `connect_client` → `AsyncClient::connect_tls`. The
//! self-signed CA is pinned into the rustls root store via
//! `EmitterConfig::tls_config`, so chain + SNI hostname (`localhost`)
//! verification runs for real rather than being disabled — `default_config`
//! (public webpki roots) would reject the local cert.
//!
//! The server also opens a plaintext native port used only for the
//! readiness probe and the verifying `clickhouse client` query; the
//! emitter only ever touches the secure port.
//!
//! Skips when `clickhouse` or `openssl` is not on PATH.

#![cfg(target_os = "linux")]

use std::io;
use std::net::{TcpListener, TcpStream};
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::time::{Duration, Instant};

use clickhouse_c::tls::{self, rustls};
use walross::pg::walparser::RelFileNode;
use walshadow::ch_emitter::{
    ColumnMapping, CompressionChoice, EmitterConfig, EmitterStats, TableMapping,
};
use walshadow::heap_decoder::{ColumnValue, CommittedTuple, DecodedHeap, DecodedTuple, HeapOp};
use walshadow::pipeline::batcher::{BatcherMsg, RoutedRow};
use walshadow::pipeline::{Fatal, tail};
use walshadow::shadow_catalog::{RelDescriptor, ReplIdent};

const RFN: RelFileNode = RelFileNode {
    spc_node: 1663,
    db_node: 5,
    rel_node: 16385,
};

fn on_path(bin: &str, arg: &str) -> bool {
    Command::new(bin)
        .arg(arg)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn skip() -> bool {
    if !on_path("clickhouse", "--version") {
        eprintln!("skip: no clickhouse binary on PATH");
        return true;
    }
    if !on_path("openssl", "version") {
        eprintln!("skip: no openssl binary on PATH");
        return true;
    }
    false
}

fn openssl(args: &[&std::ffi::OsStr]) {
    let status = Command::new("openssl")
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .expect("spawn openssl");
    assert!(status.success(), "openssl {:?} failed", &args[0]);
}

/// CA + a leaf server cert it signs. Returns (leaf cert PEM, leaf key
/// PEM, CA cert DER). Leaf is a proper end-entity cert (`CA:FALSE`,
/// `serverAuth` EKU, SAN `localhost`/`127.0.0.1`) so rustls accepts it;
/// pinning the CA DER as the sole root exercises real chain + hostname
/// verification rather than disabling it.
fn make_cert(dir: &Path) -> (PathBuf, PathBuf, Vec<u8>) {
    let p = |n: &str| dir.join(n);
    let oss = |path: &Path| path.as_os_str().to_owned();

    let ext = p("leaf.ext");
    std::fs::write(
        &ext,
        "basicConstraints=critical,CA:FALSE\n\
         keyUsage=critical,digitalSignature,keyEncipherment\n\
         extendedKeyUsage=serverAuth\n\
         subjectAltName=DNS:localhost,IP:127.0.0.1\n",
    )
    .unwrap();

    // Self-signed CA.
    openssl(&[
        "req".as_ref(),
        "-x509".as_ref(),
        "-newkey".as_ref(),
        "rsa:2048".as_ref(),
        "-nodes".as_ref(),
        "-days".as_ref(),
        "1".as_ref(),
        "-subj".as_ref(),
        "/CN=walshadow test CA".as_ref(),
        "-keyout".as_ref(),
        &oss(&p("ca.key")),
        "-out".as_ref(),
        &oss(&p("ca.pem")),
    ]);

    // Leaf key + CSR.
    openssl(&[
        "req".as_ref(),
        "-newkey".as_ref(),
        "rsa:2048".as_ref(),
        "-nodes".as_ref(),
        "-subj".as_ref(),
        "/CN=localhost".as_ref(),
        "-keyout".as_ref(),
        &oss(&p("key.pem")),
        "-out".as_ref(),
        &oss(&p("leaf.csr")),
    ]);

    // CA signs the leaf with the EE extensions.
    openssl(&[
        "x509".as_ref(),
        "-req".as_ref(),
        "-in".as_ref(),
        &oss(&p("leaf.csr")),
        "-CA".as_ref(),
        &oss(&p("ca.pem")),
        "-CAkey".as_ref(),
        &oss(&p("ca.key")),
        "-CAcreateserial".as_ref(),
        "-days".as_ref(),
        "1".as_ref(),
        "-extfile".as_ref(),
        &oss(&ext),
        "-out".as_ref(),
        &oss(&p("cert.pem")),
    ]);

    // CA DER for the client root store.
    openssl(&[
        "x509".as_ref(),
        "-in".as_ref(),
        &oss(&p("ca.pem")),
        "-outform".as_ref(),
        "DER".as_ref(),
        "-out".as_ref(),
        &oss(&p("ca.der")),
    ]);

    let der = std::fs::read(p("ca.der")).unwrap();
    (p("cert.pem"), p("key.pem"), der)
}

fn pick_ports() -> (u16, u16, u16, u16) {
    let socks = [
        TcpListener::bind(("127.0.0.1", 0)).unwrap(),
        TcpListener::bind(("127.0.0.1", 0)).unwrap(),
        TcpListener::bind(("127.0.0.1", 0)).unwrap(),
        TcpListener::bind(("127.0.0.1", 0)).unwrap(),
    ];
    let ports = (
        socks[0].local_addr().unwrap().port(),
        socks[1].local_addr().unwrap().port(),
        socks[2].local_addr().unwrap().port(),
        socks[3].local_addr().unwrap().port(),
    );
    drop(socks);
    ports
}

/// Spawned `clickhouse server` with a plaintext native port (readiness +
/// verifying queries) and a TLS native port (the emitter's path).
struct TlsChServer {
    child: Child,
    plain_port: u16,
    secure_port: u16,
    ca_der: Vec<u8>,
    _tmp: tempfile::TempDir,
}

impl TlsChServer {
    fn spawn() -> Self {
        let tmp = tempfile::tempdir().unwrap();
        let data_dir = tmp.path().join("ch");
        let log_dir = tmp.path().join("ch-logs");
        std::fs::create_dir_all(&data_dir).unwrap();
        std::fs::create_dir_all(&log_dir).unwrap();

        let (cert_pem, key_pem, ca_der) = make_cert(tmp.path());
        let (plain_port, secure_port, http_port, interserver_port) = pick_ports();

        let child = Command::new("clickhouse")
            .args([
                "server",
                "--",
                &format!("--tcp_port={plain_port}"),
                &format!("--tcp_port_secure={secure_port}"),
                &format!("--http_port={http_port}"),
                &format!("--interserver_http_port={interserver_port}"),
                "--mysql_port=",
                "--postgresql_port=",
                "--grpc_port=",
                "--prometheus.port=",
                "--listen_host=127.0.0.1",
                &format!("--path={}/", data_dir.display()),
                &format!("--logger.log={}/server.log", log_dir.display()),
                &format!("--logger.errorlog={}/error.log", log_dir.display()),
                "--logger.level=warning",
                &format!("--openSSL.server.certificateFile={}", cert_pem.display()),
                &format!("--openSSL.server.privateKeyFile={}", key_pem.display()),
                "--openSSL.server.verificationMode=none",
            ])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .process_group(0)
            .spawn()
            .expect("spawn clickhouse server");

        let server = Self {
            child,
            plain_port,
            secure_port,
            ca_der,
            _tmp: tmp,
        };
        server.wait_for_ready();
        server
    }

    fn wait_for_ready(&self) {
        let start = Instant::now();
        let addr = format!("127.0.0.1:{}", self.plain_port);
        while start.elapsed() < Duration::from_secs(60) {
            if TcpStream::connect_timeout(&addr.parse().unwrap(), Duration::from_millis(200))
                .is_ok()
                && self.query("SELECT 1").is_ok()
            {
                return;
            }
            std::thread::sleep(Duration::from_millis(200));
        }
        panic!("clickhouse server did not become ready");
    }

    /// Plaintext-port query via `clickhouse client` — readiness + result
    /// verification. The emitter never uses this port.
    fn query(&self, sql: &str) -> io::Result<String> {
        let out = Command::new("clickhouse")
            .args([
                "client",
                "--host",
                "127.0.0.1",
                "--port",
                &self.plain_port.to_string(),
                "--query",
                sql,
            ])
            .output()?;
        if !out.status.success() {
            return Err(io::Error::other(format!(
                "clickhouse query failed: {sql}, stderr={}",
                String::from_utf8_lossy(&out.stderr)
            )));
        }
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    }

    /// rustls config pinning the test CA as the sole root.
    fn pinned_config(&self) -> Arc<rustls::ClientConfig> {
        let mut roots = rustls::RootCertStore::empty();
        roots
            .add(rustls::pki_types::CertificateDer::from(self.ca_der.clone()))
            .unwrap();
        tls::config_with_roots(roots)
    }
}

impl Drop for TlsChServer {
    fn drop(&mut self) {
        // Single foreground process under its own group; SIGKILL the PID
        // and reap. Mirrors the clickhouse-c-rs TLS harness.
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn rel_descriptor() -> RelDescriptor {
    RelDescriptor {
        rfn: RFN,
        oid: 16385,
        namespace_oid: 2200,
        namespace_name: "public".into(),
        name: "things".into(),
        qualified_name: RelDescriptor::build_qualified_name("public", "things"),
        kind: 'r',
        persistence: 'p',
        replident: ReplIdent::Default { pk_attnums: None },
        attributes: vec![],
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn emitter_tls_round_trip() {
    if skip() {
        return;
    }

    let ch = TlsChServer::spawn();
    ch.query("CREATE DATABASE IF NOT EXISTS walshadow_test")
        .expect("create db");
    ch.query(
        "CREATE OR REPLACE TABLE walshadow_test.things (\
            id Int32,\
            name Nullable(String),\
            _lsn UInt64,\
            _xid UInt32,\
            _commit_ts DateTime64(6, 'UTC'), _is_deleted Bool\
         ) ENGINE = ReplacingMergeTree(_lsn, _is_deleted) ORDER BY id",
    )
    .expect("create dest table");

    // Production secure path: host drives both TCP connect (localhost →
    // 127.0.0.1) and SNI/cert verification (cert SAN includes localhost).
    // tls_config pins the self-signed CA; default_config (public roots)
    // would reject it.
    let cfg = EmitterConfig {
        host: "localhost".into(),
        port: ch.secure_port,
        database: "walshadow_test".into(),
        secure: true,
        tls_config: Some(ch.pinned_config()),
        compression: CompressionChoice::Lz4,
        ..Default::default()
    };
    // The batcher builds its plan from the `RoutedRow`'s mapping.
    let rel = Arc::new(rel_descriptor());
    let mapping = Arc::new(TableMapping {
        target: "walshadow_test.things".into(),
        columns: vec![
            ColumnMapping {
                src_attnum: 1,
                target_name: "id".into(),
                target_type: "Int32".into(),
            },
            ColumnMapping {
                src_attnum: 2,
                target_name: "name".into(),
                target_type: "Nullable(String)".into(),
            },
        ],
    });

    // Every inserter in the pool connects via `connect_client` →
    // `AsyncClient::connect_tls`, so spawning the tail exercises the secure
    // path the same way the WAL/bootstrap producers do.
    let stats = Arc::new(EmitterStats::default());
    let emitter_ack = Arc::new(AtomicU64::new(0));
    let fatal = Fatal::new();
    let (msg_tx, ack, tail_parts) = tail::spawn(&cfg, 1, stats, emitter_ack, fatal.clone())
        .await
        .expect("spawn tail over TLS");

    let tuple = CommittedTuple {
        decoded: DecodedHeap {
            rfn: RFN,
            xid: 7,
            source_lsn: 0x2000,
            op: HeapOp::Insert,
            new: Some(DecodedTuple {
                columns: vec![
                    Some(ColumnValue::Int4(1)),
                    Some(ColumnValue::Text("over-tls".into())),
                ],
                partial: false,
            }),
            old: None,
        },
        commit_ts: 1_000_000,
        commit_lsn: 0xABCD,
    };
    ack.register(0, tuple.commit_lsn);
    msg_tx
        .send(BatcherMsg::Row(RoutedRow {
            seq: 0,
            rel,
            mapping,
            committed: tuple,
        }))
        .await
        .expect("route row");
    ack.placed(0, 1);
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    msg_tx
        .send(BatcherMsg::FlushAll(reply_tx))
        .await
        .expect("send flush");
    reply_rx.await.expect("flush ack");
    ack.wait_through(1).await;
    drop(msg_tx);
    drop(ack);
    tail_parts.join().await;
    assert!(fatal.message().is_none(), "no fatal: {:?}", fatal.message());

    let row = ch
        .query("SELECT id, name FROM walshadow_test.things FINAL WHERE id = 1")
        .expect("ch select");
    assert_eq!(row, "1\tover-tls");
}
