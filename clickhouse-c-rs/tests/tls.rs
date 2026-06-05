//! TLS coverage (feature `tls`) against a spawned `clickhouse server`
//! listening on a secure native port with a self-signed cert.
//!
//! The cert is generated with `openssl` and pinned into the rustls root
//! store, so verification (chain + SNI hostname `localhost`) is exercised
//! for real rather than disabled. Skips when `clickhouse` or `openssl` is
//! not on PATH.
//!
//! Covers both the blocking `Client` over `tls::TlsIo` and the async
//! `AsyncClient::connect_tls`.

use std::io;
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use clickhouse_c::tls::{self, rustls};
use clickhouse_c::{Allocator, AsyncClient, Client, ClientOpts, Event};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn on_path(bin: &str, arg: &str) -> bool {
    Command::new(bin)
        .arg(arg)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn openssl(args: &[&std::ffi::OsStr]) -> TestResult {
    let status = Command::new("openssl")
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()?;
    if !status.success() {
        return Err(io::Error::other(format!("openssl {:?} failed", &args[0])).into());
    }
    Ok(())
}

/// Generate a CA + a leaf server cert it signs. Returns (leaf cert PEM,
/// leaf key PEM, CA cert DER). The leaf is a proper end-entity cert
/// (`CA:FALSE`, `serverAuth` EKU, SAN `localhost`/`127.0.0.1`) so rustls
/// accepts it; pinning the CA DER as the sole root exercises real chain +
/// hostname verification rather than disabling it.
fn make_cert(dir: &Path) -> TestResult<(PathBuf, PathBuf, Vec<u8>)> {
    let p = |n: &str| dir.join(n);
    let oss = |path: &Path| path.as_os_str().to_owned();

    // Leaf extensions written to a config file (no shell process subst).
    let ext = p("leaf.ext");
    std::fs::write(
        &ext,
        "basicConstraints=critical,CA:FALSE\n\
         keyUsage=critical,digitalSignature,keyEncipherment\n\
         extendedKeyUsage=serverAuth\n\
         subjectAltName=DNS:localhost,IP:127.0.0.1\n",
    )?;

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
        "/CN=clickhouse-c-rs test CA".as_ref(),
        "-keyout".as_ref(),
        &oss(&p("ca.key")),
        "-out".as_ref(),
        &oss(&p("ca.pem")),
    ])?;

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
    ])?;

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
    ])?;

    // CA DER for the client root store.
    openssl(&[
        "x509".as_ref(),
        "-in".as_ref(),
        &oss(&p("ca.pem")),
        "-outform".as_ref(),
        "DER".as_ref(),
        "-out".as_ref(),
        &oss(&p("ca.der")),
    ])?;

    let der = std::fs::read(p("ca.der"))?;
    Ok((p("cert.pem"), p("key.pem"), der))
}

struct ChServer {
    child: Child,
    secure_port: u16,
    _tmp: tempfile::TempDir,
}

impl ChServer {
    async fn spawn() -> TestResult<Self> {
        let tmp = tempfile::tempdir()?;
        let data_dir = tmp.path().join("ch");
        let log_dir = tmp.path().join("ch-logs");
        std::fs::create_dir_all(&data_dir)?;
        std::fs::create_dir_all(&log_dir)?;

        let (cert_pem, key_pem, _der) = make_cert(tmp.path())?;
        let (tcp_port, secure_port, http_port, interserver_port) = pick_ports()?;

        let mut cmd = Command::new("clickhouse");
        cmd.args([
            "server",
            "--",
            &format!("--tcp_port={tcp_port}"),
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
        .stderr(Stdio::null());
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            cmd.process_group(0);
        }
        let child = cmd.spawn()?;

        let server = Self {
            child,
            secure_port,
            _tmp: tmp,
        };
        server.wait_for_ready(tcp_port).await?;
        Ok(server)
    }

    async fn wait_for_ready(&self, plain_port: u16) -> TestResult {
        let start = Instant::now();
        let addr = format!("127.0.0.1:{plain_port}");
        while start.elapsed() < Duration::from_secs(60) {
            if TcpStream::connect_timeout(&addr.parse()?, Duration::from_millis(200)).is_ok()
                && self.query(plain_port, "SELECT 1").is_ok()
            {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        Err(io::Error::other("clickhouse server did not become ready").into())
    }

    fn query(&self, plain_port: u16, sql: &str) -> TestResult<String> {
        let out = Command::new("clickhouse")
            .args([
                "client",
                "--host",
                "127.0.0.1",
                "--port",
                &plain_port.to_string(),
                "--query",
                sql,
            ])
            .output()?;
        if !out.status.success() {
            return Err(io::Error::other(format!(
                "clickhouse query failed: {sql}, stderr={}",
                String::from_utf8_lossy(&out.stderr)
            ))
            .into());
        }
        Ok(String::from_utf8_lossy(&out.stdout).trim().to_string())
    }
}

impl Drop for ChServer {
    fn drop(&mut self) {
        // SIGKILL the server PID directly. `clickhouse server` runs as one
        // foreground process, so killing the PID (its threads die with it)
        // is enough; reap to avoid a zombie. A bare shell `kill -KILL
        // -<pgid>` misparses the negative arg, leaving the server alive and
        // wedging child.wait().
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

fn pick_ports() -> io::Result<(u16, u16, u16, u16)> {
    let socks = [
        TcpListener::bind(("127.0.0.1", 0))?,
        TcpListener::bind(("127.0.0.1", 0))?,
        TcpListener::bind(("127.0.0.1", 0))?,
        TcpListener::bind(("127.0.0.1", 0))?,
    ];
    let ports = (
        socks[0].local_addr()?.port(),
        socks[1].local_addr()?.port(),
        socks[2].local_addr()?.port(),
        socks[3].local_addr()?.port(),
    );
    drop(socks);
    Ok(ports)
}

/// rustls config pinning the test CA as the sole root.
fn pinned_config(server: &ChServer) -> TestResult<Arc<rustls::ClientConfig>> {
    let der = std::fs::read(server._tmp.path().join("ca.der"))?;
    let mut roots = rustls::RootCertStore::empty();
    roots.add(rustls::pki_types::CertificateDer::from(der))?;
    Ok(tls::config_with_roots(roots))
}

fn skip() -> bool {
    if !on_path("clickhouse", "--version") {
        eprintln!("clickhouse binary not found, skipping");
        return true;
    }
    if !on_path("openssl", "version") {
        eprintln!("openssl binary not found, skipping");
        return true;
    }
    false
}

#[tokio::test(flavor = "current_thread")]
async fn async_tls_roundtrip() -> TestResult {
    if skip() {
        return Ok(());
    }
    let server = ChServer::spawn().await?;
    let config = pinned_config(&server)?;

    let mut client = AsyncClient::connect_tls(
        ("127.0.0.1", server.secure_port),
        "localhost",
        ClientOpts::new(),
        None,
        config,
    )
    .await?;
    assert!(client.server_info().is_some());

    client.send_query("SELECT toUInt64(42) AS x", None).await?;
    let mut got = None;
    loop {
        match client.recv_event().await? {
            Event::Data(block) => {
                if block.n_rows() == 1 {
                    let (_, bytes) = block.column(0).and_then(|c| c.fixed()).expect("x col");
                    got = Some(u64::from_le_bytes(bytes[..8].try_into().unwrap()));
                }
            }
            Event::EndOfStream => break,
            Event::Exception(e) => return Err(e.into()),
            _ => {}
        }
    }
    assert_eq!(got, Some(42));
    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn sync_tls_roundtrip() -> TestResult {
    if skip() {
        return Ok(());
    }
    let server = ChServer::spawn().await?;
    let config = pinned_config(&server)?;

    // Blocking Client over TlsIo. No `.await` between connect and drain,
    // so the !Sync client never crosses an await point.
    let tcp = TcpStream::connect(("127.0.0.1", server.secure_port))?;
    tcp.set_nodelay(true).ok();
    let io = tls::TlsIo::connect(tcp, "localhost", config)?;
    let mut client = Client::init(&ClientOpts::new(), Allocator::stdlib(), io, None)?;
    assert!(client.server_info().is_some());

    client.send_query("SELECT toUInt64(42) AS x", None)?;
    let mut got = None;
    loop {
        match client.recv_event()? {
            Event::Data(block) => {
                if block.n_rows() == 1 {
                    let (_, bytes) = block.column(0).and_then(|c| c.fixed()).expect("x col");
                    got = Some(u64::from_le_bytes(bytes[..8].try_into().unwrap()));
                }
            }
            Event::EndOfStream => break,
            Event::Exception(e) => return Err(e.into()),
            _ => {}
        }
    }
    assert_eq!(got, Some(42));
    Ok(())
}
