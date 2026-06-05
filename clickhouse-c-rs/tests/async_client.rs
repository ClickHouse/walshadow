//! Tokio async TCP client coverage over spawned `clickhouse server`.
//!
//! Skips when `clickhouse` is not on PATH.

use std::io;
use std::net::{TcpListener, TcpStream};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use clickhouse_c::{AsyncClient, Block, BlockBuilder, ClientOpts, Event, TypeAst};

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

fn clickhouse_on_path() -> bool {
    Command::new("clickhouse")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

struct ChServer {
    child: Child,
    port: u16,
    _tmp: tempfile::TempDir,
}

impl ChServer {
    async fn spawn() -> TestResult<Self> {
        let tmp = tempfile::tempdir()?;
        let data_dir = tmp.path().join("ch");
        let log_dir = tmp.path().join("ch-logs");
        std::fs::create_dir_all(&data_dir)?;
        std::fs::create_dir_all(&log_dir)?;

        let (tcp_port, http_port, interserver_port) = pick_ports()?;
        let mut cmd = Command::new("clickhouse");
        cmd.args([
            "server",
            "--",
            &format!("--tcp_port={tcp_port}"),
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
            port: tcp_port,
            _tmp: tmp,
        };
        server.wait_for_ready().await?;
        Ok(server)
    }

    async fn wait_for_ready(&self) -> TestResult {
        let start = Instant::now();
        let addr = format!("127.0.0.1:{}", self.port);
        while start.elapsed() < Duration::from_secs(60) {
            if TcpStream::connect_timeout(&addr.parse()?, Duration::from_millis(200)).is_ok()
                && self.query("SELECT 1").is_ok()
            {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        Err(io::Error::other("clickhouse server did not become ready").into())
    }

    fn query(&self, sql: &str) -> TestResult<String> {
        let out = Command::new("clickhouse")
            .args([
                "client",
                "--host",
                "127.0.0.1",
                "--port",
                &self.port.to_string(),
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
        let _ = self.query("SYSTEM SHUTDOWN");
        for _ in 0..50 {
            match self.child.try_wait() {
                Ok(Some(_)) => return,
                Ok(None) => std::thread::sleep(Duration::from_millis(100)),
                Err(_) => break,
            }
        }
        #[cfg(unix)]
        {
            let pgid = self.child.id() as i32;
            let _ = Command::new("kill")
                .args(["-KILL", &format!("-{pgid}")])
                .stderr(Stdio::null())
                .status();
        }
        #[cfg(not(unix))]
        {
            let _ = self.child.kill();
        }
        let _ = self.child.wait();
    }
}

fn pick_ports() -> io::Result<(u16, u16, u16)> {
    let tcp = TcpListener::bind(("127.0.0.1", 0))?;
    let http = TcpListener::bind(("127.0.0.1", 0))?;
    let interserver = TcpListener::bind(("127.0.0.1", 0))?;
    let ports = (
        tcp.local_addr()?.port(),
        http.local_addr()?.port(),
        interserver.local_addr()?.port(),
    );
    drop((tcp, http, interserver));
    Ok(ports)
}

async fn connect(server: &ChServer) -> clickhouse_c::Result<AsyncClient> {
    AsyncClient::connect(("127.0.0.1", server.port), ClientOpts::new(), None).await
}

async fn drain(client: &mut AsyncClient) -> TestResult {
    loop {
        match client.recv_event().await? {
            Event::EndOfStream => return Ok(()),
            Event::Exception(e) => return Err(boxed(e)),
            _ => {}
        }
    }
}

#[tokio::test(flavor = "current_thread")]
async fn async_insert_select_roundtrip() -> TestResult {
    if !clickhouse_on_path() {
        eprintln!("clickhouse binary not found, skipping");
        return Ok(());
    }

    let server = ChServer::spawn().await?;
    let mut client = connect(&server).await?;
    assert!(client.server_info().is_some());

    client
        .send_query(
            "CREATE TABLE async_roundtrip (id Int32, name String) ENGINE = Memory",
            None,
        )
        .await?;
    drain(&mut client).await?;

    client
        .send_query("INSERT INTO async_roundtrip FORMAT Native", None)
        .await?;
    let ids = [10i32, 20, 30];
    let id_bytes = unsafe {
        core::slice::from_raw_parts(ids.as_ptr().cast::<u8>(), std::mem::size_of_val(&ids))
    };
    let names = ["alpha", "beta", "gamma"];
    let (name_offsets, name_data) = string_column(&names);
    let alloc = clickhouse_c::Allocator::stdlib();
    let id_type = TypeAst::parse("Int32", alloc)?;
    let mut block = BlockBuilder::new(alloc)?;
    block.append_fixed("id", id_type.view(), id_bytes, ids.len())?;
    block.append_string("name", &name_offsets, &name_data, names.len())?;
    client.send_data(Some(&block)).await?;
    client.send_data_end().await?;
    drain(&mut client).await?;

    client
        .send_query("SELECT id, name FROM async_roundtrip ORDER BY id", None)
        .await?;
    let mut rows = Vec::new();
    loop {
        match client.recv_event().await? {
            Event::Data(block) => collect_rows(&block, &mut rows),
            Event::EndOfStream => break,
            Event::Exception(e) => return Err(boxed(e)),
            _ => {}
        }
    }

    assert_eq!(
        rows,
        vec![
            (10, "alpha".to_string()),
            (20, "beta".to_string()),
            (30, "gamma".to_string()),
        ]
    );

    Ok(())
}

#[tokio::test(flavor = "current_thread")]
async fn async_bad_sql_returns_exception() -> TestResult {
    if !clickhouse_on_path() {
        eprintln!("clickhouse binary not found, skipping");
        return Ok(());
    }

    let server = ChServer::spawn().await?;
    let mut client = connect(&server).await?;
    client
        .send_query("SELECT * FROM definitely_missing_async_table", None)
        .await?;

    loop {
        match client.recv_event().await? {
            Event::Exception(e) => {
                assert_ne!(e.code(), 0);
                assert!(!e.display_text().is_empty());
                return Ok(());
            }
            Event::EndOfStream => panic!("bad SQL ended without exception"),
            _ => {}
        }
    }
}

fn string_column(values: &[&str]) -> (Vec<u64>, Vec<u8>) {
    let mut offsets = Vec::with_capacity(values.len());
    let mut data = Vec::new();
    for value in values {
        data.extend_from_slice(value.as_bytes());
        offsets.push(data.len() as u64);
    }
    (offsets, data)
}

fn boxed<E>(e: E) -> Box<dyn std::error::Error>
where
    E: std::error::Error + 'static,
{
    Box::new(e)
}

fn collect_rows(block: &Block, rows: &mut Vec<(i32, String)>) {
    if block.n_rows() == 0 {
        return;
    }
    assert_eq!(block.n_columns(), 2);

    let (id_size, id_bytes) = block.column(0).and_then(|c| c.fixed()).expect("id column");
    assert_eq!(id_size, 4);

    let (name_offsets, name_data) = block
        .column(1)
        .and_then(|c| c.string())
        .expect("name column");

    for row in 0..block.n_rows() {
        let id_start = row * id_size;
        let id = i32::from_le_bytes(id_bytes[id_start..id_start + id_size].try_into().unwrap());
        let name_start = if row == 0 {
            0
        } else {
            name_offsets[row - 1] as usize
        };
        let name_end = name_offsets[row] as usize;
        rows.push((
            id,
            String::from_utf8(name_data[name_start..name_end].to_vec()).unwrap(),
        ));
    }
}
