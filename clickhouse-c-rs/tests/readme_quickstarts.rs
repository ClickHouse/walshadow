//! Verifies the three Quickstarts in README.md compile & run as advertised.
//!
//! Snippets are transcribed as literally as possible. Each test skips when
//! the external dependency (clickhouse binary, TCP server on :9000) is not
//! available so CI without them stays green.

use std::io::Read;
use std::net::TcpStream;
use std::os::fd::AsFd;
use std::process::{Command, Stdio};

use clickhouse_c::{
    Allocator, Block, BlockBuilder, BlockOpts, Client, ClientOpts, Codec, Compression, PacketKind,
    PosixIo, TypeAst,
};

fn clickhouse_on_path() -> bool {
    Command::new("clickhouse")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

// ---------------------------------------------------------------------------
// Quickstart 1: Decode `clickhouse local`'s stdout.
// ---------------------------------------------------------------------------

#[test]
fn quickstart_decode_clickhouse_local() -> Result<(), Box<dyn std::error::Error>> {
    if !clickhouse_on_path() {
        eprintln!("clickhouse binary not found, skipping");
        return Ok(());
    }

    // --- README snippet begins ---
    let mut child = Command::new("clickhouse")
        .args([
            "local",
            "--format",
            "Native",
            "--output_format_native_encode_types_in_binary_format=0",
            "-q",
            "SELECT number FROM numbers(5)",
        ])
        .stdout(Stdio::piped())
        .spawn()?;
    let stdout = child.stdout.take().unwrap();
    let mut io = PosixIo::new(stdout.as_fd());

    let alloc = Allocator::stdlib();
    let mut total_rows = 0usize;
    let mut saw_values: Vec<u64> = Vec::new();
    while let Some(block) = Block::read(io.as_mut(), alloc, BlockOpts::default())? {
        total_rows += block.n_rows();
        // README placeholder: block.n_rows(), block.column(i).fixed() / .string() / ...
        if let Some(c) = block.column(0)
            && let Some((es, bytes)) = c.fixed()
        {
            assert_eq!(es, 8, "numbers() is UInt64");
            for row in 0..block.n_rows() {
                let p = &bytes[row * es..(row + 1) * es];
                saw_values.push(u64::from_le_bytes(p.try_into().unwrap()));
            }
        }
    }
    drop(io);
    drop(stdout);
    child.wait()?;
    // --- README snippet ends ---

    assert_eq!(total_rows, 5);
    assert_eq!(saw_values, vec![0, 1, 2, 3, 4]);
    Ok(())
}

// ---------------------------------------------------------------------------
// Quickstart 2: Encode a block & feed it back in.
// ---------------------------------------------------------------------------

#[test]
fn quickstart_encode_block() -> Result<(), Box<dyn std::error::Error>> {
    if !clickhouse_on_path() {
        eprintln!("clickhouse binary not found, skipping");
        return Ok(());
    }

    // Read child stdout out-of-band so the test can assert on the result;
    // README snippet only needs stdin.
    let mut child = Command::new("clickhouse")
        .args([
            "local",
            "--input-format",
            "Native",
            "--structure",
            "x UInt32",
            "--format",
            "TSV",
            "-q",
            "SELECT sum(x) FROM table",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()?;
    let mut child_stdout = child.stdout.take().unwrap();

    // --- README snippet begins ---
    let stdin = child.stdin.take().unwrap();
    let mut io = PosixIo::new(stdin.as_fd());

    let alloc = Allocator::stdlib();
    let ty = TypeAst::parse("UInt32", alloc)?;
    let data: Vec<u32> = (0..1000).collect();
    let bytes: &[u8] = unsafe {
        core::slice::from_raw_parts(data.as_ptr().cast(), std::mem::size_of_val(&data[..]))
    };

    let mut bb = BlockBuilder::new(alloc)?;
    bb.append_fixed("x", ty.view(), bytes, data.len())?;
    bb.write(io.as_mut(), BlockOpts::default())?;
    drop(io);
    drop(stdin); // EOF for the child
    child.wait()?;
    // --- README snippet ends ---

    let mut buf = String::new();
    child_stdout.read_to_string(&mut buf)?;
    let sum: u64 = buf.trim_end_matches('\n').parse()?;
    assert_eq!(sum, (0u64..1000).sum::<u64>());

    Ok(())
}

// ---------------------------------------------------------------------------
// Quickstart 3: TCP client.
//
// Requires a ClickHouse server reachable at localhost:9000. Skips when not.
// ---------------------------------------------------------------------------

#[test]
fn quickstart_tcp_client() -> Result<(), Box<dyn std::error::Error>> {
    let sock = match TcpStream::connect("localhost:9000") {
        Ok(s) => s,
        Err(e) => {
            eprintln!("no ClickHouse on localhost:9000 ({e}), skipping");
            return Ok(());
        }
    };

    // --- README snippet begins ---
    let io = PosixIo::new_owned(sock);

    let codec = Codec::lz4(); // feature = "lz4" (default)
    let mut opts = ClientOpts::new()
        .database("default")
        .user("default")
        .password("");
    opts.compression = Compression::Lz4;

    let mut client = Client::init(&opts, Allocator::stdlib(), io, Some(codec))?;
    // --- README snippet ends ---

    // Bring up a table for the INSERT FORMAT Native path the README shows.
    client.send_query(
        "CREATE TABLE IF NOT EXISTS readme_qs_tcp(x UInt32) ENGINE = Memory",
        None,
    )?;
    drain(&mut client)?;

    // --- README snippet (INSERT) begins ---
    client.send_query("INSERT INTO readme_qs_tcp FORMAT Native", None)?;
    // send one or more data blocks via client.send_data(Some(&bb)),
    // then close the INSERT with the empty terminator:
    let alloc = Allocator::stdlib();
    let ty = TypeAst::parse("UInt32", alloc)?;
    let data: Vec<u32> = (0..10).collect();
    let bytes: &[u8] = unsafe {
        core::slice::from_raw_parts(data.as_ptr().cast(), std::mem::size_of_val(&data[..]))
    };
    let mut bb = BlockBuilder::new(alloc)?;
    bb.append_fixed("x", ty.view(), bytes, data.len())?;
    client.send_data(Some(&bb))?;
    drop(bb);
    client.send_data(None)?;

    loop {
        let mut pkt = client.recv_packet()?;
        match pkt.kind() {
            Some(PacketKind::EndOfStream) => break,
            Some(PacketKind::Exception) => {
                return Err(pkt.take_exception().unwrap().into());
            }
            _ => {}
        }
    }
    // --- README snippet ends ---

    client.send_query("DROP TABLE readme_qs_tcp", None)?;
    drain(&mut client)?;

    Ok(())
}

fn drain(client: &mut Client<'_>) -> Result<(), Box<dyn std::error::Error>> {
    loop {
        let mut pkt = client.recv_packet()?;
        match pkt.kind() {
            Some(PacketKind::EndOfStream) => return Ok(()),
            Some(PacketKind::Exception) => return Err(pkt.take_exception().unwrap().into()),
            _ => {}
        }
    }
}
