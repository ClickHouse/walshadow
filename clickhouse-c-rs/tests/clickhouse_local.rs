//! Smoke test that round-trips a built block through `clickhouse local`.
//!
//! Skipped when `clickhouse` is not on PATH so CI without it stays green.

use std::io::Read;
use std::os::fd::AsFd;
use std::process::{Command, Stdio};

use clickhouse_c::{Allocator, BlockBuilder, BlockOpts, PosixIo, TypeAst};

fn clickhouse_on_path() -> bool {
    Command::new("clickhouse")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

#[test]
fn write_then_read_back_uint32() {
    if !clickhouse_on_path() {
        eprintln!("clickhouse binary not found, skipping");
        return;
    }

    let alloc = Allocator::stdlib();
    let mut child = Command::new("clickhouse")
        .args([
            "local",
            "--input-format",
            "Native",
            "--structure",
            "x UInt32",
            "--output_format_native_encode_types_in_binary_format=0",
            "-q",
            "SELECT sum(x) FROM table",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn clickhouse local");

    let stdin = child.stdin.take().expect("stdin piped");
    let mut stdout = child.stdout.take().expect("stdout piped");

    let mut io = PosixIo::new(stdin.as_fd());

    let ty = TypeAst::parse("UInt32", alloc).expect("UInt32");
    let data: [u32; 5] = [10, 20, 30, 40, 50];
    let bytes: &[u8] = unsafe {
        std::slice::from_raw_parts(data.as_ptr().cast::<u8>(), std::mem::size_of_val(&data))
    };

    let mut bb = BlockBuilder::new(alloc).expect("builder");
    bb.append_fixed("x", ty.view(), bytes, data.len())
        .expect("append");
    bb.write(io.as_mut(), BlockOpts::default()).expect("write");

    // Drop the borrowing PosixIo first, then the stdin handle, which
    // closes the pipe and lets clickhouse local see EOF.
    drop(io);
    drop(stdin);

    let mut buf = Vec::new();
    stdout.read_to_end(&mut buf).expect("read stdout");
    let status = child.wait().expect("wait");
    assert!(status.success(), "clickhouse local exit: {status:?}");
    // clickhouse local emits the SELECT result back as Native; for sanity
    // just confirm something came through and the SQL didn't error.
    assert!(!buf.is_empty(), "no stdout from clickhouse local");
}
