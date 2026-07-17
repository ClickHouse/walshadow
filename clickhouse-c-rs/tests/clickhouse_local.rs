//! Smoke test that round-trips a built block through `clickhouse local`.
//!
//! Skipped when `clickhouse` is not on PATH so CI without it stays green.

use std::io::Read;
use std::os::fd::AsFd;
use std::process::{Command, Stdio};

use clickhouse_c::{
    Allocator, BlockBuilder, BlockOpts, BlockReader, ColumnBuilder, ColumnLayout, Kind, PosixIo,
    TypeAst,
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
    let bytes: Vec<u8> = data.iter().flat_map(|v| v.to_le_bytes()).collect();

    let col = ColumnBuilder::fixed(&bytes, ty.view().elem_size(), data.len()).expect("fixed");
    let mut bb = BlockBuilder::new();
    bb.append("x", ty.view(), &col).expect("append");
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

/// Round-trip a `Tuple(a UInt32, b String)` column through `clickhouse
/// local`: our `ColumnBuilder::tuple` bytes must be wire-compatible enough
/// for CH to accept them, and CH's re-emitted Native must decode back through
/// `Column::tuple_child`. Named-tuple field names survive both directions.
#[test]
fn write_then_read_back_tuple() {
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
            "t Tuple(a UInt32, b String)",
            "--format",
            "Native",
            "--output_format_native_encode_types_in_binary_format=0",
            "-q",
            "SELECT t FROM table ORDER BY t.1",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn clickhouse local");

    let stdin = child.stdin.take().expect("stdin piped");
    let stdout = child.stdout.take().expect("stdout piped");

    let ty = TypeAst::parse("Tuple(a UInt32, b String)", alloc).expect("tuple type");
    let a: [u32; 3] = [10, 20, 30];
    let a_bytes: Vec<u8> = a.iter().flat_map(|v| v.to_le_bytes()).collect();
    let a_col = ColumnBuilder::fixed(&a_bytes, 4, a.len()).expect("uint32 leaf");
    let (b_offsets, b_data) = string_column(&["x", "yy", "zzz"]);
    let b_col = ColumnBuilder::string(&b_offsets, &b_data, 3).expect("string leaf");

    // Tuple aliases a scratch `*mut chc_column` array; keep it and the child
    // nodes borrowed until the write.
    let children = [a_col, b_col];
    let mut ptrs = [std::ptr::null_mut(); 2];
    let tuple = ColumnBuilder::tuple(&children, &mut ptrs).expect("tuple");

    let mut io = PosixIo::new(stdin.as_fd());
    let mut bb = BlockBuilder::new();
    bb.append("t", ty.view(), &tuple).expect("append tuple");
    bb.write(io.as_mut(), BlockOpts::default()).expect("write");
    drop(io);
    drop(stdin); // EOF for the child

    // Decode CH's re-emitted Native off stdout. The block is tiny, so writing
    // it all before reading cannot deadlock the pipe.
    let mut read_io = PosixIo::new(stdout.as_fd());
    let mut reader =
        BlockReader::new(read_io.as_mut(), alloc, BlockOpts::default()).expect("reader");

    let mut a_out: Vec<u32> = Vec::new();
    let mut b_out: Vec<String> = Vec::new();
    while let Some(block) = reader.read().expect("read block") {
        if block.n_rows() == 0 {
            continue;
        }
        assert_eq!(block.n_columns(), 1);

        let col_ty = block.column_type(0).expect("column type");
        assert_eq!(col_ty.kind(), Some(Kind::Tuple));
        assert_eq!(col_ty.tuple_field_name(0), Some(&b"a"[..]));
        assert_eq!(col_ty.tuple_field_name(1), Some(&b"b"[..]));

        let col = block.column(0).expect("tuple column");
        assert!(matches!(col.layout(), Some(ColumnLayout::Tuple)));
        assert_eq!(col.tuple_arity(), 2);

        let a_child = col.tuple_child(0).expect("tuple child 0");
        let (es, bytes) = a_child.fixed().expect("uint32 child");
        assert_eq!(es, 4);
        for row in 0..block.n_rows() {
            let p = &bytes[row * es..(row + 1) * es];
            a_out.push(u32::from_le_bytes(p.try_into().unwrap()));
        }

        let b_child = col.tuple_child(1).expect("tuple child 1");
        let (offsets, data) = b_child.string().expect("string child");
        for row in 0..block.n_rows() {
            let start = if row == 0 {
                0
            } else {
                offsets[row - 1] as usize
            };
            let end = offsets[row] as usize;
            b_out.push(String::from_utf8(data[start..end].to_vec()).unwrap());
        }
    }

    drop(reader);
    drop(read_io);
    drop(stdout);
    let status = child.wait().expect("wait");
    assert!(status.success(), "clickhouse local exit: {status:?}");

    assert_eq!(a_out, vec![10, 20, 30]);
    assert_eq!(b_out, vec!["x", "yy", "zzz"]);
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
