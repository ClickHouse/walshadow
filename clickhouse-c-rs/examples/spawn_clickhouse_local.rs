//! Decode a `clickhouse local` query through clickhouse-c-rs.
//!
//! ```sh
//! cargo run --example spawn_clickhouse_local -- "SELECT number, toString(number*number) FROM numbers(5)"
//! ```

use std::io::Write;
use std::os::fd::AsFd;
use std::process::{Command, Stdio};

use clickhouse_c::{Allocator, Block, BlockOpts, BlockReader, ColumnLayout, Kind, PosixIo};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let query = std::env::args()
        .nth(1)
        .ok_or("usage: spawn_clickhouse_local <SQL>")?;

    let mut child = Command::new("clickhouse")
        .args([
            "local",
            "--format",
            "Native",
            "--output_format_native_encode_types_in_binary_format=0",
            "-q",
            &query,
        ])
        .stdout(Stdio::piped())
        .spawn()?;

    let stdout = child.stdout.take().expect("piped");

    let alloc = Allocator::stdlib();
    let mut io = PosixIo::new(stdout.as_fd());
    let opts = BlockOpts::default();

    let stdout_lock = std::io::stdout();
    let mut out = stdout_lock.lock();

    let mut reader = BlockReader::new(io.as_mut(), alloc, opts)?;
    while let Some(block) = reader.read()? {
        for row in 0..block.n_rows() {
            for col in 0..block.n_columns() {
                if col > 0 {
                    out.write_all(b"\t")?;
                }
                print_value(&mut out, &block, col, row)?;
            }
            out.write_all(b"\n")?;
        }
    }

    drop(reader);
    drop(io);
    drop(stdout);
    child.wait()?;
    Ok(())
}

fn print_value(out: &mut impl Write, block: &Block, col: usize, row: usize) -> std::io::Result<()> {
    let Some(c) = block.column(col) else {
        return Ok(());
    };
    let Some(t) = block.column_type(col) else {
        return Ok(());
    };

    let (layout, t, c) = if matches!(c.layout(), Some(ColumnLayout::Nullable)) {
        if c.null_map().map(|m| m[row] != 0).unwrap_or(false) {
            return out.write_all(b"\\N");
        }
        let inner_c = c.nullable_inner().expect("Nullable inner");
        let inner_t = t.child(0).expect("Nullable child type");
        (inner_c.layout(), inner_t, inner_c)
    } else {
        (c.layout(), t, c)
    };

    match layout {
        Some(ColumnLayout::Fixed) => {
            let (es, bytes) = c.fixed().expect("Fixed column");
            let p = &bytes[row * es..(row + 1) * es];
            match t.kind() {
                Some(Kind::Int8) => write!(out, "{}", p[0] as i8),
                Some(Kind::Int16) => write!(out, "{}", i16::from_le_bytes(p.try_into().unwrap())),
                Some(Kind::Int32) => write!(out, "{}", i32::from_le_bytes(p.try_into().unwrap())),
                Some(Kind::Int64) => write!(out, "{}", i64::from_le_bytes(p.try_into().unwrap())),
                Some(Kind::UInt8) => write!(out, "{}", p[0]),
                Some(Kind::UInt16) => write!(out, "{}", u16::from_le_bytes(p.try_into().unwrap())),
                Some(Kind::UInt32) => write!(out, "{}", u32::from_le_bytes(p.try_into().unwrap())),
                Some(Kind::UInt64) => write!(out, "{}", u64::from_le_bytes(p.try_into().unwrap())),
                Some(Kind::Float32) => {
                    write!(out, "{}", f32::from_le_bytes(p.try_into().unwrap()))
                }
                Some(Kind::Float64) => {
                    write!(out, "{}", f64::from_le_bytes(p.try_into().unwrap()))
                }
                Some(Kind::Bool) => out.write_all(if p[0] != 0 { b"true" } else { b"false" }),
                _ => write!(out, "<{}>", t.format()),
            }
        }
        Some(ColumnLayout::String) => {
            let (offsets, data) = c.string().expect("String column");
            let start = if row == 0 {
                0
            } else {
                offsets[row - 1] as usize
            };
            let end = offsets[row] as usize;
            out.write_all(&data[start..end])
        }
        _ => write!(out, "<{}>", t.format()),
    }
}
