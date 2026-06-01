//! Self-contained coverage for `Column`/`Block::validate` and
//! `PosixIo::set_read_timeout`, over a loopback TCP pair so no external
//! `clickhouse` binary is needed.

use std::net::{TcpListener, TcpStream};
use std::os::fd::AsFd;
use std::time::{Duration, Instant};

use clickhouse_c::{Allocator, Block, BlockBuilder, BlockOpts, ErrorKind, PosixIo, TypeAst};

/// Connected (writer, reader) TCP pair on loopback.
fn loopback_pair() -> (TcpStream, TcpStream) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
    let addr = listener.local_addr().expect("addr");
    let writer = TcpStream::connect(addr).expect("connect");
    let (reader, _) = listener.accept().expect("accept");
    (writer, reader)
}

#[test]
fn validate_accepts_roundtripped_block() {
    let alloc = Allocator::stdlib();
    let (writer, reader) = loopback_pair();

    let ty = TypeAst::parse("UInt32", alloc).expect("UInt32");
    let data: [u32; 4] = [1, 2, 3, 4];
    let bytes: &[u8] = unsafe {
        std::slice::from_raw_parts(data.as_ptr().cast::<u8>(), std::mem::size_of_val(&data))
    };

    let mut wio = PosixIo::new(writer.as_fd());
    let mut bb = BlockBuilder::new(alloc).expect("builder");
    bb.append_fixed("x", ty.view(), bytes, data.len())
        .expect("append");
    bb.write(wio.as_mut(), BlockOpts::default()).expect("write");
    // Block is tiny, fits the socket buffer; closing flushes it to the reader.
    drop(wio);
    drop(writer);

    let mut rio = PosixIo::new(reader.as_fd());
    let block = Block::read(rio.as_mut(), alloc, BlockOpts::default())
        .expect("read")
        .expect("a block");

    block.validate().expect("block validates");
    assert_eq!(block.n_columns(), 1);
    block
        .column(0)
        .expect("col 0")
        .validate()
        .expect("col validates");
}

#[test]
fn read_timeout_fires_on_idle_socket() {
    let alloc = Allocator::stdlib();
    // Keep the server end alive but silent: no bytes, no EOF.
    let (writer, reader) = loopback_pair();

    let mut rio = PosixIo::new(reader.as_fd());
    rio.as_mut()
        .set_read_timeout(Some(Duration::from_millis(50)));

    let start = Instant::now();
    let Err(err) = Block::read(rio.as_mut(), alloc, BlockOpts::default()) else {
        panic!("idle read must hit the deadline, not return a block/EOF");
    };
    let elapsed = start.elapsed();

    assert_eq!(err.kind, ErrorKind::Io, "got {err:?}");
    assert!(
        elapsed < Duration::from_secs(2),
        "read blocked past the deadline: {elapsed:?}"
    );

    // Clearing the deadline restores blocking semantics for later reads.
    rio.as_mut().set_read_timeout(None);
    drop(writer);
}
