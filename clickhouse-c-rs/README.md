# clickhouse-c-rs

Rust bindings for [clickhouse-c], a header-only C client for the
ClickHouse Native wire format. Two entry points:

- raw block frames over any fd (TCP socket, pipe to `clickhouse local`)
- TCP packet loop (Hello / Query / Data / EOS / Exception / Progress)
  with optional LZ4 / ZSTD compression

[clickhouse-c]: https://github.com/ClickHouse/clickhouse-c

## Architecture

1. **Vendored headers** under `clickhouse-c/`. Will become a git
   submodule. Override location with `CHC_INCLUDE_DIR=<path>`.
2. **`src/wrapper.c`** — single TU that `#define`s `CHC_IMPLEMENTATION`
   & includes each header the configured features select. `build.rs`
   compiles it via the `cc` crate into a static library. LZ4 / ZSTD
   link separately under their feature flags.
3. **`src/sys.rs`** — FFI declarations for every public symbol &
   struct from `clickhouse.h`, `clickhouse-posix-io.h`,
   `clickhouse-compression.h`, `clickhouse-client.h`, plus the
   feature-gated codec inits. Integer constants from `enum` blocks
   (`chc_kind`, `chc_col_kind`, `chc_compression`, `chc_packet_kind`,
   error codes) & a couple of `#define`s are scanned out of the
   headers by `build.rs` into `$OUT_DIR/sys_constants.rs` & pulled in
   via `include!`.
4. **Safe wrappers** in `src/{error,alloc,io,types,block,builder,codec,client}.rs`.
   Each owning C struct gets a Drop impl that calls the matching
   `chc_*_destroy` / `_close` / `_free`. Borrowed views ride lifetimes
   tied to their owner.

## Safety model

**Allocators thread through every owning constructor.** `chc_alloc` is
a vtable. `Allocator` wraps it `Copy + Send + Sync`. `TypeAst` /
`Block` / `BlockBuilder` / `Client` each take an `Allocator` at
construction & store it; `Drop` calls the matching destroy with the
same allocator the C side used.

**No-copy column slabs.** `chc_block_builder_append_*` retains raw
pointers to caller-owned bytes until `chc_block_write`. Mirrored as
`BlockBuilder<'a>`; each `append_*` takes `&'a [u8]` / `&'a [u64]` &
each appended `TypeRef<'a>`. Caller keeps slabs alive for `'a`.

**Self-referential C structs are pinned.** `chc_io` carries a pointer
back into the `chc_posix_io` state it was initialized from. `chc_codec`
is similarly addressed by code that calls into its function-pointer
table. `PosixIo` & `Codec` ship behind `Pin<Box<Self>>` & expose
internals only through `Pin<&mut _>` / `Pin<&_>`.

**C-side strings.** `chc_err.msg` is a fixed-size char buffer;
`Error::from_raw` copies it through `from_utf8_lossy` because the C
struct goes out of scope at the call boundary. `chc_exception` is a
heap chain in the C allocator; [`Exception`] is a thin owning wrapper
over the head pointer, accessors return `&[u8]` borrowed from C
memory, and `Drop` calls `chc_exception_free` to walk & release the
chain. Convert to `String` lossy at the consumer if needed.

**Send / Sync.** Every owning handle is `Send`; none are `Sync`. Each
`chc_client` is single-threaded upstream; block & builder objects
follow. `Allocator` is the only `Sync` type (stateless function-pointer
vtable).

## Quickstart

### Decode `clickhouse local`'s stdout

```rust
use clickhouse_c::{Allocator, Block, BlockOpts, PosixIo};
use std::os::fd::AsRawFd;
use std::process::{Command, Stdio};

let mut child = Command::new("clickhouse")
    .args(["local", "--format", "Native",
           "--output_format_native_encode_types_in_binary_format=0",
           "-q", "SELECT number FROM numbers(5)"])
    .stdout(Stdio::piped())
    .spawn()?;
let stdout = child.stdout.take().unwrap();
let mut io = PosixIo::new(stdout.as_raw_fd());

let alloc = Allocator::stdlib();
while let Some(block) = Block::read(io.as_mut(), alloc, BlockOpts::default())? {
    // block.n_rows(), block.column(i).fixed() / .string() / ...
}
drop(stdout);     // close pipe
child.wait()?;
```

`clickhouse local` emits Native without `BlockInfo` or
`has_custom_serialization`, so `BlockOpts::default()` is correct. TCP
needs both flags depending on negotiated server revision.

### Encode a block & feed it back in

```rust
use clickhouse_c::{Allocator, BlockBuilder, BlockOpts, PosixIo, TypeAst};
use std::os::fd::AsRawFd;
use std::process::{Command, Stdio};

let mut child = Command::new("clickhouse")
    .args(["local", "--input-format", "Native", "--structure", "x UInt32",
           "-q", "SELECT sum(x) FROM table"])
    .stdin(Stdio::piped())
    .spawn()?;
let stdin = child.stdin.take().unwrap();
let mut io = PosixIo::new(stdin.as_raw_fd());

let alloc = Allocator::stdlib();
let ty = TypeAst::parse("UInt32", alloc)?;
let data: Vec<u32> = (0..1000).collect();
let bytes: &[u8] = unsafe {
    core::slice::from_raw_parts(data.as_ptr().cast(), std::mem::size_of_val(&data[..]))
};

let mut bb = BlockBuilder::new(alloc)?;
bb.append_fixed("x", ty.view(), bytes, data.len())?;
bb.write(io.as_mut(), BlockOpts::default())?;
drop(stdin);      // EOF for the child
child.wait()?;
```

ClickHouse Native is little-endian on the wire & `append_fixed`
expects LE bytes. Big-endian hosts swap before append.

### TCP client

```rust
use clickhouse_c::{Allocator, Client, ClientOpts, Codec, Compression, PacketKind, PosixIo};
use std::net::TcpStream;
use std::os::fd::IntoRawFd;

let sock = TcpStream::connect("localhost:9000")?;
let fd = sock.into_raw_fd();
let mut io = PosixIo::new(fd);

let codec = Codec::lz4();        // feature = "lz4" (default)
let mut opts = ClientOpts::new()
    .database("default")
    .user("default")
    .password("");
opts.compression = Compression::Lz4;

let mut client = Client::init(&opts, Allocator::stdlib(),
                              io.as_mut(), Some(codec.as_ref()))?;

client.send_query("INSERT INTO t FORMAT Native", None)?;
// send one or more data blocks via client.send_data(Some(&bb)),
// then close the INSERT with the empty terminator:
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
```

## Feature flags

| Feature | Default | Effect |
|---|---|---|
| `lz4`   | on      | include `clickhouse-lz4.h`, link `-llz4`, expose `Codec::lz4()` |
| `zstd`  | off     | include `clickhouse-zstd.h`, link `-lzstd`, expose `Codec::zstd()` |

`default-features = false` for an uncompressed-only build with no
compression libs linked.

## Header vendoring

Headers live under `clickhouse-c/` so the crate builds straight from a
`git clone`. Pin against an out-of-tree checkout with:

```sh
CHC_INCLUDE_DIR=/abs/path/to/clickhouse-c cargo build
```

Submodule swap planned; the env-var override stays the canonical
escape hatch.

## Non-goals

Mirrors upstream's list plus Rust-specific items:

- HTTP — wrap libcurl or a Rust HTTP client
- DNS, endpoint round-robin, pooling, retry / backoff — caller-driven;
  `PosixIo` only wraps a connected fd
- TLS — caller drives OpenSSL / rustls & feeds bytes through a custom
  `chc_io`. `clickhouse-openssl.h` not wired into the Rust layer yet
- Threading — each `Client` is single-threaded, matching upstream
- Async I/O — `chc_io.read` / `.write` are called synchronously; wrap
  the fd in a blocking transport
- `Variant` / `Dynamic` / `JSON` / `AggregateFunction` decoding —
  upstream excludes from v1 (25.x / 26.x wire format still shifting).
  `BlockBuilder::append_json_string` covers the STRING-serialization
  path for `JSON`

## License

Apache-2.0. Inherits clickhouse-c's license; see
`clickhouse-c/LICENSE`.
