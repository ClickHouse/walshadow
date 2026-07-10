# clickhouse-c-rs

Rust bindings for [clickhouse-c], a header-only C client for the
ClickHouse Native wire format. Two entry points:

- raw block frames over any fd (TCP socket, pipe to `clickhouse local`)
- TCP packet loop (Hello / Query / Data / EOS / Exception / Progress)
  with optional LZ4 / ZSTD compression
- Tokio async TCP packet loop with feature `tokio`
- TLS (rustls) for both the blocking and async clients with feature `tls`

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

**Trusted base.** Soundness of every non-`unsafe` API in this crate is
conditional on `clickhouse-c` (at the vendored revision) holding the
invariants its headers document — chiefly that the `chc_column`-side
length counters (`n_rows`, `offsets.last()`, `name_len`, etc.) match
the buffer the same struct points at. Where the cross-check is
expressible in a line, `debug_assert!`s trip in debug builds; release
builds trust the C side. Bounds against the underlying allocation are
not checked because `clickhouse-c` exposes no buffer-capacity API.

**Allocators thread through every owning constructor.** `chc_alloc` is
a vtable. `Allocator` wraps it `Copy + Send + Sync`. `TypeAst` /
`Block` / `BlockBuilder` / `Client` each take an `Allocator` at
construction & store it; `Drop` calls the matching destroy with the
same allocator the C side used. `Client` boxes its `Allocator` so the
heap address the C side stashes in `c->al` stays valid through every
later call & through `chc_client_close`.

**No-copy columns.** `chc_block_builder_append_*` retains raw pointers
to caller-owned names and bytes for the builder lifetime. Mirrored as
`BlockBuilder<'a>`; each `append_*` takes `&'a str`, `&'a [u8]` /
`&'a [u64]` & each appended `TypeRef<'a>`. Caller keeps inputs alive
for `'a`.

**Self-referential C structs.** `chc_io` carries a pointer back into the
`chc_posix_io` state it was initialized from; `PosixIo` holds both inline
& lets `chc_posix_io_init` wire the back-pointer, so it is genuinely
pinned (`PhantomPinned`) — mirroring how `TlsIo` embeds a `chc_io` whose
`ud` points at its own rustls stream. `chc_codec` is addressed by
compression code calling into its function-pointer table, so `Codec` is
likewise pinned. All ship behind `Pin<Box<Self>>` & expose internals
through `Pin<&mut _>` / `Pin<&_>`: `PosixIo` for ownership-passing into
[`Client`] / [`Block`] / [`BlockBuilder`], `Codec` because it must not
move. `Codec::raw_mut` is `unsafe`: caller must
populate the function-pointer table to match the [`Compression`] the
codec is paired with.

**`Client` owns its I/O + codec.** `chc_client` stashes raw pointers
to `chc_io` & `chc_codec` for the connection's lifetime; using
borrowed references would let safe code drop them out from under the C
side. `Client::init` takes `Pin<Box<PosixIo<'fd>>>` &
`Option<Pin<Box<Codec>>>` by ownership so the back-pointers stay valid
through `Drop`. `Client<'fd>` carries the fd lifetime; constructed via
`PosixIo::new(fd.as_fd())` it ties the client to a borrowed fd, or via
`PosixIo::new_owned(fd_owner)` it takes the fd and closes it on drop.

**C-side strings.** `chc_err.msg` is a fixed-size char buffer;
`Error::from_raw` copies it through `from_utf8_lossy` because the C
struct goes out of scope at the call boundary. `chc_exception` is a
heap chain in the C allocator; [`Exception`] is a thin owning wrapper
over the head pointer, accessors return `&[u8]` borrowed from C
memory, and `Drop` calls `chc_exception_free` to walk & release the
chain. Server-controlled text accessors on `Block` / `TypeRef`
(`column_name`, `name`, `timezone`, `enum_at`, `tuple_field_name`)
likewise return `&[u8]` so the UTF-8 question stays at the
consumer; `TypeRef::format` is the one place a `String` is materialized
& uses `from_utf8_lossy`.

**Packet payloads alias a union.** `chc_packet` is a `kind` tag plus a
`payload` union — `block`, `exception`, `progress` and `profile` share
one slot, mirroring the C header. Exactly one arm is live, selected by
`kind`; reading any other is UB. `chc_packet_payload` therefore makes
every read `unsafe`, and a single reader — `Event::from_raw`, shared by
the blocking `Client` and the async client — converts a recv'd packet
into an owned `Event`, reading each arm only inside its `kind` match. A
new `chc_packet` member must be a union arm, never a parallel struct
field: a field laid out past the union's offset reads zero for every
packet, silently turning exception payloads into NULL.

**Send / Sync.** Every owning handle is `Send`; none are `Sync`. Each
`chc_client` is single-threaded upstream; block & builder objects
follow. `Allocator` is the only `Sync` type (stateless function-pointer
vtable). `AsyncClient` (feature `tokio`) is `Send` too, and its method
futures stay `Send` because no raw FFI pointer is held across an
`.await` — each `chc_async_*` call resolves the C-owned slice or pointer
in a tight scope and awaits only on the copied `&[u8]`.

## Quickstart

### Decode `clickhouse local`'s stdout

```rust
use clickhouse_c::{Allocator, Block, BlockOpts, PosixIo};
use std::os::fd::AsFd;
use std::process::{Command, Stdio};

let mut child = Command::new("clickhouse")
    .args(["local", "--format", "Native",
           "--output_format_native_encode_types_in_binary_format=0",
           "-q", "SELECT number FROM numbers(5)"])
    .stdout(Stdio::piped())
    .spawn()?;
let stdout = child.stdout.take().unwrap();
let mut io = PosixIo::new(stdout.as_fd());

let alloc = Allocator::stdlib();
while let Some(block) = Block::read(io.as_mut(), alloc, BlockOpts::default())? {
    // block.n_rows(), block.column(i).fixed() / .string() / ...
}
drop(io);
drop(stdout);     // close pipe
child.wait()?;
```

`clickhouse local` emits Native without `BlockInfo` or
`has_custom_serialization`, so `BlockOpts::default()` is correct. TCP
needs both flags depending on negotiated server revision.

### Encode a block & feed it back in

```rust
use clickhouse_c::{Allocator, BlockBuilder, BlockOpts, PosixIo, TypeAst};
use std::os::fd::AsFd;
use std::process::{Command, Stdio};

let mut child = Command::new("clickhouse")
    .args(["local", "--input-format", "Native", "--structure", "x UInt32",
           "-q", "SELECT sum(x) FROM table"])
    .stdin(Stdio::piped())
    .spawn()?;
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
drop(stdin);      // EOF for the child
child.wait()?;
```

ClickHouse Native is little-endian on the wire & `append_fixed`
expects LE bytes. Big-endian hosts swap before append.

### TCP client

```rust
use clickhouse_c::{Allocator, Client, ClientOpts, Codec, Compression, Event, PosixIo};
use std::net::TcpStream;

let sock = TcpStream::connect("localhost:9000")?;
// `Client` will own the fd through `PosixIo::new_owned` and close it
// on drop. For a borrowed-fd variant, keep `sock` in scope and pass
// `PosixIo::new(sock.as_fd())` — `Client<'_>` then borrows from `sock`.
let io = PosixIo::new_owned(sock);

let codec = Codec::lz4();        // feature = "lz4" (default)
let mut opts = ClientOpts::new()
    .database("default")
    .user("default")
    .password("");
opts.compression = Compression::Lz4;

let mut client = Client::init(&opts, Allocator::stdlib(), io, Some(codec))?;
// Refresh before each blocking operation to apply a fresh absolute deadline:
// client.set_read_timeout(Some(std::time::Duration::from_secs(30)))?;

client.send_query("INSERT INTO t FORMAT Native", None)?;
// send one or more data blocks via client.send_data(Some(&bb)),
// then close the INSERT with the empty terminator:
client.send_data(None)?;

loop {
    match client.recv_event()? {
        Event::EndOfStream => break,
        Event::Exception(exc) => return Err(exc.into()),
        _ => {}
    }
}
```

### TLS (feature `tls`)

rustls verifies the peer against `tls::default_config()` (Mozilla webpki
roots, no client auth). `rustls` is re-exported as `clickhouse_c::tls::rustls`
so callers can build a bespoke `ClientConfig` (private CA, mTLS) and pass it
in. The native secure port is `9440`.

Async — wraps the `tokio::net::TcpStream` in a TLS stream (also needs
feature `tokio`):

```rust,ignore
use clickhouse_c::{AsyncClient, ClientOpts};

let mut client = AsyncClient::connect_tls(
    ("myhost.clickhouse.cloud", 9440),
    "myhost.clickhouse.cloud",          // SNI + cert hostname
    ClientOpts::new().user("default").password("…"),
    None,                               // or Some(Codec::lz4())
    clickhouse_c::tls::default_config(),
).await?;
```

Blocking — `tls::TlsIo` is a `ClientIo` backend over an owned `TcpStream`;
hand it to the same `Client::init` the plaintext path uses:

```rust,ignore
use clickhouse_c::{Allocator, Client, ClientOpts, tls};
use std::net::TcpStream;

let tcp = TcpStream::connect(("myhost.clickhouse.cloud", 9440))?;
tcp.set_nodelay(true).ok();
let io = tls::TlsIo::connect(tcp, "myhost.clickhouse.cloud", tls::default_config())?;
let mut client = Client::init(
    &ClientOpts::new().user("default").password("…"),
    Allocator::stdlib(),
    io,
    None,
)?;
```

## Feature flags

| Feature | Default | Effect |
|---|---|---|
| `lz4`   | on      | compile clickhouse-compression.h's LZ4 wrapper, link `-llz4`, expose `Codec::lz4()` |
| `tls`   | off     | rustls TLS: `tls::TlsIo` backend for the blocking `Client`, `AsyncClient::connect_tls`, `tls::default_config()` (webpki roots) |
| `tokio` | off     | expose `AsyncClient` over `tokio::net::TcpStream` |
| `zstd`  | off     | compile clickhouse-compression.h's ZSTD wrapper, link `-lzstd`, expose `Codec::zstd()` |

`tls` pulls in `rustls` + `webpki-roots` (+ `tokio-rustls` for the async
path). Async TLS needs both `tls` and `tokio`.

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
- TLS beyond rustls — the `tls` feature ships a rustls backend
  (`tls::TlsIo` / `AsyncClient::connect_tls`); for a different stack the
  caller can still drive OpenSSL through a custom `chc_io`
  (`clickhouse-openssl.h`) or hand `connect_tls` a bespoke
  `rustls::ClientConfig`
- Threading — each `Client` is single-threaded, matching upstream
- Runtime-neutral Rust async — `AsyncClient` is Tokio-native; custom
  event loops can drive `chc_async_*` through `sys`
- `Variant` / `Dynamic` / `JSON` / `AggregateFunction` decoding —
  upstream excludes from v1 (25.x / 26.x wire format still shifting).
  `BlockBuilder::append_json_string` covers the STRING-serialization
  path for `JSON`

## License

Apache-2.0. Inherits clickhouse-c's license; see
`clickhouse-c/LICENSE`.
