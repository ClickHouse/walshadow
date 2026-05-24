# safety — clickhouse-c-rs trust boundary

Soundness contract for the FFI surface walshadow's CH emitter consumes.
Authoritative copy of the contract lives in
[`clickhouse-c-rs/README.md`](../clickhouse-c-rs/README.md) "Safety
model"; this doc summarizes for walshadow callers.

## Purpose

[`clickhouse-c-rs`](../clickhouse-c-rs/) wraps ClickHouse's Native C
client. Public non-`unsafe` API must be sound under arbitrary safe-code
use, modulo one documented trust boundary at `clickhouse-c` itself.
walshadow's emitter ([emitter.md](emitter.md)) is the only consumer
today; this doc enumerates the invariants the FFI side requires of
Rust callers so emitter changes don't silently re-open the holes the
UNSAFE landing closed.

## Trust boundary

Safe for Rust callers to assume:

- every owning handle's `Drop` runs the matching C destructor with the
  same allocator used at construction
- borrowed views (`Column<'b>`, `TypeRef<'a>`, `ExceptionRef<'e>`) do
  not outlive their owners — enforced by lifetimes, not docs
- text-returning accessors hand back `&[u8]` not `&str`; UTF-8
  interpretation stays at the consumer

Caller's responsibility:

- keep slabs passed to `BlockBuilder::append_*` alive for the
  builder's `'a` lifetime (no-copy column slabs)
- ensure the fd backing a `PosixIo<'fd>` stays open through `'fd`
  (borrowed path) or hand the fd to `PosixIo::new_owned`
- treat `Codec::raw_mut` as unsafe and uphold the function-pointer
  signature contract

Trusted base (out of scope for Rust enforcement):

- `clickhouse-c`'s published invariants — `n_rows`, `offsets.last()`,
  `name_len` etc. consistent with the buffers the same struct points
  at. `debug_assert!`s trip in debug builds where the cross-check is
  expressible in one line; release trusts the C side. No public C API
  to query buffer capacity, so bounds against the underlying
  allocation are unchecked.

See "Safety model" in
[`clickhouse-c-rs/README.md`](../clickhouse-c-rs/README.md) for the
full statement.

## `Client<'fd>`

[`src/client.rs:142-154`](../clickhouse-c-rs/src/client.rs).

```rust
pub struct Client<'fd> {
    raw: NonNull<sys::chc_client>,
    alloc: Box<Allocator>,
    _codec: Option<Pin<Box<Codec>>>,
    _io: Pin<Box<PosixIo<'fd>>>,
}
```

`chc_client_init` stashes raw `chc_io *` and `chc_codec *` into
`c->io` / `c->codec` for the connection lifetime, plus `c->al = al`
into the allocator slot. `Client` owns the `Pin<Box<...>>` so back-
pointers stay valid through `Drop`. The `'fd` lifetime parameter ties
`Client` to the borrowed fd through `PosixIo`.

Why `Pin<Box<Codec>>` and `Pin<Box<PosixIo>>`: compression code calls
back into the codec's function-pointer table by address;
`chc_posix_io` carries a back-pointer into the `chc_io` vtable it
initialized. Both structs are `!Unpin` so safe code cannot move them
out of the `Pin<Box<_>>`.

`alloc: Box<Allocator>` — **not** pinned. UNSAFE 7c flagged the
original `Pin<Box<Allocator>>` as theatre: `Allocator: Copy` so
pinning doesn't prevent C from operating on a bit-identical copy, and
`Packet::take_block` already copies the allocator value-wise. Current
shape uses bare `Box<Allocator>` because the `chc_alloc_stdlib`
allocator is value-stable (null `ud`, static fn ptrs); the Box gives
heap-stable address for `c->al` without lying about pin guarantees.
If a non-stdlib allocator ever lands the comment at `client.rs:144`
needs revisiting.

## `PosixIo<'fd>`

[`src/io.rs:17-94`](../clickhouse-c-rs/src/io.rs).

Wraps `BorrowedFd<'fd>` (UNSAFE 6 — was `c_int`). Two constructors:

- `PosixIo::new(fd: BorrowedFd<'fd>) -> Pin<Box<Self>>` — borrowed-fd
  path; `Client<'fd>` ties to the borrow
- `PosixIo::new_owned(fd: impl Into<OwnedFd>) -> Pin<Box<Self>>` —
  takes ownership, closes fd on drop. Returns `Pin<Box<PosixIo<'static>>>`
  so `Client<'static>` is achievable when the fd is owned

`PhantomData<BorrowedFd<'fd>>` carries the lifetime; `PhantomPinned`
plus structural pin keeps the C-side back-pointer between `state` and
`io` valid.

`io_ptr()` is `pub(crate)` — handed only to `Client::init` and
`Block::read` inside the crate.

## `Codec::raw_mut`

[`src/codec.rs:83-85`](../clickhouse-c-rs/src/codec.rs).

```rust
pub unsafe fn raw_mut(self: Pin<&mut Self>) -> &mut sys::chc_codec
```

Marked `unsafe` in the UNSAFE landing. Safety clause requires caller:

- install function pointers matching `chc_codec` field signatures
  exactly
- populate every field the paired `Compression` will exercise
  (`Compression::Lz4` needs `lz4_compress` / `lz4_decompress` /
  `lz4_bound`; leaving them `None` is a null call)
- keep any `ud` pointer alive for the codec's lifetime and
  dereferenceable from every thread the codec runs on

No internal call sites; pure API-marker change. walshadow uses
`Codec::lz4()` / `Codec::zstd()` factories exclusively.

## Block column safety

[`src/block.rs`](../clickhouse-c-rs/src/block.rs).

- `column_name(i)` returns `Option<&[u8]>` — no `from_utf8_unchecked`,
  no UTF-8 assumption. Same for `TypeRef::{name, timezone, enum_at,
  tuple_field_name}`. `TypeRef::format` is the one materializing site
  and uses `from_utf8_lossy` explicitly
- `Column::fixed` / `Column::low_cardinality` use
  `n_rows().checked_mul(elem_size)?` — overflow returns `None` rather
  than wrapping. Adversarial server (2^60-row column, elem_size=256)
  cannot trigger UB through length wraparound
- `Exception::cstr_bytes` carries a `debug_assert!(len <= isize::MAX)`

## Walshadow consumption

CH emitter ([emitter.md](emitter.md)) uses `Client` +
`BlockBuilder<'a>`. Lifetime contract:

- per-batch slab buffers held in the emitter outlive every
  `append_*` call into the builder
- `Client::init` happens once per connection with a `PosixIo::new_owned`
  wrap of the TCP socket; emitter owns the resulting `Client<'static>`
  for the daemon's life
- batch flush: `client.send_query`, `client.send_data(Some(&bb))`,
  `client.send_data(None)`, `recv_packet` loop until `EndOfStream`

Pinning of `Allocator` removed when `Client` reshaped to own the alloc
directly — the field comment at
[`src/client.rs:144-150`](../clickhouse-c-rs/src/client.rs) documents
why bare `Box` is sufficient for the stdlib allocator.

Latent invariants (UNSAFE §7) that walshadow must not break:

- `unsafe impl<'a> Send for BlockBuilder<'a>` is unconditional —
  sound iff every appended slab type is `Sync`. Adding a non-`Sync`
  append type silently breaks it
- `unsafe impl Sync for Allocator` rides on `Allocator::stdlib` being
  the only constructor. A `with_raw(chc_alloc)` constructor must be
  `unsafe` and either drop `Sync` or scope it
