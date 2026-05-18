# UNSAFE — clickhouse-c-rs safety contract fixes

Address soundness holes in [`clickhouse-c-rs`](../clickhouse-c-rs/) where
safe code can trigger UB. Findings from the audit on 2026-05-17 against
commit `b5af579`.

Goal: every public non-`unsafe` API in `clickhouse-c-rs` must be sound
under arbitrary safe-code use, modulo a documented trust boundary at the
C library itself.

## Scope summary

| # | Hole                                          | Severity | Shape of fix          |
|---|-----------------------------------------------|----------|-----------------------|
| 1 | `Client` lifetime detached from `PosixIo` / `Codec` | hard UB  | move ownership in     |
| 2 | `from_utf8_unchecked` on server bytes         | hard UB  | return `&[u8]`        |
| 3 | `Codec::raw_mut` exposed safe                 | hard UB  | mark `unsafe`         |
| 4 | `from_raw_parts` trusts C-side counters       | TCB doc  | document + assert     |
| 5 | `n_rows * elem_size` overflow                 | hard UB  | `checked_mul`         |
| 6 | `PosixIo::new(fd: c_int)` safe                | API smell| `BorrowedFd<'_>`      |

Holes (1)–(3) and (5) are fixable without changing observable wire
behavior. (4) is partly a documentation fix, partly a hardening pass.
(6) is a typed-API refactor that touches every call site.

## Strategy

Land as one bundled change. Each hole is independently small and the
README's "Safety model" section needs a single rewrite at the end; six
sequential PRs would just churn the docs six times. The audit already
demonstrates each is reachable from safe code, so there's no value in
splitting "verify each" into separate landings.

Order within the change is bottom-up: types-of-things first (return
`&[u8]` from accessors, mark `Codec::raw_mut` unsafe), then the
ownership refactors (Client absorbs PosixIo / Codec), then the README.

## 1. Bind `Client` to `PosixIo` and `Codec`

[`src/client.rs:141-175`](../clickhouse-c-rs/src/client.rs),
[`src/io.rs:17-65`](../clickhouse-c-rs/src/io.rs),
[`src/codec.rs:24-75`](../clickhouse-c-rs/src/codec.rs).

`chc_client` stores `chc_io *` and `chc_codec *` for the life of the
connection (`clickhouse-client.h:237-246`). The Rust wrapper currently
takes both as `Pin<&mut PosixIo>` / `Pin<&Codec>` borrows that expire
when `Client::init` returns. After init the user owns the
`Pin<Box<PosixIo>>` and can drop it; any subsequent `Client::send_*`
dereferences a freed `chc_io`.

Pick the **move-in** variant over `Client<'io, 'codec>`. Both close the
hole; the lifetime form pollutes every downstream type with a parameter
that the user can never actually satisfy without keeping the
`PosixIo`/`Codec` alive at the same scope as the `Client` anyway. Moving
the `Pin<Box<...>>` into `Client` colocates the lifetime guarantee with
the only object that can violate it.

```rust
pub struct Client {
    raw: NonNull<sys::chc_client>,
    alloc: Pin<Box<Allocator>>,
    _io: Pin<Box<PosixIo>>,
    _codec: Option<Pin<Box<Codec>>>,
}

impl Client {
    pub fn init(
        opts: &ClientOpts,
        alloc: Allocator,
        io: Pin<Box<PosixIo>>,
        codec: Option<Pin<Box<Codec>>>,
    ) -> Result<Self> { … }
}
```

The `io_ptr` accessor on `PosixIo` becomes `pub(crate)` only, since
nothing outside the crate has a use for it once `Client` owns the box.
For the non-Client `Block::read` path, callers still hold their own
`Pin<Box<PosixIo>>` and pass `Pin<&mut>` — that signature is sound,
because the borrow lifetime covers exactly the call.

Test that demonstrates the old UB and now fails to compile lands in
[`tests/clickhouse_local.rs`](../clickhouse-c-rs/tests/clickhouse_local.rs)
as a `compile_fail` doctest.

## 2. Return `&[u8]`, not `&str`, from server-controlled accessors

[`src/block.rs:105-116`](../clickhouse-c-rs/src/block.rs),
[`src/types.rs:227-286`](../clickhouse-c-rs/src/types.rs).

Six sites today call `core::str::from_utf8_unchecked` on bytes the C
library copied verbatim from the wire or from a user-supplied type
string:

- `Block::column_name`
- `TypeRef::timezone`
- `TypeRef::name`
- `TypeRef::enum_at` (name field)
- `TypeRef::tuple_field_name`

Two fix shapes:

- Return `&[u8]`, leaving UTF-8 interpretation to the consumer (matches
  what `Exception::name` etc. already do).
- Return `Option<&str>` via `core::str::from_utf8` (checked) and let
  invalid UTF-8 collapse to `None`.

Pick `&[u8]`. The consumer in
[`walshadow_oracle`](../walshadow_oracle/) currently converts to owned
`String` lossily at the boundary anyway; returning `&[u8]` is closer to
truth and removes a hidden allocation in the hot path
(`TypeRef::format` already uses `from_utf8_lossy`, which is the
explicit-cost equivalent — callers who want a `String` can do the same).

Touch every consumer site:

- [`walshadow_oracle/src/.../mod.rs`](../walshadow_oracle/) wherever
  `column_name` or `TypeRef::name` is currently consumed as `&str`.
- [`clickhouse-c-rs/examples/spawn_clickhouse_local.rs`](../clickhouse-c-rs/examples/spawn_clickhouse_local.rs).
- [`clickhouse-c-rs/tests/clickhouse_local.rs`](../clickhouse-c-rs/tests/clickhouse_local.rs)
  and [`readme_quickstarts.rs`](../clickhouse-c-rs/tests/readme_quickstarts.rs).

`TypeRef::format` stays as-is (returns `String` via `from_utf8_lossy`)
— that's the single place that owns the lossy decision.

## 3. `Codec::raw_mut` must be `unsafe`

[`src/codec.rs:67-69`](../clickhouse-c-rs/src/codec.rs).

```rust
pub unsafe fn raw_mut(self: Pin<&mut Self>) -> &mut sys::chc_codec
```

Add a `# Safety` clause: caller must install function pointers whose
signatures match `sys::chc_codec`'s fields exactly, must not leave a
field set to `None` if the configured `Compression` will exercise it,
and must keep any `ud` pointer alive for the codec's lifetime.

No internal call sites use `raw_mut`; this is a pure API-marker change.

## 4. Document the C-side trust boundary; add a few `debug_assert`s

[`src/block.rs:176-300`](../clickhouse-c-rs/src/block.rs),
[`src/client.rs:388-393`](../clickhouse-c-rs/src/client.rs).

The wrappers construct `&[u8]` / `&[u64]` slices using lengths read back
from the C object (`n_rows`, `offsets.last()`, `name_len`, etc.). If the
C library ever publishes a mismatch — e.g. an `offsets` array whose
last element exceeds the allocated `string_data` buffer — safe Rust
returns a slice over memory the C lib never owned. Reading any byte is
UB.

Two-part fix:

1. **README**: rewrite the "Safety model" section to spell out that the
   C library (`clickhouse-c` at the vendored revision) is part of the
   trusted base. Soundness of the safe Rust API is conditional on the C
   library's published invariants holding.

2. **`debug_assert`s** at each `from_raw_parts` site that has an
   internally-consistent cross-check the C lib *should* satisfy:

   - `Column::string`: `data_len >= offsets[n-1] >= … >= offsets[0]`
     (monotone non-decreasing exclusive ends).
   - `Column::low_cardinality`: `key_size ∈ {1, 2, 4, 8}` (already
     bounded by `chc_column_lc_key_size`, but assert it).
   - `Exception::cstr_bytes`: `len <= isize::MAX`.

   These trip in debug builds when the C lib violates contract; in
   release they cost nothing. Don't add asserts where the C lib's
   contract isn't expressible in one line — leave those at the README's
   trust statement.

The wrappers don't add full bounds checking against the allocation,
because there's no public C API to query an `chc_column`'s buffer
capacity. Doing it manually would require extending `clickhouse-c`'s
header surface, which is out of scope here.

## 5. Use `checked_mul` for slice lengths

[`src/block.rs:185`](../clickhouse-c-rs/src/block.rs),
[`src/block.rs:290`](../clickhouse-c-rs/src/block.rs).

```rust
let n = self.n_rows().checked_mul(elem_size)?;
```

Returns `None` from `Column::fixed` / `Column::low_cardinality` on
overflow rather than wrapping silently. Realistic data sizes will never
trip this; adversarial values (a server sending a 2^60-row column with
elem_size=256) could.

Add the same to
[`src/types.rs:296`](../clickhouse-c-rs/src/types.rs) `needed + 1`
(unlikely to overflow but free to harden).

## 6. `PosixIo::new` takes `BorrowedFd<'_>`

[`src/io.rs:29-57`](../clickhouse-c-rs/src/io.rs).

Not strictly a memory-safety hole — fd misuse goes through the kernel,
which can't violate Rust's memory model. But the `c_int` parameter
silently invites:

- closing the fd while `PosixIo` still references it (subsequent reads
  hit a recycled fd → writes ClickHouse handshake bytes to whatever
  file the kernel reassigned the number to)
- passing a `c_int` that was never an fd at all

```rust
pub struct PosixIo<'fd> {
    state: sys::chc_posix_io,
    io: sys::chc_io,
    _fd: PhantomData<BorrowedFd<'fd>>,
    _pin: PhantomPinned,
}

impl<'fd> PosixIo<'fd> {
    pub fn new(fd: BorrowedFd<'fd>) -> Pin<Box<Self>> { … }
}
```

This is the largest mechanical change: every `PosixIo::new(raw_fd)`
caller in [`walshadow_oracle`](../walshadow_oracle/), the example, and
the tests becomes `PosixIo::new(socket.as_fd())`. The example's
`into_raw_fd` + manual `close` dance disappears — the `ChildStdout`
stays in scope and gets dropped naturally.

The `'fd` parameter also propagates into `Client` (since Client now
owns the `Pin<Box<PosixIo<'fd>>>`), giving the Client a lifetime tied
to the connected socket. Acceptable: connections never outlive their
sockets in practice, and the lifetime appearing in `Client<'fd>` is
honest.

## 7. Latent invariants (FYI, not landing)

Three things audit-flagged as currently sound but only because no public
constructor exists for the relevant footgun. Documented here so a future
reviewer touching this surface knows what's load-bearing; no code change
proposed.

### 7a. `unsafe impl<'a> Send for BlockBuilder<'a>` is unconditional
[`src/builder.rs:276`](../clickhouse-c-rs/src/builder.rs).

The impl is `Send` regardless of what `'a` references. Sound today
because every `append_*` takes `&'a [u8]` or `&'a [u64]`, and `&[T]:
Send` iff `T: Sync` — `u8` / `u64` are both `Sync`. The moment a future
append takes `&'a Cell<u8>` (or any other non-`Sync` slab type), the
impl silently becomes unsound.

Lock-down would be `unsafe impl<'a> Send for BlockBuilder<'a> where
&'a [u8]: Send, &'a [u64]: Send {}` or equivalent, but the bound list
grows linearly with append variants and Rust has no `where for<all
fields> T: Send` shorthand. Easier: add a comment at the impl saying
"sound iff every appended slab type is `Sync`; revisit when adding
non-byte-slab appends".

### 7b. `unsafe impl Sync for Allocator` rides on closed constructor set
[`src/alloc.rs:32-33`](../clickhouse-c-rs/src/alloc.rs).

`Allocator` is `Sync` because today the only constructor is
`Allocator::stdlib()`, whose `chc_alloc` has `ud == NULL` and pure
static fn pointers — copies across threads are interchangeable. The
`raw: chc_alloc` field is `pub(crate)`, so external code cannot
construct an `Allocator` with a non-`Send`/`Sync` `ud`.

The moment a `pub fn with_raw(raw: chc_alloc) -> Self` lands (e.g. to
wire a PG `palloc` context), the impl becomes a soundness hole — that
caller can stash a `*mut SomeNonSync` in `ud`. Lock-down: when that
constructor is added, mark it `unsafe` and either drop the `Sync` impl
or scope it to a typed wrapper.

### 7c. `Client.alloc: Pin<Box<Allocator>>` is dead pinning
[`src/client.rs:143-149`](../clickhouse-c-rs/src/client.rs).

The comment claims the `Box` keeps the `chc_alloc` address stable so
the C side's stored `c->al` pointer stays valid. Two problems:

- `Allocator: Copy`, so `Pin<Box<Allocator>>` doesn't prevent the C
  library from operating on a different bit-identical copy. Pinning
  isn't doing what the comment says.
- `Packet::take_block` (`client.rs:459`) explicitly dereferences and
  copies the `Allocator` into the returned `Block` anyway.

What actually keeps this sound: `chc_alloc_stdlib()` produces a value
that's safe to copy (null `ud`, static fn ptrs). The Box's pinning is
theatre.

Lock-down options (not worth doing today): drop the `Pin<Box<_>>` and
just hold `Allocator` by value, rewriting the comment to explain "safe
because stdlib alloc is value-stable"; or, if a non-trivial allocator
ever arrives, change `Allocator: !Copy` and keep the Box pin honest.

## Out of scope

- Switching the FFI surface to `bindgen` (still hand-written for
  audit reasons; see [`src/sys.rs:1-9`](../clickhouse-c-rs/src/sys.rs)).
- Replacing `unsafe impl Send for Block` / `Client` / `Exception` with
  derive-time analysis — current claims hold under the stdlib allocator
  and there's no public path to construct a non-stdlib allocator.
- Adding bounds checking against the underlying C-side allocation for
  columns (would require extending `clickhouse-c`'s public API).

## Acceptance

- `cargo build --features lz4,zstd` clean.
- `cargo test -p clickhouse-c-rs` plus a new `compile_fail` test
  demonstrating the (1) UAF no longer compiles.
- `walshadow_oracle` compiles against the new API; existing integration
  tests pass.
- README "Safety model" section rewritten to reflect (a) Client now
  owns the IO+codec, (b) text accessors return `&[u8]`, (c) trust
  boundary at the C library is explicit.
