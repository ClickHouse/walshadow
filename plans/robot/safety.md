# robot:safety

## sources of truth

- plans/safety.md
- clickhouse-c-rs/ (src/, lib.rs — confirm types: Client<'fd>, PosixIo<'fd>, Codec)
- audit commit hash from plans/safety.md (b5af579 @ 2026-05-17)

## subsumes

plans/safety.md § unsafe surface enumeration + lifetime relationships

## concept

Rust↔C FFI trust boundary at clickhouse-c-rs. Rust side owns BorrowedFd<'fd>; Client<'fd> composes PosixIo<'fd> + Codec; PosixIo<'fd> wraps BorrowedFd. Six audited unsafe holes annotate boundary points

## clusters

| id | label | purpose |
|---|---|---|
| rust | clickhouse-c-rs Rust side | Client + composed types |
| ffi | FFI boundary | extern "C" surface |
| cpp | clickhouse-c (C library) | out-of-scope, drawn as opaque |
| holes | unsafe surface | six annotation nodes attached to owners |

## key nodes

Rust side:
- client: "Client<'fd>" — #5D4628, shape=record
- posix: "PosixIo<'fd>" — #5D4628
- codec: "Codec" — #5D4628
- bfd: "BorrowedFd<'fd>\n(lifetime root)" — #5D4628, shape=cylinder
- buffer: "Codec::buffer\n(Vec<u8>)" — #5D4628, shape=parallelogram

FFI:
- extern: "extern \"C\" surface" — #5D4628, shape=note
- boundary: "TRUST BOUNDARY" — #4c4641 (cluster label, no node)

C side (clickhouse-c, NOT C++ — pure C library):
- cpp_client: "clickhouse-c\n(unmodified upstream)" — #4D4128, cylinder

Unsafe holes (distinct accent: fillcolor=#6D2D2D, dashed border):
- h1: "①\nClient ownership of\nPosixIo + Codec\n(drop order)" — #6D2D2D
- h2: "②\n&[u8] over\nfrom_utf8_unchecked\n(encoding contract)" — #6D2D2D
- h3: "③\nCodec::raw_mut unsafe\n(buffer aliasing)" — #6D2D2D
- h4: "④\nC-side trust\n(invariants delegated)" — #6D2D2D
- h5: "⑤\nchecked_mul\n(size overflow)" — #6D2D2D
- h6: "⑥\nBorrowedFd discipline\n(lifetime-tied fd)" — #6D2D2D

## key edges

Composition (ownership):
| from | to | color | style | label |
|---|---|---|---|---|
| client | posix | default | solid | owns |
| client | codec | default | solid | owns |
| posix | bfd | default | solid | wraps |
| codec | buffer | default | solid | owns |

FFI crossings:
| client | extern | #BF8C5F, penwidth=2 | solid | call |
| extern | cpp_client | #BF8C5F, penwidth=2 | solid | C ABI |
| cpp_client | extern | #BF8C5F | dashed | return |
| extern | client | #BF8C5F | dashed | return |

Hole annotations (constraint=false, dashed, edge color #B58B86):
| h1 | client | #B58B86 | dashed, constraint=false | |
| h2 | codec | #B58B86 | dashed, constraint=false | |
| h3 | codec | #B58B86 | dashed, constraint=false | |
| h4 | extern | #B58B86 | dashed, constraint=false | |
| h5 | extern | #B58B86 | dashed, constraint=false | |
| h6 | posix | #B58B86 | dashed, constraint=false | |

## legend rows

- node-fill key (Rust side, C side, unsafe annotation accent)
- edge-color key (composition default, FFI orange, unsafe annotation pink)
- audit reference: commit b5af579 @ 2026-05-17 (date-stamped per safety.md)
- six unsafe holes subtable (one-line each, matching node labels)

## layout hints

- rankdir=LR (Rust left, C right, boundary in middle)
- splines=spline (default); fall back to splines=ortho if structural layout looks better
- composition tree on Rust side: client at top, posix and codec siblings beneath, bfd and buffer at leaves
- holes anchored adjacent to their owner, not in their own column
- C cluster minimal — single cylinder, no internal structure (opaque); label as `clickhouse-c` (pure C, NOT C++)

## quality bar

- six holes visible without overlapping their owners
- ownership tree (Client → PosixIo → BorrowedFd) reads top-to-bottom or left-to-right cleanly
- FFI crossings clearly bidirectional (call + return)
- "TRUST BOUNDARY" label or visual divider unambiguous
