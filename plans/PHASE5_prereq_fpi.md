# PHASE5 prereq — FPI compression

Implements `restore_block_image` per `plans/FPI_COMPRESSION.md`,
unblocking Phase 5 heap decoder under `wal_compression = pglz|lz4|zstd`

## What landed

walshadow:
- `src/fpi.rs` new, 260 LOC (impl + 9 inline tests)
- `src/lib.rs` +1 line `pub mod fpi;`
- `Cargo.toml` +3 deps `pglz = "0.1"`, `lz4_flex = "0.11"`, `zstd = "0.13"`

wal-rs:
- `src/pg/walparser/types.rs` +~60 LOC `FpiCompressionMethod` enum,
  `XLogRecordBlockImageHeader::compression_method`, cold PG-14 branch
- `src/pg/walparser/types.rs` +~70 LOC three tests
  (`compression_method_pg15`, `compression_method_pg14`,
  `compression_method_magic_split`)
- `src/pg/walparser/mod.rs` re-exports `FpiCompressionMethod`,
  `BKP_BLOCK_HAS_IMAGE`, `BKP_IMAGE_HAS_HOLE`,
  `BKP_IMAGE_IS_COMPRESSED_PG14`, `BKP_IMAGE_COMPRESS_PGLZ`,
  `BKP_IMAGE_COMPRESS_LZ4`, `BKP_IMAGE_COMPRESS_ZSTD`,
  `BKP_IMAGE_COMPRESS_MASK_PG15`

## Public API — walshadow::fpi

```rust
pub const PAGE_BYTES: usize = 8192;

pub enum FpiError {
    NoImage,
    UnknownCodec(u8),
    BadHole { offset: u16, length: u16 },
    Pglz,
    Lz4(String),
    Zstd(String),
    SizeMismatch { got: usize, expected: usize },
}

pub fn restore_block_image(
    block: &XLogRecordBlock,
    page_magic: u16,
) -> Result<[u8; PAGE_BYTES], FpiError>;
```

Behaviour mirrors PG's `xlogreader.c::RestoreBlockImage`: codec
dispatch via `XLogRecordBlockImageHeader::compression_method`,
decoded body sized `BLCKSZ - hole_length`, hole region zeroed,
surrounding bytes spliced in

## wal-rs surface added

```rust
pub enum FpiCompressionMethod { Pglz, Lz4, Zstd }

impl XLogRecordBlockImageHeader {
    pub fn compression_method(&self, page_magic: u16) -> Option<FpiCompressionMethod>;
}
```

PG-14 branch marked `#[cold]` (walshadow rejects PG ≤ 14 captures).
Matches the existing `is_compressed(page_magic)` dispatch table 1:1

## Test coverage

walshadow `src/fpi.rs` 9 inline tests:
- `uncompressed_no_hole_round_trip` — identity copy through
- `uncompressed_with_hole_splices` — hole_offset 1024, hole_length
  2048, asserts hole zeroed + surrounding bytes intact
- `no_image_errors` — `has_image() == false`
- `bad_hole_errors` — `hole_offset + hole_length > BLCKSZ`
- `uncompressed_size_mismatch_errors` — image len != BLCKSZ - hole
- `pglz_round_trip` — full 8 KiB page via `pglz::compress` / restore
- `pglz_round_trip_with_hole` — pglz + hole splice combined
- `lz4_round_trip` — `lz4_flex::block::compress` / restore
- `zstd_round_trip` — `zstd::bulk::compress` / restore

wal-rs `src/pg/walparser/types.rs` 3 new tests:
- `compression_method_pg15` — each codec bit, APPLY-only resolves
  to None, codec + APPLY + HAS_HOLE coexist
- `compression_method_pg14` — IS_COMPRESSED_PG14 -> Pglz, PG-14
  ignores PG-15 codec bits
- `compression_method_magic_split` — same info byte resolves
  differently across magic threshold

## Known gaps / followups

- Live-PG capture fixture deferred per spec's "Integration" section
  (out of scope for this prereq commit)
- `tests/fpi_round_trip.rs` not created (spec mentions, but live-PG
  fixtures owned by future capture-script work)
- Cross-codec equivalence test (same logical op via four
  `wal_compression` settings) also deferred to capture-fixture commit

## Build + test command output

```
$ cargo build -p walshadow --lib
    Finished `dev` profile [unoptimized + debuginfo] target(s)

$ cargo test --lib -p walshadow
test result: ok. 103 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out

$ cargo test --lib -p walshadow fpi
test result: ok. 9 passed; 0 failed; 0 ignored; 0 measured; 94 filtered out

$ cargo test -p wal-rs --lib walparser::types::
test result: ok. 7 passed; 0 failed; 0 ignored; 0 measured; 174 filtered out
```

Clippy on touched files: clean. Existing warnings/errors in
`src/wal_stream.rs`, `src/bin/stream.rs`, `tests/multi_segment_filter.rs`
belong to parallel async-sink prereq, not touched here

## Boundary notes

- `src/wal_stream.rs`, `src/bin/stream.rs`, `src/filter_segment.rs`
  owned by async-sink prereq; their compile errors pre-existed,
  left untouched
- `src/shadow_catalog.rs` ReplIdent enum owned by Default-PK-attnums
  prereq; not touched
