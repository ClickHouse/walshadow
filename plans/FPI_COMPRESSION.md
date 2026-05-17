# FPI_COMPRESSION — restore compressed full-page images

Evaluation. Sibling to [SEGMENT_COMPRESSION.md](SEGMENT_COMPRESSION.md);
the two are independent and either may ship first. Status: **not
committed work**.

Walshadow-side per the project boundary: pglz, lz4, and zstd all
have usable upstream Rust crates (`pglz` 0.1+, `lz4_flex`, `zstd`),
and the consumer is a walshadow decoder (Phase 5/6 and
[BASEBACKUP.md](BASEBACKUP.md)'s 1B+2A path) not a wal-rs primitive.
Lift in wal-rs is bounded to extending existing flag predicates;
new code is here.

## Why

`wal_compression` (PG GUC, default `off`) tells the backend to
compress full-page images written into WAL records.  Methods: `off`
(default), `pglz` (PG 14+, always available), `lz4` (PG 15+, needs
`--with-lz4`), `zstd` (PG 15+, needs `--with-zstd`). Operators
running larger clusters frequently flip this on for the WAL-volume
savings; production source clusters with `wal_compression = lz4`
are common.

wal-rs reads the `BKP_IMAGE_*` flags off `image_header.info`
(`wal-rs/src/pg/walparser/types.rs:60-67`) and the
`is_compressed(page_magic)` predicate (`types.rs:256`) gets the
PG-14-vs-15 bit-layout right. But `parse.rs:354
read_block_data_and_images` (`wal-rs/src/pg/walparser/parse.rs:354`)
copies `image_length` bytes verbatim into `XLogRecordBlock.image` —
**raw, never inflated**. The consumer gets compressed bytes back if
the source compressed them.

Walshadow today has zero consumers of `block.image`:

* Filter pipeline (`src/filter.rs:135`) looks at
  `block.header.location.rel` — flag bits, no image bytes.
* pg_class decoder (`src/pg_class_decoder.rs:90`) consumes
  `block.data` (the tuple-bytes payload), parallel to `block.image`.
  Both `b.data` and `b.image` are present when an FPI carries a
  same-record `xl_heap_*` insertion with `XLOG_HEAP_INIT_PAGE`; the
  tuple bytes are in `b.data`, the page image is in `b.image`.
  Today's decoder works without touching `b.image`.
* CRC rewriter (`src/rewrite.rs`) only re-CRCs after NOOP-rewrite;
  doesn't inspect image bytes.

So **today's correctness does not depend on FPI decompression**, but
three forward paths block on it:

1. **Phase 5/6 heap-tuple decoder under FPI-only updates.** PG can
   emit an FPI without a same-record `xl_heap_*` tuple payload in
   the case of `XLOG_FPI` (rmgr `RM_XLOG`, info `0xA0` —
   `XLOG_FPI_FOR_HINT` / `XLOG_FPI`). Hint-bit page updates use
   this form. For pg_class today: hint-bit changes against pg_class
   produce `XLOG_FPI_FOR_HINT` records carrying the full page image
   with no tuple payload. The catalog tracker misses the
   filenode-rotated state if it never reads the image.
2. **[BASEBACKUP.md](BASEBACKUP.md) §"Path 1B + 2A — streamed
   page-walk".** Quoted: "Decoder buffers the page subset that WAL
   window touches (MiB out of GB/TB), applies FPIs in-memory, walks
   decoded." Hard requirement; cannot ship the 1B+2A bootstrap path
   without `restore_block_image`.
3. **Differential oracle (Phase 9, future).** Comparing
   walshadow's decoded WAL projection against ground-truth page
   contents wants page bytes; FPI bytes are the cheapest source
   that doesn't require a separate `pg_buffercache`-style query.

[FPI_COMPRESSION.md](FPI_COMPRESSION.md) and
[SEGMENT_COMPRESSION.md](SEGMENT_COMPRESSION.md) are routinely
conflated. They are unrelated: this plan covers the *record-level*
flag, [SEGMENT_COMPRESSION.md](SEGMENT_COMPRESSION.md) covers the
*file-level* framing. A source running
`wal_compression = lz4` on uncompressed segment files (default
archive) hits this plan only.

## What restoring a page image actually requires

PG's `xlogreader.c::RestoreBlockImage` (PG 16+
`src/backend/access/transam/xlogreader.c:2050`):

```c
if (bkpb->bimg_info & BKPIMAGE_COMPRESSED) {
    char    tmp[BLCKSZ];
    int     decomp_len;
    bool    decompress_ok;

    if      (bkpb->bimg_info & BKPIMAGE_COMPRESS_PGLZ) decomp_len = pglz_decompress(record->data + offset, bkpb->bimg_len, tmp, BLCKSZ - bkpb->hole_length, true);
    else if (bkpb->bimg_info & BKPIMAGE_COMPRESS_LZ4)  decomp_len = LZ4_decompress_safe(record->data + offset, tmp, bkpb->bimg_len, BLCKSZ - bkpb->hole_length);
    else if (bkpb->bimg_info & BKPIMAGE_COMPRESS_ZSTD) decomp_len = ZSTD_decompress(tmp, BLCKSZ - bkpb->hole_length, record->data + offset, bkpb->bimg_len);

    if (decomp_len != BLCKSZ - bkpb->hole_length) /* error */;

    if (bkpb->hole_length == 0) {
        memcpy(page, tmp, BLCKSZ);
    } else {
        memcpy(page,                                 tmp,                       bkpb->hole_offset);
        memset(page + bkpb->hole_offset,             0,                         bkpb->hole_length);
        memcpy(page + bkpb->hole_offset + bkpb->hole_length,
               tmp  + bkpb->hole_offset,
               BLCKSZ - (bkpb->hole_offset + bkpb->hole_length));
    }
}
```

Three things this plan needs to do per FPI:

1. **Decode the codec.** pglz / lz4 raw block / zstd frame.
   `compressed_bytes → BLCKSZ - hole_length` scratch.
2. **Splice the hole.** Zero out `hole_offset .. hole_offset +
   hole_length`; surrounding bytes shift to make room. Already valid
   for uncompressed FPIs that have a hole (today's
   `block.image.len() = BLCKSZ - hole_length`).
3. **Validate.** Decoded length must equal `BLCKSZ - hole_length`;
   ill-formed image → error.

## Module shape (walshadow side)

```
src/fpi.rs           new — ~150 LOC  restore_block_image entry
                                     point; codec dispatch; hole
                                     splice; error type.
src/lib.rs           +~1             pub mod fpi;
Cargo.toml           +~3             pglz = "0.1" (sync
                                     decompress_into mirrors PG's
                                     pglz_decompress signature 1:1,
                                     PostgreSQL-licensed),
                                     lz4_flex = "0.11" (pure-Rust,
                                     no_std raw-block decoder),
                                     zstd-safe = "7" (single-pass
                                     bulk API, no streaming alloc)
                                     or zstd = "0.13" (already in
                                     the transitive tree, frame API
                                     covers ZSTD_decompress one-shot).
```

### pglz crate

`pglz` 0.1 (https://crates.io/crates/pglz) ports
`src/common/pg_lzcompress.c` directly. Decompress entry walshadow
uses:

```rust
pub fn decompress_into(source: &[u8], dest: &mut [u8], check_complete: bool) -> Option<usize>;
```

Argument-for-argument equivalent of PG's
`pglz_decompress(source, source_len, dest, rawsize, check_complete)`,
returning bytes written or `None` on a corrupt stream (truncated
tag, invalid back-reference). `fpi.rs` maps `None` to
`FpiError::Pglz`.

### `src/fpi.rs`

```rust
//! Reconstruct a full 8 KiB page from an XLogRecordBlock.image. PG's
//! recovery code calls this "RestoreBlockImage"; walshadow's name
//! mirrors the surface.

use wal_rs::pg::walparser::{
    BLOCK_SIZE,
    XLogRecordBlock,
    BKP_IMAGE_COMPRESS_PGLZ,
    BKP_IMAGE_COMPRESS_LZ4,
    BKP_IMAGE_COMPRESS_ZSTD,
    BKP_IMAGE_IS_COMPRESSED_PG14,
};

pub const PAGE_BYTES: usize = BLOCK_SIZE as usize; // 8192

#[derive(Debug, thiserror::Error)]
pub enum FpiError {
    #[error("block carries no image")]
    NoImage,
    #[error("unrecognised compression bits {0:#04x}")]
    UnknownCodec(u8),
    #[error("hole offset {offset} + length {length} > BLCKSZ")]
    BadHole { offset: u16, length: u16 },
    #[error("pglz: corrupt stream")]
    Pglz,
    #[error("lz4: {0}")]
    Lz4(String),
    #[error("zstd: {0}")]
    Zstd(String),
    #[error("decoded size {got} != BLCKSZ - hole_length ({expected})")]
    SizeMismatch { got: usize, expected: usize },
}

/// Reconstruct the 8 KiB page this block's FPI represents.
/// `page_magic` is the `XLogPageHeader.magic` of the page that
/// started the record (already tracked by `SegmentWalker`); selects
/// the PG-14 vs PG-15 bimg_info bit layout.
pub fn restore_block_image(
    block: &XLogRecordBlock,
    page_magic: u16,
) -> Result<[u8; PAGE_BYTES], FpiError>;
```

Behaviour:

* `block.header.has_image() == false` → `NoImage`.
* Hole geometry sanity: `hole_offset + hole_length <= BLCKSZ`.
* Compression selector (PG-14 bit layout: `IS_COMPRESSED_PG14`
  implies pglz; PG-15+ bit layout: one of `COMPRESS_PGLZ`,
  `COMPRESS_LZ4`, `COMPRESS_ZSTD` exclusive). Cross-validated via
  the existing `image_header.is_compressed(page_magic)` predicate
  plus a new helper `image_header.compression_method(page_magic) ->
  Option<Codec>`.
* Uncompressed path: image is exactly `BLCKSZ - hole_length` bytes;
  splice. (Already correct today; this code path doubles as the
  uncompressed branch and exercises hole-splice logic regardless of
  codec.)
* Compressed paths: decode → `BLCKSZ - hole_length` scratch on
  stack, splice hole, return.

Stack scratch (8 KiB) is fine for `restore_block_image`'s
intended call sites (one page at a time). If a future caller wants
to batch-restore N images, switch to a caller-supplied
`&mut [u8; PAGE_BYTES]` overload.

### wal-rs surface additions (minor)

```rust
// src/pg/walparser/types.rs

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FpiCompressionMethod {
    Pglz,
    Lz4,
    Zstd,
}

impl XLogRecordBlockImageHeader {
    /// Resolve the (PG-15-layout) compression method, or `None` for
    /// uncompressed. PG-14 fixtures collapse to `Some(Pglz)` when
    /// `IS_COMPRESSED_PG14` is set.
    pub fn compression_method(&self, page_magic: u16) -> Option<FpiCompressionMethod>;
}
```

About 30 LOC + ~30 LOC tests on wal-rs. Walshadow's `fpi.rs`
dispatches off this enum.

## Choice of codecs

* **pglz**: `pglz` crate (0.1, PostgreSQL-licensed, pure Rust,
  ~380 LOC). Direct port of `src/common/pg_lzcompress.c`. Sync API
  `decompress_into(src, dst, check_complete) -> Option<usize>`
  mirrors PG's signature 1:1, no FFI, no unsafe. Older crates
  (`pg-lz`, prior `pglz` snapshots) had quality issues; this 0.1
  release is fresh and built specifically against PG's current
  source for ongoing audit. Tokio feature exists for streaming use
  but walshadow's call site is one-shot per page so the sync API
  is the fit.
* **lz4 raw block**: `lz4_flex` (pure Rust, no_std, raw-block API
  matches what PG calls). Alternative: `lz4-sys` (already in the
  transitive dep graph via async-compression). `lz4_flex`
  eliminates a C dep + matches walshadow's other pure-Rust leanings.
* **zstd frame**: `zstd` crate's `bulk::decompress` (FFI to
  upstream zstd, already transitive). Frame format is what PG
  produces (`ZSTD_compress` writes a single ZSTD frame).
  `zstd-safe` is the lower-level wrapper if `zstd` pulls in too
  much; either works.

Direct deps walshadow adds: `pglz`, `lz4_flex`, `zstd`. All have
stable releases; all are commonly used in the Rust DB ecosystem or
purpose-built for it.

## Test strategy

### Unit

* `pglz` round-trip: covered upstream by the crate's `tests/regress.rs`.
  Walshadow's coverage focuses on the FPI splice rather than the
  codec — capture WAL with `wal_compression = pglz` from a live PG
  instance and assert `restore_block_image` outputs match
  `pg_relpages` / `pg_buffercache` page bytes.
* Hole-splice with `hole_length = 0` (no hole, copy through).
* Hole-splice with `hole_offset = N, hole_length = M` for several
  `(N, M)` pairs covering: hole at start, hole at end, hole in
  middle, full-page hole-free.

### Integration

* Extend `fixtures/wal/` capture script to emit four variants per
  scenario: `wal_compression = off|pglz|lz4|zstd`. Round-trip every
  captured segment through filter + restore-image.
* New `tests/fpi_round_trip.rs`: for each captured segment, walk
  records, for every block with `has_image()`, call
  `restore_block_image`, assert the result re-parses as a valid PG
  page (`XLogPageHeader` magic / checksum where available, or just
  "first 24 bytes look like a PageHeaderData").
* Cross-codec equivalence: capture the **same operation** (a single
  pg_bench `BEGIN; UPDATE …; COMMIT`) four times against a source
  with the four `wal_compression` settings; assert
  `restore_block_image` outputs are byte-equal across all four.
  Same logical page state, four codecs, identical output.

### Property

* For randomly generated 8 KiB pages: `pglz::compress` →
  `pglz::decompress_into`, assert round-trip. Same for lz4 / zstd
  via their encode counterparts. Confidence on the FPI splice
  layer without needing PG running. (Codec correctness is the
  upstream crate's responsibility; this exercises the dispatch +
  hole-splice glue.)

## Pitfalls

### 1. PG 14 bit layout vs PG 15+

`BKP_IMAGE_IS_COMPRESSED_PG14 (0x02)` and `_BKP_IMAGE_APPLY_PG15
(0x02)` occupy the same bit. PG 14 set 0x02 to mean "compressed
with pglz"; PG 15+ rearranged so 0x02 means "apply this FPI on
recovery" and codec is one of bits 0x04 / 0x08 / 0x10. wal-rs's
`is_compressed(page_magic)` already selects via the page magic
threshold. `compression_method` must match the same dispatch — keep
both functions in lockstep, ideally generating both from one
match-on-magic table. Walshadow's CLAUDE constraint of "PG 15+
only" (`src/segment.rs:28`) means PG-14 FPIs are rejected upstream,
so the PG-14 branch is defensive-only. Keep it documented but mark
`#[cold]`.

### 2. `XLOG_FPI` vs `XLOG_FPI_FOR_HINT`

Two RM_XLOG info codes carrying nothing but an FPI: `XLOG_FPI =
0xA0` (full page image, real change) and `XLOG_FPI_FOR_HINT =
0xB0` (hint-bit update — page contents changed but not via a
user-visible op). The filter today classifies these as `Special`
(rmgr `RM_XLOG`), so they're kept by default policy. When the
heap-tuple decoder lands (Phase 5), the FPI-only records carry
page state that may include the only signal a relfilenode rotation
occurred. The decoder needs to call `restore_block_image` and
inspect the page header's `pd_lower` / `pd_upper` to find the new
ItemId slots. Out of scope here; `fpi.rs` exposes the primitive,
decoder layer wires it.

### 3. Image bytes are zero-length when `BKP_BLOCK_HAS_IMAGE` is set with a NULL-bit pattern

Not actually possible in valid WAL — `image_length` is always >0
when the flag is set. wal-rs's parser would return a `take(0)`
slice without error, which `restore_block_image` then surfaces as
`Pglz` or a similar codec-specific error. Tests should assert this
case errors cleanly rather than panicking; no special handling
needed.

### 4. Codec disabled at compile time on source

`wal_compression = lz4` against a PG built `--without-lz4`
produces a backend error at GUC set time; the source never emits
lz4 FPIs. `wal_compression = zstd` likewise. So a source instance
emits only the codecs its build supports. No need for the decoder
to gracefully degrade — an unrecognised codec bit *is* corruption
relative to the page-magic version it claims.

### 5. Hole-removed bytes are zero-filled

`hole_length == 0` paths (page is entirely meaningful) skip the
memset. PG never compresses an empty hole; hole removal happens
*before* compression so the compressor sees `BLCKSZ - hole_length`
bytes. Decoded scratch must match that size or the image is
malformed.

### 6. Endianness

PG WAL is little-endian on every platform PG runs on (it's a
serialisation format that crosses architectures); walshadow runs
on LE only anyway. No byte-swapping in the FPI path. Codec crates
(`pglz`, `lz4_flex`, `zstd`) all operate on raw byte sequences and
inherit the LE assumption from PG's on-disk format.

### 7. Crate-side wal-rs alternative

`fpi` and the per-codec dispatch arguably belong in wal-rs
alongside `parse_record_from_bytes` — it operates on
`XLogRecordBlock` fields the parser produces. Two reasons to keep
it walshadow-side instead:

* wal-rs deliberately exposes raw `b.image` rather than a
  reconstructed page; consumers other than walshadow may have
  reasons to inspect the compressed bytes directly. Adding
  `restore_block_image` to wal-rs is additive (doesn't change the
  existing surface) but pulls the codec dep set (`pglz`,
  `lz4_flex`, `zstd`) into every wal-rs user. wal-g (the other
  primary wal-rs consumer) has no need for FPI decompression.
* Codec selection — `lz4_flex` vs `lz4-sys`, `zstd` vs `zstd-safe`
  — is a project-level decision walshadow is making against its
  own constraints (pure-Rust preference for `lz4_flex`,
  recovery-style `bulk::decompress` for `zstd`). Pinning these on
  wal-rs would lock other consumers.

If wal-g (or a future wal-rs consumer) acquires the same need,
revisit: lift `fpi` upstream, keep walshadow's wrapper thin. The
interface stays the same; the file just moves. Reversible.

## Estimate

```
src/fpi.rs                        new — ~150 LOC  + ~120 LOC tests
src/lib.rs                        +~1
Cargo.toml                        +~3   pglz, lz4_flex, zstd
fixtures/wal/                     +~80  capture script extension
                                       to emit pglz/lz4/zstd cases
tests/fpi_round_trip.rs           new — ~150 LOC
plans/FPI_COMPRESSION.md          this

wal-rs (small lift):
  src/pg/walparser/types.rs       +~30   FpiCompressionMethod + helper
  tests                           +~30
```

Walshadow total: ~400 LOC src + ~270 LOC tests + ~80 LOC fixtures.
wal-rs delta: ~60 LOC total. The ~180 LOC pglz port shifts upstream
to the `pglz` crate.

## Sequencing

* Lands independently of [SEGMENT_COMPRESSION.md](SEGMENT_COMPRESSION.md).
  Either may go first.
* Blocks: [BASEBACKUP.md](BASEBACKUP.md) Path 1B+2A (page-walk
  bootstrap). Does not block Path 1A+2B or 1B+2C.
* Blocks: future Phase-5 handling of `XLOG_FPI_FOR_HINT` for
  pg_class. (Workaround until then: tolerate the silent miss via
  `pg_class_writes_undecoded` counter — already tracked.)
* Out of scope: Phase 9 decode-oracle differential vs page bytes
  is a separate consumer; FPI primitive feeds it but the oracle
  itself is its own design.

## Recommendation

1. Walshadow gets `src/fpi.rs` (dispatch + hole splice). Public
   re-export under `walshadow::fpi`. Codec deps: `pglz`,
   `lz4_flex`, `zstd`.
2. wal-rs gets `XLogRecordBlockImageHeader::compression_method`
   plus `FpiCompressionMethod` enum. Symmetric with the existing
   `is_compressed` predicate; small lift.
3. Test corpus expansion: capture script under `fixtures/wal/`
   emits one segment per `wal_compression` setting. Cross-codec
   equivalence test (same logical operation, four codecs, decoded
   pages byte-equal) is the strongest correctness check available
   without a running PG in CI.
4. Filter / classifier surface unchanged. FPI compression is
   transparent to the keep/drop decision today — `block.image`
   bytes are forwarded by the byte-preserving filter regardless of
   their codec. New API is consumed-on-demand by future decoders.
5. Revisit hosting on wal-rs once a second consumer (wal-g, or
   another wal-rs user) acquires the same need. Until then,
   walshadow owns the implementation.
