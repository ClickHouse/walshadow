# SEGMENT_COMPRESSION — compressed WAL segment file ingestion

Evaluation. Pairs with [FPI_COMPRESSION.md](FPI_COMPRESSION.md): both
arrive together but are independently shippable. Status: **not
committed work**.

Lift sits on the wal-rs side per the project boundary
([BASEBACKUP.md §"Library surface to shape on wal-rs"](BASEBACKUP.md)
sets the precedent). Walshadow keeps its sync, byte-slice consumer
contract.

## Why

Walshadow today takes WAL segment bytes through three ingress points:

* `walshadow-stream` → `SourceFeed` → `START_REPLICATION PHYSICAL`.
  Wire bytes are uncompressed regardless of source's
  `wal_compression` GUC (that GUC controls **FPI** compression inside
  records, not segment framing). Out of scope here. See
  [FPI_COMPRESSION.md](FPI_COMPRESSION.md).
* `walshadow-filter --in <path>` (`src/bin/filter.rs:54`):
  `fs::read(path)` returns raw segment bytes. Today refuses anything
  but a 16 MiB uncompressed file.
* Tests under `tests/filter_round_trip.rs:33` and
  `tests/classify_fixture.rs:23` carry their own `decompress_gz`
  helper because the captured fixtures are stored gzipped in the
  repo. Two copies of the same logic, neither generalising to other
  codecs.

Three upcoming ingresses will need compressed-segment support:

* **`restore_command` shim for shadow.** Once shadow's `pg_wal/`
  feeds from filtered segments out of `--out-dir`, a future variant
  must accept segments that arrived in an archive form (operator's
  `archive_command` writes `.lz4`/`.zst`, walshadow consumes those).
* **BASE_BACKUP catch-up replay.** Per
  [BASEBACKUP.md §"5. WAL during backup"](BASEBACKUP.md): when the
  slot doesn't reach end_lsn yet, walshadow needs to feed shadow
  from the WAL *archive* between start_lsn and live-stream catch-up.
  Archives are commonly `.zst` (wal-g, pgbackrest) or `.lz4`.
* **Fixture capture.** Extending `fixtures/wal/` to cover
  `wal_compression={pglz,lz4,zstd}` scenarios for
  [FPI_COMPRESSION.md](FPI_COMPRESSION.md) wants compressed
  *fixture* files too (otherwise the repo bloats).

Codec coverage target: `zstd`, `lz4`, `gzip`, `lzma` (wal-g), plus
uncompressed. `brotli` is wal-rs's choice; walshadow inherits but
doesn't push the matrix.

## What wal-rs already gives us

* `compression::Method` with `from_extension(".zst"|".lz4"|"…")`,
  `from_name("zstd"|"lz4"|"…")`, `extension(self) -> &'static str`.
  Covers `None | Zstd | Brotli | Lz4 | Lzma`. **Gap: no `Gz`** —
  wal-rs writes archive bytes but never gzip; the existing tests'
  `decompress_gz` helper is doing this only because the fixtures were
  captured via `gzip`. Adding `Gz` is a sympathetic ~20 LOC lift on
  wal-rs (flate2 already transitive via reqwest).
* `compression::decode(method, AsyncReader) -> AsyncReader` —
  reader-to-reader transform via async-compression. Memory cost is
  codec-window-sized, not segment-sized.
* `pg::wal::fetch` walks `CANDIDATE_EXTS: &[&str] = &["zst", "br",
  "lz4", "lzma", ""]` so a bucket written by any of those codecs is
  readable. Object-store side, not local-file side.
* `pg::wal::show.rs:109` strips one compression suffix from the
  filename to recover the segment name. Reusable.

Missing on the wal-rs side: a **sync, file-path-keyed** helper that
auto-detects from the suffix and returns uncompressed segment bytes.
The async machinery is good for object-store streaming; walshadow's
sync `filter_segment(&[u8], &str, &mut Filter)` contract wants bytes
in hand.

## Proposed wal-rs surface

```rust
// src/pg/wal/segment_file.rs (new)

#[derive(Debug, thiserror::Error)]
pub enum SegmentFileError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("compression: {0}")]
    Decompress(#[from] crate::compression::CompressionError),
    #[error("unsupported suffix {0:?}")]
    UnsupportedSuffix(String),
    #[error("decoded size {got} != expected {expected}")]
    SizeMismatch { got: usize, expected: usize },
}

/// Detect compression from the trailing suffix (after stripping a
/// single `.partial`), inflate to a `Vec<u8>`, optionally verify
/// against `expected_size` (typically `DEFAULT_WAL_SEG_SIZE`).
///
/// `path` may name an uncompressed segment (no suffix), an archived
/// segment (`*.zst`, `*.lz4`, `*.gz`, `*.lzma`, `*.br`), or a
/// `pg_receivewal --compress` partial (`*.zst.partial`, etc).
pub fn read_segment_file_sync(
    path: &std::path::Path,
    expected_size: Option<u64>,
) -> Result<Vec<u8>, SegmentFileError>;

/// Async counterpart for callers driving wal-rs's existing
/// AsyncReader pipeline. Detects suffix, returns the streaming
/// reader. Caller decides whether to materialise.
pub fn read_segment_file_async(
    path: &std::path::Path,
) -> Result<crate::compression::AsyncReader, SegmentFileError>;

/// Pure suffix → Method classifier with `.partial` peel-off. Public
/// so the existing fetch path and walshadow's restore_command shim
/// share one source of truth (currently fetch.rs has its own
/// `CANDIDATE_EXTS` and `show.rs` has its own stripper).
pub fn classify_segment_path(
    path: &std::path::Path,
) -> Result<(SegmentName, crate::compression::Method), SegmentFileError>;
```

Implementation budget on wal-rs:

```
src/compression/mod.rs            +~30   Method::Gz variant + flate2
                                        (gated behind existing async-
                                        compression feature flag)
src/pg/wal/segment_file.rs        new — ~120 LOC  sync + async helpers,
                                                  partial-suffix peel,
                                                  size verification
src/pg/wal/mod.rs                 +~3    re-export
tests                             +~150  per-codec round-trip on a
                                        16 MiB zeroed segment plus
                                        one real captured segment per
                                        codec for fixture parity
```

Total wal-rs: ~150 LOC src + ~150 LOC tests. No breaking changes:
`Method::Gz` is additive; existing call sites (`pg/wal/fetch.rs`'s
`CANDIDATE_EXTS`) opt in by extending the slice when gzip enters
their codec matrix. The classifier helper supersedes private logic
in `fetch.rs` and `show.rs` but those keep their current behaviour
during migration — flip them in a follow-up commit so the surface
change is observable in isolation.

### Synchronous bridge

`read_segment_file_sync` needs the same async-compression pipeline
as the async path; running tokio for one file would be heavy. Two
implementation choices:

* **A.** Pull the codecs from the sync side directly: `lz4`,
  `zstd::stream::read::Decoder`, `flate2::read::GzDecoder`,
  `xz2::read::XzDecoder`, `brotli::Decompressor`. All sync, all in
  the existing transitive dep graph or one-step adds. Replaces the
  async pipeline for the sync helper only.
* **B.** Drive the async pipeline via a `Runtime::new_current_thread()`
  inside the helper. Adds tokio overhead per call; simpler code.

**A** is preferred. Memory cost is bounded (single segment ≤ 16 MiB),
and the sync codec crates are the same C libs the async ones wrap;
no duplication.

## Walshadow consumers

### `walshadow-filter` binary

`src/bin/filter.rs:54`:

```rust
let bytes =
    fs::read(&args.input).with_context(|| format!("read input {}", args.input.display()))?;
let name = args.input.file_name()…;
```

Becomes:

```rust
let (seg_name, _method) = wal_rs::pg::wal::classify_segment_path(&args.input)
    .with_context(|| format!("classify {}", args.input.display()))?;
let bytes = wal_rs::pg::wal::read_segment_file_sync(&args.input, Some(WAL_SEG_SIZE))
    .with_context(|| format!("read input {}", args.input.display()))?;
let name = seg_name.format();
```

Side-effects: the manifest's `source_segment` now consistently uses
the canonical 24-hex name, dropping any suffix the operator passed
on the command line. Today the binary uses
`args.input.file_name()` raw, so `00000001000000000000001A.zst` rides
through into the manifest. The classifier-keyed lookup is the right
fix — but flag this as an observable change to anyone parsing
manifest sidecars.

### `walshadow-stream` binary

Live wire stream is uncompressed. No change unless a future
`--archive-source` flag lands (would pump segments out of an object
store between live-stream and on-disk archive). Out of scope; the
hook would be a new ingress, not a modification of the existing
`SourceFeed` path.

### Tests

`tests/filter_round_trip.rs:33` + `tests/classify_fixture.rs:23`
collapse to:

```rust
let bytes = wal_rs::pg::wal::read_segment_file_sync(&seg, Some(WAL_SEG_SIZE))
    .expect("read fixture");
```

The local `decompress_gz` helpers go away. Existing `.gz` fixtures
work after wal-rs gains `Method::Gz`; nothing about the captured
files changes.

### Manifest

`Manifest::source_segment` is a free-form string today
(`filter_segment.rs:95`). Future readers may want to recover the
original codec for diagnostic output. Out of scope for this plan —
manifest stays segment-name-only. If a downstream consumer needs
codec metadata, add a sidecar field with a major version bump on
`FILTER_VERSION`.

## Pitfalls

### 1. Suffix ambiguity

`pg_receivewal --compress=zstd` writes `*.zst.partial` until the
segment fills, then renames to `*.zst`. wal-g writes `*.br` or
`*.lzma` per its config. `archive_command` operators write anything
the script emits, including `.bz2` (out of scope; bz2 is rare for
WAL and not in wal-rs's matrix). Classifier rule: peel exactly one
`.partial` suffix, then exactly one compression suffix; everything
else is the segment name. Path with no compression suffix → `Method::None`.

### 2. `.partial` not the same as truncated

A `.partial` segment is well-formed up to its byte count: pages
beyond the write are zero-padded by pg_receivewal. `filter_segment`
already tolerates this via the zero-padded-page terminator in
`SegmentWalker`. `expected_size = Some(WAL_SEG_SIZE)` size-check
should treat `.partial` callers leniently — caller knows whether to
require full size. Surface as `expected_size: Option<u64>` rather
than mandatory; default `Some(WAL_SEG_SIZE)` in the binary, `None`
in tests that hand-craft sub-segment fixtures.

### 3. Suffix vs magic-bytes detection

Suffix-keyed detection is the file-system convention every WAL
archive tool uses. Magic-bytes sniffing (zstd's `0x28B52FFD`,
gzip's `0x1F8B`, etc.) would be more robust against renamed files
but adds I/O surface for no real benefit — operators never rename
WAL files in archive. Skip magic-bytes; fail loudly on unrecognised
suffix.

### 4. Decoded-size verification

Compressed-segment writers always produce exactly `WAL_SEG_SIZE`
bytes when inflated (PG segment files are fixed-size). Verifying
this catches corruption in transit and operator mistakes (truncated
download, half-written archive). Cheap; `expected_size` knob makes
it opt-in for fixture authors who genuinely need sub-segment files.

### 5. Live wire path is genuinely uncompressed

`START_REPLICATION PHYSICAL` emits CopyData(`'w'`) frames carrying
raw WAL bytes. No matter what `wal_compression` is set to on source,
the wire never compresses the segment framing. This plan does not
need to touch `SourceFeed` or `WalStream`. Confusion between
"compressed WAL on the wire" (does not exist) and "compressed FPI
inside a WAL record" (does exist, see
[FPI_COMPRESSION.md](FPI_COMPRESSION.md)) is the most common
mis-framing — call it out in the binary `--help` text.

### 6. `Method::Gz` lift on wal-rs

wal-rs doesn't write gzip anywhere today. Adding `Gz` only to the
sync helper risks divergence (decoders that accept gzip, encoders
that don't). Cleanest path: add `Gz` to both `encode` and `decode`
in `compression::mod.rs` with `async-compression`'s `GzipEncoder` /
`GzipDecoder`, plus the sync helper's `flate2` codec. Symmetric
matrix.

### 7. Path-keyed surface vs reader-keyed surface

`read_segment_file_sync(&Path, …)` is concrete; alternative shape
is `decode_with_method_sync(Method, &mut dyn Read) -> Vec<u8>` and
let the caller open the file. Path-keyed is the right default
because suffix detection lives one step removed from the caller —
otherwise every caller re-implements suffix parsing. Reader-keyed
variant can land as a follow-up if the future restore_command shim
wants in-memory inputs (e.g. blob fetched from object store
already in a `Bytes` buffer); thin wrapper, doesn't change the
plan's surface count.

### 8. Compression-suffix in `SegmentName::parse`

`SegmentName::parse` (`wal_rs::pg::wal::segment`) today rejects
anything that isn't exactly 24 hex chars. The classifier strips the
suffix before parsing, so the bare segment name reaches
`SegmentName::parse` unchanged — no `SegmentName` API change
needed. Keep the parser strict.

## Test plan

```
wal-rs tests:
  segment_file_sync_per_codec     16 MiB zeroed bytes, encode via
                                   compression::encode, write to
                                   tmpfile with the codec's extension,
                                   read_segment_file_sync, assert
                                   round-trip equal — one case per
                                   {None, Gz, Lz4, Zstd, Lzma, Br}.
  partial_suffix_peel              fixture named `…0001A.zst.partial`
                                   classifies as Zstd + the bare name.
  size_mismatch_errors             feed 8 MiB of zeros via Zstd,
                                   ask for Some(16 MiB), expect
                                   SegmentFileError::SizeMismatch.
  unsupported_suffix_errors        file `foo.7z`; expect Err.

walshadow tests (re-using new wal-rs helper):
  filter_round_trip                drop local decompress_gz, switch
                                   fixture reader to read_segment_file_sync.
                                   Tests pass byte-identically.
  classify_fixture                 same drop-in.
  walshadow-filter cli             new test: pass a Zstd-compressed
                                   captured segment to the binary,
                                   assert the resulting filtered
                                   segment + manifest match the
                                   uncompressed-fixture baseline.
```

## Estimate

```
wal-rs:
  src/compression/mod.rs            +~30  Gz variant + level mapping
  src/pg/wal/segment_file.rs        new — ~120
  src/pg/wal/mod.rs                 +~3   re-export
  Cargo.toml                        +~5   flate2 (or feature-on
                                          async-compression["gzip"])
                                          plus sync codec deps for the
                                          sync helper: zstd, lz4,
                                          flate2, xz2, brotli
  tests                             +~200 (replaces ~60 LOC of inline
                                           gunzip in existing tests)

walshadow:
  src/bin/filter.rs                 ±~10  delegate to wal-rs helper
  tests/filter_round_trip.rs        -~30  drop decompress_gz
  tests/classify_fixture.rs         -~30  drop decompress_gz
  fixtures/wal/README.md            +~10  document codec matrix
  plans/SEGMENT_COMPRESSION.md      this
```

Combined: ~350 LOC src + ~200 LOC tests, of which walshadow accounts
for ~50 LOC of *delta* (most movement is line-deletes).

## Sequencing

* Standalone. Doesn't block any phase; doesn't block
  [FPI_COMPRESSION.md](FPI_COMPRESSION.md). Land before
  [BASEBACKUP.md](BASEBACKUP.md)'s Phase 6.5 if archive replay is
  chosen for catch-up; otherwise sequence freely.
* Sibling commits possible across repos. wal-rs side lands first
  (gates the walshadow-side `cargo update`); walshadow follow-up
  flips ingress + drops the test gunzip helpers.

## Recommendation

1. Land `Method::Gz` + `read_segment_file_sync` +
   `classify_segment_path` in wal-rs. Sync helper uses sync codec
   crates (option A); async helper continues through async-compression.
2. Walshadow flips `walshadow-filter` to the new entry point and
   drops the two test-local `decompress_gz` helpers. Fixture files
   stay `.gz` for repo size; classifier transparently inflates.
3. Manifest `source_segment` field gets the canonical 24-hex name
   (drops any suffix the operator passed). Note in CHANGELOG; no
   `FILTER_VERSION` bump (no schema change, just normalisation).
4. Defer object-store ingress (`--archive-source` on
   `walshadow-stream`) until [BASEBACKUP.md](BASEBACKUP.md) Phase
   6.5's catch-up replay materialises. The helpers in this plan
   support that future ingress without further wal-rs change.
