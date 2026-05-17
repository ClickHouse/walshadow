# SEGMENT_COMPRESSION — compressed WAL segment file ingestion

Evaluation. Pairs with [FPI_COMPRESSION.md](FPI_COMPRESSION.md): both
arrive together but are independently shippable. Status: **not
committed work**.

Lift sits on the wal-rs side per the project boundary
([BASEBACKUP.md §"Library surface to shape on wal-rs"](BASEBACKUP.md)
sets the precedent). Walshadow consumes via the binaries' tokio
runtimes; `filter_segment` itself stays sync.

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

## Async layering

`filter_segment` is pure CPU with random-access rewrite
(`filter_segment.rs:73` scatters NOOP bytes back into `out` at the
walker-recorded ranges). Stays sync. Wrapping it in `async`
would add poll boilerplate around a synchronous CPU walk with zero
suspension points.

The binaries drive the async pipeline:

* `walshadow-stream` is already `#[tokio::main(flavor = "multi_thread")]`.
* `walshadow-filter` flips from sync `fn main` to
  `#[tokio::main(flavor = "current_thread")]`. Tokio is already a
  walshadow direct dep (`Cargo.toml:37`), so binary-size and
  build-graph cost is nil. Per-invocation runtime cost on a CLI
  tool is sub-millisecond, irrelevant against 16 MiB of segment I/O.

Bytes flow: file path → wal-rs async decoder → `read_to_end` into a
`Vec<u8>` → `filter_segment(&bytes, &name, &mut filter)`.

Materialising into a `Vec<u8>` (single 16 MiB allocation per segment)
is the right call for now: `filter_segment` needs the whole segment
in hand to walk records and scatter rewrites. A future
chunk-driven walker (already noted in `wal_stream.rs:13-16`) could
push back to a streaming `AsyncRead` consumer, but that's an
orthogonal redesign — not gated on compression support.

## What wal-rs already gives us

* `compression::Method` with `from_extension(".zst"|".lz4"|"…")`,
  `from_name("zstd"|"lz4"|"…")`, `extension(self) -> &'static str`.
  Covers `None | Zstd | Brotli | Lz4 | Lzma`. **Gap: no `Gz`** —
  wal-rs writes archive bytes but never gzip; the existing tests'
  `decompress_gz` helper exists only because the fixtures were
  captured via `gzip`. Adding `Gz` is a sympathetic ~20 LOC lift
  (async-compression's `GzipEncoder` / `GzipDecoder` already
  reachable via feature flag).
* `compression::decode(method, AsyncReader) -> AsyncReader` —
  reader-to-reader transform via async-compression. Memory cost is
  codec-window-sized, not segment-sized.
* `pg::wal::fetch` walks `CANDIDATE_EXTS: &[&str] = &["zst", "br",
  "lz4", "lzma", ""]` so a bucket written by any of those codecs is
  readable. Object-store side, not local-file side.
* `pg::wal::show.rs:109` strips one compression suffix from the
  filename to recover the segment name. Reusable.

Missing on the wal-rs side:

* Path-suffix classifier shared between `fetch.rs`, `show.rs`, and
  walshadow's binary.
* Thin async helper that opens a file, attaches the decoder based
  on the classifier, returns an `AsyncReader`.

## Proposed wal-rs surface

```rust
// src/pg/wal/segment_file.rs (new)

#[derive(Debug, thiserror::Error)]
pub enum SegmentFileError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("unsupported suffix {0:?}")]
    UnsupportedSuffix(String),
    #[error("not a segment name: {0:?}")]
    BadSegmentName(String),
}

/// Pure suffix → (canonical segment name, codec) classifier with one
/// `.partial` peel-off. Public so the existing fetch path and
/// walshadow's binary share one source of truth.
pub fn classify_segment_path(
    path: &std::path::Path,
) -> Result<(SegmentName, crate::compression::Method), SegmentFileError>;

/// Open a segment file, attach the decoder selected by
/// `classify_segment_path`. Caller drives the reader (typically
/// `tokio::io::AsyncReadExt::read_to_end` into a `Vec<u8>`; future
/// chunk-driven walker streams directly).
pub async fn open_segment_file(
    path: &std::path::Path,
) -> Result<
    (SegmentName, crate::compression::AsyncReader),
    SegmentFileError,
>;
```

Implementation budget on wal-rs:

```
src/compression/mod.rs            +~30   Method::Gz variant
src/pg/wal/segment_file.rs        new — ~80 LOC   classifier + async opener,
                                                   partial-suffix peel
src/pg/wal/mod.rs                 +~3    re-export
Cargo.toml                        +~1    async-compression["gzip"] feature
tests                             +~120  per-codec round-trip on a
                                         16 MiB zeroed segment plus
                                         classifier unit tests
```

Total wal-rs: ~110 LOC src + ~120 LOC tests. No breaking changes:
`Method::Gz` is additive; existing call sites (`pg/wal/fetch.rs`'s
`CANDIDATE_EXTS`) opt in by extending the slice when gzip enters
their codec matrix. The classifier helper supersedes private logic
in `fetch.rs` and `show.rs` but those keep their current behaviour
during migration — flip them in a follow-up commit so the surface
change is observable in isolation.

## Walshadow consumers

### `walshadow-filter` binary

`src/bin/filter.rs:42` becomes:

```rust
#[tokio::main(flavor = "current_thread")]
async fn main() -> ExitCode {
    match run(Args::parse()).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => { eprintln!("walshadow-filter: {e:#}"); ExitCode::FAILURE }
    }
}

async fn run(args: Args) -> Result<()> {
    let (seg_name, reader) = wal_rs::pg::wal::open_segment_file(&args.input)
        .await
        .with_context(|| format!("open {}", args.input.display()))?;
    let mut bytes = Vec::with_capacity(WAL_SEG_SIZE as usize);
    tokio::pin!(reader);
    reader.read_to_end(&mut bytes)
        .await
        .with_context(|| format!("decode {}", args.input.display()))?;
    let name = seg_name.format();

    let mut filter = Filter::new();
    let (filtered, manifest) = filter_segment(&bytes, &name, &mut filter)
        .with_context(|| format!("filter {}", args.input.display()))?;
    // …existing write logic, now async-friendly fs ops or tokio::fs.
}
```

Side-effects:

* Manifest `source_segment` now uses the canonical 24-hex name from
  the classifier rather than `args.input.file_name()` raw. Today
  `00000001000000000000001A.zst` rides through into the manifest;
  the change normalises it. Flag in CHANGELOG; no `FILTER_VERSION`
  bump (no schema change).
* File-write side (`fs::write` of filtered segment + manifest)
  stays `std::fs` — fast, sync, no contention worth tokio. Or flip
  to `tokio::fs` for stylistic consistency; doesn't matter for
  correctness.

### `walshadow-stream` binary

No change. Live wire is uncompressed. Future `--archive-source`
flag (BASE_BACKUP catch-up replay) reuses `open_segment_file`
naturally — same async surface as `SourceFeed`.

### Tests

`tests/filter_round_trip.rs:33` + `tests/classify_fixture.rs:23`
collapse the local `decompress_gz` helpers. Three test shapes
available:

* **`#[tokio::test]`** for tests that aren't doing much beyond
  loading a fixture and calling `filter_segment`. Existing
  `tests/wal_stream_e2e.rs` is already this shape.
* **Sync test + `block_on` helper.** For tests that prefer to stay
  sync, add a `walshadow::test_support::load_segment_blocking(path)
  -> Vec<u8>` that builds a current-thread runtime and runs
  `open_segment_file + read_to_end`. ~10 LOC of helper; lets
  fixture loaders stay outside the async colour.
* **Pre-baked `.bin` fixtures.** Keep one uncompressed fixture per
  scenario as a sanity baseline; the compressed variants exercise
  the new code path.

Recommendation: `#[tokio::test]` for fixture-heavy round-trip tests
(`filter_round_trip`, `classify_fixture`); `block_on` helper if any
existing sync test bodies bristle at conversion.

### Manifest

`Manifest::source_segment` stays a free-form string. Codec
metadata is a non-goal for this plan. Future readers wanting the
original codec can add a sidecar field with a `FILTER_VERSION`
bump.

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
`SegmentWalker`. Size verification is opt-in by the caller: the
binary checks `bytes.len() == WAL_SEG_SIZE` for `.zst` etc., skips
the check for `.partial`.

### 3. Suffix vs magic-bytes detection

Suffix-keyed detection is the file-system convention every WAL
archive tool uses. Magic-bytes sniffing (zstd's `0x28B52FFD`,
gzip's `0x1F8B`, etc.) would be more robust against renamed files
but adds I/O surface for no real benefit — operators never rename
WAL files in archive. Skip magic-bytes; fail loudly on unrecognised
suffix.

### 4. Live wire path is genuinely uncompressed

`START_REPLICATION PHYSICAL` emits CopyData(`'w'`) frames carrying
raw WAL bytes. No matter what `wal_compression` is set to on source,
the wire never compresses the segment framing. This plan does not
need to touch `SourceFeed` or `WalStream`. Confusion between
"compressed WAL on the wire" (does not exist) and "compressed FPI
inside a WAL record" (does exist, see
[FPI_COMPRESSION.md](FPI_COMPRESSION.md)) is the most common
mis-framing — call it out in the binary `--help` text.

### 5. `Method::Gz` lift on wal-rs

wal-rs doesn't write gzip anywhere today. Adding `Gz` only as a
decoder risks asymmetry. Cleanest path: add to both `encode` and
`decode` in `compression::mod.rs` via async-compression's
`GzipEncoder` / `GzipDecoder`, gated behind the
`async-compression["gzip"]` feature. Symmetric matrix.

### 6. `current_thread` runtime in `walshadow-filter`

`flavor = "current_thread"` avoids the multi-thread runtime's
worker-pool startup cost (microseconds, but appreciable on a
short-lived CLI tool). The decoder is single-threaded anyway —
async-compression doesn't fan out across cores. No reason to pay
for multi-thread runtime here.

### 7. Sync `filter_segment` stays sync

Explicit non-goal: do not change `filter_segment` to `async`. CPU
work, no I/O, random-access rewrite — async surface would be poll
boilerplate around synchronous code. The binary materialises bytes
then calls into sync code; this is the right layering.

### 8. Compression-suffix in `SegmentName::parse`

`SegmentName::parse` (`wal_rs::pg::wal::segment`) today rejects
anything that isn't exactly 24 hex chars. The classifier strips the
suffix before parsing, so the bare segment name reaches
`SegmentName::parse` unchanged — no `SegmentName` API change
needed. Keep the parser strict.

## Test plan

```
wal-rs tests:
  classify_segment_path            (name.zst, name.zst.partial,
                                    name.lz4, name, name.bogus) →
                                    (SegmentName, Method) or Err.
  open_segment_file_per_codec      16 MiB zeroed bytes, encode via
                                    compression::encode, write to
                                    tmpfile, open_segment_file +
                                    read_to_end, assert round-trip
                                    equal — one case per
                                    {None, Gz, Lz4, Zstd, Lzma, Br}.
  partial_suffix_peel              fixture named `…0001A.zst.partial`
                                    classifies as Zstd + the bare
                                    name.
  unsupported_suffix_errors        file `foo.7z`; expect Err.

walshadow tests (re-using new wal-rs helper):
  filter_round_trip                drop local decompress_gz, switch
                                    fixture reader to
                                    open_segment_file via either
                                    #[tokio::test] or load_segment_blocking.
                                    Tests pass byte-identically.
  classify_fixture                 same drop-in.
  walshadow-filter cli             new #[tokio::test]: pass a
                                    Zstd-compressed captured segment
                                    to the binary (via assert_cmd or
                                    Command), assert the filtered
                                    segment + manifest match the
                                    uncompressed-fixture baseline.
```

## Estimate

```
wal-rs:
  src/compression/mod.rs            +~30   Method::Gz variant + level
  src/pg/wal/segment_file.rs        new — ~80   classifier + async open
  src/pg/wal/mod.rs                 +~3    re-export
  Cargo.toml                        +~1    async-compression["gzip"]
  tests                             +~120

walshadow:
  src/bin/filter.rs                 ±~30  flip to #[tokio::main],
                                          delegate to open_segment_file
  src/lib.rs (or test_support.rs)   +~15  block_on helper for tests
  tests/filter_round_trip.rs        -~25  drop decompress_gz, swap
                                          fixture loader
  tests/classify_fixture.rs         -~25  drop decompress_gz, swap
                                          fixture loader
  fixtures/wal/README.md            +~10  document codec matrix
  plans/SEGMENT_COMPRESSION.md      this
```

Combined: ~260 LOC src + ~120 LOC tests, of which walshadow accounts
for ~50 LOC of *delta* (most movement is line-deletes).

## Sequencing

* Standalone. Doesn't block any phase; doesn't block
  [FPI_COMPRESSION.md](FPI_COMPRESSION.md). Land before
  [BASEBACKUP.md](BASEBACKUP.md)'s Phase 6.5 if archive replay is
  chosen for catch-up; otherwise sequence freely.
* Sibling commits possible across repos. wal-rs side lands first
  (gates the walshadow-side `cargo update`); walshadow follow-up
  flips the binary + drops the test gunzip helpers.

## Recommendation

1. Land `Method::Gz` + `classify_segment_path` + `open_segment_file`
   in wal-rs. All async; no sync bridge needed.
2. Walshadow flips `walshadow-filter` to `#[tokio::main(flavor =
   "current_thread")]` and feeds the decoder output into the
   existing sync `filter_segment`. Drop the two test-local
   `decompress_gz` helpers; fixture loaders pick one of
   `#[tokio::test]` or a `block_on` helper.
3. `filter_segment` itself stays sync. The async layer lives at
   the binary boundary, not inside the rewrite hot path.
4. Manifest `source_segment` field gets the canonical 24-hex name
   (drops any suffix the operator passed). Note in CHANGELOG; no
   `FILTER_VERSION` bump.
5. Defer object-store ingress (`--archive-source` on
   `walshadow-stream`) until [BASEBACKUP.md](BASEBACKUP.md) Phase
   6.5's catch-up replay materialises. `open_segment_file`'s async
   shape composes naturally with that ingress without further
   wal-rs change.
