# compressed WAL segment file ingestion

`walshadow-filter` today refuses anything but a 16 MiB uncompressed
segment file. Operators archiving WAL via `pg_receivewal
--compress=zstd`, wal-g (`.lzma`, `.br`), or `archive_command`
emitting gzip/lz4 must decompress out-of-band before feeding the
binary. Lift compressed-segment ingest so the binary accepts `*.zst`,
`*.lz4`, `*.gz`, `*.lzma` (plus their `.partial` peers) by suffix
classification. Pairs with FPI_COMPRESSION (already shipped); both
target WAL compression but operate at orthogonal layers — segment
framing vs FPI-inside-record

## Where the work lands

Split across two repos per the existing wal-rs / walshadow boundary,
mirroring how base-backup support is split (compressed-archive
classification lives in wal-rs, filter wiring lives in walshadow)

### wal-rs additions

- `compression::Method::Gz` variant. wal-rs covers `None | Zstd |
  Brotli | Lz4 | Lzma` today; gap is gzip because wal-rs writes
  archive bytes but never gzip. Add to both `encode` and `decode`
  symmetrically via `async-compression`'s `GzipEncoder` / `GzipDecoder`,
  gated behind `async-compression["gzip"]` feature
- `pg::wal::segment_file::classify_segment_path(path) ->
  (SegmentName, Method)`. Pure suffix classifier with one `.partial`
  peel-off, then exactly one compression suffix. Shared between
  `fetch.rs`'s `CANDIDATE_EXTS`, `show.rs`'s suffix-strip, and
  walshadow's binary so all three converge on one source of truth.
  Unknown suffix → loud error
- `pg::wal::segment_file::open_segment_file(path) async ->
  (SegmentName, AsyncReader)`. Opens file, attaches decoder selected
  by classifier, returns reader. Caller drives via `read_to_end`
  into `Vec<u8>` (today) or chunk-walker (future)

Budget: ~110 LOC src + ~120 LOC tests. No breaking changes — gzip
is additive; existing `CANDIDATE_EXTS` slices opt in by extending
when gzip enters their matrix

### walshadow flip

`walshadow-filter` switches from `fn main` to `#[tokio::main(flavor
= "current_thread")]`. Tokio is already a direct dep so binary-size
and build-graph cost is nil; per-invocation runtime cost on a CLI
tool is sub-ms, irrelevant against 16 MiB of segment I/O.
`current_thread` avoids multi-thread worker-pool startup cost (the
decoder is single-threaded; async-compression doesn't fan out across
cores)

Bytes flow: file path → `open_segment_file` → `read_to_end` into
`Vec<u8>` (single 16 MiB allocation per segment) → existing sync
`filter_segment(&bytes, &name, &mut filter)`. `filter_segment` stays
sync — it's pure CPU with random-access rewrite scattering NOOP
bytes back into the output buffer at walker-recorded ranges.
Wrapping it in async would add poll boilerplate around a synchronous
walk with zero suspension points

Test-local `decompress_gz` helpers in `tests/filter_round_trip.rs`
and `tests/classify_fixture.rs` go away. Fixture loaders pick one
of `#[tokio::test]` or a `walshadow::test_support::load_segment_blocking`
helper (~10 LOC) that builds a current-thread runtime and runs
`open_segment_file + read_to_end`. Both shapes available so sync test
bodies don't bristle at async-colour conversion

Side-effect: manifest `source_segment` field uses canonical 24-hex
name from classifier rather than `args.input.file_name()` raw.
Today `00000001000000000000001A.zst` rides through into manifest;
normalisation drops the suffix. Note in CHANGELOG, no
`FILTER_VERSION` bump (no schema change)

## Codec matrix

`zstd`, `lz4`, `gzip`, `lzma`, plus uncompressed. `brotli` is
wal-rs's choice; walshadow inherits but doesn't push the matrix.
`bz2` out of scope — rare for WAL, not in wal-rs's matrix.
Suffix-keyed detection only; magic-bytes sniffing rejected. Operators
never rename WAL files in archive, and adding I/O surface for
robustness against a non-existent failure mode buys nothing. Fail
loudly on unrecognised suffix

## Independence

Sibling of FPI_COMPRESSION (already shipped). Two layers, both named
"WAL compression", easy to conflate:

- **FPI compression** (shipped): `wal_compression` GUC compresses
  FPIs *inside* WAL records. Decoder-side concern, handled in
  filter_segment's walker
- **Segment compression** (this doc): archive tool wraps the whole
  16 MiB segment file in a codec envelope after PG emits it. File
  I/O concern, handled at the binary boundary

Live wire (`START_REPLICATION PHYSICAL`) emits raw WAL bytes
regardless of `wal_compression` — the wire never compresses segment
framing. This plan doesn't touch `SourceFeed` or `WalStream`.
Confusion between "compressed WAL on the wire" (doesn't exist) and
"compressed FPI inside a WAL record" (does exist, separate plan) is
the most common mis-framing; call out in `--help` text

## Why deferred

Operationally infrequent. Archived WAL is usually uncompressed in
the deployments walshadow has seen so far; the uncompressed-only
fast path covers ~90% of production. Codec matrix coverage is
engineering-bandwidth gated, not architecture-gated. Add when an
operator deployment forces it — easier to land then with the
specific codec they care about already mapped to a fixture, than to
ship the full matrix speculatively. The two test-local
`decompress_gz` helpers continue to work in the meantime; they're
ugly but functional

Upcoming ingresses that *would* force the work, none of them
shipped:

- `restore_command` shim for shadow's `pg_wal/`: once shadow feeds
  from filtered segments out of `--out-dir`, a future variant must
  accept segments arriving in archive form
- BASE_BACKUP catch-up replay: per BASEBACKUP.md §5, walshadow may
  need to feed shadow from WAL archive between start_lsn and
  live-stream catch-up. Archives commonly `.zst` (wal-g, pgbackrest)
  or `.lz4`
- Fixture capture: extending `fixtures/wal/` for
  `wal_compression={pglz,lz4,zstd}` scenarios wants compressed
  fixture files (otherwise repo bloats)

None of those have a hard date. Each lands its own scope, and the
segment-compression lift slots in cleanly alongside whichever
materialises first

## Dependencies

None hard. Engineering bandwidth is the only gate. Sibling commits
across repos: wal-rs side lands first (gates walshadow-side `cargo
update`), walshadow follow-up flips the binary and drops gunzip
helpers. Doesn't block FPI_COMPRESSION (already shipped) or
BASEBACKUP

## Acceptance

- `classify_segment_path` returns expected `(SegmentName, Method)`
  for `name.zst`, `name.zst.partial`, `name.lz4`, `name`, errors on
  `name.bogus`
- `open_segment_file_per_codec`: 16 MiB zeroed bytes, encode via
  `compression::encode`, write to tmpfile, `open_segment_file +
  read_to_end`, byte-identical round-trip per codec in {None, Gz,
  Lz4, Zstd, Lzma, Br}
- `partial_suffix_peel`: fixture `…0001A.zst.partial` classifies as
  `(SegmentName, Zstd)` with bare 24-hex name
- `filter_round_trip` + `classify_fixture`: local gunzip helpers
  dropped, fixture loader switched to `open_segment_file`, tests
  pass byte-identically
- New `walshadow-filter` CLI test passes Zstd-compressed captured
  segment to binary (via `assert_cmd`), filtered output + manifest
  match uncompressed-fixture baseline byte-for-byte
- Manifest `source_segment` carries canonical 24-hex name in all
  paths
- `--help` text documents codec matrix and the "live wire is
  uncompressed" disclaimer

## Pitfalls

- **Suffix ambiguity.** `pg_receivewal --compress=zstd` writes
  `*.zst.partial` until segment fills, then renames to `*.zst`.
  Classifier peels exactly one `.partial` then exactly one
  compression suffix; path with no compression suffix → `Method::None`
- **`.partial` ≠ truncated.** A `.partial` segment is well-formed
  up to its byte count; pages beyond write are zero-padded by
  pg_receivewal. `filter_segment` already tolerates this via
  zero-padded-page terminator in `SegmentWalker`. Size check is
  opt-in: binary verifies `bytes.len() == WAL_SEG_SIZE` for `.zst`
  etc., skips check for `.partial`
- **`Method::Gz` asymmetry.** wal-rs doesn't write gzip anywhere
  today. Adding `Gz` only as decoder risks the codec matrix going
  one-way. Cleanest path: add to both `encode` and `decode`
  symmetrically, gated by feature flag. Cost is trivial; payoff is
  a uniform matrix
- **`SegmentName::parse` strictness.** Today rejects anything that
  isn't exactly 24 hex chars. Classifier strips suffix *before*
  parsing, so bare segment name reaches `SegmentName::parse`
  unchanged — no API change needed. Keep the parser strict
