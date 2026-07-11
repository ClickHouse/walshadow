# plans/future, continuous fuzzing across the stack

Stand up coverage-guided fuzzing over every byte-parsing surface walshadow
owns, sized to run unattended on a VM for weeks. Three crates carry untrusted
input: `wal-rus` (Postgres WAL off the replication wire),
walshadow itself (record bodies, codecs, cursor/config files), and
[`clickhouse-c-rs`](../../clickhouse-c-rs) (ClickHouse Native protocol, type
strings, all crossing into C). Each is a fuzz target tier below.

Method follows the [Rust Fuzz Book](https://rust-fuzz.github.io/book):
`cargo-fuzz` + `libFuzzer` as the engine, the `arbitrary` crate for
structure-aware targets. AFL.rs stays a fallback for targets where libFuzzer's
in-process model fights the C client's global state.

Companion to [coverage100.md](coverage100.md): llvm-cov measures what the test
suite reaches, fuzzing manufactures the inputs that reach the rest. Crashes
become regression seeds, which become covered lines.

## Threat model, what a bug looks like

Inputs are adversarial bytes from a compromised or buggy upstream: a Postgres
that emits malformed WAL, a man-in-the-middle on the replication stream, a
ClickHouse server (or impostor) sending a hostile Native packet, a corrupted
on-disk cursor/spill file. A finding is any of:

- **Panic** in a parser. Malformed input must return `Err`, never `unwrap`/index
  out of bounds. libFuzzer aborts on panic, so every panic is a crash artifact
- **Memory unsafety** in the C client or at the FFI boundary, caught by
  AddressSanitizer (ASan ships by default under `cargo fuzz`)
- **Unbounded allocation** from an attacker-controlled length prefix. WAL and
  Native both carry `u32`/`varint` lengths, parsers must bound them.
  `-rss_limit_mb` + `-malloc_limit_mb` turn an OOM into a reportable crash
- **Invariant break** caught by an in-target assertion (round-trip / re-parse
  oracles below), not just a hard crash

See the [clickhouse-c-rs Safety model](../../clickhouse-c-rs/README.md#safety-model)
for the existing `clickhouse-c-rs` FFI trust
boundary, and [reference: PG WAL struct alignment](reference_pg_wal_alignment.md)
for why hand-rolled WAL struct parsers under-read on padding.

## Crate layout under workspace + submodule shape

`cargo fuzz` wants a `fuzz/` member crate sitting beside the crate under test.
Two complications here:

- `wal-rus` is an external crates.io dependency (own repo, own `Cargo.toml`).
  Standalone/upstreamable fuzz targets live in its own repo; walshadow's `fuzz/`
  crate exercises it transitively and via a direct dep
- `clickhouse-c-rs` is a workspace member and links C via `cc` in its
  `build.rs`. Its fuzz targets need the sanitizer plumbed into the C TU, see
  the C-boundary section

Land one fuzz crate here; standalone wal-rus targets belong upstream in its
own repo:

```
fuzz/                      # walshadow + clickhouse-c-rs targets
  Cargo.toml               # depends on walshadow (pulls wal-rus + chc transitively)
  fuzz_targets/*.rs
  corpus/<target>/
  artifacts/<target>/
```

Generate with `cargo +nightly fuzz init` at the fuzz root, then
`cargo +nightly fuzz add <target>`. The fuzz crate is itself `exclude`d
from the parent workspace (cargo-fuzz does this automatically) so the stable
build is untouched.

`fuzz/Cargo.toml` for the top crate:

```toml
[package]
name = "walshadow-fuzz"
version = "0.0.0"
publish = false
edition = "2024"

[package.metadata]
cargo-fuzz = true

[dependencies]
libfuzzer-sys = { version = "0.4", features = ["arbitrary-derive"] }
arbitrary = { version = "1", features = ["derive"] }
walshadow = { path = ".." }
clickhouse-c-rs = { path = "../clickhouse-c-rs", default-features = false, features = ["lz4"] }
wal-rus = "0.1"

[[bin]]
name = "wal_parse_record"
path = "fuzz_targets/wal_parse_record.rs"
test = false
doc = false
# ... one [[bin]] per target
```

Toolchain: `rustup install nightly` + `cargo install cargo-fuzz`. The same C
deps CI installs (`liblz4-dev libzstd-dev`, see
[`.github/workflows/ci.yml`](../../.github/workflows/ci.yml)) are needed because
the chc link still pulls lz4/zstd

## Target inventory

Anchored on file + function, not line numbers (they go stale, see
coverage100.md). Risk = blast radius if it mishandles bytes.

### Tier A, pure byte→struct parsers

Cheapest to stand up, `fuzz_target!(|data: &[u8]|)` with no setup. Highest
ratio of coverage to effort.

| target | entry | crate | risk |
|---|---|---|---|
| `wal_parse_record` | `walrus::pg::walparser::parse_record_from_bytes(data, page_magic)` | wal-rus | HIGH, core XLogRecord header + block headers + FPI metadata |
| `wal_extract_locations` | `walrus::pg::walparser::extract_block_locations` / `extract_locations_from_wal_file<R: Read>` | wal-rus | HIGH, full-segment walk, integrates lower parsers |
| `wal_decode_frame` | `walrus::pg::replication::stream::decode_frame(payload)` | wal-rus | MED-HIGH, network-facing `'w'`/`'k'` CopyData frames |
| `daemon_parse_args` | `walrus::daemon::protocol::parse_args(body)` | wal-rus | MED, length-prefixed arg vector |
| `wal_page_header` | `walshadow::wal_page::parse_page_header(bytes, page_start)` | walshadow | MED, PG15+ page header magic/flag validation |
| `heap_truncate` | `walshadow::main_data::parse_xl_heap_truncate(md)` | walshadow | MED, array-count loop over relids |
| `numeric` | `walshadow::codecs::decode_numeric(body)` | walshadow | MED, weight/dscale/ndigits consistency, NaN/±Inf |
| `inet` | `walshadow::codecs::decode_inet(body, is_cidr)` | walshadow | MED, family/addr_len/bits |
| `interval` | `walshadow::codecs::decode_interval(body)` | walshadow | LOW, fixed 16-byte struct |
| `ch_type_parse` | `clickhouse_c_rs::TypeAst::parse(s, alloc)` | chc | HIGH, nested type-DSL grammar, crosses into C |

### Tier B, round-trip / differential oracles

The high-yield tier. These assert an invariant inside the target so the fuzzer
hunts for inputs that violate it, not just inputs that crash.

- **`filter_roundtrip`** — `walshadow::filter_segment::filter_segment(bytes,
  name, &mut Filter)`. Feed an arbitrary segment, take the rewritten output,
  re-walk it with `SegmentWalker` + `parse_record_from_bytes`, assert every
  rewritten record re-parses and its CRC validates via
  `walshadow::rewrite::compute_crc`. This is the load-bearing safety property:
  filtered WAL must stay replayable (see [filter.md](filter.md), and
  [reference: cross-seg records](reference_walshadow_cross_seg_records.md) for
  the NOOP-over-fork corner the oracle must not regress)
- **`noop_replace`** — `walshadow::rewrite::noop_replace(&mut bytes)` then
  assert `parse_record_from_bytes` accepts the result and `compute_crc` matches
  the rewritten header. Catches CRC/length drift in the in-place rewriter
- **`cursor_roundtrip`** — `walshadow::cursor::{encode, decode}`. Two halves:
  `decode(arbitrary_bytes)` must never panic and must reject bad CRC/magic;
  `decode(encode(c)) == c` for any `Arbitrary` `Cursor`. 64-byte format, low
  entropy, fuzzer finds the CRC boundary fast
- **`numeric_text`** — decode then render to text, assert no panic/overflow in
  the float-emulation path (no encoder to round-trip against, so it is a
  panic-freedom + output-sanity oracle)

### Tier C, structure-aware (arbitrary)

Deep paths gated behind a valid header the fuzzer would rarely guess from raw
bytes. Use `#[derive(Arbitrary)]` to synthesize the typed precondition, let the
fuzzer drive the body.

- **`heap_decode`** — `walshadow::heap_decoder::decode_heap_record(record,
  source_lsn, rel)` needs a `RelDescriptor` matching the tuple. A raw `&[u8]`
  almost never produces a self-consistent (record, descriptor) pair, so derive
  both: an `Arbitrary` column-shape spec builds a synthetic `RelDescriptor`,
  the same `Unstructured` drives tuple bytes. Exercises bitmap walk,
  prefix/suffix compression, dropped-column and varlena arms
  (see [decoder.md](decoder.md))
- **`wal_parser_stateful`** — `walrus::pg::walparser::WalParser::parse_records_from_page`
  fed a `Vec<[u8; 8192]>` so continuation-record stitching across page
  boundaries is reachable. Carry the `WalParser` across the vec in one target
  invocation (see [reference: body block ids](reference_walrs_block_ids.md) for
  the 252/253/254/255 sentinels this must survive)

Sketch:

```rust
#![no_main]
use libfuzzer_sys::fuzz_target;
use arbitrary::Arbitrary;

#[derive(Arbitrary, Debug)]
struct HeapCase {
    cols: Vec<ColSpec>,   // ColSpec: Arbitrary enum {Int4, Text, ...}
    info_op: u8,
    tuple: Vec<u8>,
}

fuzz_target!(|case: HeapCase| {
    let rel = build_rel_descriptor(&case.cols);
    let record = synth_xlog_record(case.info_op, &case.tuple);
    let _ = walshadow::heap_decoder::decode_heap_record(&record, 0, &rel);
});
```

### Tier D, the C boundary

Highest memory-safety payoff, hardest setup. Parsing happens in the vendored
`clickhouse-c` C library; the Rust side tracks lifetimes. ASan must instrument
the C TU or it sees nothing.

- **`ch_block_read`** — `clickhouse_c_rs::BlockReader::new(io, alloc, opts)`
  then `read()`. Wrap the fuzz `&[u8]` in an in-memory `PosixIo` (no socket),
  vary `BlockOpts` (`has_block_info`, `has_custom_serialization`) from the
  input. Decodes a Native columnar block, the densest C parsing surface
- **`ch_type_parse`** (also Tier A) — `TypeAst::parse` bottoms out in
  `chc_type_parse`, the type-string grammar is a classic fuzzing win
- **`chc_async_submit`** — raw wire bytes into the async client, exercises the
  sans-io decode core (gated on the clickhouse-c sans-io async work, whose
  design doc is not in this tree; activate when `clickhouse-async.h` ships)

C-side instrumentation: cargo-fuzz sets `RUSTFLAGS` for sanitizer + sancov on
Rust, but `cc`-compiled `wrapper.c` and the vendored library are invisible
unless their flags match. Export before building these targets:

```sh
export CFLAGS="-fsanitize=address -fsanitize-coverage=trace-pc-guard,trace-cmp -g -O1"
cargo +nightly fuzz run ch_block_read
```

The `cc` crate honors `CFLAGS`. Without it ASan still guards Rust allocations
but C heap corruption inside the client goes uncaught, and the fuzzer is blind
to C branches so coverage feedback stalls. Re-audit on every chc submodule bump,
this couples to the manual sys.rs/wrapper.c review in
[reference: chc bump audit](reference_chc_rs_bump_audit.md). If the in-process
ASan build proves flaky against the client's global state, fall back to AFL.rs
with persistent-mode off, or fuzz against `clickhouse local` as an external
process oracle (the existing `examples/spawn_clickhouse_local.rs` harness).

## Corpus seeding

Coverage-guided fuzzing converges far faster from real inputs than from zero.

- **WAL segments** live gzipped under
  [`fixtures/wal/*/segments/`](../../fixtures/wal) (`classify`, `filter`,
  `xlog_switch`, `vacuum_full_pg_depend`). Gunzip into
  `fuzz/corpus/wal_extract_locations/`. Split into 8 KiB pages for
  `wal_parser_stateful`, into individual record byte-ranges (via `SegmentWalker`)
  for `wal_parse_record`. `workload.sql` + `capture.sh` regenerate them against
  any PG major if more variety is wanted
- **Unit-test vectors** embedded in `wal-rus` `parse.rs` tests and
  `stream.rs` frame tests are hand-built valid/truncated cases, dump them to
  seed files
- **Codec seeds**: the numeric NaN/±Inf constants, IPv4/IPv6 inet bodies, and
  interval patterns already asserted in `src/codecs.rs` tests
- **Cursor seeds**: `encode` a few valid `Cursor`s + flip bytes for the
  bad-CRC path

A `fuzz/seed.sh` extracts all of the above. Keep generated corpora out of git
(`/target`, `coverage/` already ignored, add `fuzz/corpus` + `fuzz/artifacts`
to `.gitignore`); persist them on the VM and back to object storage instead, the
corpus is large and churny. Crash-artifact files that pin a real bug DO get
checked in, as regression seeds.

## Running indefinitely on a VM

The goal is weeks of unattended fuzzing that survives finding bugs rather than
halting on the first.

- **`-fork=N -ignore_crashes=1`** is the keystone. libFuzzer fork mode runs N
  child fuzzers, reaps any that crash/OOM/timeout, files the artifact, and keeps
  going. Without it the campaign stops at crash #1
- **Resource caps** so one input cannot wedge the VM:
  `-rss_limit_mb=4096 -malloc_limit_mb=2048 -timeout=25`
- **Fair scheduling across targets.** cargo-fuzz runs one target per process; a
  supervisor round-robins so a cheap target does not starve the C-boundary one.
  Per target, `-max_total_time=3600`, loop:

```sh
#!/usr/bin/env bash
# fuzz/run-forever.sh — round-robin every target, persist shared corpus
set -euo pipefail
targets=$(cargo +nightly fuzz list)
while true; do
  for t in $targets; do
    cargo +nightly fuzz run "$t" -- \
      -fork=4 -ignore_crashes=1 \
      -rss_limit_mb=4096 -malloc_limit_mb=2048 -timeout=25 \
      -max_total_time=3600 || true   # never let one target kill the loop
  done
done
```

  Drive it from a `systemd` unit (`Restart=always`) or tmux so a host reboot
  resumes. Honor [feedback: test timeouts stay short](feedback_test_timeouts.md):
  the per-input `-timeout` is small on purpose, a 25s single input is a hang to
  report, not a slow path to wait out
- **Periodic `cargo fuzz cmin <target>`** keeps each corpus minimal as it grows,
  run it from a separate weekly timer, not inside the hot loop
- **Crash handling.** Watch `fuzz/artifacts/<target>/`. On a new file: copy to
  durable storage, reproduce with `cargo +nightly fuzz run <target>
  <artifact>`, minimize with `cargo +nightly fuzz tmin <target> <artifact>`,
  open an issue, commit the minimized artifact as a regression seed
- **Coverage drift.** Weekly `cargo +nightly fuzz coverage <target>` →
  `cargo cov` HTML. Feed the report back into corpus/target work, same lens as
  coverage100.md. A target whose coverage plateaus early wants better seeds or a
  structure-aware rewrite

OSS-Fuzz / ClusterFuzz is the heavier managed alternative if this graduates to
a hosted continuous service; the single-VM supervisor above is the starting
point and needs no external infra.

## CI integration

CI guards against bitrot and replays known crashes; it does not do the long
campaign (that is the VM).

- **Build smoke**, every PR: a `fuzz-build` job on nightly running
  `cargo +nightly fuzz build` for both fuzz crates. Targets that stop compiling
  (signature drift after a refactor) fail here.
- **Regression replay**, every PR: run each committed crash artifact through its
  target as a one-shot (`cargo +nightly fuzz run <target> <artifact>`), or add a
  plain `#[test]` that calls the same entry on the saved bytes so it runs in the
  normal stable suite without nightly. The latter folds fuzz regressions into
  the coverage baseline
- **Optional nightly cron**, short `-max_total_time=120` per target on the CI
  runner as a canary between VM syncs

## Phasing

- **P0** — wal-rus `parse_record_from_bytes` + `extract_locations` (Tier A),
  `filter_roundtrip` + `noop_replace` oracles (Tier B). Core replay-safety
  surface, pure Rust, no external deps, runs on the VM immediately
- **P1** — remaining Tier A (codecs, page header, frame, daemon, truncate) +
  `cursor_roundtrip`. Broad cheap coverage
- **P2** — `heap_decode` + `wal_parser_stateful` structure-aware targets. The
  arbitrary scaffolding is the real work here
- **P3** — Tier D C boundary under ASan with `CFLAGS` plumbed. Highest
  memory-safety value, gated on the sanitizer build being stable

## Gotchas specific to this repo

- **Nightly only.** ASan/sancov need `-Z` flags; the stable test + coverage jobs
  stay untouched. Edition 2024 needs a recent nightly
- **C deps + submodule.** Fuzz builds still link lz4/zstd and need the
  clickhouse-c submodule initialized, mirror `ci.yml`
- **`decode_heap_record` is not `&[u8]`-shaped**, it takes a `&RelDescriptor`.
  Tier C exists precisely because a naive byte target would reject ~everything
  at the descriptor mismatch
- **Re-exported paths only.** `parse_record_from_bytes` and
  `extract_block_locations` are re-exported from `walrus::pg::walparser`;
  internal helpers (`for_each_block_location_in_record`,
  `read_xlog_record_header`), fuzz them through the public surface
- **CRC, not just length.** WAL record validity is a CRC over the body; the
  round-trip oracles must check `compute_crc`, a length-only check passes
  corrupt records (see [filter.md](filter.md))
- **No artifacts in git** except minimized regression seeds. Add `fuzz/corpus`
  and `fuzz/artifacts` to `.gitignore`

## Links

- [coverage100.md](coverage100.md) — fuzzing manufactures the inputs llvm-cov
  shows as missed
- [clickhouse-c-rs Safety model](../../clickhouse-c-rs/README.md#safety-model) — FFI trust boundary the Tier D targets stress
- [filter.md](filter.md) / [decoder.md](decoder.md) — invariants the Tier B/C
  oracles assert
- [oracle.md](../oracle.md) — existing shadow-PG differential oracle, a future
  Tier-3-codec fuzz target could diff against it (live-PG, nightly job, not the
  pure VM loop)
