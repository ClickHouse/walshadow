# Phase 5 prerequisite — async `RecordSink`

Flip `RecordSink::on_record` async so Phase 5's heap-tuple decoder can
`await ShadowCatalog::relation_at` from inside the sink on the hot
path. `WalStream::push` / `WalStream::close` / `flush_current` follow
suit. `SegmentSink` also async — see "SegmentSink follow-up" below;
initial commit kept it sync, follow-up flipped it once the
runtime-blocking cost became obvious.

## Surface change

`src/wal_stream.rs::RecordSink`:

Before:

```rust
pub trait RecordSink {
    fn on_record(&mut self, record: &Record) -> Result<(), SinkError>;
}
```

After:

```rust
pub trait RecordSink {
    fn on_record<'a>(
        &'a mut self,
        record: &'a Record,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>>;
}
```

`WalStream::{push, close}` and the internal `flush_current` are now
`async fn`, returning the same `Result` shape as before.

## Dyn-compatibility approach

Chose manual `Pin<Box<dyn Future<Output = …> + Send + 'a>>` desugaring
over both raw `async fn` in trait & the `async-trait` crate.

* Raw `async fn` in trait (stable since 1.75, available on Rust 2024
  edition) does not produce dyn-compatible traits. `CompositeRecordSink`
  holds `Vec<Box<dyn RecordSink + Send>>` and is therefore non-negotiable
  on dyn safety.
* `async-trait` would work but adds a proc-macro dependency for a
  three-method-impl surface. Manual desugaring expresses the same shape
  in source, keeps the dep graph unchanged, no compile-time macro
  expansion overhead, and the boxed-future allocation cost matches what
  `async-trait` would emit anyway.
* Trade-off: one `Box::pin(async move { … })` per `on_record` call site
  per impl. Six impls in lib + three in integration tests. Hot-path
  allocator cost is one `Box<F>` per record dispatch, dwarfed by
  `parse_record_from_bytes` + filter `decide` upstream of every call.

Documented inline at the `RecordSink` trait definition with a pointer
back to this file.

## Files touched

| File | LOC change |
|---|---|
| `src/wal_stream.rs` | +109 / -53 |
| `src/bin/stream.rs` | +11 / -4 |
| `tests/wal_stream_chunk_boundary.rs` | +13 / -6 |
| `tests/wal_stream_e2e.rs` | +5 / -0 |
| `tests/multi_segment_filter.rs` | +49 / -23 |
| **total** | **+187 / -86 ≈ +101 net** |

Matches the plan's "~150 LOC async-sink refactor" budget.

## Test file audit

| Test file | RecordSink/WalStream usage | Action |
|---|---|---|
| `tests/wal_stream_chunk_boundary.rs` | drives `WalStream::push` over fixture | flip helpers async, `#[tokio::test(flavor = "current_thread")]` |
| `tests/wal_stream_e2e.rs` | live-PG, already `#[tokio::test]` | added `.await` on push/close call sites |
| `tests/multi_segment_filter.rs` | three custom `RecordSink` impls + push calls | flip impls to boxed-future, tests to `#[tokio::test]` |
| `tests/filter_round_trip.rs` | only `filter_segment` (sync), no sink | unchanged |
| `tests/classify_fixture.rs` | no sink, no WalStream | unchanged |
| `tests/catalog_seed.rs` | no sink, no WalStream | unchanged |
| `tests/shadow_catalog.rs` | catalog-only | unchanged |
| `tests/shadow_lifecycle.rs` | shadow PG only | unchanged |

Five of eight existing integration test files unchanged; matches the
plan's "eight existing integration tests" wording (those eight is the
denominator, not the numerator).

## Build + test output

`cargo build -p walshadow --all-targets`:

```
Finished `dev` profile [unoptimized + debuginfo] target(s) in 5.11s
```

`cargo test -p walshadow --lib`:

```
test result: ok. 103 passed; 0 failed; 0 ignored; 0 measured; 0 filtered out
```

`cargo test -p walshadow --test '*'` (last few lines per suite):

```
catalog_seed:               3 passed
classify_fixture:           2 passed
filter_round_trip:          5 passed
multi_segment_filter:       4 passed
shadow_catalog:             9 passed (live PG)
shadow_lifecycle:           3 passed (live PG)
wal_stream_chunk_boundary:  2 passed
wal_stream_e2e:             4 passed (live PG)
```

`cargo clippy -p walshadow --all-targets`:

```
Finished `dev` profile [unoptimized + debuginfo] target(s) in 0.91s
```

No warnings. Hit one transient `clippy::collapsible_if` on the
push-loop's `if … { if let Err(e) = self.flush_current(…).await }`
shape; collapsed via `let`-chain `&&` to silence.

## Known gaps / followups

* Sink fan-out inside `CompositeRecordSink` runs sinks sequentially with
  `await?`. Phase 5's decoder is the only sink with measurable await
  cost; `MetricsRecordSink` plus any future tap sinks are effectively
  sync after the boxed-future stub. Parallel fan-out via
  `futures::future::try_join_all` is possible if a future tap blocks too
  — not now.
* `SegmentSink` stayed sync in the initial commit on the rationale that
  `DirSegmentSink` used `std::fs::write` + atomic rename. Flipped to
  async in the follow-up below once the runtime-blocking cost was
  obvious.
* `flush_current`'s record-sink loop sits in tail position of the
  per-segment dispatch. Currently dispatches one record at a time
  serially; Phase 5 decoder calls `ShadowCatalog::relation_at` which
  consults the in-process LRU cache (cache hit ≈ free, miss ≈ one libpq
  round-trip). If miss rate measured high enough to matter, sink can
  pre-fetch relations in batch before draining the record loop —
  trait surface already supports it (sink owns its own concurrency).
* No backpressure surfaced on `RecordSink`. Sink that wants to apply
  backpressure to the WAL chunk feed today must do so by stalling
  `on_record` (await internal channel send). Documented as sink-owned
  responsibility on the trait.

## SegmentSink follow-up

The "stays sync" rationale held only outside a tokio context. Once
`WalStream::push` became async, the inline `segment_sink.on_segment(…)`
call inside `flush_current` blocked the worker for the duration of the
16 MiB segment write. With `walshadow-stream`'s `worker_threads = 2`,
that parks half the pool; ctrl_c handler, status timer, and
invalidation drain compete for the remaining worker until the write
returns

Flip:

* `SegmentSink::on_segment` / `on_partial_segment` use the same manual
  `Pin<Box<dyn Future<Output = …> + Send + 'a>>` desugaring as
  `RecordSink`. `Vec<Box<dyn SegmentSink + Send>>` is not held anywhere
  in tree today but the symmetry keeps the trait dyn-compatible for
  free
* `DirSegmentSink` switched to `tokio::fs::write` + `tokio::fs::rename`
  (both implemented via `spawn_blocking` internally, so the worker is
  released for other tasks during the write). Manifest is serialised
  via `serde_json::to_vec_pretty` (sync, KiB-sized) and written async
* `CollectingSegmentSink` + adversarial `ErrSegmentSink` test sink
  flipped to boxed-future shape
* `WalStream::flush_current` and `WalStream::close` await the segment
  sink calls
* Inline `dir_sink_*` tests switched to
  `#[tokio::test(flavor = "current_thread")]`

Callers outside `wal_stream.rs` (the `walshadow-stream` binary, every
integration test) only thread sinks through `stream.push(…).await` —
the `.await` already covered both record and segment dispatch, so no
caller-side churn

`DirSegmentSink::new` stays sync (called once at startup before any
tokio runtime work)

103 lib tests + 6 boundary/multi-segment/round-trip integration tests
pass; clippy `-D warnings` clean

## SmallVec follow-up

Same commit cluster: `src/segment.rs` swaps `WalkedRecord::byte_ranges`
and `Pending::byte_ranges` from `Vec<(usize, usize)>` to a new
`pub type ByteRanges = SmallVec<[(usize, usize); 1]>`. Records almost
always live on one WAL page (single range); multi-range only when a
record straddles a page boundary. Allocated per walked record
(millions per segment) so the inline case skips a heap alloc on the
hot path

Not applied to `ReplIdent::Default { pk_attnums }` or
`ReplIdent::UsingIndex { key_attnums }`: tokio-postgres `FromSql`
only impls for `Vec<T>` on PG arrays, so a `SmallVec` field would
need a `Vec → SmallVec` conversion at every cache miss, negating
the alloc-elision benefit. Kept as `Vec<i16>` there

Dep added: `smallvec = "1"`. No new transitive deps
