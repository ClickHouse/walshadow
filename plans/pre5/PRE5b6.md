# PRE5b6 — sink fan-out (retrospective)

[PRE5b](PRE5b.md) item S2. Second of the foundation changes, built
on [PRE5b5](PRE5b5.md)'s `Record` shape. Unblocks
[Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix)
attaching a `DecoderSink` alongside the existing `SegmentSink` writer
without rewriting [`WalStream::push`](../src/wal_stream.rs).

## Why (preserved)

`WalStream::push` took `&mut dyn RecordSink` singular
(`wal_stream.rs:270` in PRE5b5's tree).
[Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix)'s
`DecoderSink` will run alongside `CollectingRecordSink` in tests and
the production `SegmentSink` writer; future metrics or oracle taps
may want a third position on the same chain. PRE5b6 lands the
fan-out container so the `push` signature does not have to grow per
consumer.

## What landed

* **`CompositeRecordSink { inner: Vec<Box<dyn RecordSink + Send>> }`.**
  Public struct in `src/wal_stream.rs`. `inner` is `pub` so callers
  can rearrange or append after construction; the canonical entry
  point is `CompositeRecordSink::new(vec![...])`. Implements
  `RecordSink::on_record` by iterating `inner` and calling each
  child's `on_record(record)?` — short-circuits on the first `Err`,
  no inner sink runs after a failing one. The `Send` bound on the
  trait object keeps the surface usable from any future async
  daemon orchestration without re-boxing; the trait itself is
  unchanged.
* **`CountingRecordSink`.** New `pub` `RecordSink` helper that only
  increments a `u64`. Pairs with `CollectingRecordSink` under
  `CompositeRecordSink` when a test wants a second observer without
  holding clones. Same shape as `CollectingRecordSink` but cheaper
  for stress sizes — Phase 5's heap-decoder test can substitute the
  counting sink anywhere it needs "did this record reach the chain"
  rather than "what did this record look like".
* **Post-error contract documented on `CompositeRecordSink`.** The
  doc block enumerates: inner sinks before the failing one observed
  the record, the failing sink may have observed it partially, sinks
  after have not. Error propagates as `WalStreamError::Sink`. The
  poisoned-stream consequence (don't call `push` again after a sink
  error) is cross-referenced to [PRE5b10](PRE5b10.md) item 4 — that
  item formalises the `next_lsn` rollback decision and adds the
  state-field guard.

## Tests

* `cargo test --lib`: 85 pass (was 83; +2).
  * `wal_stream::tests::composite_record_sink_fans_out_to_all_inner_sinks_in_order`
    builds three synthetic `Record`s with distinct rmids
    (Heap / Xact / RelMap), wraps two `SharedRmidLog` sinks (each
    appends the observed `resource_manager_id` to its
    `Arc<Mutex<Vec<u8>>>`) under a `CompositeRecordSink`, dispatches
    the records, and asserts both logs read back the rmid sequence
    `[Heap, Xact, RelMap]` — proves both inner sinks observed every
    record in source order.
  * `wal_stream::tests::composite_record_sink_short_circuits_on_first_err`
    chains `SharedRmidLog` → `ErrAt { fail_at: 1 }` → `SharedRmidLog`.
    First `on_record` succeeds (ErrAt's `seen == 0 != 1`); second
    fires the error. Asserts the resulting `Err` is
    `SinkError::Other` with the synthetic message, and that the
    post-error state matches the documented contract:
    `before == 2`, `err_seen == 2`, `after == 1`. Reasoning for
    `after == 1`: the first dispatch ran every sink in order
    (after-log saw it), the second dispatch stopped at ErrAt.
* `cargo test --tests`: 26 pass (was 24; +2). Both new cases live in
  `tests/multi_segment_filter.rs` (deviation from plan; see below):
  * `composite_sink_fans_out_to_all_inner_sinks` builds a four-record
    single-page segment of pg_class heap-insert records (all
    `Class::Catalog` by the bootstrap rule, all Kept), pushes through
    `WalStream::push` with a `CompositeRecordSink` wrapping
    `SharedCollectingSink` + `SharedCountingSink`. Asserts the
    collecting sink saw four records with `block_no` matching the
    build-time iteration index, and the counting sink saw four. This
    is the "push one segment" coverage the plan called for.
  * `composite_sink_propagates_inner_error_and_short_circuits` chains
    `SharedCountingSink` → `FailOnNth { fail_at: 1 }` →
    `SharedCountingSink` against a three-record segment. Asserts
    `WalStream::push` returns
    `WalStreamError::Sink(SinkError::Other(_))`, the leading sink
    saw two records, the failing sink saw two (Ok then Err), and the
    trailing sink saw one. Also asserts `segs.segments.len() == 0`
    to pin the documented behaviour that `segment_sink.on_segment`
    does not run when per-record dispatch errors mid-segment.
* `cargo fmt --all -- --check` clean.
* `cargo clippy --all-targets -- -D warnings` clean.

## Deviations from plan

* **Tests live in `tests/multi_segment_filter.rs`, not
  `tests/wal_stream_e2e.rs`.** The plan listed
  `tests/wal_stream_e2e.rs` under "Files expected to change". That
  file is gated on `initdb` being on `$PATH` and runs every test
  inside a live PG with `START_REPLICATION PHYSICAL`. The PRE5b6
  fan-out claims are trait-level: any per-record dispatch through
  `WalStream::push` exercises them, and the synthetic byte-builders
  already in `multi_segment_filter.rs`
  (`build_record_with_block_ref_xid`, `build_one_page_segment`)
  produce a deterministic segment in microseconds without an
  `initdb` dependency. PRE5b5 set the precedent (its
  `records_in_one_xact_share_xact_id_through_stream` test also lives
  in `multi_segment_filter.rs` for the same reason). Coverage is
  identical; CI cost is lower.
* **Unit-level tests added in `src/wal_stream.rs`'s `mod tests`.**
  The plan named two tests, both implied integration-shaped. Two
  tests at unit level + two at integration level keeps the trait
  contract pinned in both shapes — the inline unit tests use
  `Record::from_parsed` directly (no segment build), the integration
  tests exercise `WalStream::push` end-to-end. Total +4 tests vs
  the plan's +2; the additional cost is two ~30-line cases against
  a `+2` lib-test count, which the test budget absorbs without
  complaint.
* **`CountingRecordSink` exported, not test-private.** Plan mentioned
  `CountingRecordSink` only by name in the example test sketch. The
  PRE5b6 implementation makes it a `pub` sibling of
  `CollectingRecordSink` in `src/wal_stream.rs`. Reasoning: Phase 5's
  heap-decoder tests will compose `DecoderSink` against the same
  "did this record reach the chain" probe; making it a public helper
  avoids duplication when that lands. It costs one `pub` struct on
  the surface today.
* **Shared-handle idiom for fan-out observation.** The plan didn't
  prescribe an observation strategy. The naive approach (boxing
  `CollectingRecordSink` directly and downcasting back through `dyn
  RecordSink` to inspect) fails because `RecordSink` does not extend
  `Any`. Two clean alternatives: add `Any` as a supertrait (broadens
  the public surface for one testing convenience), or wrap each
  observer's state in a shared cell and retain it outside the
  composite. The second option keeps `RecordSink` minimal and reads
  cleanly at the test site. Pure counters use `Arc<AtomicU64>` with
  `fetch_add(_, Ordering::Relaxed)`; the rmid log and record log
  carry `Vec<_>` and need `Arc<Mutex<Vec<_>>>`. `SharedRmidLog` /
  `SharedCollectingSink` use the mutex flavour;
  `SharedCountingSink` / `FailOnNth` / `ErrAt` use the atomic
  flavour.
* **Tuple impl `RecordSink for (A, B)` deferred.** Plan called it
  out as "could land alongside as a zero-alloc convenience for
  tests; not required". Skipped — `CompositeRecordSink::new(vec![…])`
  is the single canonical entry point, and a tuple convenience adds
  surface area without removing an actual cost site. Easy to add
  if Phase 5 or Phase 7 demonstrates a hot construction path that
  the `Box`+`Vec` allocation pinches.
* **Doc cross-reference to PRE5b10 from source code.** The plan
  said "document the post-error state explicitly (cross-reference
  with [PRE5b10](PRE5b10.md)'s `next_lsn` rollback decision)".
  Implemented as a `// See plans/PRE5b10.md item 4` pointer in the
  `CompositeRecordSink` doc comment, not an intra-rustdoc `[link]`
  — the `PRE5b10.md` reference is a path into the planning tree, not
  a rustdoc target.

## Implementation notes for follow-on work

`CompositeRecordSink::inner` is `pub Vec<Box<dyn RecordSink + Send>>`,
not a private field with `push`/`extend` accessors. This lets
Phase 5 attach `DecoderSink` to an existing composite without going
through a builder — the daemon's startup wiring can be one
`composite.inner.push(Box::new(decoder))` after the segment sink is
already in. The `Send` bound on the trait object is there for the
same reason: future daemon orchestration that owns the composite
across tasks will not have to re-box.

The poisoned-stream-on-error contract documented on
`CompositeRecordSink` is a *consequence* of `WalStream::push` not
rolling back `next_lsn` after a sink error. [PRE5b10](PRE5b10.md)
item 4 owns the formal mechanic — a state field on `WalStream`
that rejects subsequent `push` calls. PRE5b6 documents the
contract on the new public type but does not add the guard; the
existing test
(`composite_sink_propagates_inner_error_and_short_circuits`) only
asserts the per-call behaviour, not the rejection of a follow-on
push.

`CountingRecordSink` and `CollectingRecordSink` are public types
in `src/wal_stream.rs`. They are not feature-gated behind `cfg(test)`
because integration tests in `tests/` consume them as production
items (Rust's test layout requires public types for that), and they
are useful as diagnostic taps for any caller — `bin/stream.rs`
already wires `CollectingRecordSink::default()` to read back the
per-record sequence after a segment ships. Keep them exported even
after Phase 5 lands its own `DecoderSink`.

When Phase 5 wires `DecoderSink`, the natural pattern is:

```rust
let decoder = DecoderSink::new(/* config */);
let mut record_sink = CompositeRecordSink::new(vec![
    Box::new(decoder),
    Box::new(metrics_tap),       // optional
    Box::new(CollectingRecordSink::default()), // debug only
]);
stream.push(lsn, bytes, &mut record_sink, &mut segment_sink)?;
```

`record_sink` carries `'static + Send` so it can move into a task
if the daemon's pump split eventually goes async per-segment. The
existing sync `push` call shape works today; the future async
revision (sketched in the module doc at `wal_stream.rs:11-16`)
keeps the same trait surface.

## Files actually changed

```
src/wal_stream.rs                  CompositeRecordSink + CountingRecordSink;
                                   two #[cfg(test)] mod tests cases for
                                   the fan-out contract
tests/multi_segment_filter.rs      two integration tests exercising
                                   WalStream::push + CompositeRecordSink
                                   via the synthetic segment builders
plans/PRE5b6.md                    this retrospective
```
