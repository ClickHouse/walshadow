# PRE5b6 — sink fan-out

[PRE5b](PRE5b.md) item S2. Depends on [PRE5b5](PRE5b5.md) having
landed the `Record` shape.

## Why

`WalStream::push` takes `&mut dyn RecordSink` singular
(`wal_stream.rs:249`).
[Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix)'s
`DecoderSink` runs alongside `SegmentSink` (the `ShadowWalSink`);
metrics may want a third tap.

## Implementation

```rust
pub struct CompositeRecordSink {
    pub inner: Vec<Box<dyn RecordSink + Send>>,
}

impl RecordSink for CompositeRecordSink {
    fn on_record(&mut self, r: &Record) -> Result<(), SinkError> {
        for s in &mut self.inner {
            s.on_record(r)?;
        }
        Ok(())
    }
}
```

Short-circuits on the first `Err`. Tuple `impl<A, B> RecordSink for
(A, B)` could land alongside as a zero-alloc convenience for tests;
not required.

## Tests

* `CompositeRecordSink` of `CollectingRecordSink + CountingRecordSink`,
  push one segment, assert both observers see the full event sequence
  in order.
* Error propagation: one inner sink returns `Err` on a specific
  record. Assert `push` returns `Err` and document the post-error
  state explicitly (cross-reference with
  [PRE5b10](PRE5b10.md)'s `next_lsn` rollback decision).

## Exit criteria

1. `cargo test --lib && cargo test --tests` clean, including the
   two new fan-out cases.
2. `cargo clippy --all-targets -- -D warnings` clean.
3. `CompositeRecordSink` covered by both happy-path and error-path
   tests; behaviour on inner-sink error documented in source.

## Files expected to change

```
src/wal_stream.rs                  add CompositeRecordSink
tests/wal_stream_e2e.rs            CompositeRecordSink cases
plans/PRE5b6.md                    this doc
```
