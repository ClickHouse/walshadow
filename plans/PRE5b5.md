# PRE5b5 — widen `RecordEvent` to a real `Record`

[PRE5b](PRE5b.md) item S1. First of the foundation changes. Lands
the parsed-record handoff that
[Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix)'s
`DecoderSink` will consume.

## Why

`RecordEvent` (`wal_stream.rs:80-92`) carries `source_lsn, len, rmid,
info, decision`.
[Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix)'s
`Tuple { rfn, xid, op, new, old }` needs the parsed `XLogRecord` (for
`main_data` and per-block payload), the `RelFileLocator` (for
`relation_at` lookup), and `xact_id`. `filter_segment` already parses
all of this (`filter_segment.rs:46`) to feed `Filter::decide`, then
discards it; the `Manifest` is written as flat scalars. A `RecordSink`
consumer must re-parse from segment bytes to get back what was just
thrown away.

[PRE5.md:58-64](PRE5.md) spec'd the right shape (`Record { parsed:
XLogRecord, logical_bytes, byte_ranges, source_lsn, page_magic }`) and
the contract was watered down during PRE5 implementation. PRE5b5
closes the gap, with one simplification (drop `logical_bytes` and
`byte_ranges`,
[Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix)'s
decoder consumes the parsed form, not raw bytes).

## Implementation

```rust
pub struct Record {
    pub parsed: XLogRecord,
    pub source_lsn: u64,
    pub page_magic: u16,
    pub decision: Decision,
}
```

`parsed.header.{resource_manager_id, info, xact_id}`,
`parsed.blocks[i].header.location.rel`, `parsed.main_data` cover every
field a downstream consumer needs.

* `filter_segment` returns `(Vec<u8>, Manifest, Vec<XLogRecord>)`
  (parses-once contract). `Manifest` continues to be the
  on-disk-serializable view; `Vec<XLogRecord>` is the in-memory
  hand-off to the `RecordSink`.
* `WalStream::flush_current` zips parsed records with manifest entries
  by index, dispatches `Record` to `RecordSink::on_record(&Record)`.

## Tests

* Update `tests/wal_stream_e2e.rs` to assert
  `Record.parsed.header.resource_manager_id`,
  `Record.parsed.blocks[0].header.location.rel`,
  `Record.parsed.header.xact_id` directly.
* New: a fixture xact's records (all bracketed by one BEGIN…COMMIT)
  all carry the same `xact_id`.
* `tests/filter_round_trip.rs` updated for the new Record shape so
  byte-for-byte equivalence still holds.

## Exit criteria

1. `cargo test --lib && cargo test --tests` clean, including the
   xact_id-cohesion assertion.
2. `cargo fmt --all -- --check` and
   `cargo clippy --all-targets -- -D warnings` clean. Run both at
   the end of the implementing phase before commit.
3. `Record` carries `parsed: XLogRecord, source_lsn, page_magic,
   decision`; every existing `RecordEvent` field consumer reads
   through the new shape.

## Files expected to change

```
src/filter_segment.rs              return Vec<XLogRecord>
src/wal_stream.rs                  widen RecordEvent → Record;
                                   zip parsed records with manifest
src/bin/filter.rs                  follow Record shape
src/bin/stream.rs                  follow Record shape
tests/wal_stream_e2e.rs            assertions on Record.parsed fields
tests/filter_round_trip.rs         updated for Record shape
plans/PRE5b5.md                    this doc
```
