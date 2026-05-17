# PRE5b9 — `walshadow-stream` shutdown + memory hygiene

[PRE5b](PRE5b.md) item L1. Depends on [PRE5b2](PRE5b2.md)
(`seed_from_source`) and [PRE5b5](PRE5b5.md) (`Record`) having
landed; the daemon's pipeline uses both.

## Why

* `bin/stream.rs:151` uses `CollectingRecordSink::default()` in the
  production binary. `Vec<Record>` grows forever; long-running daemon
  OOMs on its own success.
* `bin/stream.rs:189` `break` exits the loop, function returns without
  calling `WalStream::close()`. Partial segment vanishes despite
  `bin/stream.rs:22-24` claiming "writes the current partial segment
  (if any)". No SIGINT/SIGTERM handler.
* `src/source_feed.rs:184` drops `server_wal_end`; `:209-215`
  `tracing_debug` is a no-op stub.

## Implementation

* Replace `CollectingRecordSink` in `bin/stream.rs` with a
  `MetricsRecordSink` that maintains counters per `(rmid, decision)`
  and discards events. Periodic print on segment emit.
* `tokio::select!` between `feed.next_chunk(...)` and
  `tokio::signal::ctrl_c()`. On signal: drop out of the loop, call
  `stream.close(Some(&mut segment_sink), &mut record_sink)`,
  flushing the partial as `.partial` per `DirSegmentSink` convention.
* Surface `chunk.server_wal_end - dispatched_lsn` as a "source ahead
  by N bytes" log line. Operator visibility, no behaviour change.

## Tests

* Add a variant of `tests/wal_stream_e2e.rs` that signals the daemon
  mid-stream (or asserts `close()` path via direct API). Confirm a
  `.partial` lands and a subsequent `--start-lsn` resumes cleanly.

## Exit criteria

1. `cargo test --lib && cargo test --tests` clean, including the
   shutdown drill.
2. `cargo clippy --all-targets -- -D warnings` clean.
3. `bin/stream.rs` no longer leaks `Record`s and writes a `.partial`
   segment on SIGINT, resumable via `--start-lsn`.

## Files expected to change

```
src/source_feed.rs                 surface server_wal_end
src/bin/stream.rs                  ctrl_c; close() on shutdown;
                                   MetricsRecordSink
src/wal_stream.rs                  MetricsRecordSink (if it lands here)
tests/wal_stream_e2e.rs            shutdown + resume drill
plans/PRE5b9.md                    this doc
```
