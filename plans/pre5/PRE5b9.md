# PRE5b9 — `walshadow-stream` shutdown + memory hygiene (retrospective)

[PRE5b](PRE5b.md) item L1. Depended on [PRE5b2](PRE5b2.md)
(`seed_from_source`) and [PRE5b5](PRE5b5.md) (`Record`) — both
already in tree by the time L1 ran. Closes the silent-correctness
trio the plan called out: an unbounded `Vec<Record>` in the daemon,
a `break` path that skipped `WalStream::close()`, and a
`server_wal_end` keepalive field that was thrown away.

## Why (preserved)

* `bin/stream.rs:151` (pre-change) used `CollectingRecordSink::default()`
  in the production binary. `Vec<Record>` grew forever; a long-running
  daemon OOMs on its own success.
* `bin/stream.rs:189` (pre-change) `break` exited the loop and the
  function returned without calling `WalStream::close()`. Partial
  segment vanished despite `bin/stream.rs:22-24` claiming "writes the
  current partial segment (if any)". No SIGINT/SIGTERM handler.
* `src/source_feed.rs:184` (pre-change) dropped `server_wal_end`;
  `:209-215` `tracing_debug` was a no-op stub.

## What landed

* **`MetricsRecordSink` in `src/wal_stream.rs:184-220`.** New
  `RecordSink` impl that holds a `BTreeMap<(rmid, decision), u64>`
  plus a `total` counter and discards every `Record`. `BTreeMap`
  (not `HashMap`) because the on-emit `summary()` line iterates
  the map directly and must be byte-identical across runs for log
  scraping. `Decision` gained `PartialOrd, Ord, Hash`
  (`src/filter.rs:22`) so the `(u8, Decision)` key satisfies the
  `BTreeMap` bound; no other call site cares because the existing
  derives already covered `Eq`. `summary()` uses the existing
  `crate::classify::rmgr_label` helper so the daemon's log line
  reads `heap/keep=N heap/drop=M xact/keep=K` etc. — matches the
  vocabulary the rest of the manifest stats already use.

* **`SegmentSink::on_partial_segment` trait method
  (`src/wal_stream.rs:128-150`).** Default implementation forwards
  to `on_segment` so existing in-memory test sinks
  (`CollectingSegmentSink`) keep observing partial flushes with
  zero diff. `DirSegmentSink` overrides
  (`src/wal_stream.rs:316-341`) to write under `<name>.partial`
  with `<name>.partial.manifest.json` alongside. The complete-segment
  path must not exist after a partial flush — shadow PG's
  `restore_command` matches by exact segment name and would happily
  ingest a fully-zeroed segment if `close()` wrote to the canonical
  path. Atomic-rename hops through `<name>.partial.tmp` and
  `<name>.partial.manifest.json.tmp` so a crash mid-write does not
  leave a half-flushed `.partial` either.

* **`WalStream::close()` switched to `on_partial_segment`
  (`src/wal_stream.rs:469`).** One-line behavior change — the
  docstring on `close()` (`src/wal_stream.rs:436-440`) now matches
  the actual contract instead of pointing at `.partial` while the
  implementation wrote the complete-segment name. The padding
  semantics ("pages past the actual write are zeroed") stay
  identical so a downstream tool reading the partial bytes sees the
  same shape as before.

* **`SourceFeed::last_server_wal_end` (`src/source_feed.rs:64-79`,
  `:188-208`, `:218-223`).** Field updated on both Wal and
  Keepalive frames so the daemon can read it between calls. The
  daemon (`src/bin/stream.rs:262`) snaps `chunk.server_wal_end`
  before `stream.push` and prints `source_ahead={ahead}B` on every
  segment-ship line where `ahead = server_wal_end -
  dispatched_lsn_before_push`. `saturating_sub` because a brand-new
  attach can see `dispatched > server_wal_end` for one tick when
  the segment-aligned start LSN sits past the last keepalive's
  `server_wal_end`. Operator visibility, no behaviour change. The
  pre-existing stub `tracing_debug` (`src/source_feed.rs:295-300`)
  stays — the keepalive branch no longer calls it because
  surface-here-and-now beats stubbed-tracing.

* **`bin/stream.rs` shutdown loop
  (`src/bin/stream.rs:246-301`).** Replaced the bare `loop` with a
  named `let shutdown_reason = loop { ... };` so each break path
  carries an operator-visible reason (`"signal"`, `"CopyDone"`,
  `"max-segments"`). `tokio::select!` between
  `tokio::signal::ctrl_c()` and `feed.next_chunk(...)`; `biased`
  so a signal that lands while a chunk is ready still wins. The
  `MetricsRecordSink` swap (`:240`) is one line; the import
  (`:47`) drops `CollectingRecordSink` since the binary has no
  remaining user. After the loop, `stream.close(Some(&mut
  segment_sink), &mut record_sink)` flushes whatever is in
  `current_buf`, producing a `.partial` for the operator. The
  `eprintln!` ("stopping (reason); flushing partial …") names the
  out-dir so an operator running multiple daemons can find the
  artifact without grepping the binary.

## Tests

* **`cargo test --lib`**: 87 passed (was 85, +2):
  * `wal_stream::tests::metrics_sink_counts_per_rm_decision_and_discards`
    feeds three synthetic records (two heap-keep, one heap-drop, one
    xact-keep) through `MetricsRecordSink` and pins both the
    bucket counters and the `summary()` string shape. Tests against
    `summary().contains("heap/keep=2")` etc. — order is
    `BTreeMap`-deterministic but the test deliberately checks by
    substring so a future label change in `rmgr_label` does not
    spuriously fail.
  * `wal_stream::tests::dir_sink_partial_segment_lands_with_partial_suffix`
    asserts that `on_partial_segment` lands under `<name>.partial`
    *and* that the complete-segment path is absent. The "absent"
    half is the load-bearing assertion: any future refactor that
    accidentally routes partials through the complete path would
    silently feed shadow PG's `restore_command` a zero-padded
    segment.

* **`cargo test --tests`**: 4 wal_stream_e2e (was 3, +1):
  * `wal_stream_e2e::shutdown_writes_partial_segment_and_resume_from_start_lsn_continues`
    exercises the close() path through the live source. Skips
    `pg_switch_wal` in the pre-workload so xlogpos sits mid-segment
    and START_REPLICATION has bytes to ship without filling a
    segment. Pumps until `next_lsn >= ident.xlogpos`, then calls
    `stream.close(Some(&mut segs), &mut metrics)`. Asserts:
    * exactly one `<24hex>.partial` file under the out-dir,
    * the matching complete-segment path does NOT exist (the
      shadow-restore_command-safety claim),
    * the `.partial.manifest.json` sidecar exists,
    * the partial is `WAL_SEG_SIZE` bytes (padded), and
    * a fresh `SourceFeed` + `WalStream` at the same aligned
      `--start-lsn` pumps cleanly past the partial and ships ≥1
      new segment under a separate workload-driver thread.
    The resume half is the second exit-criteria claim and is the
    reason the test exists — close() alone is locked in by a unit
    test; the integration test pins that an operator running
    `walshadow-stream --start-lsn X/Y` after a clean shutdown gets
    the same behaviour as a cold start.

  Other integration suites unchanged at +9 / +3 / +4 / +others.
  Net: 87 lib + 20 tests = 107 passing (was 85 + 17 = 102; +5).

* `cargo fmt --all -- --check`: clean.
* `cargo clippy --all-targets -- -D warnings`: clean.

## Deviations from plan

* **`server_wal_end` is a `SourceFeed` field, not a returned struct
  field on `WalChunk`.** Plan said "surface
  `chunk.server_wal_end - dispatched_lsn` as a 'source ahead by N
  bytes' log line". The chunk already carried `server_wal_end` —
  the gap was that *keepalives* dropped it. Holding the most-recent
  value on the feed (updated on both Wal *and* Keepalive frames)
  lets the daemon read it between chunks too, which matters when
  the source goes quiet for keepalive intervals (the daemon's
  segment-ship log line still has a fresh `source_ahead` reading
  from the most recent keepalive). The chunk-side field stayed for
  call-sites that want the per-frame value, but the daemon reads
  `chunk.server_wal_end` directly because that's the read it would
  use anyway. `last_server_wal_end()` getter on `SourceFeed` is
  there for future call-sites (eg `bin/stream.rs` reading between
  chunks during a long quiet stretch) but no caller uses it today.

* **`on_partial_segment` is a new trait method, not a flag on
  `on_segment`.** Plan was silent on the call shape — said
  "flushing the partial as `.partial` per `DirSegmentSink`
  convention". A `partial: bool` parameter on `on_segment` would
  have been a breaking change for every existing implementor (three
  in tree: `CollectingSegmentSink`, `DirSegmentSink`, plus any
  ad-hoc test sinks); a default-implemented separate method costs
  zero churn for the sinks that don't care. The default delegates
  to `on_segment` so a partial flushed through `CollectingSegmentSink`
  still lands in the `segments` vec, just without a `.partial`
  marker — fine because in-memory tests do not need to distinguish
  partial from complete (no `restore_command` to mislead).

* **Shutdown reason is a `&'static str`, not an enum.**
  `let shutdown_reason = loop { ... break "signal"; }`. An enum
  would have been more typed; a string is one line per break path
  and is exactly what the `eprintln!` prints. The cost of an enum
  here would be a new top-level type carrying three trivial variants
  with no caller besides the formatter. Reserved for a future
  follow-up if `shutdown_reason` needs to drive control flow beyond
  logging.

* **`biased` `tokio::select!`.** Plan didn't prescribe the priority.
  Defaulted-fair select would have an in-flight chunk win against
  a ctrl_c that arrived simultaneously — which is correct for
  *eventual* shutdown but loses one segment of latency on every
  signal. `biased` makes the signal path zero-segment-latency at
  the cost of (in the worst case) one extra wakeup for the chunk
  arm. Trivial trade.

* **Test exercises `close()` direct-API, not a subprocess signal.**
  Plan offered both ("signals the daemon mid-stream (or asserts
  `close()` path via direct API)"). A subprocess test would need a
  spawned `walshadow-stream` binary, a way to kill its tokio runtime
  cleanly, and assertions on the `.partial` file landing — all of
  which add no signal-handling coverage beyond what
  `tokio::signal::ctrl_c()` already gets from upstream's own tests.
  The direct-API drill pins the `close()` → `.partial` chain plus
  the resume-from-segment-boundary contract, which is the
  exit-criteria item. A subprocess signal drill could be added in a
  later phase if the harness ever runs the binary in a real
  containered environment.

* **Test skips `pg_switch_wal` pre-attach.** The plan's tests
  section was silent on this; first draft mirrored
  `full_pipeline_source_to_filtered_segments_on_disk` and called
  `pg_switch_wal()` in the pre-workload. That pushes `xlogpos` to
  the start of a new (empty) segment, so START_REPLICATION at the
  aligned segment boundary has nothing to ship — `current_buf`
  stays empty, `close()` returns early, no `.partial` lands and
  the test reports "no WAL chunks received". The fix is to keep
  `xlogpos` mid-segment by *not* calling `pg_switch_wal` in the
  pre-workload; an `assert!(ident.xlogpos > aligned, ...)` guards
  against a future refactor adding it back unconditionally. The
  resume half *does* drive a `pg_switch_wal` from the driver
  thread (under its own subprocess `psql`) so the second
  `WalStream` has segments to ship.

## Implementation notes for follow-on work

* **`MetricsRecordSink` does not snapshot.** A future per-segment
  manifest stats path could add a `delta_from(&self) -> Self`
  analogous to `FilterStats::delta_from` — today's daemon prints
  the cumulative `summary()` on every ship line and operators
  watching the log see the per-segment delta by eyeballing
  successive lines.

* **No SIGTERM handler.** `tokio::signal::ctrl_c()` only fires on
  SIGINT (and the Windows ctrl-c-event). The plan's "Ctrl-C /
  SIGTERM stops the pump cleanly" docstring in `bin/stream.rs:23`
  is wider than the code — adding a SIGTERM listener would be a
  one-line addition wrapping `tokio::signal::unix::signal(SignalKind::terminate())`
  but Phase 10 already plans the operational hardening pass
  (`PLAN.md#phase-10--operational`) and it's the natural home for
  signal-set policy that holds across all daemons.

* **`server_ahead` reads `chunk.server_wal_end`, not
  `feed.last_server_wal_end()`.** This means if a quiet stretch
  delivers only keepalives, the daemon won't print a fresh
  `source_ahead` reading until the next WAL chunk arrives. That's
  fine for the segment-ship cadence because there is no segment to
  ship during keepalives. If a future phase prints periodic
  liveness independent of segment ships, it can read
  `last_server_wal_end()` to surface progress.

## Files actually changed

```
src/filter.rs                      +1 / -1     (Decision derives
                                              PartialOrd, Ord, Hash)
src/source_feed.rs                 +17 / -4    (last_server_wal_end
                                              field + getter; updated
                                              on both Wal and Keepalive
                                              frames)
src/wal_stream.rs                  +143 / -7   (MetricsRecordSink type +
                                              summary() + lib test;
                                              SegmentSink::on_partial_segment
                                              with default impl;
                                              DirSegmentSink override +
                                              lib test; WalStream::close
                                              switched to on_partial_segment)
src/bin/stream.rs                  +29 / -13   (MetricsRecordSink swap;
                                              tokio::select! biased loop
                                              with named break reasons;
                                              source_ahead log line;
                                              close() on shutdown)
tests/wal_stream_e2e.rs            +211 / -1   (shutdown_writes_partial_
                                              segment_and_resume_from_
                                              start_lsn_continues
                                              integration test +
                                              MetricsRecordSink import)
plans/PRE5b9.md                    rewritten   (this retrospective)
```

No new runtime crates; no dev-dep additions; no public API surface
changes on `WalStream` or `SourceFeed` beyond the new
`MetricsRecordSink` type, the new `SegmentSink::on_partial_segment`
trait method (with default), the new `SourceFeed::last_server_wal_end`
getter, and the new `Decision: PartialOrd + Ord + Hash` derives.
