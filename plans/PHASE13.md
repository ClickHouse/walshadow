# PHASE13 — sub-segment record latency

Today `walshadow-stream` parses + dispatches WAL one 16 MiB segment at
a time. A single small UPDATE on a quiet source therefore waits until
either the segment fills or source rolls it (`pg_switch_wal` /
`archive_timeout`). The demo's recipe surfacing this — `UPDATE; SELECT
pg_switch_wal()` per change — is the operator-visible smell

Bottleneck is acknowledged inline in
[`src/wal_stream.rs:12-16`](../src/wal_stream.rs):

> For per-record streaming (sub-segment latency for the decoder), a
> future revision can switch `WalStream::push` from "accumulate whole
> segment then call `filter_segment`" to a chunk-driven walker that
> yields records as soon as they complete. The sink protocol defined
> here is shape-compatible with both

Phase 13 picks up that note. Goal: record dispatch on roughly
page-cadence (~8 KiB worst case), independent of segment fill

## Why this isn't just "swap the walker"

Two downstream consumers ride on `WalStream::push` and they don't
share a latency model:

| sink | needs | cadence today |
|---|---|---|
| `DirSegmentSink` (segment writer → shadow PG `restore_command`) | complete 16 MiB segment files; partials are explicitly NOT consumed by `restore_command` (`on_partial_segment` doc) | per-segment (correct) |
| `BufferingDecoderSink` + `XactRecordSink` (decoder → xact buffer → CH emitter) | every record, ASAP, in WAL order | per-segment (latency floor) |

A naive flip — dispatch records as they parse, leave segment dispatch
unchanged — also collides with the catalog freshness gate:

- `BufferingDecoderSink::on_record` calls
  `ShadowCatalog::relation_at(rfn, source_lsn)`
  ([`src/xact_buffer.rs:809`](../src/xact_buffer.rs)).
- `relation_at` gates on `wait_for_replay(at_lsn)`
  ([`src/shadow_catalog.rs:446`](../src/shadow_catalog.rs)) which polls
  shadow PG's `pg_last_wal_replay_lsn() >= at_lsn`.
- Shadow advances replay only when its `restore_command` finds the
  segment file in `out/`.
- `flush_current` documents the resulting order: segment sink first,
  then per-record dispatch — *exactly* so the catalog gate clears by
  the time the decoder reaches each record
  ([`src/wal_stream.rs:612-617`](../src/wal_stream.rs)).

Per-record dispatch BEFORE the segment writes inverts that ordering
and the gate stalls (or times out, which today
`BufferingDecoderSink::on_record` swallows as a silent
`stats.replay_timeout += 1` — see Anti-goals below). Phase 13 has to
solve both halves: sub-segment record yield AND a faster catalog
freshness path

## Strategy

Three coupled changes, sequenced so each lands testable on its own

### 1. Chunk-driven walker

Lift `SegmentWalker`'s page-by-page state machine
([`src/segment.rs:66-109`](../src/segment.rs)) into a stateful struct
that `WalStream::push` drives as bytes arrive. State that must survive
across `push` calls:

- current page parser cursor + page magic
- pending record being stitched across pages (already modelled by
  `Pending`)
- cumulative manifest entries for the current segment
- the 16 MiB rewrite buffer (Vec<u8>) — still accumulated; in-place
  noop rewrites of dropped records continue to land there
- byte-range index from logical bytes → physical offsets in the
  buffer (for cross-page record rewrites)

The walker yields a `(record, byte_ranges)` pair as soon as the
record's last byte arrives. `WalStream::push` then:

1. parse → `Filter::decide`
2. if `Drop`: rewrite in-place via `noop_replace` at the recorded
   byte ranges
3. dispatch `record_sink.on_record`
4. append manifest entry

When the 16 MiB buffer is full, dispatch `segment_sink.on_segment`
(filtered bytes + accumulated manifest). No re-parse, no rewrite
re-walk — both already happened inline

### 2. Catalog gate uplift

The hot path is `relation_at(rfn, source_lsn)`. Today the gate uses
shadow's replay LSN as a freshness proxy: "I won't trust my
in-memory descriptor until shadow has replayed at least up to this
record." With sub-segment record dispatch, shadow's replay can lag
the record-of-interest by up to one segment

Two paths to unblock that gate, depending on shadow's pg_wal status:

- **Fast path (cached, no churn).** If `by_filenode` has an entry for
  `rfn` with `generation == self.generation` AND no relmap update or
  pg_class write has invalidated `rfn`'s descriptor since the last
  `wait_for_replay`, return the cached desc unconditionally. This is
  already most of the code path post-cache-warmup; the gate just
  short-circuits earlier. The catalog tracker (`Filter::tracker`)
  already counts `relmap_updates` + `pg_class_writes_undecoded` /
  `_oid_in_prefix` per segment — the same counters can drive an
  "rfn-may-be-stale" check
- **Slow path (cache miss / pending invalidation).** Fall back to the
  current `wait_for_replay`. Records that hit this path see
  segment-cadence latency, same as today. UPDATEs on long-lived
  tables almost never hit it after first warmup

The win: steady-state UPDATEs reach the decoder on page cadence.
First-touch (BASE_BACKUP miss or post-DDL) stays segment-cadence

### 3. Make `ReplayTimeout` loud

[`BufferingDecoderSink::on_record:817-820`](../src/xact_buffer.rs)
swallows `CatalogError::ReplayTimeout` as `stats.replay_timeout += 1`
and returns Ok. Under phase 13 the gate is now more frequently
exercised (records hit it pre-segment-write); a silent skip would
shed user-heap writes invisibly

Fix: a `ReplayTimeout` becomes a hard error that poisons the stream
(same contract as a sink `Err` — `WalStream` flips poisoned, daemon
exits, cursor file resumes the run at the last-acked LSN). Drop the
counter; replace with the existing poison path

## What stays segment-cadence (anti-goals)

- `DirSegmentSink` write cadence — shadow's `restore_command` still
  consumes whole segments. Sub-segment `.partial` flushes would
  conflict with the partial-on-shutdown semantics
  (`on_partial_segment`) where a `.partial` suffix is the explicit
  "operator-visible artifact, do not pick up" marker
- Manifest emission — one manifest sidecar per segment, same shape.
  Per-record manifest stream would add no headroom and would change
  the on-disk contract for any tooling reading `*.manifest.json`
- Retention sweep / cursor write cadence — both already on a
  separate timer (`DEFAULT_TRIM_INTERVAL = 30s`, status loop cadence
  for cursor). Untouched
- Segment-batched filter decisions that span a whole segment
  (relmap-update authorisation, pg_class write invalidation) — these
  fire on per-record `Filter::decide` already; segment-cadence is
  only the *dispatch* boundary, not the decision boundary

## Open questions

- **Catalog "rfn-may-be-stale" predicate.** The conservative version
  (any relmap update OR any pg_class write to this rfn's database
  since last wait_for_replay invalidates the cache for that rfn)
  needs careful accounting. CatalogTracker's existing counters are
  segment-scoped; a per-rfn or per-database scope might be needed.
  Worth a small spike before committing
- **Test shape.** wal_stream_e2e + filter_segment unit tests cover
  the current segment-cadence path; phase 13 needs new fixtures that
  feed records in sub-segment chunks (a couple of pages at a time)
  and assert record_sink fires before segment_sink. The existing
  `phase11_cursor` integration test will catch any regression in the
  segment-cadence durability ceiling
- **Status-line + metric labelling.** The status-loop summary today
  reports `segments_shipped` + `decoder/decoded=N`. With sub-segment
  dispatch those two diverge — a record can be decoded with no
  segment shipped yet. Add a `records_dispatched` counter; segment
  count stays as-is
- **`partial` segment on clean shutdown.** `WalStream::close` flushes
  the partial buffer through `on_partial_segment` and dispatches
  in-flight records. With sub-segment dispatch the records will have
  already gone out; `close` should only emit the partial segment +
  any tail-records that completed since the last segment dispatch.
  Re-dispatching the same records twice would break the xact buffer
  invariant — `close` needs a "high-water dispatched offset" cursor

## Acceptance

- `UPDATE demo.users SET ... WHERE id=1` on the docker-compose demo
  source surfaces in CH within ≤ 2 s, with no
  `pg_switch_wal` / `archive_timeout` shim
- `cargo test --workspace --lib` stays green; new unit tests cover
  the chunk-driven walker (multi-page records, page-straddling
  headers, zero-padded segment tail)
- `phase11_cursor` integration test stays green — durability ceiling
  is still segment-aligned at `dispatched_lsn`
- Existing `replay_timeout` stat removed; daemon poisons + exits on
  catalog gate timeout instead. The phase 11 cursor resume path
  picks up where the last commit ack landed, so a poisoned run
  re-attempts cleanly on restart
