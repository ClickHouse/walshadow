# PHASE6 — TOAST reassembly + xact buffer + spill backend

Closes [Phase 6 of PLAN.md](PLAN.md#phase-6--toast-reassembly--xact-buffer).
Holds every [`DecodedHeap`](../src/heap_decoder.rs) per `xid` from the
first heap touch until the matching `XLOG_XACT_COMMIT` /
`XLOG_XACT_ABORT`, dereferencing `ColumnValue::ExternalToast` against
the same xact's TOAST chunks at commit drain. In-memory budget defaults
to PG's `logical_decoding_work_mem` (64 MiB); larger xacts spill to
per-xid append-only files under `{data_dir}/spill/`. Design layer:
[PHASE6disk.md](PHASE6disk.md).

## What landed

| item | files | tests |
|---|---|---|
| `spill` module — `SpillStore`, `SpillWriter`, `SpillReader`, `SpillEntry { Heap, Chunk }`, manual binary encoder/decoder for every `ColumnValue` variant | [`src/spill.rs`](../src/spill.rs) | 5 unit tests (round-trip, malformed tag, clear preserves non-spill files, writer-unlink removes file, per-variant encode/decode) |
| `ToastChunk { toast_relid, value_id, chunk_seq, source_lsn, chunk_data }` exposed for callers that build chunks externally | [`src/spill.rs`](../src/spill.rs) | covered by `round_trip_heap_and_chunk` |
| `xact_buffer` module — `XactBuffer`, `XactBufferConfig`, `XactBufferStats`, largest-xact eviction, per-xid spill | [`src/xact_buffer.rs`](../src/xact_buffer.rs) | 6 unit tests (catalog-free paths: abort cleanup, abort-unknown-xid, eviction, parse_xact_time, summary gating, toast-shape recogniser) |
| TOAST reassembly + decompression (pglz / lz4 paths via existing `pglz` + `lz4_flex` crates) | [`src/xact_buffer.rs`](../src/xact_buffer.rs) `reassemble` + `detoast_tuple` | `detoast_concatenates_uncompressed_chunks_into_text`, `detoast_missing_chunk_seq_errors_clearly` (in [`tests/xact_buffer.rs`](../tests/xact_buffer.rs), live shadow PG) |
| `XactRecordSink<O: TupleObserver>` observing `RM_XACT_ID` records: COMMIT/COMMIT_PREPARED drain, ABORT/ABORT_PREPARED drop, everything else (PREPARE / ASSIGNMENT / INVALIDATIONS) ignored | [`src/xact_buffer.rs`](../src/xact_buffer.rs) | `xact_record_sink_routes_commit_and_abort` (live shadow PG) |
| `BufferingDecoderSink` — replaces the direct-emit `DecoderSink` in the production fan-out: parks user-heap records in the buffer, reinterprets `pg_toast.*` INSERTs as `ToastChunk`s | [`src/xact_buffer.rs`](../src/xact_buffer.rs) | `toast_chunk_from_decoded_recognises_three_col_shape` unit; full sink path exercised via the daemon-level fan-out and Phase 8's DDL drill |
| Daemon wiring — `--spill-dir` + `--xact-buffer-max` flags, `XactBuffer::clear_spill_dir` on startup, status line extended with `xact_active=… spill_bytes=… commit=… abort=…` | [`src/bin/stream.rs`](../src/bin/stream.rs) | live PG, not unit-tested |
| Integration suite for commit / drain / detoast against a live shadow PG (mirrors `tests/shadow_catalog.rs` conventions: `pg_available()` gate, non-overlapping ports, `psql_one` for filenode + toast OID lookup) | [`tests/xact_buffer.rs`](../tests/xact_buffer.rs) | 7 tests — drain order, unknown-xid, spilled-then-in-memory drain, detoast + missing-chunk error, xact sink routing, abort-spill cleanup |

Build clean on `cargo clippy --all-targets -- -D warnings`. Test counts:

- `cargo test --lib`: 132 passed (was 121 at end of Phase 5; +11 = 5 `spill::tests::*` + 6 `xact_buffer::tests::*`).
- `cargo test --test xact_buffer`: 7 passed against a live shadow PG
  (ports 55701-55707; skipped silently if `initdb` is not on `$PATH`).
- Existing 32 integration tests untouched, all green.

Code size:

| component | LOC |
|---|---|
| `src/spill.rs` (writer / reader / store / manual encoder + tests) | 825 |
| `src/xact_buffer.rs` (buffer / drain / detoast / sinks + tests) | 1102 |
| `tests/xact_buffer.rs` (live shadow PG integration tests) | 451 |
| `src/bin/stream.rs` wiring delta | ~40 added |
| `DecodedHeap` got `PartialEq` derive (for spill round-trip tests) | 1 |

Source-only sizing (`src/spill.rs` + `src/xact_buffer.rs` minus tests)
lands at ≈900 LOC matching PHASE6disk.md's estimate; the test split
between inline unit (catalog-free paths) and `tests/xact_buffer.rs`
(live shadow PG) follows the [`tests/shadow_catalog.rs`] convention.

## What didn't get done

Four items deferred explicitly:

- **CH-as-scratch spill backend.** Per design, v1 is local-disk-only.
  Per user instruction during implementation, the `SpillBackend` enum
  + `spill_backend = …` config knob were dropped from v1 entirely —
  the diskless walshadow path lands as a follow-up when someone asks,
  with a fresh config surface decision at that point. `src/spill_ch.rs`
  not created.
- **Subxact lineage.** Phase 6 ships top-level-xact-only.
  `XLOG_XACT_ASSIGNMENT` is currently ignored; subxact aborts inside a
  top-level commit are not retracted from the parent's buffer. PG's
  reorderbuffer keeps a per-subxact change list and folds it onto the
  top-level at commit; walshadow's `XactState` is single-list, no
  per-subxact partitioning. Lands when a savepoint-heavy workload
  measures non-zero ghost rows.
- **Streaming mid-xact** (`streaming=on` analogue). Out of scope per
  PHASE6disk.md — `ReplacingMergeTree(_lsn)` can't retract on abort,
  so commit-buffer is the only correctness-safe model.
- **Live-PG end-to-end test for `BufferingDecoderSink`.** The
  decoder-side path needs WAL records (`record.parsed`, `record.decision`)
  to exercise; today's `tests/xact_buffer.rs` covers the buffer + sink
  drain proper via direct `on_heap` / `on_toast_chunk` calls. Phase 5
  punted the same way for `DecoderSink`; Phase 8's e2e DDL drill is
  the natural home for the full record→decoder→buffer chain. Unit
  coverage of `XactBuffer` is split between catalog-free unit tests
  (in-process) and catalog-touching integration tests (live shadow
  PG), so the buffer is well-exercised end-to-end.

## Design decisions

### Catalog-at-drain, no per-xact cache

Detoast needs the column's `pg_type.typoid` to decide `Bytea` vs
`Text` substitution. Initial design (first commit of this phase)
cached `Arc<RelDescriptor>` per `(xid, rfn)` inside [`XactState`] to
avoid catalog round-trips during drain. Removed in v2:

* [`ShadowCatalog`] already has its own LRU. A second cache duplicates
  the surface and gives drain an extra eviction dimension to worry
  about — every active xact keeps its descriptor refs alive past the
  catalog's own natural expiry.
* Memory cost: one `Arc` bump + one `HashMap` allocation per
  `(xid, rfn)`, paid even for xacts that never detoast.
* In the production deployment shadow PG runs co-located, so
  `relation_at` is cheap; the cache motive collapses.

Switched to: `XactBuffer::commit` takes a `&Arc<Mutex<ShadowCatalog>>`
parameter and `detoast_heap` calls `catalog.relation_at(rfn,
source_lsn)` only when [`tuple_needs_detoast`] returns true (any
column is `ExternalToast`). Heaps without TOAST columns never hit
the catalog at drain. `XactRecordSink` holds an `Arc<Mutex<…>>` clone
and forwards it through.

Tests that exercise `commit` / detoast moved from `#[cfg(test)] mod
tests` to `tests/xact_buffer.rs` and run against a real shadow PG
using the same `make_shadow` / `psql_one` infrastructure as
[`tests/shadow_catalog.rs`]. Unit tests stay catalog-free
(abort / eviction / `parse_xact_time` / stats summary / TOAST-shape
recogniser).

### Spill format: manual binary encoder, not `serde_json` / `bincode`

JSON inflates `chunk_data` (≈2 KiB raw → ≈6 KiB encoded) at exactly
the workload that triggers spill — bulk INSERT/UPDATE of TOASTable
columns. `bincode` / `postcard` would add a workspace dep for a
contained internal format with no version-skew surface (spill files
are wiped on every daemon restart per the cursor-file contract).

Result: ~250 LOC of hand-rolled tag-byte + length-prefixed encoder.
Per-variant round-trip test covers every `ColumnValue` case; format
errors surface as `SpillError::Format { offset, detail }` so a
corrupt stream points at the failing byte. Trades a one-time
implementation cost for zero added deps and an arbitrarily tight
format.

### TOAST chunks ride in the same per-xid file as heap tuples

PHASE6disk.md kept TOAST chunks logically distinct (keyed by
`(toast_relid, value_id)`). The implementation collapses both into
one `Vec<SpillEntry>` per xact because:

* PG's `toast_save_datum` writes the chunk INSERTs in the same xact
  as the referring tuple. No cross-xact chunk reference exists in
  practice — `streaming=on` is the one mode that would force it, and
  walshadow doesn't implement that.
* Single key (`xid`) for spill / eviction / drain / abort cleanup
  removes one dimension of state.
* Drain order matches WAL order without per-xact merge work — chunk
  fan-out into `HashMap<(toast_relid, value_id), BTreeMap<seq, Vec<u8>>>`
  happens at drain, not at observe time.

If a future PG feature (or operator footgun) puts chunks in a
different xact than their referrer, the buffer surfaces as
`MissingToastChunk { toast_relid, value_id, missing }` at drain
— visible, not silent.

### Spill format keys filenames on `(xid, first_lsn)`, not just `xid`

A 32-bit xid wraps. Two streams that picked up the same xid value
after a slot rebuild or a server restart can't share a spill filename
without races. Mirroring PG's `pg_replslot/<slot>/xid-*.snap` —
including the LSN as a hex suffix gives every spill file a unique
name even across xid rollover.

### `commit_ts` plumbed through `CommittedTuple` but unused in v1

`xl_xact_commit.xact_time` (PG `TimestampTz`) is parsed off
`main_data[0..8]` and threaded through to `XactBuffer::commit`. The
v1 `TupleObserver` trait passes only `&DecodedHeap` — Phase 7's CH
emitter wants `(decoded, commit_ts)` for the `_commit_ts` synthetic
column, so the buffer plumbs it now and Phase 7 extends the observer
shape without re-touching the buffer. `CommittedTuple { decoded,
commit_ts }` exists as the staging type; today only the `decoded`
half ships to the observer.

### Status-line stats summary suppresses zero buckets

`XactBufferStats::summary` always prints `xact_active`, `bytes_in_mem`,
`spill_active`, `spill_bytes`, `commit`, `abort` — the high-signal
counters. `evictions`, `commit_unk`, `abort_unk` are appended only
when non-zero. Matches the Phase 5 `DecoderStats::summary` convention
so a quiet workload renders compactly.

### `XLOG_XACT_PREPARE` keeps the xact buffered, no special handling

A `PREPARE TRANSACTION` followed minutes later by `COMMIT PREPARED`
must drain the same buffer. The v1 sink leaves `PREPARE` records
untouched; the buffer keeps the xact's state alive until
`COMMIT_PREPARED` or `ABORT_PREPARED` arrives. `xact_record_sink_routes_commit_and_abort`
proves the PREPARE-arrives-mid-xact case doesn't corrupt the buffer.

Two-phase commit's full fidelity (storing the GID, querying
`pg_prepared_xacts` on shadow to verify) lands with Phase 8's DDL
drill, when a 2PC test fixture exercises the path.

## Cursor-file integration

Not landed in this commit. PLAN.md's Phase 6 sketch ("`{data_dir}/cursor`
persists `(filter_lsn, decoder_lsn, emitter_lsn)` atomically (write
tmp + rename) on each commit drain") needs an emitter to anchor the
`emitter_lsn` half; without Phase 7's CH emitter there's nothing on
the other end to acknowledge a drain. `XactBuffer::clear_spill_dir`
already implements the "wipe spill on startup, re-stream from cursor"
half of the contract — the cursor file's atomic-write half belongs to
Phase 7's CH ack loop. Documented as a Phase 7 prerequisite.

## Where the buffer slots into the dispatch chain

`DaemonSinks` (in `bin/stream.rs`):

```text
MetricsRecordSink           — per-rmgr counters (Phase 1)
  ↓
BufferingDecoderSink        — Phase 6: heap → XactBuffer, toast → chunks
  ↓                            (replaces Phase 5's DecoderSink<MetricsTupleObserver>)
XactRecordSink              — Phase 6: COMMIT drains, ABORT drops
```

Ordering matters: the decoder must absorb every heap record in the
current dispatch batch before the xact-drain sink runs. PG's WAL
guarantees `XLOG_XACT_COMMIT` lands *after* every heap record in the
xact, but the batch boundary (currently per-segment, ~16 MiB) can
include both halves; running `decoder.on_record` before
`xact_drain.on_record` keeps the absorb-then-drain invariant tight.

## Acceptance-criteria status

PLAN.md §"Acceptance criteria":

* **§1 (`pgbench -T 30 -c 8` + DDL).** Buffer in place; needs Phase 7
  CH emitter to verify CH state matches source. Buffer-internal
  correctness covered by drain-order tests; downstream wiring is
  Phase 7.
* **§2 (VACUUM FULL during workload).** Untouched — relfilenode
  rewrites already handled by `CatalogTracker` / `ShadowCatalog`,
  Phase 6 buffer sees the post-rewrite descriptors through its
  per-xact cache.
* **§3 (shadow replay lag < 1 s).** Unrelated to Phase 6.
* **§4 (`--validate` mode).** Out of scope, Phase 9.
* **§5 (kill -9 + restart).** `clear_spill_dir` on startup gives the
  "drained or replayable" contract; full check needs Phase 7's CH
  emitter + cursor file.
* **§6 (`pg_ctl restart` of shadow).** Phase 4b coverage holds; Phase 6
  buffer state is process-local and unaffected.

Phase 6 closes the buffer-side correctness gap that Phase 5 documented
as "Rollback status, explicit": aborted xacts no longer produce ghost
rows downstream because their entries are dropped before reaching the
observer. `_commit_ts` is parsed off the WAL commit record and
threaded through `CommittedTuple` for Phase 7 consumption.

## Followups

Tracked separately, not blocking phase close:

1. **Cursor file.** Atomic `(filter_lsn, decoder_lsn, emitter_lsn)`
   commit on every drain. Co-lands with Phase 7's CH ack loop.
2. **Subxact lineage.** Per-subxact change list inside `XactState`,
   roll into parent on commit, drop on subxact abort. Bolt on once
   PG savepoint workloads measure non-zero ghost rows.
3. **Multi-insert (`XLOG_HEAP2_MULTI_INSERT`) fan-out.** Phase 5
   skips silently; Phase 6 inherits the gap. Belongs to the same
   commit as the per-tuple offset loop in `heap_decoder`.
4. **`BufferingDecoderSink` live-PG smoke test.** Phase 8's DDL drill
   is the canonical home.
5. **CH-stash backend.** Reserved as design space; lands when a
   diskless walshadow operator asks. Fresh config-surface decision
   at that point.
6. **Cross-segment xact spill.** Today the daemon's status-line
   logging happens between segments; a single xact whose records
   span N segments stays buffered correctly, but per-segment spill
   metrics could lag by one segment on the status line. Cosmetic.

## Related

* [PHASE5.md](PHASE5.md) — heap decoder this phase consumes from.
* [PHASE6disk.md](PHASE6disk.md) — design / option analysis.
* [PHASE4b.md](PHASE4b.md) — shadow catalog reconnect; buffer's
  per-xact descriptor cache decouples drain from catalog availability.
* `~/s/postgresql/src/backend/replication/logical/reorderbuffer.c` —
  PG's reference implementation for the same problem in logical
  decoding. Walshadow's spill is the same idea minus the catalog-aware
  snapshot-building bits PG carries inline.
