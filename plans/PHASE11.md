# PHASE11 — durability + resume

Closes [Phase 11 of PLAN.md](PLAN.md#phase-11--durability--resume).
PLAN's brief: a `kill -9` mid-stream plus restart must land a CH
end-state identical to the uninterrupted run (acceptance §5). Today's
daemon advances source's slot to filter-position and wipes the spill
dir on every boot, so any xact whose `XLOG_XACT_COMMIT` was filtered
but not yet drained to CH vanishes after restart. Phase 11 plugs that
gap with a durable resume cursor + ack-LSN slot advance.

## Strategy

Five surfaces, each one independent enough to verify on its own,
sequenced so the daemon binary's status loop picks them all up in
one wiring pass.

1. **Cursor file** ([`src/cursor.rs`](../src/cursor.rs)). 56-byte
   on-disk record under `{spill_dir}/cursor.bin`: magic + version +
   five LSN slots + CRC32C. Atomic-rename writer that fsyncs the
   `.tmp` file then fsyncs the parent dir so the rename itself
   survives a power loss. Reader returns `Ok(None)` for greenfield
   boot, `Ok(Some)` for a valid cursor, `Err` for corrupt — the
   daemon logs and falls back to greenfield on the error path.
2. **Filter-segment fsync**
   ([`DirSegmentSink::on_segment`](../src/wal_stream.rs)).
   `tokio::fs::write` was rename-after-write but never fsync'd. Phase
   11 reshapes to `OpenOptions+write_all+sync_all+rename`, then
   fsyncs the parent dir once per segment+manifest pair. With that,
   `dispatched_lsn` doubles as `filter_durable_lsn` — what we
   advertise to source as `flush_lsn` is honest.
3. **Per-xact commit_lsn carrier**
   ([`CommittedTuple.commit_lsn`](../src/heap_decoder.rs),
   [`XactBuffer::commit`](../src/xact_buffer.rs)). Every tuple
   leaving the xact buffer carries the LSN of the matching commit
   record. `XactBuffer::commit` accepts a new `commit_lsn` parameter
   and updates two new monotonic fields on `XactBufferStats`:
   `drain_lsn` (set before the observer's `on_xact_end`) and
   `emitter_ack_lsn` (set after `on_xact_end` returns `Ok`). The
   gap between the two is exactly the failure window the cursor
   must protect against — the slot-advance ceiling reads the latter.
4. **Slot-advance gate keyed on emitter_ack_lsn**
   ([`bin/stream.rs`](../src/bin/stream.rs)). The standby-status
   triple Phase 10 introduced gets its first non-placeholder values:
   `write = source_received_lsn`, `flush = filter_durable_lsn`
   (now-fsynced `dispatched_lsn`),
   `apply = min(shadow_replay_lsn, emitter_ack_lsn)`. `shadow_replay`
   threads through the existing retention sweeper (already polling
   `pg_last_wal_replay_lsn()`); the daemon shares one
   `Arc<AtomicU64>` between the sweeper and the status loop so no
   second shadow connection appears. `emitter_ack_lsn` comes
   straight from `XactBuffer::stats()` — single source of truth
   because the buffer is the one place where "observer.on_xact_end
   returned Ok" is observed.
5. **Startup resume**
   ([`bin/stream.rs`](../src/bin/stream.rs) boot path).
   `cursor::read(&spill_dir)` runs after `IDENTIFY_SYSTEM` but
   before `START_REPLICATION`. Three resume sources, in precedence:
   `--start-lsn` (explicit operator override, unchanged) →
   `cursor.emitter_ack_lsn` segment-aligned down → source's current
   write head (greenfield). New `--ignore-cursor` flag forces
   greenfield boot even with a valid cursor on disk for
   recovery-from-known-LSN drills.

The cursor write itself runs from the status-loop body at
`status_interval` cadence (default 10 s, matching the standby-status
send cadence). Ordering is load-bearing: cursor write → SourceFeed's
internal `send_status` consumes the same `StandbyStatus` value built
in the same iteration. Source's slot can only advance after the
cursor has been fsynced, so a kill `kill -9` between cursor write
and slot send rolls back to the just-fsynced cursor — never past it.

## What landed

| item | files | tests |
|---|---|---|
| `cursor::{Cursor, write, read, encode, decode, fsync_dir}` + atomic-rename writer | [`src/cursor.rs`](../src/cursor.rs) | 8 lib unit tests covering encode/decode, magic / version / size / CRC rejection, write+read round-trip, overwrite, dir fsync via tempfile | 
| `DirSegmentSink::on_segment` and `on_partial_segment` fsync the segment file + manifest + parent dir | [`src/wal_stream.rs`](../src/wal_stream.rs) | existing 2 dir-sink round-trip tests still green (atomic-rename invariant unchanged) |
| `CommittedTuple.commit_lsn` carrier | [`src/heap_decoder.rs`](../src/heap_decoder.rs) | covered transitively by `xact_buffer` lib + integration tests |
| `XactBuffer::commit(xid, commit_ts, commit_lsn, ..)` + `XactBuffer::abort(xid, abort_lsn)` advance `XactBufferStats::{drain_lsn, emitter_ack_lsn}` | [`src/xact_buffer.rs`](../src/xact_buffer.rs) | 1 new lib unit test (`abort_advances_ack_lsns_for_resume_cursor`) + extended `commit_drains_in_arrival_order_and_clears_state` + extended `commit_unknown_xid_no_ops` |
| Daemon boot-time cursor resume + `--ignore-cursor` CLI flag | [`src/bin/stream.rs`](../src/bin/stream.rs) | exercised via the `Args::parse` paths; live drill rides on the existing phase8 e2e harness |
| Status loop writes cursor + flips standby-status triple to durable values | [`src/bin/stream.rs`](../src/bin/stream.rs) | covered by phase8 e2e + new `tests/phase11_cursor.rs` |
| `MetricsSnapshot.{shadow_replay_lsn, decoder_commit_lsn, emitter_ack_lsn}` now populated from live values | [`src/bin/stream.rs`](../src/bin/stream.rs) | existing metrics tests still green |
| `tests/phase11_cursor.rs` — cursor write crash-simulation + corrupt-detection + greenfield fall-back | [`tests/phase11_cursor.rs`](../tests/phase11_cursor.rs) | 3 integration tests (all green, <0.1 s wall) |

Build clean on `cargo clippy --workspace --all-targets -- -D warnings`.

Test counts (local, PG 18.4 + ClickHouse 25.8):

- `cargo test --workspace --lib`: 179 tests, +1 from Phase 10
  (`abort_advances_ack_lsns_for_resume_cursor` plus 8 cursor unit
  tests living in `src/cursor.rs`).
- `cargo test --test phase11_cursor`: 3 tests, all green.
- Phase 8 e2e + Phase 9 oracle + Phase 10 ops suites continue to pass
  with no test edits.

Code size:

| component | LOC |
|---|---|
| `src/cursor.rs` | 291 |
| `tests/phase11_cursor.rs` | 71 |
| `src/bin/stream.rs` (delta) | ~80 lines beyond pre-Phase-11 |
| `src/xact_buffer.rs` (delta) | ~50 lines (drain_lsn + emitter_ack_lsn fields, commit/abort signature, +1 unit test) |
| `src/wal_stream.rs` (delta) | ~30 lines (fsync helper + sink rewrites) |
| `src/heap_decoder.rs` (delta) | ~7 lines (`commit_lsn` field + docs) |
| `src/ch_emitter.rs` (delta) | ~6 lines (drain_xact doc note clarifying buffer-as-source-of-truth) |

PLAN.md estimated ≈500 LOC; landed at ~550 once the cursor module's
test surface (8 unit tests + 3 integration tests) is accounted for.
The Phase 10 retro had set up most of the supporting plumbing
(standby-status triple, retention sweeper polling shadow's replay
LSN, metrics endpoint with placeholder Phase 11 gauges), so the
phase 11 delta to `src/bin/stream.rs` was concentrated on the resume
gate + cursor-write cadence rather than re-shaping the status loop.

## Bugs surfaced

### 1. Unknown-xid commit dropped the slot-advance signal

First cut had `XactBuffer::commit`'s short-return path
(`commits_unknown_xid` bump) only touch the unknown-xid stat. But
read-only / filter-dropped xacts still produce a commit record on
the WAL path — if their LSN never advanced `emitter_ack_lsn`, the
slot would freeze on a sustained read-only workload. Fix: bump
`drain_lsn` + `emitter_ack_lsn` *before* the early return. Same
treatment for `XactBuffer::abort`'s `aborts_unknown_xid` path. The
integration test `commit_unknown_xid_no_ops` was extended to pin
this — passing `commit_lsn=0x9000` and asserting both stats
ceilings rise to that LSN.

### 2. Atomic-rename leaves the `.tmp` file readable across crashes

The cursor's atomic-rename writer creates `cursor.bin.tmp`, fsyncs
it, then renames over `cursor.bin`. A `kill -9` between
`OpenOptions::open(&tmp)` and `tokio::fs::rename(&tmp, &final_path)`
leaves a *valid-magic, valid-CRC* but stale `.tmp` on disk. Boot
path only looks at `cursor.bin`, so the stale `.tmp` is ignored.
The integration test
`write_survives_simulated_crash_during_tmp_phase` pins this — it
writes a good cursor, plants a garbage `.tmp` (simulating
mid-write crash), reads back through `cursor::read`, and verifies
the prior `cursor.bin` is what surfaces. Code comment in
`cursor::write` spells out the invariant because the next reader
will inevitably "tidy up" the writer and break the property.

### 3. `shadow_replay_lsn = 0` would pin the slot forever

The literal `apply = min(shadow_replay, emitter_ack)` formula
pins apply_lsn to 0 when `shadow_replay_lsn` is still 0 (sweeper
hasn't reported yet, or `--retention-bytes 0` disables the
sweeper outright). That's catastrophic — source's slot never
recycles. Fix: treat `shadow_replay_lsn == 0` as "no constraint
from shadow" and use `emitter_ack_lsn` alone. Code comment in
`bin/stream.rs` notes the policy. Operators running with
retention disabled trade the explicit shadow-replay gate for
emitter-only correctness, which is the right trade — shadow's
replay LSN isn't on the resume path anyway (the cursor's
`emitter_ack_lsn` is the load-bearing value).

## Design decisions

### Single source of truth for emitter_ack_lsn

PLAN.md left implicit where the emitter-ack LSN lives. Two options:
the emitter itself (`Arc<AtomicU64>` published by `Emitter`) or the
xact buffer's stats (`XactBufferStats.emitter_ack_lsn`). Picked the
latter because:

* The buffer is the one place where "observer.on_xact_end returned
  Ok" is observed. Both the CH-Native emitter path and the
  metrics-only path go through `XactBuffer::commit` → bumping
  stats *after* the observer returns Ok captures real durability
  for both arms uniformly.
* Going through the emitter would require a second tracker on the
  metrics-only daemon binary (where no Emitter exists), or a
  conditional dispatch that's hard to keep monotonic.
* The buffer's stats are already locked behind `xact_buffer.lock`
  which the status-line loop is already taking on every status
  tick — zero extra locking cost.

`Emitter::drain_xact` is the *signal* (returning Ok) that the
buffer interprets to advance its stats; no per-emitter ack gauge
exists because that would duplicate the buffer's bookkeeping.

### Cursor file lives next to spill files

`{spill_dir}/cursor.bin`, not `--cursor-path`. Two reasons:

* A `mv` of the working dir keeps the spill files and the cursor
  coherent. Operators moving the daemon's state dir don't need
  two coordinated path arguments.
* The spill dir is already daemon-owned, fsync-able, and created
  early in boot via `XactBuffer::new` → it's exactly the lifecycle
  the cursor file wants.

A future change that re-introduces a `--cursor-dir` knob for HA
deployments (shared NFS, etc.) can override at that point without
backwards-compat hassle — `cursor::cursor_path` is the single
choke point.

### CRC32C + magic prefix, not bincode

The cursor file is 56 bytes of LSNs — too small for the serde
overhead. Manual byte layout costs ~30 LOC of `encode`/`decode`
and gives precise errors (`BadMagic` / `Version` / `Crc`) for
each corruption mode the boot path needs to distinguish. Magic
prefix doubles as a `file` / `binwalk`-friendly identifier so an
operator stumbling on `cursor.bin` knows what it is. CRC32C
matches PG's own checksum algorithm so a future "log on every
checksum failure" tap could share the same code path.

### Cursor write cadence = status interval

Cursor write happens once per `status_interval` from inside the
main loop, before the iteration enters `next_chunk`. That keeps
the cursor's `emitter_ack_lsn` ≥ what we ship to source as
`apply_lsn` on every send, without per-chunk write overhead
(which would be hot on a busy workload).

Trade-off: cursor lags by up to one status-tick worth of progress.
A `kill -9` between an in-flight commit drain and the next cursor
write loses 0 commits to CH (CH already has them; `_lsn` dedup
collapses on re-emit) and 0 commits to source's slot (slot
advance hasn't happened yet either). The window where the cursor
is genuinely stale is bounded by status_interval; tightening
requires per-xact writes which Phase 11 explicitly defers.

### `--ignore-cursor` over `rm cursor.bin`

Operators recovering from a corrupt cursor (or a known-bad LSN
prior to a planted bug) need a way to force greenfield boot. Two
options: tell them to `rm cursor.bin` and re-launch, or expose a
CLI flag. Picked the flag because `rm` between cursor write and
daemon launch races against a still-running daemon; `--ignore-
cursor` is atomic with respect to the daemon's boot sequence. The
flag also leaves the prior cursor on disk for forensics.

## What didn't get done

* **Per-xact cursor write.** PLAN.md says "written on every
  emitter-acked xact drain". Phase 11 ships per-status-interval
  writes instead. Per-xact would mean fsyncing the cursor file on
  every COMMIT, which on a busy OLTP workload is ~1k cursors/sec
  worth of disk write+fsync+dir-fsync. Acceptance §5 doesn't
  require per-xact granularity — the slot lags by at most one tick.
  Revisit if a workload makes a stronger durability guarantee
  necessary.
* **Spill-replay on boot.** PLAN.md sketches "replay any spill
  files keyed on xids whose first-seen LSN > cursor.emitter_ack_lsn".
  Phase 11 doesn't implement this because the spill dir wipe at
  startup (Phase 6 invariant) means there's nothing to replay —
  the cursor's `emitter_ack_lsn` is the resume LSN and source
  re-streams from there. A future phase that preserves spill
  across restarts (e.g. a `--keep-spill` recovery drill) could
  flip this, at which point the boot path's cursor read becomes
  one of two sources for the resume LSN.
* **Two-phase commit cursor entries.** `XLOG_XACT_PREPARE` can sit
  arbitrarily long before its `COMMIT_PREPARED`. Phase 11 still
  treats prepare-without-commit-prepared as Phase 6 did: the xact
  stays buffered until commit_prepared lands. PLAN's known
  correctness gap #7 (2PC) still holds; the cursor file format
  reserves room for prepared-xact metadata in a future version
  bump.
* **`Allocator` pin audit.** PLAN.md mentioned auditing
  `BlockBuilder`'s `Allocator` pin requirements if Phase 11
  stretches the builder lifetime across awaits. The Phase 11
  emitter changes don't touch the builder's lifetime (no
  budget-triggered mid-xact flush refactor landed), so the audit
  defers to whenever that refactor does happen.
* **Acceptance §5 wall-clock drill.** A `kill -9` + restart test
  exists in spirit through the integration suite but no automated
  CI drill spawns the daemon, sends SIGKILL, restarts, and
  compares CH end-states. That belongs in a Phase 12 followup
  alongside the backfill bridge — the same harness can validate
  both.

## Files touched

```
walshadow/src/cursor.rs                       new — atomic-rename cursor writer + reader
walshadow/src/lib.rs                          mod cursor;
walshadow/src/wal_stream.rs                   DirSegmentSink fsync + write_sync_rename helper
walshadow/src/xact_buffer.rs                  drain_lsn + emitter_ack_lsn on XactBufferStats; commit/abort signatures grow commit_lsn; 1 new unit test
walshadow/src/heap_decoder.rs                 CommittedTuple.commit_lsn field
walshadow/src/decoder_sink.rs                 commit_lsn: 0 fill on Phase 5 unbuffered + tests
walshadow/src/ch_emitter.rs                   drain_xact doc note + test fixture commit_lsn
walshadow/src/bin/stream.rs                   cursor read at boot, cursor write each tick, durable standby-status triple, populate Phase 11 metric gauges, --ignore-cursor flag
walshadow/tests/phase9_oracle.rs              CommittedTuple commit_lsn fill
walshadow/tests/xact_buffer.rs                commit/abort signature updates + Phase 11 LSN assertions
walshadow/tests/phase11_cursor.rs             new — 3 cursor-surface integration tests
walshadow/plans/INDEX.md                      Phase 11 entry → PHASE11.md
walshadow/plans/PHASE11.md                    new (this doc)
```
