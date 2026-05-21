# PRE15 — cleanup before PHASE15

Pre-[PHASE15](PHASE15.md) sweep of friction picked up while evaluating
the plans/done split. Six items, all safe (none touch wire formats
that ship across daemon restarts, none change observer contracts).
Goal: shrink the eventual PHASE15 diff & close two PHASE14-retro
debts (toast-counter visibility, spill-format bump) so the next
phase doesn't pile new surface on top of stale internals

## What landed

### §1 Delete `DecoderSink` (the Phase-5 unbuffered direct-emit path)

[`src/decoder_sink.rs`](../src/decoder_sink.rs) carried two
[`RecordSink`](../src/wal_stream.rs) impls: `DecoderSink<O>` (Phase 5,
eager-emit per record) & the production
[`BufferingDecoderSink`](../src/xact_buffer.rs) (Phase 6, xact-buffered).
Production wired only the buffered one — `DecoderSink` survived as
doc-comments + one test reference. Removed the struct + its
`RecordSink` impl, kept [`TupleObserver`] / [`DecoderStats`] /
[`DecoderSinkError`] / the `MetricsTupleObserver` +
`CollectingTupleObserver` impls

Module-level doc rewritten: it's no longer the Phase-5 dispatch
adapter, it's the shared types module for the heap-tuple fan-out
([`BufferingDecoderSink`] → `TupleObserver` → emitter / oracle /
metrics)

Sweep of stale `[DecoderSink]` doc-links in `xact_buffer.rs`,
`shadow_catalog.rs`, `heap_decoder.rs`, `tests/shadow_catalog.rs`,
`src/bin/stream.rs`

### §2 One source of truth for `HeapOp → DecoderStats` mapping

The op-bumping match (`Insert → inserts += 1`, etc.) was transcribed
three times before this sweep: `DecoderSink::on_record`,
`BufferingDecoderSink::on_record`, `MetricsTupleObserver::on_tuple`.
§1 dropped one copy. §2 collapsed the remaining two onto a new
`DecoderStats::record(&DecodedHeap)` helper. `BufferingDecoderSink`'s
truncate path also routes through it now via the synthesised
`HeapOp::Truncate` decode

Surprise: PHASE15 §1's `SchemaEvent` will reach back here when the
`TupleObserver` widens with `on_schema_event`. With the helper in
place, the catalog-event drain only adds the new `on_schema_event`
call & doesn't need to re-encode the per-op mapping per consumer

### §3 Lift `toast_chunks_*` into `DecoderStats`

`BufferingDecoderSink.toast_chunks_buffered` /
`toast_chunks_malformed` were `pub` fields on the sink struct,
disconnected from the `Arc<Mutex<DecoderStats>>` the daemon's status
loop reads through `stats_handle()`. The metrics endpoint had no
visibility on them either. Moved both counters onto `DecoderStats`,
routed the bumps through the existing `bump()` helper,
extended `DecoderStats::summary()` & the Prom endpoint with
`walshadow_decoder_toast_chunks_total` /
`walshadow_decoder_toast_malformed_total`. `MetricsSnapshot`
shape stays additive — existing callers untouched

### §4 Lift `ChServer` into `tests/common/`

[`tests/common/bootstrap_ch_fixture.rs`](../tests/common/bootstrap_ch_fixture.rs)
already owned the canonical `ChServer` subprocess wrapper; the
private copy in `tests/phase8_e2e.rs` was an ~140 LOC duplicate.
PHASE14 retro flagged this with a "when the third caller lands"
gate; PHASE15 §6's drop-table test is that caller, & lifting now
keeps the PHASE15 test file from forking a fourth copy. `phase8_e2e`
now includes the common fixture via the existing
`#[path = "common/bootstrap_ch_fixture.rs"] mod fx;` pattern other
phase-14 tests use, then `use fx::{ChServer, clickhouse_available,
pg_available, pg_basebackup_available}`

### §5 Spill format version header

[`spill.rs`](../src/spill.rs) wrote entries as `[tag][len][body]`
with no leading magic / version field. PHASE14 retro flagged that
`HeapOp::Truncate`'s tag-4 addition (PHASE14 §3) silently changed
the body encoding without a version bump; PHASE15 §6 will do the
same for `DrainEntry::Catalog`. Resume contract wipes spill on
boot so the missing version was academic, but a future format
change would catch a stale daemon-version cohort mid-restart with
no diagnostic

Header: 2-byte ASCII magic `WS` + u16 LE version `2`, 4 bytes total.
`SpillWriter` writes it on construction inside `SpillStore::writer`;
`SpillReader` verifies lazily on first `next()` call via a
`header_checked` flag. `SpillError::Format` surfaces both "bad
magic" & "unsupported version" with offset 0/2 respectively. Two new
unit tests (`missing_magic_surfaces_format_error`, the existing
`malformed_tag_surfaces_format_error` re-seeded with a valid header).
Picked v2 (not v1) for honesty: PHASE14 §3 already changed the body
encoding, so calling the pre-PRE15 unversioned shape "v1" would be
backdating

### §6 Bootstrap autospawn — already landed

PHASE14 retro called out `--bootstrap-autospawn-shadow` not
rewriting shadow's `port` / `unix_socket_directories` /
`listen_addresses` into the cloned `postgresql.auto.conf`.
Investigated & found [`write_shadow_listener_overrides`](../src/bin/stream.rs)
already does this — it landed in the PHASE14 commit `142681a`
itself, called from `autospawn_shadow_and_wait` immediately before
`shadow.start()`. The retro lagged the code. Tests still hand-edit
source's `postgresql.auto.conf` via `append_source_conf` but only
for the orthogonal `wal_level = logical` + `max_wal_senders`
overrides, not the port-collision fix

Marked done with no code change; relisting the retro item here so
future readers don't re-investigate

## Tests + verification

- `cargo check --workspace --tests` green
- `cargo test --workspace --lib` — **290 passed**, 0 failed, 0 ignored
- `cargo clippy --workspace --lib --tests --all-targets -- -D warnings` clean
- Two new spill unit tests cover the header (`missing_magic_*` +
  the re-seeded `malformed_tag_*`)

Diff: ~187 insertions / ~402 deletions across 10 files. Net negative;
the only growth is `DecoderStats::record`, the toast counters,
spill header, & two new tests

## What didn't land here

These items came out of the same evaluation pass but are
PHASE15-shaped — doing them as pre-cleanup would force PHASE15 to
either rebase on speculative shapes or land the same code twice.
Listed so PHASE15 can pick them up at the right place

### `MappingHandle` → `watch::Receiver<Arc<ResolvedConfig>>`

`Arc<RwLock<HashMap<String, TableMapping>>>` at
[`ch_emitter.rs:971`](../src/ch_emitter.rs) is exactly the shape
PHASE15 §5 declares `ResolvedConfig` against, with PHASE16 §3
plugging more sources into the same merge point. The lift is mostly
mechanical (RwLock → watch channel) but PHASE15 §5's struct shape
(`global` / `namespaces` / `tables` / `columns`) is the schema this
needs to land against. Pre-doing it would commit to a shape PHASE15
might still tune. Defer

### `ShadowCatalog` `tokio::sync::Mutex` → `RwLock` (or interior-mut)

[`shadow_catalog.rs`](../src/shadow_catalog.rs) lives behind
`Arc<tokio::sync::Mutex<_>>` at every consumer (decoder, xact_drain,
detoast, oracle, retention sweep). PHASE15 §1's `subscribe()`
channel adds a notifier through the same lock; PHASE16 §2's
`config_decoder` adds another reader. The PRE5b7 deferral note
inside the module already documents this — the lock-free hit path
lands "when the lookup-rate hot path actually exists." PHASE15 is
likely that point. Defer to PHASE15 §1 so the event-channel
producer + the RwLock split can land in one cut without
intermediate state where the producer holds `&mut self` through a
notifier

### Drive PHASE14's `#[ignore]` integration tests

`phase14_pgbench_acceptance`, `phase14_kill_restart`,
`phase14_bootstrap_direct_ch`, `phase14_bootstrap_object_store_ch`,
`phase14_add_column_default`, `phase14_truncate`, `phase14_subxact`
all ship `#[ignore]` per the PHASE14 retro. Code-complete; never
driven end-to-end against a live topology. Cheapest correctness
win available, but the unblock is CI-fixture work (PG matrix + CH
binary on the runner), not a Rust refactor. Defer to a separate
sweep

## Surprises worth carrying forward

- **Retros lag commits.** Two of the six items (§6 bootstrap-port
  override, & the absence of any pre-existing spill version field)
  showed PHASE14 retro entries that don't reflect what the PHASE14
  commit actually shipped. The override was already in the same
  commit; the spill format was version-less, not "version 1". Future
  retros benefit from a "verify against `git show HEAD`" pass before
  publishing
- **`DecoderSink`'s reach was wider than it looked.** The struct
  itself had no callers, but eight doc-comments across five
  modules referenced it. Sweeping the doc-links matters more than
  the struct removal because grep on the type name was the only
  way to find the stale references — `cargo` was happy with the
  dangling intra-doc links
- **`DecoderStats::record` shape.** Took `&DecodedHeap` because the
  caller already has it; routing through a `&CommittedTuple` would
  force the unbuffered Metrics observer to extract `decoded` & flip
  the bound. Aligned both call sites on `&DecodedHeap` instead, &
  the observer just borrows `&committed.decoded`
- **Spill header place.** First instinct was to write the header in
  `SpillWriter::finish()`, but that fires _after_ entries — wrong
  end of the file. Moved the write into `SpillStore::writer` (file
  open path) so the header lands as bytes 0..6 of every spill file
  by construction. Reader symmetry stays clean (`check_header` runs
  before the first entry read, gated by `header_checked: bool` so a
  caller that constructs `SpillReader { .. }` directly still
  triggers it on first `next()`)
- **Test fixture lift surfaced an import drift.** Removing
  `phase8_e2e`'s private `ChServer` block also removed its
  `std::net::TcpStream` / `std::process::Command` imports that the
  fixture-internal code carried implicitly; remaining bodies still
  used both. Compile error caught it; clippy would have too.
  Confirms: imports survive deletion only when the deletion is
  clean about what it removes

## Sequencing for PHASE15

PHASE15 §1 (`SchemaEvent` channel on `ShadowCatalog`) is now the
right point to fold in the catalog-lock refactor — same cut, no
intermediate state. PHASE15 §5 (resolver shape) is the right point
for the `MappingHandle` → `watch::Receiver` lift, again single cut.
The toast-counter, spill-header, & ChServer-lift work this phase
shipped removes the three "I'd need to do this first" footguns
PHASE15's per-section diffs would otherwise carry. PHASE16 §4
(`DrainEntry::Config`) inherits the spill header at v2 cleanly
