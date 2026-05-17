# PRE5b4 — `CatalogTracker` → `ShadowCatalog::invalidate` (retrospective)

[PRE5b](PRE5b.md) item B4. Last of the four B-item correctness fixes;
no dependency on [PRE5b1](PRE5b1.md)–[PRE5b3](PRE5b3.md), independently
shipped. Closes the never-wired
[Phase 4b](PHASE4b.md) generation-bump path so the descriptor cache
stops staling on DDL.

## Why (preserved)

`ShadowCatalog::invalidate` (`src/shadow_catalog.rs:213`) was called
only from `tests/shadow_catalog.rs:218`. The module doc at
`shadow_catalog.rs:18-21` claimed an upstream caller; none existed.
[Phase 4b](PHASE4b.md)'s "generation bump on commit-LSN observed to
write into pg_catalog relfilenodes" never wired.

Cached `RelDescriptor`s went stale at the first DDL on shadow and stayed
stale until shadow PG bounced.
[Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix)
calling `relation_at(rfn, commit_lsn)` would have handed back pre-DDL
column shape for post-DDL records.

## What landed

* **`CatalogTracker.invalidation_signal`.** Optional
  `tokio::sync::mpsc::UnboundedSender<()>` field. New
  `set_invalidation_signal(tx)` setter; senderless trackers (offline
  CLI use of `walshadow-filter`, batch fixture tests) send nothing.
  `signal_invalidation` is a private helper that does a best-effort
  send and swallows `SendError` so a torn-down drain can't poison
  subsequent observes. The successful-send counter
  `invalidation_signals_sent` is surfaced on the tracker for symmetry
  with the other `pg_class_writes_*` counters.
* **Tracker observe points fire on three paths.**
  `handle_relmap_update` signals once after the for-loop completes
  (skipped on malformed-relmap early-returns).
  `harvest_pg_class_blocks` signals on `DecodeOutcome::Decoded` AND on
  `DecodeOutcome::OidInPrefix` — see deviations below. `Undecoded`
  doesn't signal.
* **`shadow_catalog::spawn_invalidation_drain`.** Free function that
  spawns a tokio task draining the mpsc end. Each wake coalesces every
  queued tick into a single `ShadowCatalog::invalidate` call — over-
  invalidation is cheap (lazy eviction) so adjacency coalescing is the
  right trade. Task terminates when every sender drops; the returned
  `JoinHandle` is optional (long-running daemons detach it).
* **`bin/stream.rs` wires the channel.** Creates an
  `unbounded_channel::<()>`, gives the sender to
  `stream.filter_mut().tracker`, and spawns a stub drain that
  consumes-and-discards. Phase 5 will swap the stub for
  `spawn_invalidation_drain(catalog_arc, rx)` once the daemon holds a
  `ShadowCatalog`. The stub exists so the unbounded buffer can't grow
  under production observe rates while the catalog stays absent.
* **Module doc update.** `shadow_catalog.rs:18-21` now points at the
  `set_invalidation_signal` + `spawn_invalidation_drain` pair and notes
  that PRE5b4 callers wrap the catalog in
  `Arc<tokio::sync::Mutex<_>>`. PRE5b7 promotes the wrap to the daemon.
* **Tokio `sync` feature.** Added to the workspace `Cargo.toml`. The
  rest of the feature list unchanged.

## Tests

* `cargo test --lib`: 83 pass (was 76; +7 unit). New unit tests in
  `src/catalog_tracker.rs`:
  * `observe_relmap_update_fires_signal_when_attached`
  * `observe_pg_class_decoded_fires_signal_when_attached`
  * `observe_pg_class_oid_in_prefix_fires_signal_when_attached`
  * `observe_pg_class_undecoded_does_not_fire_signal`
  * `observe_without_sender_is_a_no_op`
  * `signal_swallows_closed_receiver`
  * `observe_non_catalog_record_does_not_signal`
* `cargo test --tests`: 23 integration tests pass (was 22; +1).
  `tests/shadow_catalog.rs::tracker_signal_drives_invalidate_and_refetches_after_ddl`
  spins live PG, primes the descriptor cache for `wc.things`, runs
  `ALTER TABLE wc.things ADD COLUMN extra text`, sends one signal
  through the mpsc channel, waits up to 2 s for `stats().generation_bumps`
  to advance (counter assertion per exit criterion 3), then re-fetches
  the descriptor and asserts the new column appears (behavioural
  verification of the end-to-end cache invalidation).
* `cargo fmt --all -- --check` clean.
* `cargo clippy --all-targets -- -D warnings` clean.

## Deviations from plan

* **`OidInPrefix` also signals.** Plan called for signaling only on
  paths where `pg_class_writes_decoded` ticks. In practice the
  descriptor-cache-stales-on-DDL bug is dominated by `ALTER TABLE` and
  similar `pg_class` UPDATEs that PG prefix-compresses past the OID
  column (`relname` through `relfilenode` unchanged → `prefixlen ≈ 88`),
  surfacing as `OidInPrefix` not `Decoded`. Signaling only on `Decoded`
  would leave the central scenario uninvalidated — the test would
  literally not see ADD COLUMN through the natural wire — and reduce
  PRE5b4 to fixing a symptom (CREATE TABLE bookkeeping) rather than the
  root cause. Signaling on both is the smaller change to keep PLAN.md's
  "intentional over-invalidation" honest. `Undecoded` still doesn't
  signal: garbage shouldn't trigger work.
* **Test sends through the channel directly.** Plan offered "Send the
  invalidation signal (or drive it end-to-end via the channel)" and
  the integration test takes the former. The send-side wire is unit-
  tested separately in `src/catalog_tracker.rs::tests`, which has
  crate access to the synthetic-record helpers. Driving end-to-end
  from the integration test would have meant either making the
  helpers `pub` (API pollution) or duplicating their bodies in
  `tests/shadow_catalog.rs`. Splitting the verification across two
  test surfaces keeps each one tight.
* **`bin/stream.rs` drain is a stub.** Plan implied a single drain task
  in `stream.rs`. The daemon doesn't hold a `ShadowCatalog` yet (Phase
  5 attaches one), so the production drain has no catalog to invalidate
  against. The stub consumes-and-discards so the unbounded buffer can't
  grow under observe rates; Phase 5 swaps it for the real
  `spawn_invalidation_drain` call. The wire (`set_invalidation_signal`
  attached) is intact — tracker counters
  (`invalidation_signals_sent`) already reflect what the production
  drain will see.
* **`invalidation_signals_sent` counter added.** Plan was silent on
  observability. The counter on `CatalogTracker` increments per
  successful enqueue; surfaced for unit-test assertions and operator
  visibility post-Phase-5. The drain task's effective work is visible
  on the catalog side as `ShadowCatalogStats::generation_bumps`.
* **No daemon-side `Arc<Mutex<ShadowCatalog>>` yet.** The integration
  test wraps in `Arc<tokio::sync::Mutex<_>>` because the drain task
  needs `&mut self` access alongside the test's own reads. The daemon
  doesn't wrap yet — that's PRE5b7's scope. Module doc flags the
  hand-off.

## Implementation notes for follow-on work

`spawn_invalidation_drain`'s body is three lines plus the coalesce
loop — the entire reason it exists as a public helper is so Phase 5
gets a one-call drop-in when it adds the catalog. The stub drain in
`bin/stream.rs` is the call site to replace. Reuse `_invalidation_rx`
(currently `Receiver<()>` moved into the spawn closure) by hoisting
the channel build above whatever owns the catalog, then handing the
receiver to `spawn_invalidation_drain` once the catalog Arc is in
scope.

The drain calls `ShadowCatalog::invalidate` under the mutex held for
the bump's duration. `invalidate` itself is fast (one counter add) so
contention with `relation_at` callers is negligible — the lock-free
hit-path refactor stays out of scope per PRE5b7.

`OidInPrefix` signaling deliberately over-invalidates on every
`VACUUM FULL pg_<non-mapped>`. The vacuum churn in OLTP workloads is
low enough that the generation bumps stay sparse; if a future profile
shows generation bumps eating real CPU on the cache-miss path, narrow
the signal to "the row's filenode is one we have cached" — but that
needs the tracker to know what the catalog has cached, which means
plumbing a back-channel and is much bigger than this fix.

The seed path (`seed_from_source`) does NOT signal. Seeds run before
any descriptor cache exists, so the signal would invalidate nothing.
If a future caller invokes `seed_from_source` mid-stream (e.g. forced
catalog re-bootstrap), it should call `cat.invalidate()` explicitly
out-of-band.

## Files actually changed

```
Cargo.toml                         tokio "sync" feature
src/catalog_tracker.rs             invalidation_signal field + setter; signal on relmap,
                                   Decoded, OidInPrefix; module doc; 7 unit tests
src/shadow_catalog.rs              spawn_invalidation_drain; module doc update
src/bin/stream.rs                  wire sender into tracker; stub drain pending Phase 5
tests/shadow_catalog.rs            tracker_signal_drives_invalidate_and_refetches_after_ddl
plans/PRE5b4.md                    this retrospective
```
