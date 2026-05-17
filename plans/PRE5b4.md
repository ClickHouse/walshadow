# PRE5b4 — `CatalogTracker` → `ShadowCatalog::invalidate`

[PRE5b](PRE5b.md) item B4. Closes the never-wired
[Phase 4b](PHASE4b.md) generation-bump path so the descriptor cache
stops staling on DDL.

## Why

`ShadowCatalog::invalidate` (`src/shadow_catalog.rs:213`) is called
only from `tests/shadow_catalog.rs:218`. The module doc at
`shadow_catalog.rs:18-21` claims an upstream caller; none exists.
[Phase 4b](PHASE4b.md)'s "generation bump on commit-LSN observed to
write into pg_catalog relfilenodes" never wired.

Cached `RelDescriptor`s go stale at the first DDL on shadow and stay
stale until shadow PG bounces.
[Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix)
calling `relation_at(rfn, commit_lsn)` will hand back pre-DDL column
shape for post-DDL records.

## Implementation

* `CatalogTracker` carries an optional
  `tokio::sync::mpsc::UnboundedSender<()>`. Sync side; async side runs
  a small drain task that calls `cat.invalidate()` per signal,
  coalescing adjacent signals if backpressure permits.
* On `handle_relmap_update` and on `harvest_pg_class_blocks` paths
  where `pg_class_writes_decoded` ticks, send one signal. Both paths
  know they touched the catalog set; granularity stays coarse (bump
  generation), matching
  [PLAN.md](PLAN.md#risks--open-questions)'s intentional
  over-invalidation.
* No signal sent when the tracker has no consumer attached
  (offline CLI use of `walshadow-filter`).

## Tests

* New `tests/shadow_catalog.rs` case: spin live PG, populate one user
  table, fetch `relation_at`. Issue `ALTER TABLE ... ADD COLUMN` via
  SQL helper. Send the invalidation signal (or drive it end-to-end via
  the channel). Re-fetch and assert the new column appears in
  `RelDescriptor.attributes`.

## Exit criteria

1. `cargo test --lib && cargo test --tests` clean, including the new
   ALTER-driven re-fetch case.
2. `cargo fmt --all -- --check` and
   `cargo clippy --all-targets -- -D warnings` clean. Run both at
   the end of the implementing phase before commit.
3. `CatalogTracker` mutations on the catalog set produce
   `ShadowCatalog::invalidate` calls in production paths
   (verified by a counter assertion or trace, not by behavioural
   inspection alone).

## Files expected to change

```
src/catalog_tracker.rs             mpsc sender to ShadowCatalog::invalidate
src/shadow_catalog.rs              drain task; module doc update
src/bin/stream.rs                  wire sender into the daemon's tracker
tests/shadow_catalog.rs            tracker-driven invalidation scenario
plans/PRE5b4.md                    this doc
```
