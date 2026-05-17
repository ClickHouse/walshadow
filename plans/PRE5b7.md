# PRE5b7 — `ShadowCatalog` concurrency

[PRE5b](PRE5b.md) item S3. Independent of the other foundation items;
sequenced anywhere after the B-items.

## Why

`relation_at` (`shadow_catalog.rs:361`), `relation_by_oid`,
`wait_for_replay`, `invalidate` all take `&mut self`. PLAN.md:217
specified `&self`. Single-tasked use today is fine; future emitter
and oracle want concurrent lookups.

## Decision

Defer the interior-mutable refactor. PRE5b7 wraps the catalog in
`Arc<tokio::sync::Mutex<ShadowCatalog>>` at the daemon level so
[Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix)'s
call shape works without surgery, and tracks the refactor as a
follow-up. Rationale: optimising lock contention before the lookup-
rate hot path exists is speculative; the cache-hit path is cheap
enough that single-task serialisation dwarfs nothing measurable yet.

## Implementation

* Daemon binary holds `Arc<Mutex<ShadowCatalog>>`. Pass clones to
  every component that touches the cache.
* Internal `ShadowCatalog` API unchanged.
* Module doc at `shadow_catalog.rs:18-21` updated to reflect the
  `&mut self` reality and the planned refactor.

## Tests

* Add to `tests/shadow_catalog.rs`: hold the mutex across
  `relation_at` from one task, await from another, confirm clean
  serialisation (no `would deadlock` panic, no hang). Sanity-check
  the wrap, not the lock-free path that isn't built yet.

## Exit criteria

1. `cargo test --lib && cargo test --tests` clean, including the
   mutex-sanity case.
2. `cargo clippy --all-targets -- -D warnings` clean.
3. Daemon binary holds the catalog as `Arc<Mutex<_>>`; module doc
   reflects current `&mut self` API and the deferred refactor.

## Files expected to change

```
src/shadow_catalog.rs              module doc update
src/bin/stream.rs                  Arc<Mutex<ShadowCatalog>> at daemon level
tests/shadow_catalog.rs            cross-task mutex sanity
plans/PRE5b7.md                    this doc
```
