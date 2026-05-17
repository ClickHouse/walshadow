# PRE5b7 — `ShadowCatalog` concurrency wrap (retrospective)

[PRE5b](PRE5b.md) item S3. Third of the foundation changes; independent
of [PRE5b5](PRE5b5.md) / [PRE5b6](PRE5b6.md). Sets up the
[`ShadowCatalog`](../src/shadow_catalog.rs) for the multi-consumer call
shape [Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix)
needs (`DecoderSink` + tracker drain holding the catalog concurrently)
without rewriting the cache's `&mut self` surface.

## Why (preserved)

[`ShadowCatalog::relation_at`](../src/shadow_catalog.rs),
`relation_by_oid`, `wait_for_replay`, `invalidate` all take `&mut self`.
[PLAN.md §Phase 4 sketch](PLAN.md) specs `&self`. The single-tasked
daemon today (`bin/stream.rs` pre-PRE5b7 didn't even own a catalog —
just a stub drain) is fine, but Phase 5's `DecoderSink` plus the
existing tracker→drain wire would have wanted concurrent lookups.

## Decision (preserved)

Defer the interior-mutable refactor (`RwLock` over the two `HashMap`s
+ atomics for stats). PRE5b7 wraps the catalog in
`Arc<tokio::sync::Mutex<ShadowCatalog>>` at the daemon level so
[Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix)'s
call shape works without surgery; the spec-vs-implementation gap is
tracked as a follow-up to revisit when the lookup-rate hot path
actually exists. Optimising lock contention before that point is
speculative; the cache-hit path is cheap enough that single-task
serialisation dwarfs nothing measurable today.

## What landed

* **Daemon binary constructs the catalog.** `bin/stream.rs` previously
  had no `ShadowCatalog` at all — a stub drain task consumed and
  discarded invalidation ticks because the daemon couldn't dispatch
  them anywhere. PRE5b7 adds five `--shadow-*` clap args
  (`--shadow-socket-dir`, `--shadow-port`, `--shadow-user`,
  `--shadow-dbname`, `--shadow-connect-timeout`), builds the conninfo
  via [`socket_conninfo`](../src/shadow_catalog.rs), and runs
  [`ShadowCatalog::connect`](../src/shadow_catalog.rs) inside
  [`with_transient_retry`](../src/shadow_catalog.rs) so a
  still-warming shadow at boot doesn't fail the daemon. The connect
  budget defaults to 30 s, with the backoff initial / max pulled
  straight from [`ShadowCatalogConfig::default()`](../src/shadow_catalog.rs)
  (100 ms initial, 1 s ceiling — matches [PHASE4b](PHASE4b.md)).
* **`Arc<tokio::sync::Mutex<_>>` wrap at the daemon level.** After
  connect, the catalog is moved into `Arc::new(Mutex::new(catalog))`.
  The drain task gets one clone via
  [`spawn_invalidation_drain(catalog.clone(), invalidation_rx)`](../src/shadow_catalog.rs).
  Phase 5's `DecoderSink` clones again from `catalog` when it lands.
  No other component touches the cache today — the wrap is in place
  for the multi-consumer shape Phase 5 needs, not for an existing hot
  path.
* **Stub drain → real drain.** The previous stub task that just drained
  the mpsc channel (`bin/stream.rs:174-178` pre-PRE5b7) is replaced
  with [`spawn_invalidation_drain`](../src/shadow_catalog.rs). Tracker
  ticks now reach [`ShadowCatalog::invalidate`](../src/shadow_catalog.rs)
  in the production daemon path — also closes
  [PRE5b](PRE5b.md) exit criterion 8 ("`CatalogTracker` mutations on
  the catalog set produce `ShadowCatalog::invalidate` calls in
  production paths") as a side effect of moving catalog ownership into
  the daemon.
* **Module doc rewrite.** `src/shadow_catalog.rs:17-37` replaces the
  one-liner "the catalog itself is `&mut self`; PRE5b4 callers wrap
  it in `Arc<tokio::sync::Mutex<_>>` for the drain task. PRE5b7
  promotes the wrap to the daemon level" with a "Concurrency:"
  paragraph that enumerates the four `&mut self` methods, names the
  sketched interior-mutable refactor (RwLock over the `HashMap`s +
  atomics for stats), points at [PLAN.md §Phase 4](PLAN.md) for the
  spec'd `&self` shape, and lists the wrap consumers (drain task,
  Phase 5 `DecoderSink`, oracle). The "cheap enough today, lock-free
  refactor lands when the hot path exists" rationale is preserved
  inline so future readers don't reopen the question on speculation.

## Tests

* `cargo test --lib`: 85 passed (unchanged from PRE5b6 baseline). The
  wrap is daemon-side plumbing — no new library-level surface, no new
  lib tests.
* `cargo test --tests`: 27 passed (was 26, +1):
  * `tests/shadow_catalog::arc_mutex_catalog_serialises_relation_at_across_tasks`
    spins a fresh shadow PG on port 55608, applies a one-table schema
    dump (`wc.things`), opens a real catalog via `open_catalog`,
    wraps it in `Arc<tokio::sync::Mutex<_>>`, and joins two tokio
    tasks:
    * **Task A** acquires the mutex first, runs `relation_at` for
      `pg_class`, sleeps 50 ms with the guard held, then runs a
      second `relation_at` on the same rfn (cache hit) — proves the
      guard survives across an await without spurious panic.
    * **Task B** yields once so A wins the lock, then acquires and
      runs `relation_at` for `wc.things`. Must wait for A's guard to
      drop before its query reaches PG.
    The test joins both with `tokio::join!`, asserts both descriptors
    surface with distinct oids, bounds the total elapsed at 5 s
    (any hang surfaces here), and finally re-acquires the mutex to
    check `cached() >= 2` and `stats().reconnects == 0`. Sanity-
    checks the wrap shape, not a lock-free path that doesn't exist
    yet.
* `cargo fmt --all -- --check`: clean (one auto-format pass on the
  daemon binary collapsed a long-form `with_transient_retry` call to
  the inline-closure shape; matched `cargo fmt`'s preference).
* `cargo clippy --all-targets -- -D warnings`: clean.

Total post-PRE5b7: 85 lib + 27 integration = 112 passing (was 111).

## Deviations from plan

* **Daemon args added, not assumed.** The plan said "Daemon binary
  holds `Arc<Mutex<ShadowCatalog>>`. Pass clones to every component
  that touches the cache." but didn't address the fact that `bin/stream.rs`
  pre-PRE5b7 didn't construct a `ShadowCatalog` at all — there was no
  shadow connection info on the daemon's CLI. PRE5b7 adds five
  `--shadow-*` clap args (`--shadow-socket-dir` required,
  `--shadow-port`/`--shadow-user`/`--shadow-dbname` defaulted to
  `5432`/`postgres`/`postgres`, `--shadow-connect-timeout` defaulted
  to `30 s`). The `socket_dir` is required because there's no sane
  default — every operator's path differs. The other three default to
  match the source-side defaults already on the binary, mirroring the
  symmetry the operator would write anyway.
* **`with_transient_retry` wraps the initial connect.** Plan didn't
  prescribe how the daemon recovers from a still-warming shadow at
  boot. The pattern is well-established in [PHASE4b](PHASE4b.md):
  reuse `with_transient_retry` over `ShadowCatalog::connect` with
  the configured backoff knobs. Costs one extra dependency on the
  catalog's transient-retry surface from the binary, which was already
  public for the same reason in test code.
* **Drain swap is part of PRE5b7, not a follow-up.** The plan focused
  on the wrap shape and didn't explicitly list "replace stub drain
  with `spawn_invalidation_drain`". But the wrap only exists to give
  the drain task something to call into — leaving the stub in place
  would have left the catalog dangling, which would defeat the point
  and would re-open PRE5b exit criterion 8. The drain swap is one
  line and ships in the same commit.
* **Catalog config knobs reused, not re-exposed.** Plan didn't address
  whether the daemon should re-expose `replay_poll` / `replay_timeout`
  / `max_entries` as CLI args. PRE5b7 uses `ShadowCatalogConfig::default()`
  for the daemon's catalog and exposes only `--shadow-connect-timeout`
  as a knob (the one operators are most likely to need for slow-start
  recovery). The rest can be plumbed through if a workload demonstrates
  it; defaulting now keeps the surface tight.
* **Cross-task test in `tests/shadow_catalog.rs`, not a unit test in
  `src/shadow_catalog.rs`.** Plan said "Add to `tests/shadow_catalog.rs`",
  which is what landed. Noting it as deliberate parallel to
  [PRE5b6](PRE5b6.md)'s deviation discussion: unit-level mutex sanity
  could fake a `ShadowCatalog` (private fields, doesn't compose), so
  the integration test against live shadow PG is the right shape.

## Implementation notes for follow-on work

`spawn_invalidation_drain(catalog.clone(), invalidation_rx)` returns a
detached `JoinHandle<()>` that the daemon binds to
`_invalidation_drain` (underscore-prefix so clippy doesn't complain).
The handle isn't joined on shutdown today — PRE5b9 will own the
clean-shutdown sequence (`tokio::select!` on `ctrl_c`, partial-segment
flush, drain join). The current detached form survives the daemon's
process exit cleanly because tokio's runtime drops the handle and the
underlying task at runtime shutdown.

Phase 5's `DecoderSink` should take `Arc<Mutex<ShadowCatalog>>` by
construction, store the clone, and run

```rust
let mut guard = self.catalog.lock().await;
let desc = guard.relation_at(record.rfn, record.commit_lsn).await?;
```

per heap record. The `await` inside the lock is fine because the
catalog's internal queries are async tokio-postgres calls — the lock
remains held across the await but doesn't block the runtime worker
(tokio's `Mutex` is async). When the lock-free refactor lands,
`DecoderSink` swaps `lock().await` for nothing and the call site stops
needing `mut`.

The interior-mutable refactor (deferred):

* `by_filenode` / `by_oid` → `RwLock<HashMap<_, _>>` or
  `dashmap::DashMap`. Reads on the hit path go through a read guard;
  miss path takes the write guard for `insert` only.
* `generation` → `AtomicU64`. The wrapping-add stays correct under
  `fetch_add(_, Ordering::Relaxed)`.
* `last_replay_lsn` → `AtomicU64` (with `None` represented as `0` and a
  flag, or a separate `AtomicBool` for the populated bit). The
  monotone-max update needs a CAS loop.
* `stats` → individual `AtomicU64`s under `ShadowCatalogStats` (or a
  parallel `AtomicShadowCatalogStats` so `stats()` keeps returning a
  `Snapshot`).
* `client` → would need `Mutex<Client>` or a per-query-checkout pool
  unless tokio-postgres exposes `Client` as `&self`-callable, which it
  does for the `query*` helpers — verify before assuming.
* `reconnect()` → still needs `&mut self` semantically because it
  swaps `self.client`; either keep it `&mut self` and document that
  callers must coordinate, or move the client into its own
  `Mutex<Client>` and let `reconnect` take that mutex internally.

None of the above is needed before a measured contention problem on
the lookup path.

## Files actually changed

```
src/bin/stream.rs                  +69 / -10  (--shadow-* args,
                                              ShadowCatalog connect via
                                              with_transient_retry,
                                              Arc<Mutex<_>> wrap,
                                              spawn_invalidation_drain)
src/shadow_catalog.rs              +16 / -3   (module doc Concurrency:
                                              paragraph, &mut self
                                              reality + deferred refactor)
tests/shadow_catalog.rs            +107 / -0  (cross-task mutex sanity
                                              integration test)
plans/PRE5b7.md                    rewritten (this retrospective)
```

No new runtime crates; no dev-dep additions; no public API surface
changes on `ShadowCatalog`.
