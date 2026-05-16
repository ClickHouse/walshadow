# PHASE4b — restart resilience

Closes [Phase 4b of `PLAN.md`](PLAN.md#phase-4b--restart-resilience). Extends `walshadow::shadow_catalog` so a
shadow PG bounce (operator-initiated `pg_ctl restart`, OOM kill that
systemd recovers from, kernel signal) no longer wedges the daemon. The
catalog's libpq client now transparently rebuilds on closed-connection
errors and bumps generation so any cache entries are treated stale on
next access; a top-level `with_transient_retry` helper layers
exponential backoff for the "PG is still starting up" window.

## What landed

| item | files | tests |
|---|---|---|
| `conninfo` stash on `ShadowCatalog` | `src/shadow_catalog.rs` | indirect via reconnect tests |
| `ShadowCatalog::reconnect` (rebuild client, bump generation, reset `last_replay_lsn`) | `src/shadow_catalog.rs` | `catalog_reconnects_after_pg_restart` |
| `ShadowCatalog::ensure_open` (pre-flight `is_closed()` check) | `src/shadow_catalog.rs` | covered alongside reconnect |
| `query_one_retry` / `query_opt_retry` / `query_retry` (one-shot retry on closed-conn error) | `src/shadow_catalog.rs` | `catalog_reconnects_after_pg_restart` |
| All `client.query*` call sites routed through retry helpers | `src/shadow_catalog.rs` | every existing integration test |
| `ShadowCatalogStats.reconnects` counter | `src/shadow_catalog.rs` | `catalog_reconnects_after_pg_restart` |
| `ShadowCatalogConfig.reconnect_backoff_initial` / `reconnect_backoff_max` knobs | `src/shadow_catalog.rs` | `config_default_is_sane` |
| `with_transient_retry` free function (exponential backoff, capped at caller-supplied timeout) | `src/shadow_catalog.rs` | `with_transient_retry_*` (unit + integration) |
| `is_transient` classifier (matches `CatalogError::Pg(_)` only) | `src/shadow_catalog.rs` | `is_transient_classifies_known_variants` |
| `catalog_reconnects_after_pg_restart` integration scenario | `tests/shadow_catalog.rs` | new |
| `with_transient_retry_outlasts_a_pg_restart` integration scenario | `tests/shadow_catalog.rs` | new |

No new runtime dependencies. `AsyncFnMut` (stable in Rust 1.85, native
to edition 2024) drives the closure shape for `with_transient_retry`,
so callers can write `async move || { cat.relation_at(...).await }`
without boxing.

## Design decisions

### Auto-reconnect inside `ShadowCatalog`, retry policy outside

`ShadowCatalog::reconnect` rebuilds the libpq client one shot. It does
not loop and does not back off — a single failed reconnect propagates a
`CatalogError::Pg(_)` to the caller. The exponential-backoff retry loop
lives in `with_transient_retry`, a free function the caller wraps
around a `ShadowCatalog` operation.

The split keeps the cache's bookkeeping (generation counter,
`last_replay_lsn`, hit/miss stats) unaware of in-flight retries. Each
retry attempt starts from a clean slate as far as the cache is
concerned: either the inner call succeeds and the catalog observes one
fetch, or it fails and the catalog observes one error. The retry loop
runs entirely above the catalog surface.

The alternative — burying the backoff loop inside `relation_at` and
friends — bloats the cache module with timekeeping, makes the retry
policy harder to vary per call site, and tangles `replay_timeout`
semantics (is it "how long to wait for replay catch-up" or "how long to
wait for PG to come back"? They should be different knobs at the right
abstraction level).

### Retry-once-on-close inside query helpers

Each of `query_one_retry`, `query_opt_retry`, `query_retry`:

1. `ensure_open().await?` — if the client is already known closed,
   reconnect synchronously before the query.
2. Run the query.
3. On error, check `client.is_closed()`. If true, the connection died
   mid-call (most common: server-side close races the pre-flight
   check); reconnect and retry the query exactly once.

The single retry is deliberate. If reconnect succeeds but the second
attempt still fails with a closed connection, that's a flapping
postmaster — the right response is to let the error propagate up to
`with_transient_retry`, which has the wall-clock budget and backoff to
handle it.

### `is_transient` matches every `Pg(_)` variant

The classifier could split `Pg(_)` finely:
`tokio_postgres::Error::is_closed`, SQLSTATE `57P03`
(`CANNOT_CONNECT_NOW`), `io::ErrorKind::ConnectionRefused` in the
source chain, etc. Phase 4b picks the conservative "every `Pg(_)` is
worth retrying within the budget" line:

* Steady-state SQL against the known-good queries we ship never fails
  for non-transient reasons (no untrusted input, no DDL paths in this
  module). A real auth/syntax error would be a code bug, and retrying
  it briefly until the wall-clock cap fires costs nothing in practice.
* Splitting transient vs non-transient correctly needs a small library
  of error-shape predicates that drift with tokio-postgres versions.
  Carry that complexity only if a workload measures the cost.

`CatalogError::{Parse, NotFoundByFilenode, NotFoundByOid,
ReplayTimeout}` all stay non-transient: each indicates a definite
outcome the caller needs to see, not a retry-worthy blip.

### Generation bump and `last_replay_lsn` reset on reconnect

The upstream catalog tracker (Phase 5/7 glue) is what normally calls
`ShadowCatalog::invalidate`. If shadow PG is unreachable when a commit
lands catalog writes, the tracker can't issue an `invalidate` — the
process boundary makes the cache and tracker independent. After the
client reconnects, the cache might be sitting on entries that look
fine (generation matches) but reflect a pre-bounce world.

Bumping generation inside `reconnect()` makes every entry stale
unconditionally. Catalog reads after reconnect re-fetch lazily on
first access, which is the same lazy-eviction path that any normal
`invalidate()` exercises. No extra plumbing needed downstream.

`last_replay_lsn = None` is the matching reset on the replay-LSN
shortcut: the monotone tracking in `wait_for_replay` is a per-instance
high-water mark, and a freshly restarted standby starts over (or, for
a normal-mode cluster, the value goes back to NULL entirely). Carrying
the pre-bounce value would cause `wait_for_replay(seen + 1)` to
short-circuit on a stale observation.

### Reuse `replay_timeout` as the retry wall-clock

`with_transient_retry` takes `timeout` as an explicit arg, but the
intended call shape is `with_transient_retry(cfg.replay_timeout, …)`.
Conceptually both bound "how long are we willing to wait for shadow PG
to be usable for this operation". Splitting them into two knobs would
force operators to keep two related values in sync and would add no
observability. Document the convention; if a workload ever needs to
distinguish (e.g., long replay catch-up + fast bounce recovery), split
the field then.

### `AsyncFnMut` closure bound

`with_transient_retry<R, F: AsyncFnMut() -> Result<R>>` is the cleanest
bound on edition 2024 / Rust 1.85+: callers write `async move ||` and
the compiler models per-call re-borrow without boxing. The older
`FnMut() -> Fut where Fut: Future` form pushes captures of `&mut
ShadowCatalog` into Pin-Box workarounds at every call site. Edition
2024 is already the floor (set in `Cargo.toml`), so the modern bound
costs nothing.

### Sync `Shadow` probe path left alone

`Shadow::psql_one` and friends in `src/shadow.rs` shell out to `psql`
per call. Each invocation establishes a fresh libpq connection;
existing error propagation on "connection refused" or "server closed
the connection" is already correct (one failed probe, one fresh
attempt on the next call). Re-running an old probe with retry would
require teaching `psql` error-classification logic that is not worth
having. Phase 7's daemon, which orchestrates probes at human cadence,
owns retry at its own layer.

## Deviations from [PLAN.md Phase 4b](PLAN.md#phase-4b--restart-resilience)

* [PLAN.md](PLAN.md#phase-4b--restart-resilience) scope bullet says
  "Generation bump on every successful reconnect"; the implementation
  also bumps `generation_bumps` in the stats counter so the bump is
  observable through the same surface as explicit `invalidate()`
  calls. [PLAN.md](PLAN.md#phase-4b--restart-resilience) is silent on
  which counter reflects the bump; treating the implicit bump and
  the explicit bump uniformly is simpler than splitting them.
* [PLAN.md](PLAN.md#phase-4b--restart-resilience) mentioned exponential-backoff defaults capped at
  `replay_timeout` but didn't pin the values. Phase 4b picks 100 ms
  initial / 1 s ceiling — fast enough to catch a sub-second restart
  without spinning, slow enough not to hammer a fully-down PG. Both
  knobs are on `ShadowCatalogConfig` for tests that need tighter
  values.

## What didn't get done

* **Shadow PG process supervision.** Out of scope per
  [PLAN.md Phase 4b](PLAN.md#phase-4b--restart-resilience):
  production deploys run shadow PG under systemd, which owns crash
  recovery; walshadow does not babysit the postmaster.
* **Fine-grained transient classification.** `is_transient` is a
  one-liner over `CatalogError::Pg(_)`. SQLSTATE 57P03 (`startup`),
  io-shape connect refused, etc. all fall under the same bucket. A
  finer scheme would be a follow-up only if a workload demonstrates
  spurious retries against non-transient failures.
* **Cancel-safety review of reconnect mid-flight.** If a caller drops
  the `relation_at` future after `reconnect()` swapped `self.client`
  but before the retry query completes, the new client survives in
  `self`, the future is dropped, and the next call observes the fresh
  client — fine. Not formally verified by a test that cancels at every
  await point; revisit if a fuzzer or shutdown-path lands.
* **Bounded reconnect attempts inside `query_*_retry`.** Single retry
  only, matching the "transient blip" model. Pathological flapping
  (PG accepting then immediately closing) would surface as a query
  error the second time, propagate to `with_transient_retry`, and
  backoff there.
* **Statement-cache invalidation on reconnect.** `tokio_postgres`
  caches prepared statements per-`Client`. Each `reconnect()` replaces
  the client, so the cache is implicitly wiped — no leak. Documented
  in case a future move to a shared statement cache crosses this
  boundary.

## Test counts

* `cargo test --lib`: 48 passed (was 45; +3 = `is_transient_classifies_known_variants`,
  `with_transient_retry_returns_immediately_on_success`,
  `with_transient_retry_fails_fast_on_non_transient`).
* `cargo test --tests`: 14 passed (2 classify fixture + 3 filter
  round-trip + 3 shadow lifecycle + 6 shadow catalog including 2 new:
  `catalog_reconnects_after_pg_restart`,
  `with_transient_retry_outlasts_a_pg_restart`).
* `cargo clippy --all-targets -- -D warnings`: clean.

Total: 62 passing (was 57 at end of Phase 4).

## Live-cluster observations

`tests/shadow_catalog.rs` against local PG 18.4 (Arch Linux):

* `shadow.stop(); shadow.start();` between two `relation_at` calls
  reliably leaves the libpq client in the closed state by the time
  control returns — `is_closed()` was true in every observed run, so
  the `ensure_open` branch caught the close before the query was
  attempted.
* Reconnect end-to-end (one `tokio_postgres::connect` + driver spawn)
  completes well inside a single test-grade poll cycle once the
  postmaster is back accepting connections.
* `with_transient_retry_outlasts_a_pg_restart` stops PG, schedules a
  delayed restart from a background thread (~300 ms after the await
  starts), and observes the retry loop succeed within two-to-three
  backoff intervals. No spinning, no cache pollution, no orphaned
  driver tasks (driver task exits cleanly when its client side is
  dropped by the next `reconnect`).
* Using a different rfn for the post-restart lookup is essential to
  the test — a same-rfn second call returns the cached descriptor
  without touching the connection, masking the bounce entirely (which
  is the correct behavior for relfilenode lookups across a bounce,
  just not what we wanted to exercise).

## Files touched

```
walshadow/src/shadow_catalog.rs       +218 / -15  (reconnect plumbing,
                                                  retry helpers,
                                                  with_transient_retry,
                                                  unit tests)
walshadow/tests/shadow_catalog.rs     +135 / -1   (2 new integration
                                                  scenarios)
walshadow/PLAN.md                     status list entry for Phase 4b
walshadow/PHASE4b.md                  new (this doc)
```

No new runtime crates; no dev-dep additions.
