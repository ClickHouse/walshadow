# PHASE4 — shadow PG catalog cache

Closes [Phase 4 of `PLAN.md`](PLAN.md#phase-4--catalog-cache-integration). Lands `walshadow::shadow_catalog`: an
async libpq-style client (tokio-postgres) over shadow PG that turns
a `RelFileNode` observed in source WAL into a fully-described
`RelDescriptor`, with a generation-counter invalidation scheme and
a replay-LSN gate so reads always see catalog at least as fresh as
the decoder's read LSN.

## What landed

| item | files | tests |
|---|---|---|
| `ShadowCatalog` + `ShadowCatalogConfig` + `ShadowCatalogStats` | `src/shadow_catalog.rs` | unit + integration |
| `RelDescriptor` + `RelAttr` (decoder-shape) | `src/shadow_catalog.rs` | `catalog_relation_lookup_by_filenode`, `user_relation_lookup_and_invalidation` |
| `ShadowCatalog::connect` (key=value conninfo, spawns driver) | `src/shadow_catalog.rs` | all integration tests |
| `relation_at(rfn, at_lsn)` (replay-LSN gate + cache + fetch) | `src/shadow_catalog.rs` | `catalog_relation_lookup_by_filenode`, `user_relation_lookup_and_invalidation` |
| `relation_by_oid(oid)` (oid-only path) | `src/shadow_catalog.rs` | `user_relation_lookup_and_invalidation` |
| `wait_for_replay(target)` (poll loop with monotone tracking) | `src/shadow_catalog.rs` | `replay_lsn_gate_times_out_when_not_in_recovery` |
| `invalidate()` (generation bump) | `src/shadow_catalog.rs` | `user_relation_lookup_and_invalidation` |
| Cache: `HashMap<RelFileNode, CacheEntry>` + by-oid mirror, FIFO eviction at `max_entries` | `src/shadow_catalog.rs` | `user_relation_lookup_and_invalidation` |
| `pg_relation_filenode(oid)` resolution (mapped + non-mapped uniform) | `src/shadow_catalog.rs` | `catalog_relation_lookup_by_filenode` (pg_class is mapped) |
| `socket_conninfo` helper | `src/shadow_catalog.rs` | `socket_conninfo_includes_all_fields` |
| `CatalogError` (`Pg`, `NotFoundByFilenode`, `NotFoundByOid`, `ReplayTimeout`, `Parse`) | `src/shadow_catalog.rs` | `replay_lsn_gate_times_out_when_not_in_recovery`, `nonexistent_filenode_errors_not_found` |
| Integration scenarios against live shadow PG (4 tests) | `tests/shadow_catalog.rs` | new |

New runtime dep: `tokio-postgres = "0.7"`. New runtime feature in
`tokio`: `rt-multi-thread`, `macros`, `time`, `net`.

## Design decisions

### Async over sync

[PLAN.md](PLAN.md#decoder-catalog-interface) `relation_at` signature is `async fn`, the decoder
(walhouse, Phase 5+) is async, and a synchronous catalog client
inside an async decoder forces either a `block_in_place` shim or a
thread-pool detour every lookup. `tokio-postgres` is the canonical
choice, already in the dep tree transitively via `wal-rs`'s
`postgres-protocol`. `shadow.rs` stays sync because its operations
(initdb / pg_ctl / psql) are inherently subprocess-shaped and run
at human cadence; the catalog cache is hot-path and must compose
with async callers.

### `pg_relation_filenode(oid)` for filenode → oid

The naive query — `SELECT oid FROM pg_class WHERE relfilenode = $1`
— silently fails on mapped catalogs (`pg_class`, `pg_attribute`,
`pg_type`, `pg_proc`, the shared catalogs in `global/`) because
their `pg_class.relfilenode` field is 0; the real filenode lives
in `pg_filenode.map`. The PG-supplied `pg_relation_filenode(oid)`
function abstracts that: for mapped catalogs it consults the map,
for regular tables it reads `relfilenode`. One query handles both.

This matters because catalog WAL is dominated by writes against
those mapped catalogs — the decoder's first catalog lookups will
all hit this code path. Getting it wrong would have meant a
filter that ships catalog records to shadow but a catalog cache
that can't resolve them.

### Cache shape: HashMap by `(rfn, generation)` lazy eviction

[PLAN.md](PLAN.md#decoder-catalog-interface) spec: "caches keyed by `(rfn, generation)`". Implementation
keeps a `HashMap<RelFileNode, CacheEntry>` where each entry carries
its `generation`. On lookup: if `entry.generation == self.generation`,
hit. If not, miss → re-fetch and overwrite. Stale entries are
never explicitly purged — they get overwritten on next access, or
evicted via the FIFO cap when `max_entries` is hit.

Alternative — purging all stale entries at `invalidate()` time —
makes `invalidate()` an O(n) operation on cache size, which is
the wrong shape: a single catalog commit (one DDL) shouldn't pay
proportional to the number of cached relations. Lazy eviction
keeps `invalidate()` O(1) and amortises the work across actual
lookups, which are paying-for-themselves I/O anyway.

A mirror `HashMap<Oid, CacheEntry>` answers `relation_by_oid`
without a second SQL query when the filenode-keyed lookup
populated it. Both maps are evicted in lockstep when the FIFO cap
trims.

### `Arc<RelDescriptor>`, not `&RelDescriptor`

[PLAN.md](PLAN.md#decoder-catalog-interface) doc-block has `Result<&RelDescriptor, Error>`. Returning a
reference into a `HashMap` while allowing mutation of that map on
the next call requires either splitting the cache out behind a
`Mutex`/`RwLock` (and returning a guard, not a reference), or
returning `Arc<T>` so the caller holds an independent strong ref.
`Arc<RelDescriptor>` is the simpler API and pays roughly 16 bytes
+ atomic per lookup, negligible against the SQL round-trip cost
on the miss path. Hits are `Arc::clone` → two atomic ops.

### `at_lsn = 0` skips the replay gate

Two call-shapes overlap:

1. Decoder reading at LSN X: pass `at_lsn = X`. Gate ensures
   shadow has the catalog as of X.
2. Caller already proved freshness (e.g. by an immediately
   preceding `wait_for_replay` call, or context where any
   recently-observed catalog is sufficient): pass `at_lsn = 0`.

Same API, both call-sites covered without a second method. Tests
use the second shape because normal-mode shadow has `NULL` replay
LSN, and forcing the gate would just time out unnecessarily.

### Monotone tracking of `last_observed_replay`

`pg_last_wal_replay_lsn()` only ever advances. Caching the last
observation lets future `wait_for_replay(target)` calls return
immediately if `target ≤ last_observed`, avoiding a SQL round-trip
on the common decoder hot path where consecutive records have
increasing source LSNs. The cached value is updated on every
successful poll, taking the max so out-of-order observations
(shouldn't happen, but cheap to harden against) never regress.

Caveat: `wait_for_replay(0)` always polls — `0` is the "wait for
any observation" sentinel, distinct from the monotone shortcut
case. The check is `seen >= target && target != 0`.

### `socket_conninfo` helper

`tokio-postgres` parses libpq `key=value` strings. Constructing
that string is unergonomic at call sites (`format!`
boilerplate). The helper lives in `shadow_catalog` rather than
`shadow` because callers of the catalog cache need it; `shadow`
itself shells out to `psql` with `-h /sock -p NNNN` flags and
doesn't construct conninfo strings.

### `relation_by_oid` resolves filenode via `current_database()`

The by-oid path needs to populate `RelDescriptor.rfn` even though
oid is what was given. Two of the three `rfn` fields come from
`pg_class` directly (`reltablespace`, `pg_relation_filenode(oid)`).
The third — `db_node` — is the oid of the current database, which
we look up once. The look-up could be cached cluster-wide; deferred
because it's one extra query at the cost of zero state, and the
by-oid path is the cold path (the hot path is `relation_at`).

### Single-database scope

A `ShadowCatalog` instance is bound to one PG database. Shadow PG
is typically populated to mirror a single source-PG database, so
this matches reality. Cross-database support would require either
a `ShadowCatalog` per database (caller-managed) or a switching
`SET search_path` / connection-per-db pool. [PLAN.md](PLAN.md) doesn't
require multi-db; if it ever does, the natural extension is a
`ShadowCatalogSet` keyed by `db_node` holding one
`ShadowCatalog` per dataset.

### Casting to `text` for single-char columns

`pg_class.relkind`, `pg_class.relpersistence`, `pg_type.typalign`,
`pg_type.typstorage` are PG `"char"` (one-byte) type. `tokio-postgres`
maps `"char"` to `i8`, which then needs `as u8 as char` conversion —
clumsy and easy to get wrong. Casting to `text` in SQL yields a
proper UTF-8 string; `one_char(...)` parses out the single character
with explicit error on the empty-or-multi-char path. Tradeoff: one
extra in-PG conversion per row, but each row produces one DDL-worth
of catalog data — irrelevant against the actual catalog-fetch cost.

## Deviations from [PLAN.md Phase 4](PLAN.md#phase-4--catalog-cache-integration)

* [PLAN.md](PLAN.md#phase-4--catalog-cache-integration) says
  "Lift `pg/catalog.rs` from pgchcdc". `~/s/walhouse/pgchcdc` is
  empty (only a directory, no contents) on this checkout — pgchcdc
  has either been removed or never lived in tree. Phase 4 was
  written from [PLAN.md's interface spec](PLAN.md#decoder-catalog-interface)
  only, with no pgchcdc cross-reference. If pgchcdc's `catalog.rs`
  ever re-surfaces, reconciling field names and lookup shapes is
  mechanical; walhouse and walshadow will share the catalog
  interface from here.
* No "promote-once-and-pg_dump-from-shadow" fallback.
  [PLAN.md](PLAN.md) doesn't require it; `apply_schema_dump`
  ([Phase 3](PLAN.md#phase-3--shadow-pg-lifecycle)) is the
  bootstrap-side primitive.
* `RelDescriptor.attributes` includes dropped columns
  (`attisdropped = true`). [PLAN.md](PLAN.md) doesn't specify;
  dropped columns are required for binary heap-tuple decoding to
  walk the null bitmap correctly. Decoder filters at use-site.

## What didn't get done

* **Connection retry.** `ShadowCatalog::connect` returns an error
  on first connection failure with no retry. Phase 7 daemon owns
  retry / reconnect on transient PG bounces.
* **Statement preparation cache.** Every `relation_at` miss
  prepares the SQL afresh. `tokio-postgres` does cache prepared
  statements implicitly, so this is more about the conceptual gap
  than a measured win; revisit when a workload measures it.
* **Per-relation invalidation.** [PLAN.md "Risks"](PLAN.md#risks--open-questions): "Bumping a
  single generation counter on any catalog write over-invalidates.
  A finer scheme (per-relation invalidation keyed on which catalog
  row was touched) is possible but parses every catalog write —
  defer until a workload makes it matter." Honoured.
* **Cross-database support.** Single-DB only. See Design above.
* **Async cancel-safety audit.** `relation_at` is composed of
  `await` points; if a caller cancels mid-flight after the replay
  poll but before the fetch completes, the cache stays in its
  prior state (no half-inserted entries). Re-checked but not
  formally tested.
* **Stats persistence.** `ShadowCatalogStats` is in-process only.
  Phase 7 metrics exporter scrapes it.
* **`pg_index` join.** [PLAN.md](PLAN.md#decoder-catalog-interface) mentions
  "pg_class/pg_attribute/pg_type/pg_index for rfn". Phase 4 joins
  the first three; `pg_index` (key column ordering, AM-specific
  metadata) is only needed when the decoder reads catalog *indexes*
  to drive lookups, which it doesn't — heap-tuple decoding works
  off `pg_attribute` alone. Add when a downstream consumer asks
  for it.

## Test counts

* `cargo test --lib`: 45 passed (was 41; +4 = `one_char_accepts_single`,
  `one_char_rejects_multi_or_empty`, `socket_conninfo_includes_all_fields`,
  `config_default_is_sane`).
* `cargo test --tests`: 12 passed (2 classify fixture + 3 filter
  round-trip + 3 shadow lifecycle + 4 new shadow catalog).
* `cargo clippy --all-targets -- -D warnings`: clean.

Total: 57 passing (was 49 at end of Phase 3).

## Live-cluster observations

`tests/shadow_catalog.rs` against local PG 18.4 (Arch Linux):

* `ShadowCatalog::connect` over unix socket completes within a
  poll cycle — driver spawn + handshake is faster than the test's
  `Duration::from_millis(20)` poll interval.
* First-lookup miss against `pg_class` (mapped catalog) returns
  with a populated `attributes` vector covering all 33 columns
  of PG 18's `pg_class`. Re-fetch on cache hit takes nanoseconds.
* `pg_relation_filenode('pg_class'::regclass)` returns a value
  that differs across initdb runs (mapped catalogs are renumbered
  each time); tests compute the filenode at runtime rather than
  hard-coding any number.
* `wait_for_replay(0x0100_0000)` against a normal-mode (non-recovery)
  cluster polls `pg_last_wal_replay_lsn() = NULL` repeatedly until
  the 300 ms test timeout — no spinning, no busy-loop, exact
  `ReplayTimeout` error type returned.
* Bogus filenode (`rel_node = 99_999_999`) returns
  `NotFoundByFilenode` on the first miss; no errant cache
  insertion.
* `invalidate()` → second `relation_at` call observably increments
  the `fetches` counter by 1, confirming generation-keyed staleness.

## Files touched

```
walshadow/Cargo.toml                       + tokio, + tokio-postgres
walshadow/src/lib.rs                       declare `shadow_catalog`
walshadow/src/shadow_catalog.rs            new — Phase 4 module
walshadow/tests/shadow_catalog.rs          new — integration tests
walshadow/PLAN.md                          status list + roadmap line
walshadow/PHASE4.md                        new (this doc)
```

LOC: 518 `src/shadow_catalog.rs`, 280 `tests/shadow_catalog.rs`.
Two new runtime crates (`tokio`, `tokio-postgres`); no new
dev-dep additions.
