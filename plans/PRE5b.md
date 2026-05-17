# PRE5b — close PRE5 silent-correctness gaps before Phase 5

[PRE5](PRE5.md) landed with `cargo test --lib && cargo test --tests`
clean (66 + 18 tests, 0 ignored) and `cargo clippy --all-targets -- -D
warnings` clean. The surface is fine. Four items beneath it did not
actually wire into the production path, plus a handful of foundation
gaps that
[Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix)
will hit on its first day.

The B-items are correctness regressions that compound silently: the
daemon emits a manifest, the filter stats look reasonable, the round-
trip tests still pass, but the catalog whitelist underneath has been
wiped or the cached shape is stale. The moment Phase 5's `DecoderSink`
consumes the resulting `Record` stream, the gaps surface as data
corruption against source PG, found late (Phase 8 DDL drill or Phase 9
oracle).

## Scope

Split into ten sequential sub-phases. Each lands as its own commit
with the tests defined in its plan file.

| sub-phase | category | rationale | plan |
|---|---|---|---|
| PRE5b1 | correctness | `CatalogTracker` state thrown away every 16 MiB | [PRE5b1.md](PRE5b1.md) |
| PRE5b2 | correctness | PRE5 item 3 exists in tests only | [PRE5b2.md](PRE5b2.md) |
| PRE5b3 | correctness | mis-parses every `VACUUM FULL pg_<non-mapped>` | [PRE5b3.md](PRE5b3.md) |
| PRE5b4 | correctness | descriptor cache silently stales on first DDL | [PRE5b4.md](PRE5b4.md) |
| PRE5b5 | foundation | event lacks `XLogRecord`, `main_data`, `rfn`, `xact_id` | [PRE5b5.md](PRE5b5.md) |
| PRE5b6 | foundation | one `RecordSink` per `WalStream::push` | [PRE5b6.md](PRE5b6.md) |
| PRE5b7 | foundation | spec'd `&self`, ships `&mut self` | [PRE5b7.md](PRE5b7.md) |
| PRE5b8 | foundation | mandatory for UPDATE/DELETE old-tuple decode | [PRE5b8.md](PRE5b8.md) |
| PRE5b9 | operational | unbounded `CollectingRecordSink`, no `close()` on signal | [PRE5b9.md](PRE5b9.md) |
| PRE5b10 | hygiene | `pub mod segment`, untested classes, missing fixtures | [PRE5b10.md](PRE5b10.md) |

## Sequencing

Sub-phases ship in numeric order. Hard dependencies:

* PRE5b5 must precede PRE5b6 (sink trait shape changes with `Record`).
* PRE5b9 depends on both PRE5b2 (`seed_from_source` wired into the
  daemon) and PRE5b5 (daemon pipeline uses `Record`).

Soft dependencies: PRE5b1–4 are independently shippable B-items.
PRE5b7 and PRE5b8 are independent foundation items.

Each sub-phase plan starts life as planning content in the commit
that does the work, then gets rewritten as a retrospective once that
commit lands. Format follows [PHASE4b.md](PHASE4b.md).

## Exit criteria

PRE5b closes when:

1. PRE5b1 through PRE5b10 each pass their per-phase exit criteria.
2. `cargo test --lib && cargo test --tests` clean on the merged state.
3. `cargo clippy --all-targets -- -D warnings` clean on the merged
   state.
4. `walshadow-stream` runs against a source that had
   `VACUUM FULL pg_class` pre-attach and produces filtered output
   indistinguishable (per manifest stats) from a daemon attached to
   a fresh cluster doing the same workload.
5. `tracker.pg_class_writes_undecoded` pinned at zero (or replaced by
   `pg_class_writes_oid_in_prefix`) on the
   `VACUUM FULL pg_depend` fixture.
6. `RelDescriptor` carries `relreplident` and, for `UsingIndex`, the
   replica-identity index attribute set.
7. `bin/stream.rs` no longer leaks `Record`s and writes a `.partial`
   segment on SIGINT.
8. `CatalogTracker` mutations on the catalog set produce
   `ShadowCatalog::invalidate` calls in production paths.

After PRE5b,
[Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix)
attaches its `DecoderSink` to `WalStream`, consumes `(Record,
Decision::Drop)` events for RM_HEAP* / RM_HEAP2* user records, queries
`ShadowCatalog::relation_at(rfn, commit_lsn)` for the per-relation
descriptor (including `relreplident` and dropped columns), and emits
`Tuple { rfn, xid, op, new, old }` per
[PLAN.md](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix).

## Out of scope

* General heap tuple decoder.
  [Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix).
* Per-record streaming sub-segment latency. Deferred to whichever
  later phase forces it.
* `ShadowCatalog` interior-mutable refactor (lock-free hit path).
  Tracked as follow-up to [PRE5b7](PRE5b7.md)'s `Arc<Mutex<_>>` wrap.
* Tier 1/2 type-matrix fixture.
  [Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix).
* Observability infra (tracing subscriber).
  [Phase 10](PLAN.md#phase-10--operational).
* `bin/stream.rs` --slot keepalive policy beyond what wal-rs already
  provides. [Phase 10](PLAN.md#phase-10--operational).
