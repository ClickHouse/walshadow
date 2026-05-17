# PRE5b8 — `RelDescriptor` `relreplident` + `pg_index` (retrospective)

[PRE5b](PRE5b.md) item S4. Fourth and final foundation change in
PRE5b; independent of [PRE5b5](PRE5b5.md) / [PRE5b6](PRE5b6.md) /
[PRE5b7](PRE5b7.md). Closes the
[PRE5.md:299-301](PRE5.md) `pg_index`-deferral that
[Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix)
would have hit on its first non-FULL user table.

## Why (preserved)

[Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix)
needs `pg_class.relreplident` to decide whether
`XLH_UPDATE_CONTAINS_OLD_TUPLE` / `XLH_UPDATE_CONTAINS_OLD_KEY` are
expected on UPDATE/DELETE and how to interpret the old-tuple payload.
[PRE5](PRE5.md) deferred the `pg_index` join with the rationale "not
on the decoder's hot path" — true for *value* decoding (the indkey
list is irrelevant to NEW-tuple parse), false for *identity* decoding
under `REPLICA IDENTITY USING INDEX`. The first non-FULL user table
trips it: the old-tuple payload carries the indexed columns in
`indkey` order, and the decoder needs both `indexrelid` (to resolve
the column types from `pg_attribute`) and the `indkey` list (to know
*which* attributes appear, and in what order) before it can match
bytes to fields.

## What landed

* **`ReplIdent` enum next to `RelDescriptor`.**
  `src/shadow_catalog.rs:97-118` defines the four-variant resolved
  form: `Default`, `Nothing`, `Full`, `UsingIndex { index_oid,
  key_attnums }`. The `UsingIndex` payload carries an `Oid` (matches
  the rest of the module — `pg_class.oid`, `pg_namespace.oid` etc.
  also use `Oid = u32`) and a `Vec<i16>` for the attnum list (matches
  `RelAttr.attnum`, also `i16`).  Each variant carries a doc comment
  naming the source-PG semantics the decoder will rely on. `'n'`
  appears as a real first-class variant because the body of the plan
  flagged that a NOTHING table needs to be surfaced to Phase 5 so the
  emitter can drop UPDATE/DELETE rows that would have no key —
  collapsing it into `Default` would have silently lost that signal.

* **`RelDescriptor.replident`.** `src/shadow_catalog.rs:90-96` adds
  the field alongside the existing `kind` and `persistence`. Field is
  `pub` like the rest of the struct; `#[derive(Clone, PartialEq)]`
  already at the struct level continues to apply because `ReplIdent`
  also derives them.

* **`fetch_by_filenode` and `fetch_by_oid` populate `replident`.**
  Both queries pick up `c.relreplident::text` in the same row that
  already pulls `relkind` / `relpersistence`. Single-char parse goes
  through the existing `one_char` helper. The fan-out to a follow-up
  query lives in a new `fetch_replident(c, rel_oid)` method
  (`src/shadow_catalog.rs:550-583`) which dispatches on the char:
  `'d' / 'n' / 'f'` return their respective variants without any
  extra round-trip; `'i'` runs one `query_opt_retry` against
  `pg_index` (`WHERE indrelid = $1 AND indisreplident = true LIMIT
  1`) to pull `indexrelid::oid` and `indkey::int2[]`. Casting
  `indkey` to `int2[]` instead of leaving it as the system-internal
  `int2vector` lets tokio-postgres take the standard `Kind::Array(int2)`
  decode path → `Vec<i16>` without a custom `FromSql` impl. The cast
  matches the module's existing convention of casting every selected
  column to a known surface type (`c.oid::oid`, `n.nspname::text` etc.).

* **Defensive `Parse` errors on relreplident inconsistency.**
  `relreplident = 'i'` with no matching `indisreplident = true` row in
  `pg_index` surfaces a `CatalogError::Parse` carrying the relation
  oid; an unknown char (anything other than `d/n/f/i`) surfaces a
  `Parse` naming the unexpected value. PG itself should not produce
  either, so these stay non-transient (won't retry under
  `with_transient_retry`) and bubble up to the caller as the catalog
  bug they would indicate. Both error paths sit inside `fetch_replident`,
  next to the queries they describe.

## Tests

* `cargo test --lib`: 85 passed (unchanged from PRE5b7 baseline). The
  enum + field is end-to-end PG-only — no fixture-free unit tests
  exist because no synthetic `RelDescriptor` shape outside live PG
  needs to be parsed.

* `cargo test --tests`: 28 passed (was 27, +1):
  * `tests/shadow_catalog::replident_matrix_default_nothing_full_index`
    spins a fresh shadow PG on port 55609, applies a schema dump
    creating four `wc.*` tables — one per `relreplident` variant:
    * `wc.def_t` with a `bigint PRIMARY KEY` → `ReplIdent::Default`.
    * `wc.nothing_t` followed by `ALTER TABLE … REPLICA IDENTITY
      NOTHING` → `ReplIdent::Nothing`.
    * `wc.full_t` followed by `ALTER TABLE … REPLICA IDENTITY FULL`
      → `ReplIdent::Full`.
    * `wc.idx_t` with a two-column `UNIQUE INDEX idx_t_keys (k1, k2)`
      on `NOT NULL` columns (the constraint REPLICA IDENTITY USING
      INDEX requires) followed by `ALTER TABLE … REPLICA IDENTITY
      USING INDEX idx_t_keys` → `ReplIdent::UsingIndex { index_oid,
      key_attnums: [2, 3] }`.
    Loop pattern for the simple three: lookup via
    `relation_at(rfn, 0)`, assert `desc.replident == expected`. The
    `UsingIndex` case pulls the descriptor in a separate block,
    pattern-matches the variant out, and asserts both the resolved
    `index_oid` (matched against psql's
    `'wc.idx_t_keys'::regclass::oid`) and the `key_attnums == [2, 3]`.
    Tying both sides — the variant *and* the attnum list — to the
    schema's known column positions catches "default-attnum-zero on a
    bad parse" as well as "wrong column order" failures.

* `cargo fmt --all -- --check`: clean.
* `cargo clippy --all-targets -- -D warnings`: clean.

Total post-PRE5b8: 85 lib + 28 integration = 113 passing (was 112).

## Deviations from plan

* **Fourth variant exercised: `Nothing`.** The plan's Tests section
  said "create three tables with `REPLICA IDENTITY DEFAULT`, `FULL`,
  `USING INDEX <name>`". The plan's Implementation section flagged
  `'n'` ("legal on user tables; decoder must surface it") so the
  variant has to exist either way; adding a fourth table to the test
  matrix exercises the `'n'` branch in `fetch_replident` for the cost
  of two extra DDL statements. The test loop handles `Default /
  Nothing / Full` uniformly so the addition is one line in the
  `cases` array and one CREATE TABLE + ALTER TABLE in the schema
  dump.

* **`fetch_replident` is a method, not inlined.** Plan said "second
  query against `pg_index` … pull `indexrelid` and `indkey`. Cache
  alongside `RelDescriptor`." but didn't prescribe the call shape.
  Putting the dispatch (char → variant, optional follow-up query) in
  a dedicated `fetch_replident` keeps `fetch_by_filenode` and
  `fetch_by_oid` parallel — both gain one line each to call it. The
  alternative (inline `match` blocks in both) would have duplicated
  the four-arm dispatch and the error-formatting strings.

* **`fetch_by_oid` updated symmetrically.** Plan named only
  `fetch_by_filenode:445` as the change site, but `fetch_by_oid`
  builds the same `RelDescriptor` from the same `pg_class` row shape
  and would have shipped without `replident` if left alone — every
  caller through `ShadowCatalog::relation_by_oid` would have failed
  to compile (the struct literal can't omit a non-`Option` field).
  Adding `c.relreplident::text` to its query and threading the same
  `fetch_replident` call into the construction is the only
  consistent shape. One extra column in one extra query; no new
  round-trips because the `'d' / 'n' / 'f'` cases short-circuit
  without touching `pg_index`.

* **`indkey` cast to `int2[]`, not left as `int2vector`.**
  Plan was silent on the wire format. `int2vector` decodes to
  `Vec<i16>` natively under tokio-postgres (its `Kind` is
  `Array(int2)`), but the rest of the module uses explicit casts on
  every selected column (`c.oid::oid`, `n.nspname::text`,
  `t.typstorage::text`). Casting `indkey::int2[]` keeps the SQL
  uniform; reading the vector raw works too but breaks the "every
  column has an explicit surface type" convention the module follows.

* **Defensive parse errors on unexpected values.** Plan didn't
  prescribe the error shape for "`relreplident = 'i'` but no
  matching `pg_index` row" or "unknown `relreplident` char". Picked
  `CatalogError::Parse` because:
  * Both indicate catalog corruption rather than transient PG state,
    so they should *not* trigger `with_transient_retry`'s backoff.
    `is_transient` is `matches!(_, Pg(_))` — `Parse` falls through
    and surfaces immediately.
  * The error messages name the offending oid / char so an operator
    diagnosing it gets the same info they would get from a manual
    `SELECT relreplident FROM pg_class WHERE oid = …` probe.
  An alternative would have been adding a dedicated `CatalogError`
  variant (`InvalidReplident { oid, ch }`), but that's a one-off
  branch with no expected callers — the broader `Parse` umbrella
  already absorbs the `one_char` failures and is the right granularity.

## Implementation notes for follow-on work

[Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix)'s
`DecoderSink` reads `desc.replident` once per heap record and
branches on the variant:

* `Default` — old-tuple payload carries primary-key columns (or
  nothing if the table is keyless). PK resolution still needs a
  one-time lookup against `pg_index` (`indisprimary = true`) on
  first use — this is *not* cached on `RelDescriptor` today because
  `Default` is the common case and the lookup is cheap once the
  table is hot. If the cost ever shows up in a profile, the same
  pattern as `UsingIndex` (eager pull into `RelDescriptor`) applies.

* `Nothing` — old-tuple payload is empty by spec. Emitter drops the
  UPDATE/DELETE row at the Phase 5 layer; downstream Tier 1/2
  contracts get nothing to emit.

* `Full` — old-tuple payload mirrors every non-dropped column.
  Decoder runs the same NEW-tuple parser against the OLD payload
  using `desc.attributes`.

* `UsingIndex { index_oid, key_attnums }` — old-tuple payload
  contains *only* the columns named in `key_attnums`, in that order.
  Decoder resolves each attnum to the matching `RelAttr` on the base
  relation (not the index — the indexed table's `pg_attribute` row,
  which the index's column refers to by attnum) and parses one
  field per attnum.

The `index_oid` is currently unused by Phase 5's planned shape — the
attnum list is sufficient because the decoder reads
`desc.attributes` directly (the base table's columns are what the
WAL payload encodes; the index relation's columns are derivative).
The oid is carried for symmetry with the rest of `RelDescriptor`
(every catalog reference is `(oid, name)`-shaped) and because a
later Phase 8 DDL drill or Phase 9 oracle may want to resolve the
index relation for cross-checking. Storing it costs four bytes per
`UsingIndex` descriptor; dropping it later would not break the
external API because the field is namedly destructurable.

## Files actually changed

```
src/shadow_catalog.rs              +77 / -3   (ReplIdent enum,
                                              replident field on
                                              RelDescriptor,
                                              fetch_replident helper,
                                              relreplident column in
                                              fetch_by_filenode and
                                              fetch_by_oid)
tests/shadow_catalog.rs            +98 / -2   (ReplIdent import,
                                              replident_matrix_default_
                                              nothing_full_index
                                              integration test)
plans/PRE5b8.md                    rewritten (this retrospective)
```

No new runtime crates; no dev-dep additions; no public API surface
changes on `ShadowCatalog` beyond the new `RelDescriptor.replident`
field and the new `ReplIdent` re-export.
