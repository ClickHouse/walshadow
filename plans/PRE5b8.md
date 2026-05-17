# PRE5b8 — `RelDescriptor` `relreplident` + `pg_index`

[PRE5b](PRE5b.md) item S4. Foundation item, no dependency on the
other S-items.

## Why

[Phase 5](PLAN.md#phase-5--heap-tuple-decoder--tier-12-type-matrix)
needs `pg_class.relreplident` to decide whether
`XLH_UPDATE_CONTAINS_OLD_TUPLE` / `XLH_UPDATE_CONTAINS_OLD_KEY` are
expected on UPDATE/DELETE and how to interpret the old-tuple payload.
[PRE5.md:299-301](PRE5.md) deferred `pg_index` join with "not on the
decoder's hot path", which is true for *value* decoding, false for
*identity* decoding under `REPLICA IDENTITY USING INDEX`. First
non-FULL table trips this.

## Implementation

```rust
pub enum ReplIdent {
    Default,
    Nothing,
    Full,
    UsingIndex { index_oid: u32, key_attnums: Vec<i16> },
}

pub struct RelDescriptor {
    /* existing fields */
    pub replident: ReplIdent,
}
```

* Extend `fetch_by_filenode` SQL (`shadow_catalog.rs:445`) to select
  `c.relreplident`.
* When `relreplident = 'i'`, second query against `pg_index` filtered
  by `indrelid = $relation_oid AND indisreplident = true`; pull
  `indexrelid` and `indkey`. Cache alongside `RelDescriptor`.
* Other values map to `Default` / `Nothing` / `Full`. `n` (nothing) is
  legal on user tables; decoder must surface it to Phase 5 so the
  emitter can drop UPDATE/DELETE rows that would have no key.

## Tests

* Live: create three tables with `REPLICA IDENTITY DEFAULT`, `FULL`,
  `USING INDEX <name>`. Fetch each, assert the enum variant matches.
* For `UsingIndex`, assert `key_attnums` matches the index's column
  list.

## Exit criteria

1. `cargo test --lib && cargo test --tests` clean, including the
   three-table replident matrix.
2. `cargo clippy --all-targets -- -D warnings` clean.
3. `RelDescriptor` carries `replident` and, for `UsingIndex`, the
   replica-identity index attribute set.

## Files expected to change

```
src/shadow_catalog.rs              ReplIdent enum; relreplident +
                                   pg_index join in fetch_by_filenode
tests/shadow_catalog.rs            replident matrix scenario
plans/PRE5b8.md                    this doc
```
