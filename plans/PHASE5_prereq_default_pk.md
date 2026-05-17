# PHASE5 prereq — `ReplIdent::Default` PK attnums

Extends `ReplIdent::Default` from a unit variant to
`Default { pk_attnums: Option<Vec<i16>> }`, resolved at descriptor
build via `pg_index.indkey WHERE indisprimary = true`. Unblocks Phase
5's decoder reading `XLH_UPDATE_CONTAINS_OLD_KEY` under
`relreplident = 'd'` without a second catalog round-trip per
UPDATE/DELETE

## Enum change

Before:

```rust
pub enum ReplIdent {
    Default,
    Nothing,
    Full,
    UsingIndex { index_oid: Oid, key_attnums: Vec<i16> },
}
```

After:

```rust
pub enum ReplIdent {
    Default { pk_attnums: Option<Vec<i16>> },
    Nothing,
    Full,
    UsingIndex { index_oid: Oid, key_attnums: Vec<i16> },
}
```

`pk_attnums = None` means table has no PK, decoder emits
`old = None` always (matches PLAN.md Phase 5 relreplident table)

## `fetch_replident` 'd' branch

Replaces bare `Ok(ReplIdent::Default)` with a `pg_index` lookup over
`query_opt_retry` (mirrors the `'i'` branch's retry-aware path):

```sql
SELECT indkey::int2[]
FROM pg_index
WHERE indrelid = $1 AND indisprimary = true
LIMIT 1
```

`indkey` is PG's internal `int2vector`, cast to `int2[]` so
tokio-postgres' `Kind::Array(int2)` decode lifts it into `Vec<i16>`
straight. Missing row → `pk_attnums: None`. `'i'` branch unchanged
(still filters on `indisreplident = true`, distinct from primary key
on tables that override)

## Tests

`tests/shadow_catalog.rs::replident_matrix_default_nothing_full_index`:

| Table | PK | Assertion |
|---|---|---|
| `wc.def_t` | `(id)` | `Default { pk_attnums: Some(vec![1]) }` |
| `wc.no_pk_t` | none | `Default { pk_attnums: None }` (new) |
| `wc.composite_pk_t` | `(k1, k2)` | `Default { pk_attnums: Some(vec![1, 2]) }` (new) |
| `wc.nothing_t` | n/a | `Nothing` (unchanged) |
| `wc.full_t` | n/a | `Full` (unchanged) |
| `wc.idx_t` | n/a | `UsingIndex { … vec![2, 3] }` (unchanged) |

## LOC delta

- `src/shadow_catalog.rs` +29 / -8
- `tests/shadow_catalog.rs` +28 / -6
- Total ~+57 / -14 net

Matches the spec's "~30 LOC fetch_replident lift" plus test expansion

## Build + test

- `cargo build --all-targets`: clean
- `cargo test --lib`: 103 passed, 0 failed
- `cargo test --test shadow_catalog --no-run`: compiles
- `cargo clippy --all-targets -- -D warnings`: clean

Compose check against the parallel async-sink + FPI prereqs landed in
the same working tree: all green

## Followups

- Integration test `replident_matrix_default_nothing_full_index`
  needs live shadow PG to run (initdb + `pg_ctl start`). Compiles
  cleanly under `--no-run`; runs under any environment with `initdb`
  on PATH
- Phase 5 decoder will consume `pk_attnums` directly off
  `RelDescriptor.replident`; no further catalog round-trip needed
  per UPDATE/DELETE
- `SmallVec<[i16; 1]>` for `pk_attnums` (and `[i16; 2]` for
  `UsingIndex.key_attnums`) considered, dropped: tokio-postgres
  `FromSql` only impls for `Vec<T>` on PG `int2[]`, so the resulting
  `Vec → SmallVec` conversion at every cache miss would negate the
  alloc-elision win. Revisit if walshadow ever switches to a manual
  array decode path
