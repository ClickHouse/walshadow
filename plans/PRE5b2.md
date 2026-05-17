# PRE5b2 — wire `seed_from_source` in `walshadow-stream`

[PRE5b](PRE5b.md) item B2. Closes the pre-attach mapped-catalog-
rotation hole that PRE5 item 3 only addressed in tests.

## Why

`CatalogTracker::seed_from_source` (`src/catalog_tracker.rs:205`) is
called only from `tests/catalog_seed.rs:108,184,220`.
`src/bin/stream.rs:146` issues `START_REPLICATION` without seeding.
PRE5 item 3's whole purpose, closing the pre-attach mapped-catalog-
rotation hole, does nothing in production.

## Implementation

* `SourceFeed` exposes a sidecar libpq `tokio_postgres::Client` for
  the same `PgConfig` minus `replication=true`. Replication-mode
  connections can't run `pg_class` queries cleanly; a second
  connection is the cheapest correct path. Opened lazily on first
  `seed_from_source` call.
* `walshadow-stream` calls `tracker.seed_from_source(&sql_client)`
  after `IDENTIFY_SYSTEM`, before `START_REPLICATION`.
* Snapshot consistency: `IDENTIFY_SYSTEM` does not expose a snapshot.
  The seed query runs against the source's current catalog. If a
  rotation finalized before the seed, the seed already covered it. If
  a `XLOG_RELMAP_UPDATE` fires between seed and replication-start, the
  WAL stream re-adds it. No special coordination needed beyond
  ordering seed-then-START_REPLICATION.
* `--start-lsn` users still seed (idempotent on `HashSet`).

## Tests

* Strengthen `tests/catalog_seed.rs:144-190`: loop
  `VACUUM FULL pg_class` until `pg_relation_filenode(1259) >= 16384`.
  Today's test silently passes when the post-rewrite filenode stays
  low.
* New integration: `walshadow-stream --max-segments=1` against a
  source whose pg_class was rotated above 16384 pre-attach. Assert no
  records targeting the rotated filenode appear as `Decision::Drop`
  in the manifest.

## Exit criteria

1. `cargo test --lib && cargo test --tests` clean, including
   strengthened seed test and new daemon integration.
2. `cargo clippy --all-targets -- -D warnings` clean.
3. `walshadow-stream` against a source that had
   `VACUUM FULL pg_class` pre-attach produces filtered output
   indistinguishable (per manifest stats) from a daemon attached to
   a fresh cluster doing the same workload.

## Files expected to change

```
src/source_feed.rs                 expose sql_client() for seed_from_source
src/bin/stream.rs                  call seed_from_source after IDENTIFY_SYSTEM
tests/catalog_seed.rs              force pg_class filenode >= 16384 before assert
tests/walshadow_stream_e2e.rs      new — pre-rotated pg_class integration
plans/PRE5b2.md                    this doc
```
