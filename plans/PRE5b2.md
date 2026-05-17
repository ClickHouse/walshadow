# PRE5b2 — wire `seed_from_source` in `walshadow-stream` (retrospective)

[PRE5b](PRE5b.md) item B2. Second of the four B-item correctness fixes;
no dependency on [PRE5b1](PRE5b1.md) or the others, independently
shipped.

## Why (preserved)

`CatalogTracker::seed_from_source` (`src/catalog_tracker.rs:205`) was
called only from `tests/catalog_seed.rs`. `src/bin/stream.rs` issued
`START_REPLICATION` without seeding. PRE5 item 3's whole purpose,
closing the pre-attach mapped-catalog-rotation hole, did nothing in
production: a long-running source that had run `VACUUM FULL pg_class`
before walshadow attached would emit heap writes against a
`>= 16384` filenode whose authoritative `XLOG_RELMAP_UPDATE` lived in
pre-attach WAL the stream never sees. The filter would classify
those records User, drop them, and silently corrupt shadow's catalog.

## What landed

* `SourceFeed` gained a sidecar libpq `tokio_postgres::Client`, opened
  lazily on first `sql_client()` call. Same `PgConfig` minus
  `replication=true`. `NoTls` to match the existing tokio-postgres usage
  in `shadow_catalog`. Connection persists for the life of the feed so
  `--start-lsn` resumes pay the connect cost once.
* `WalStream::filter_mut() -> &mut Filter` mirrors the existing
  `filter()`. Pre-stream tracker setup is the only intended caller; hot-
  path code keeps using `filter()`.
* `walshadow-stream` (`src/bin/stream.rs`) constructs `WalStream`,
  opens the sidecar client, calls `stream.filter_mut().tracker
  .seed_from_source(client)`, then issues `START_REPLICATION`. The
  daemon logs `seeded N catalog filenodes from source pg_class` so the
  operator can confirm the seed actually populated something. Order is
  load-bearing: seed must precede `START_REPLICATION` so the tracker is
  authoritative before the first record arrives.
* `tests/catalog_seed.rs:seed_closes_pre_attach_pg_class_rotation_hole`
  now loops `VACUUM FULL pg_class` until the filenode crosses 16384.
  Previously a single `VACUUM FULL` on a fresh cluster could leave the
  rotated filenode below 16384, where the bootstrap rule still catches
  it — silently rendering the unseeded-tracker assertion a tautology.
  The 200-iteration guard fires loudly if PG ever changes its filenode
  allocator. Empirically PG 18.4 crosses on the first or second pass.
* `tests/wal_stream_e2e.rs:pre_rotated_pg_class_seed_keeps_catalog_writes`
  new: drives the full daemon path. Rotates pg_class above 16384,
  forces `pg_switch_wal` to bury the rotation records, attaches with
  seed, runs DDL that updates pg_class on the rotated filenode, and
  asserts the tracker recognized the writes:
  * `tracker.is_catalog(db, pg_class_fn_after) == true` — seed
    actually populated the catalog set.
  * `pg_class_writes_decoded + pg_class_writes_undecoded > 0` — the
    decoder fired on writes targeting the rotated filenode, which is
    only possible if `pg_class_filenode[db]` was set by the seed.
  * `filter.stats.kept_user > 0` — User-classified records on the
    rotated filenode were promoted to Keep through the tracker. Without
    seed every one of these would land in `stats.dropped`.

## Tests

* `cargo test --lib`: 66 pass (unchanged).
* `cargo test --tests`: 20 integration tests pass (was 19; +1 new e2e).
  Includes the strengthened `seed_closes_pre_attach_pg_class_rotation_hole`
  and the new `pre_rotated_pg_class_seed_keeps_catalog_writes`.
* `cargo clippy --all-targets -- -D warnings` clean.

## Deviations from plan

* **Assertion shape on the new e2e.** Plan called for "no records
  targeting the rotated filenode appear as `Decision::Drop` in the
  manifest." The manifest sidecar
  (`src/manifest.rs:Entry`) carries `offset`/`len`/`rmid`/`info`/`kind`
  per record but not block refs, so block-targeting filtering on the
  manifest isn't directly possible. Substituted three tracker/filter
  assertions that together form a tighter regression signal: without
  the seed all three counters stay at zero (no `pg_class_filenode[db]`
  entry → `is_pg_class_relfilenode` falls through to `rel ==
  PG_CLASS_OID` which is 1259 ≠ rotated filenode → decoder never fires
  → tracker never adds the filenode → User records on it are dropped).
  Adding block refs to manifest entries was considered and rejected:
  it'd grow every entry by 16 bytes minimum and isn't needed by any
  consumer.
* **Test file location.** Plan named `tests/walshadow_stream_e2e.rs`.
  `tests/wal_stream_e2e.rs` already had the SourceFeed→WalStream
  scaffolding (`make_source`, `append_replication_conf`, `StopOnDrop`,
  `pg_available`); a separate file would have duplicated ~60 lines.
  Added the new test there.
* **`sql_client` lifetime.** Plan said "opened lazily on first
  `seed_from_source` call". Implemented as `SourceFeed::sql_client()
  -> Result<&Client>` returning a borrowed handle so the caller chooses
  what to do with it. The seed is the only consumer today; if a future
  caller needs the same sidecar (eg replica-identity introspection) it
  reuses the existing client without a reconnect.

## Implementation notes for follow-on work

`SourceFeed::sql_client()` does NOT propagate the source's TLS mode —
`NoTls` is hard-coded. If walshadow ever needs to seed across a TLS-
required source, plug a tokio-postgres tls connector. `shadow_catalog`
hits the same wall and resolves it the same way (NoTls) — when one
moves, both should.

Pre-rotation discovery uses `pg_relation_filenode(oid)` in a SQL
predicate `WHERE c.oid < 16384 AND pg_relation_filenode(c.oid) IS NOT
NULL`. Indexes (oid >= 16384 even for catalog indexes) are not seeded.
This is fine for the heap-write tracker today; if Phase 5's decoder
needs index filenodes from the seed, extend the query (drop the oid
bound, keep the `IS NOT NULL` to skip partitioned parents) and
generalize `pg_class_filenode` storage.

Order of operations in `bin/stream.rs` now goes
`connect → IDENTIFY_SYSTEM → WalStream::new → seed → START_REPLICATION
→ pump`. `WalStream::new` moving above `START_REPLICATION` was free
(no side effects) and was the cheapest way to give the seed somewhere
to write before the wal cursor opened.

The new e2e issues DDL via a thread spawning `psql` (mirroring
`full_pipeline_source_to_filtered_segments_on_disk` from PRE5).
`tokio_postgres::Client` driven through the existing runtime would be
cleaner but introduces ordering with the replication-conn pump in the
same task; the thread + psql path keeps the test single-threaded on
the pumping side.

## Files actually changed

```
src/source_feed.rs            sql_client(): lazy sidecar tokio-postgres
src/wal_stream.rs             filter_mut() accessor
src/bin/stream.rs             seed_from_source between WalStream::new and START_REPLICATION
tests/catalog_seed.rs         loop VACUUM FULL pg_class until filenode >= 16384
tests/wal_stream_e2e.rs       new pre_rotated_pg_class_seed_keeps_catalog_writes test
plans/PRE5b2.md               this retrospective
```
