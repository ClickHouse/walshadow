# PHASE1 — WAL filter + CRC rewrite

Closes [Phase 1 of `PLAN.md`](PLAN.md#phase-1--wal-filter--crc-rewrite). Lands a byte-preserving WAL filter that
drops user-relation records, replaces them with `XLOG_NOOP`
placeholders of identical `xl_tot_len`, and recomputes CRC32C so the
filtered segment re-parses cleanly through wal-rs's `WalParser`. Also
lands a live catalog tracker keyed on `RM_RELMAP_ID` and a `main_data`
reclassifier for `Empty`-class records.

## What landed

| item | files | tests |
|---|---|---|
| Live catalog set (`CatalogTracker`) | `src/catalog_tracker.rs` | unit tests + relmap fixture decoder |
| `main_data` reclassifier (`XLOG_HEAP2_NEW_CID`) | `src/main_data.rs` | locator round-trip / negative cases |
| Keep/drop decision (`Filter`, `Decision`) | `src/filter.rs` | catalog/user/special/empty + post-relmap promotion |
| Byte-positioned segment walker | `src/segment.rs` | single-page, two-page-spanning record, partial-header-at-EOP, garbage-magic rejection |
| `XLOG_NOOP` synthesis + CRC32C | `src/rewrite.rs` | SHORT/LONG body, round-trip parse, recompute on overwrite |
| Manifest sidecar | `src/manifest.rs` | covered via `filter_segment_tests` |
| Full-segment orchestrator | `src/filter_segment.rs` | round-trip on real captures + synthetic page |
| `walshadow-filter` CLI | `src/bin/filter.rs` | exercised by integration test |
| Wire constants (header sizes, alignment, markers) | `src/wire.rs` | shared between rewrite & segment |
| OLTP fixture (segment-2 after `pg_switch_wal()`) | `fixtures/wal/filter/` | new |
| Round-trip integration test | `tests/filter_round_trip.rs` | gated on fixture presence |

CRC32C scheme mirrors `src/backend/access/transam/xlog.c:5169`:
body (offset 24..xl_tot_len), then header[0..20] (xl_tot_len through
the 2-byte padding), via `crc32c::crc32c_append(prev, …)` seeded at 0.

Filter is byte-preserving: `filtered_lsn == source_lsn` for every
record. NOOP placeholders keep `xl_prev` intact, so any record's chain
back-pointer remains valid for shadow PG's recovery state machine.

## Acceptance status

[PLAN.md §"Acceptance criteria"](PLAN.md#acceptance-criteria) §1 calls for "well under 1%" kept
fraction on a steady-state OLTP workload. Result on the new
`fixtures/wal/filter/` capture (10k UPDATEs + 500 INSERTs + 200
DELETEs against 1000-row table, no DDL inside the timed window):

```
OLTP fixture: 42091 records, kept 17 (0.0404%), dropped 42074 (99.96%)
```

`tests/filter_round_trip.rs::oltp_workload_keeps_well_under_one_percent`
enforces `kept_frac < 0.01`.

The Phase 0 fixture (DDL-heavy) is unchanged and still asserts the
loose `< 0.99` bound — that test exists to make sure the classifier
isn't bucketing everything as catalog, not to validate acceptance.

## Real bugs found / fixed

### Walker mishandled records spanning >2 pages

First fixture pass on the Phase 0 capture failed at offset 974,840
with `Truncated`. Root cause: the page header `remaining_data_len`
can exceed one page's data area when a single record spans 3+ pages
(big FPI-carrying records). Initial walker treated
`remaining_data_len > page_data_area` as a corrupt-page signal. Fixed
in `consume_continuation` by clamping the per-page contribution to
`min(remaining_data_len, page_data_area)` and jumping the cursor to
`page_end` when the record didn't complete on the current page.

### Walker mishandled record headers straddling page boundaries

Second fixture pass failed at offset 7,806,968 (8 bytes before a page
boundary). A record's 24-byte header itself was split across two
pages. Initial walker rejected partial headers. Fixed by making
`Pending::total_len` an `Option<u32>` resolved lazily once the
accumulated bytes reach `X_LOG_RECORD_HEADER_SIZE`.

### CRC32C order matches PG exactly

CRC covers (body bytes, then header[0..20] excluding `xl_crc`). Got
this wrong on first attempt by including the CRC field in the input.
Caught immediately by `parse_record_from_bytes` rejecting the round
trip; fixed by anchoring against `xlog.c:5169` directly.

## Design decisions

### Byte-preserving rewrite over pack-and-shift

[PLAN.md Phase 1](PLAN.md#phase-1--wal-filter--crc-rewrite) mentions a "(filtered_lsn, source_lsn) manifest sidecar"
implying LSN translation. Two alternatives:

1. **Byte-preserving** (chosen): drop records become same-length NOOPs.
   `filtered_lsn == source_lsn` for every offset. Manifest is just a
   record-boundary index. Shadow PG recovery LSN compares directly
   against source LSN — no translation needed at decode time.
2. Pack-kept-only: removes byte cost of dropped records but every
   downstream consumer (decoder, replay-driver, debugger) needs LSN
   translation. Strictly worse for Phase 1's bounded byte savings
   (steady-state catalog WAL is already MiB/day-scale).

### Catalog tracker keys on `(db_node, rel_node)`

Shared catalogs live in `global/` with `db_node = 0`. Per-database
catalogs have `db_node = N`. Filter checks both `(db, rel)` and
`(0, rel)` for any given block ref. Bootstrap rule
(`rel_node < 16384`) is applied unconditionally; relmap updates
record post-rewrite filenumbers under the database from the relmap
record's `dbid` field.

### `pg_class` heap-write tracking is best-effort

PG ≥ 12 stores non-mapped catalogs (pg_depend, pg_namespace, pg_index,
etc.) with `relfilenode == oid` initially; VACUUM FULL / REINDEX
rewrites them to ≥ 16384 relfilenodes. The new filenumber is recorded
in a `pg_class` heap UPDATE record. Decoding that tuple requires the
catalog cache (Phase 3's `ShadowCatalog`). Phase 1 counts
`pg_class_writes_undecoded` but does not extract the new filenumber
— surfaced in the manifest and stderr.

**Consequence**: VACUUM FULL on a non-mapped catalog will silently
break the live filter set until Phase 3 lands. Mapped catalogs
(pg_class, pg_attribute, pg_type, pg_proc, pg_database, pg_authid,
pg_shdepend, …) are covered correctly via `RM_RELMAP_ID`. Documented
limitation; Phase 3 closes the gap.

### `Empty` class default-keeps when unrecognised

Phase 0 left ~0.3% of records in `Class::Empty` (mostly
`XLOG_HEAP2_NEW_CID`). Phase 1 decodes `NEW_CID` and reclassifies
based on `target_locator`. Anything else still bucketed as `Empty` is
kept by default — correctness over efficiency. Shadow PG won't
malfunction from a few extra catalog-irrelevant records; it would
break if we dropped a recovery-critical record we didn't recognise.

### `wire.rs` carries PG ABI constants

`X_LOG_RECORD_HEADER_SIZE`, `X_LOG_RECORD_ALIGNMENT`,
`XLR_BLOCK_ID_DATA_SHORT/LONG`, `XLP_LONG_HEADER`,
`XLP_PAGE_MAGIC_PG15` live in wal-rs's `types` module but aren't
publicly re-exported. Phase 1 mirrors them in `walshadow/src/wire.rs`
to avoid widening wal-rs's public surface for walshadow-only needs.
The values are stable on-disk constants since PG 11.

### Two-segment OLTP fixture (`pg_switch_wal()`)

A single segment with bootstrap (CREATE TABLE / INSERT 1000 rows) +
DML pinned the catalog fraction at ~47% because schema setup
dominated. Phase 1 fixture's workload checkpoints, calls
`pg_switch_wal()`, checkpoints again, then runs heavy DML, and the
capture grabs segment 2 instead of segment 1. Catalog fraction in
segment 2 is effectively 0 (only special-rmgr records survive).

## Deviations from [PLAN.md Phase 1](PLAN.md#phase-1--wal-filter--crc-rewrite)

* "≈600 LOC" estimate undershot. Phase 1 added ~1,500 LOC of
  functional code + ~600 LOC of tests. The byte-positioned segment
  walker alone (`src/segment.rs`) is ~290 LOC of logic + 200 of tests
  because page-boundary stitching is the hardest piece. wal-rs's
  `WalParser` could not be reused directly because it discards record
  byte positions.
* Manifest sidecar is byte-position indexed, not LSN-pair indexed.
  See "Byte-preserving rewrite" above. [PLAN.md Phase 1](PLAN.md#phase-1--wal-filter--crc-rewrite) was ambiguous about
  whether `filtered_lsn` was a count-of-kept-records or a byte
  offset; byte offset chosen.
* `RM_RELMAP_ID` tracker is implemented; `pg_class` heap-write
  tracker is stubbed (counter only). [PLAN.md Phase 1](PLAN.md#phase-1--wal-filter--crc-rewrite) grouped both as one
  bullet; they split into "fully done" + "Phase 3 dependency".

## What didn't get done

* `pg_class` heap-write tuple decoding (gap above). Requires catalog
  cache.
* Multi-segment filter run. CLI accepts one `--in` at a time. Phase 2
  will wrap this in a streaming pipeline against wal-rs's
  replication client.
* `pg_filenode.map` initial bootstrap. The relmap tracker starts
  empty; it populates only when the source PG writes an
  `XLOG_RELMAP_UPDATE`. A long-running source might never emit one,
  in which case the bootstrap rule `rel_node < 16384` carries us. To
  recover the initial mapping at startup, Phase 2 will copy
  `global/pg_filenode.map` + `<db>/pg_filenode.map` from the source's
  base backup.
* `XLOG_SWITCH` handling on the source side. The rewriter passes
  through the record verbatim (it's a special-rmgr record so the
  filter keeps it), but no test covers the segment-boundary case yet.

## Test counts

* `cargo test --lib`: 37 passed.
* `cargo test --tests`: 5 passed (2 classify fixture + 3 filter
  round-trip; all gated, pass = skip-or-assert).
* `cargo clippy --all-targets -- -D warnings`: clean.

Total: 42 passing.

OLTP fixture capture (local PG 18):
* 42,091 records, 17 kept (0.04%), 42,074 dropped (99.96%), 0 relmap
  updates (no DDL in the timed window), 0 undecoded pg_class writes.

Phase 0 fixture capture (DDL-heavy, classify_fixture test):
* 27,495 records, 25,294 kept, 2,201 dropped (8% — bootstrap & DDL
  dominate). 967 undecoded pg_class writes (every non-mapped catalog
  table modification surfaces here, expected).

## Files touched

```
walshadow/Cargo.toml                                    +crc32c, +thiserror, +walshadow-filter bin
walshadow/src/lib.rs                                    rewrite (re-export new modules)
walshadow/src/wire.rs                                   new (PG ABI constants)
walshadow/src/catalog_tracker.rs                        new
walshadow/src/main_data.rs                              new
walshadow/src/filter.rs                                 new
walshadow/src/segment.rs                                new
walshadow/src/rewrite.rs                                new
walshadow/src/manifest.rs                               new
walshadow/src/filter_segment.rs                         new
walshadow/src/bin/filter.rs                             new
walshadow/tests/filter_round_trip.rs                    new
walshadow/fixtures/wal/filter/capture.sh                new
walshadow/fixtures/wal/filter/workload.sql              new
walshadow/fixtures/wal/filter/.gitignore                new
walshadow/PLAN.md                                       Phase 1 description amended (XLogReader→WalParser, scope additions)
walshadow/PHASE1.md                                     new (this doc)
```

LOC under `src/` (excluding tests inside source files): ~1,200. With
in-file tests: 1,837. Tests-only: 205. Estimate was 600. The ~3x
overshoot is concentrated in `segment.rs` (page-stitching) and the
fact that PG's WAL on-disk format has more corner cases than
[PLAN.md](PLAN.md) anticipated (records spanning 3+ pages, headers straddling
boundaries, mid-segment continuations).
