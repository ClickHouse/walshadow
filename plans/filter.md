# filter

Per-WAL-record keep/drop decision + byte-preserving NOOP rewrite. The
filter consumes parsed `XLogRecord`s in source order, returns
`Keep` or `Drop`, and for drops mutates the original record bytes
in-place into an `XLOG_NOOP` of identical `xl_tot_len` with CRC32C
recomputed. Output bytes re-parse cleanly through wal-rs's
`WalParser`; every record's `xl_prev` back-pointer stays valid so
shadow PG's recovery state machine never sees a chain gap.

## Purpose

Two operations fused per record:

1. Classify rmgr + block refs against a live catalog set, decide
   keep (catalog touch, recovery plumbing) vs drop (user-data heap /
   btree / etc).
2. Synthesize a same-length replacement when dropping, recompute
   CRC32C matching `xlog.c:5169` (body bytes, then header[0..20]).

`filtered_lsn == source_lsn` for every byte offset — no LSN
translation downstream. The manifest sidecar is a byte-position
index, not an LSN-pair table. See [Rewrite path](#rewrite-path) for
why packing-and-shifting was rejected.

## Classifier

`src/classify.rs::classify` buckets each record into one of:

| class | rule | filter action |
|---|---|---|
| `Special` | rmgr ∈ {xlog, xact, clog, multixact, standby, relmap, commit_ts, repl_origin, dbase, tblspc, smgr} | Keep |
| `Catalog` | any block ref hits a tracked catalog filenode | Keep |
| `User`    | block refs present, none catalog | Drop (unless tracker upgrades to Keep, see below) |
| `Empty`   | non-special rmgr, no block refs | reclassify via `main_data`; fallback Keep |

Catalog detection uses `rel_node < FirstNormalObjectId` (16384, see
`~/s/postgresql/src/include/access/transam.h`). A freshly-initdb'd
shadow has every system catalog (mapped + non-mapped) at oid < 16384,
so the bootstrap rule covers the static catalog set without any
runtime state.

Shared catalogs (`pg_database`, `pg_authid`, `pg_tablespace`,
`pg_shdepend`, …) live in `global/` and PG marks them with
`dbNode = 0`. `CatalogTracker::is_catalog` consults both
`(db_node, rel_node)` and `(0, rel_node)` so a per-db record can
match a shared-catalog entry. `rel_node == 0` is treated as
non-catalog (InvalidOid sentinel for block refs that carry no
relation).

Mixed-block records (any block ref touching catalog) classify as
`Catalog`, never `User`. PG occasionally emits records that touch
both a catalog index and a user relation in the same record; we
prefer false-keep to false-drop.

`rmgr_label` returns the human-readable rmgr name for diagnostics;
unknown ids stringify as `rmgr_N` for forward compat with future PG
majors.

## CatalogTracker

`src/catalog_tracker.rs::CatalogTracker` carries the live set of
post-bootstrap catalog filenodes plus a per-db `pg_class` filenode
map. Survives VACUUM FULL / CLUSTER / REINDEX / SET TABLESPACE on
mapped + non-mapped catalogs.

State:

- `nodes: HashSet<(u32, u32)>` — `(db_node, rel_node)` for every
  catalog filenode learned at runtime. `db_node = 0` entries shadow
  all per-db queries.
- `pg_class_filenode: HashMap<u32, u32>` — current `pg_class`
  filenode per database. Empty bootstrap falls through to
  `rel == 1259` (pg_class's initial mapped-catalog relfilenode).
- `relmap_updates`, `pg_class_writes_{decoded,undecoded,oid_in_prefix}`,
  `seeded_from_source` — diagnostic counters surfaced in the manifest.

Inputs:

- `RM_RELMAP_ID / XLOG_RELMAP_UPDATE` — authoritative for mapped
  catalogs (pg_class, pg_attribute, pg_type, pg_proc, pg_database,
  pg_authid, pg_shdepend, …). Body is `xl_relmap_update`
  (dbid+tsid+nbytes) followed by a 524-byte `RelMapFile` (magic
  `0x592717`, n, 64 mappings, CRC, see
  `~/s/postgresql/src/backend/utils/cache/relmapper.c`). Each
  non-zero `(mapoid, mapfilenumber)` adds `mapfilenumber` under the
  record's `dbid` (or shared set if `dbid == 0`). Malformed bodies
  (truncated, wrong magic, oversized n) increment the counter but
  apply nothing.
- Heap writes to `pg_class` — `pg_class_decoder` extracts
  `(oid, relfilenode)` from `XLOG_HEAP_INSERT` / `HEAP_UPDATE` /
  `HEAP_HOT_UPDATE`. Filtered on `oid < FirstNormalObjectId` so
  user-table inserts into pg_class never pollute. `VACUUM FULL` on a
  non-mapped catalog (pg_depend, pg_namespace, …) often
  prefix-compresses past the OID column with `XLH_UPDATE_PREFIX_FROM_OLD`;
  those records hit the `oid_in_prefix` counter and stay
  unidentified (closed later by snapshot seeding).
- `seed_from_source(client)` — queries the source PG once at attach
  time for every `(catalog_oid, current_filenode)` pair under
  `oid < 16384`. Closes the "long-running source already rotated a
  mapped catalog before walshadow attached" hole that the < 16384
  bootstrap rule misses on its own. Shared catalogs seed under
  `db_node = 0`, per-db under the source's `current_database()` oid.
- DROP TABLE coarse signal — `heap_delete` against the current
  `pg_class` filenode bumps an invalidation epoch so downstream
  ShadowCatalog drops cached descriptors. No tuple decode (system
  catalogs default to `relreplident = 'n'`, so the WAL omits the
  dying tuple).

`set_invalidation_epoch` / `set_pg_class_delete_epoch` attach
`Arc<AtomicU64>` counters shared with
`ShadowCatalog::sweep_dropped`. Senderless trackers (CLI, batch
tests) leave both `None` and observe is a no-op for the signal.

## Rewrite path

`src/rewrite.rs::noop_replace` takes a complete record buffer
(header + body, no page-header interruptions) and rewrites it
in-place:

- Header: preserve `xl_tot_len` (offset 0..4) and `xl_prev` (8..16),
  zero `xl_xid`, set `info = XLOG_NOOP (0x20)`, `rmid = RM_XLOG`,
  zero the 2-byte pad + 4-byte CRC placeholder.
- Body: zero-fill, then plant either a SHORT
  (`XLR_BLOCK_ID_DATA_SHORT` + 1-byte length) or LONG
  (`XLR_BLOCK_ID_DATA_LONG` + 4-byte length) main_data marker
  depending on body size. Threshold is 257 bytes (SHORT max).
- CRC32C: body bytes via `crc32c::crc32c_append` seeded at 0, then
  header[0..20]. Order matches PG's `INIT_CRC32C` /
  `COMP_CRC32C(body)` / `COMP_CRC32C(header_pre_crc)` /
  `FIN_CRC32C` exactly. Got this wrong on first attempt by
  including `xl_crc` in the input; round-trip parse caught it.

`src/filter_segment.rs::filter_segment` orchestrates one segment:

1. `SegmentWalker` (`src/segment.rs`) yields every complete record
   with `(logical_bytes, byte_ranges, start_offset, page_magic)`.
   Handles records straddling 2+ pages and headers themselves split
   across the page boundary (`Pending::total_len` resolves lazily
   once 24 header bytes accumulate).
2. `parse_record_from_bytes(logical_bytes, page_magic)` builds the
   wal-rs `XLogRecord` so the Filter sees populated block refs +
   main_data. Page magic threads through so FPI bit semantics match
   the source PG major.
3. `Filter::decide` updates the tracker, then returns Keep/Drop.
4. Drops: `noop_replace` on a clone of `logical_bytes`, then scatter
   the rewritten bytes back into the output buffer at each
   `byte_range` (cross-segment records re-stitch across page
   boundaries — both segs must NOOP-rewrite or shadow PG PANICs on
   missing pages).
5. Emit `Manifest { records: [Entry { offset, len, rmid, info, kind }], stats }`
   plus the parsed records so downstream sinks parse once.

Per-segment `ManifestStats` come from `FilterStats::delta_from`
against a snapshot taken at function entry; the long-lived `Filter`
accumulates cumulative stats across every segment in the stream.

## Filter binary

`src/bin/filter.rs` is `walshadow-filter`: one-shot CLI for offline
filtering of a single segment file. Usage:

```text
walshadow-filter --in seg.wal --out-dir filtered/ [--manifest <path>]
```

Reads the segment, constructs a local `Filter::new()`, calls
`filter_segment`, writes `filtered/<basename>` + a JSON manifest
sidecar. The fresh per-invocation filter is fine here because the
CLI takes one segment at a time; multi-segment correctness lives in
the streaming binary (`walshadow-stream`) which owns a long-lived
`Filter` on `WalStream`.

wal-rs supplies the on-wire constants directly via its public
`pg::walparser` exports — `X_LOG_RECORD_HEADER_SIZE`,
`X_LOG_RECORD_ALIGNMENT`, `XLR_BLOCK_ID_DATA_SHORT/LONG`,
`XLP_LONG_HEADER`, `XLP_PAGE_MAGIC_PG15` (kept under that name even
though it is the universal "minimum walshadow accepts" magic since
PG 15 is the FPI-layout floor). No in-tree `wire.rs`; earlier plan's
claim of a new module never materialised.

`src/bin/classify.rs` is `walshadow-classify`: early-iteration holdover
that walks segments through `WalParser` + `Summary::observe`, printing
per-class and per-rmgr counts. Does not rewrite — pure observability.

## Per-stream Filter

`Filter` is a field on `WalStream`, not constructed per segment.
Every relmap update, every decoded pg_class heap write, every
bootstrap-seeded filenode must survive across segment boundaries.
Earlier per-segment construction was masked by single-segment
fixtures, broke at the first segment rotation in a live stream.

`flush_current` and `close` thread `&mut self.filter` into
`filter_segment`. `tests/multi_segment_filter.rs` covers the
regression: seg 1 carries `XLOG_RELMAP_UPDATE` mapping `pg_class` to
filenode 50000, seg 2 carries a heap write at that filenode; the
filter must keep seg 2's record.

Per-segment manifest stats stay correct because `FilterStats`
implements `Copy` + `delta_from(prev)`; `filter_segment` snapshots
cumulative stats on entry and reports the difference.

## What gets dropped at the filter

User-data drop targets, never reach shadow PG:

- `RM_HEAP` / `RM_HEAP2` against user relations
  (rel_node ≥ 16384 and not in tracker): INSERT, UPDATE,
  HOT_UPDATE, DELETE, LOCK, INPLACE, MULTI_INSERT, FREEZE_PAGE,
  VISIBLE, PRUNE, VACUUM, CONFIRM, LOCK_UPDATED.
- `RM_BTREE` against user indexes: INSERT_LEAF/UPPER/META,
  SPLIT_L/R, DEDUP, VACUUM, DELETE, MARK_PAGE_HALFDEAD,
  UNLINK_PAGE, NEWROOT, REUSE_PAGE.
- `RM_HASH`, `RM_GIN`, `RM_GIST`, `RM_SPGIST`, `RM_BRIN` against
  user indexes: all info bytes when none of the block refs hit a
  tracked catalog filenode.
- `RM_SEQ` against user sequences.
- `RM_GENERIC` / `RM_LOGICALMSG` user-data variants when no block
  ref hits the catalog set.

Special rmgrs always passed through verbatim
(xlog, xact, clog, multixact, standby, relmap, commit_ts,
repl_origin, dbase, tblspc, smgr). `XLOG_SWITCH` is a special-rmgr
record and stays byte-identical across the rewrite
(`filter_segment_tests::xlog_switch_record_passes_through_filter`).

Steady-state OLTP keep-fraction on the
`fixtures/wal/filter/` capture is ~0.04% (17 kept of 42,091
records, no DDL in window). DDL-heavy windows shift that toward 8%+
because schema setup dominates the WAL.

## Alternative considered: PG fork with relfilenode whitelist

The other path was a patched PostgreSQL where `redo` consults a
whitelist before each record and skips user-data redo entirely.
Rejected. CRC rewrite wins because: walshadow's per-record CRC32C
on a hardware-accelerated SSE4.2 / CRC32 path is roughly 1 ns/byte
on a single core, one-time spend on records that already had to be
parsed once anyway. Maintaining a PG fork ties releases to
PostgreSQL's branch cadence, requires re-validating recovery
invariants per minor bump, and leaks complexity into the daemon
operator's deploy story. Byte-preserving rewrite keeps shadow PG
unmodified and lets walshadow ship independently.

Future budget for >1 GB/s WAL throughput would push CRC into the
parallel-segment-pipeline regime; cross-link
[future/risks.md](future/risks.md) for the throughput measurement
question.

## Cross-links

- [source.md] — `SourceFeed` / `WalStream` pump that hands parsed
  records to the filter via per-record `Filter::decide`. The
  streaming pipeline ownership story (long-lived `Filter`,
  `RecordSink` fan-out, `DirSegmentSink`) lives there.
- [shadow.md] — what consumes the filtered bytes: shadow PG's
  recovery via the rewritten segment file, plus the parsed-record
  hand-off (`ParsedRecord` in `filter_segment`) feeding the
  ClickHouse Native emitter without a re-parse.
- [overview.md](overview.md) — system shape, filter contract, why
  rewrite over fork.
- [future/risks.md](future/risks.md) — throughput measurement
  question for parallel CRC.
