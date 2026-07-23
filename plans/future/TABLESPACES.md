# source tablespaces

Replicate tables that live in non-default PG tablespaces, end to end.
Mostly already works; the gaps are bootstrap and shadow, not streaming.

Companion proposal [DESTINATIONS.md](DESTINATIONS.md) covers routing N
source relations to M ClickHouse destinations. The two were one request
but share almost nothing: this doc is a near-term correctness bug list,
that one is a forward-looking emitter rearchitecture. §3 below is the
only seam — it exposes the tablespace attribute destination routing
consumes as one (weak) key.

## 0. Physical identity

In PG a relation's physical identity is the `RelFileLocator`
`(spcNode, dbNode, relNode)` (`walrus::pg::walparser::types`), and all
three components are load-bearing. `GetNewRelFileNumber`
(`postgresql/src/backend/catalog/catalog.c`) guarantees relfilenumber
uniqueness only *within one database of one tablespace*: its pg_class
probe checks `pg_class.oid` (CREATE path only, where filenumber doubles
as the oid), and the on-disk collision probe builds its path from the
target tablespace. The rewrite / `SET TABLESPACE` path passes no
pg_class at all, and `tablecmds.c` states the consequence outright:
*relfilenumbers are not unique in databases across tablespaces*. After
OID wraparound, two live relations in one database can share `relNode`
under different tablespaces, so `(db_node, rel_node)` is not an
identity.

Surfaces keying the full physical rfn:

- descriptor log chains + lookups (`src/catalog/desc_log.rs`); capture
  resolves the `pg_class.reltablespace` 0 sentinel to `dattablespace`
  in the descriptor SQL (shadow + source), so stored rfns match WAL
  locators' concrete spcOid
- `XLOG_SMGR_CREATE` markers, pump side (`SmgrMarkers`,
  `src/filter/engine.rs`) and worker side (`XactBuffer.markers`,
  `src/xact/xact_buffer.rs`)
- decode-path descriptor lookups and the per-job memo
  (`src/emit/pipeline/decode.rs`)

Surfaces still keying `(db_node, rel_node)`, exposed only when
wraparound mints a colliding relfilenumber:

- catalog whitelist `CatalogTracker.nodes`
  (`src/filter/catalog_tracker.rs`) — a user rel aliasing a catalog
  filenode misroutes its records to shadow and fabricates boundaries;
  no misdecode, but noisy and unbounded shadow growth
- bootstrap `CatalogMap.by_filenode` + page-walk classifier
  (`src/backfill/backup_page_walk.rs`) — moot until §1 lands, since
  non-default tablespace files never reach the classifier today

Tuple layout doesn't depend on tablespace, so steady-state streaming of
a table in a non-default tablespace works: heap WAL carries the concrete
physical `spcOid`, descriptors carry the resolved tablespace, decode
matches on full rfn. *Verify, don't assume* — there is no test pinning
this end to end; see §4.

`RM_TBLSPC_ID` and `RM_SMGR_ID` records pass the filter verbatim
(`src/classify.rs:39`, `rmgr_is_special`), and `pg_tablespace` (shared
catalog, OID 1213, `dbNode = 0`) is kept unconditionally. So the
*catalog* side of tablespaces replays into shadow. The gaps are
physical: bootstrap file enumeration, and shadow's on-disk tablespace
directories.

## 1. Bootstrap page-walk skips non-default tablespaces (bug)

`PageWalkSink::begin` classifies each BASE_BACKUP tar entry by path. It
only recognizes `base/<dbid>/<filenode>`; anything else returns
`FileAction::Skip` (`src/backup_page_walk.rs:457-462`). Non-default
tablespace heap files arrive under
`pg_tblspc/<spcOid>/<TABLESPACE_VERSION_DIR>/<dbOid>/<relNode>`, never
match, and are silently dropped. **A table in a non-default tablespace
is not backfilled** — only its post-attach WAL writes reach CH, so the
initial-load row set is missing.

`StartInfo.tablespaces: Vec<Tablespace>` (`src/backup_source.rs:100`)
is populated from the BASE_BACKUP `tablespace_map` but consumed nowhere
in walshadow today (every non-test caller ignores it; tests pass
`Vec::new()`).

Fix:

1. Teach the path classifier the `pg_tblspc/...` shape. Parse `spcOid`,
   `dbOid` and `relNode` out of the deeper path (version dir skipped)
   and key the `CatalogMap` on the full rfn per §0 — the catalog seed
   already records resolved physical tablespaces, so lookups compare
   directly
2. Confirm the BASE_BACKUP actually ships non-default tablespace
   contents in the stream walshadow consumes. In `BASE_BACKUP`,
   each tablespace is a separate tar with its own root; the source
   client must request and forward all of them, not just the main
   one. Audit `src/backup_source_direct.rs` /
   `src/backup_source_object_store.rs` for whether secondary
   tablespace tars are streamed to the sink at all — if they're
   dropped upstream of `PageWalkSink`, fixing the classifier is
   necessary but not sufficient
3. Object-store bootstrap path: the same classifier feeds it; verify
   the object-store layout preserves the `pg_tblspc/...` prefix or
   carries an equivalent per-tablespace manifest

## 2. Shadow must materialize tablespace directories

Shadow is a schema-only standby replaying filtered WAL. Two record
classes force physical tablespace state onto the shadow host:

- `XLOG_TBLSPC_CREATE` (from `CREATE TABLESPACE foo LOCATION '/src/path'`)
  — recovery creates a symlink `pg_tblspc/<spcOid> -> /src/path`. That
  source path does not exist on the shadow host; recovery either fails
  creating the symlink or leaves it dangling
- `XLOG_SMGR_CREATE` / first-page writes for a new relation in that
  tablespace — recovery opens
  `pg_tblspc/<spcOid>/<ver>/<dbOid>/<relNode>`; a dangling symlink means
  the path doesn't resolve and replay errors

walshadow does not want the source's physical layout — shadow holds no
user heap (user-heap records NOOP-rewrite, see
`[[walshadow-cross-seg-records]]`). It only needs the file *paths* to
resolve so catalog-adjacent writes don't abort recovery. Options, in
order of preference:

1. **Rewrite `XLOG_TBLSPC_CREATE` in the filter** to redirect the
   symlink target into a shadow-local scratch dir
   (`<shadow_data>/wstblspc/<spcOid>`), pre-created by the daemon. This
   slots into the existing CRC-rewrite machinery (`src/filter.rs`,
   `src/rewrite.rs`): parse the record's path payload, substitute the
   target, recompute CRC32C, emit. Shadow then creates real dirs the
   daemon owns. Symmetric with how user-heap records are already
   rewritten. Cost: one more rmgr-specific rewriter, and the rewrite
   must track each redirected `spcOid` so bootstrap-time symlink
   replay (`emit_tablespace_symlink`, `src/backup_source.rs:375`) lands
   in the same scratch root
2. **`allow_in_place_tablespaces`** — PG dev GUC that makes
   `pg_tblspc/<oid>` a real directory instead of a symlink. Avoids
   absolute-path remapping but it's a developer/test setting, not
   guaranteed stable across majors; relying on it in a production
   standby is fragile. Use only if option 1 proves too invasive
3. **Pre-create symlinks pointing at scratch before promote/replay** —
   race-prone against recovery creating them; rejected

Bootstrap side (§1) must agree with whichever option: the BASE_BACKUP
`tablespace_map` symlinks shipped to shadow's data dir
(`src/backup_sink.rs`) must be rewritten to the same scratch root, or
shadow won't start. So §1 and §2 share the remap table.

`SET TABLESPACE` on a tracked table is a relfilenode rewrite already
covered by the catalog generation bump (overview pitfall 5,
`[[pg-version-wal-skew]]`); the new relfilenode just appears in a
(possibly new) tablespace whose dir §2 has already materialized.

## 3. Tablespace as an emitter-visible attribute

[DESTINATIONS.md](DESTINATIONS.md) wants the tablespace of a relation
at route time. `RelDescriptor.rfn.spc_node` carries the *resolved
physical* tablespace OID — the descriptor SQL
(`src/catalog/shadow_catalog.rs`, `src/backfill/backfill_bootstrap.rs`)
maps the `pg_class.reltablespace` 0 sentinel to the database's
`dattablespace`, matching the concrete `spcOid` in heap WAL. A routing
predicate wanting "is in the default tablespace" compares against
`dattablespace` rather than testing for 0. Mapping `spcOid -> spcname`
for human-readable config uses `pg_tablespace` (already on shadow); do
not query per row.

## 4. Tests (this is unshippable without these)

- streaming: `CREATE TABLESPACE ts LOCATION ...; CREATE TABLE t (...)
  TABLESPACE ts;` then INSERT/UPDATE/DELETE — assert rows reach CH.
  This pins §0's "already works" claim and guards against a future
  refactor keying on `spc_node`
- bootstrap: pre-populate `t` in `ts` with rows, attach greenfield,
  assert backfilled row count matches (guards §1)
- shadow restart: `CREATE TABLESPACE` mid-stream, `pg_ctl restart`
  shadow, assert recovery resumes (guards §2)
- gate all three on `initdb`/`clickhouse` presence, runtime-skip
  pattern per `[[test-timeouts-stay-short]]`

## 4b. Descriptor-log invariant

The durable descriptor log (`src/catalog/desc_log.rs`) keys chains by
the full physical `RelFileNode` per §0. The invariant making that
comparison sound: every captured descriptor's `rfn.spc_node` is the
*resolved physical* tablespace OID — the descriptor SQL maps
`reltablespace = 0` through `pg_database.dattablespace`, and shared
relations carry explicit `pg_global` (1664) in `pg_class` — so log keys
and WAL locators live in the same namespace. Any new descriptor
construction site must preserve this resolution or its entries silently
miss every lookup. Rotation detection (`src/source/catalog_capture.rs`)
compares full rfn for the same reason. Unit coverage: two relations
sharing `(db, rel)` under distinct tablespaces resolve independently
(`same_db_rel_across_tablespaces_stay_distinct`).

## 5. Multi-database adjacency (out of scope, flagged)

Tablespaces and multiple source *databases* are often conflated.
walshadow follows one database's WAL within a cluster; foreign-`dbNode`
filenodes are explicitly rejected (`src/shadow_catalog.rs:939`).
Multiple source databases is a separate, larger effort (one slot reads
the whole cluster's WAL but catalogs are per-database) and is **not**
covered here.

## Phasing

- **§1** bootstrap classifier + verify upstream tablespace tars ship
- **§2** shadow tablespace remap in filter + bootstrap symlink rewrite
- **§4** tests pin §0/§1/§2

## Dependencies

- §2 depends on the filter's CRC-rewrite path (`src/rewrite.rs`,
  `src/filter.rs`) admitting an `RM_TBLSPC_ID` rewriter — analogous to
  existing rewriters, no new infrastructure
- §3 is the only handoff to [DESTINATIONS.md](DESTINATIONS.md); it can
  land independently of any routing work

## Open questions

- **Does the BASE_BACKUP stream walshadow consumes carry secondary
  tablespace tars at all?** If the source client drops them upstream of
  `PageWalkSink`, §1's classifier fix is moot until that's repaired.
  Audit before scoping §1
- **Foreign-tablespace symlink rewrite across PG majors.** The
  `XLOG_TBLSPC_CREATE` payload layout and `TABLESPACE_VERSION_DIR` differ
  by major. §2's rewriter must be version-aware, same discipline as the
  rest of the WAL parser
- **`ALTER DATABASE ... SET TABLESPACE`** relocates every
  `reltablespace = 0` relation, preserving relfilenumbers, with no
  pg_class writes. Post-move WAL locators carry the new spcOid while
  descriptor chains keep the old physical key: DML fails closed
  (`NotCovered`), never misdecodes. Detecting a
  `pg_database.dattablespace` change and forcing capture-all would make
  it survivable without re-bootstrap
