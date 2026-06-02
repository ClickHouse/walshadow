# source tablespaces

Replicate tables that live in non-default PG tablespaces, end to end.
Mostly already works; the gaps are bootstrap and shadow, not streaming.

Companion proposal [DESTINATIONS.md](DESTINATIONS.md) covers routing N
source relations to M ClickHouse destinations. The two were one request
but share almost nothing: this doc is a near-term correctness bug list,
that one is a forward-looking emitter rearchitecture. §3 below is the
only seam — it exposes the tablespace attribute destination routing
consumes as one (weak) key.

## 0. What already works, and why

In PG a relation's physical identity is the `RelFileLocator`
`(spcNode, dbNode, relNode)` (`wal-rs/src/pg/walparser/types.rs:119`).
walshadow keys every relation on `(db_node, rel_node)` and discards
`spc_node` for identity:

- catalog whitelist: `CatalogTracker.nodes: HashSet<(u32,u32)>`
  (`src/catalog_tracker.rs:62`)
- resolver: `CatalogMapResolver::relation_at` calls
  `get(rfn.db_node, rfn.rel_node)` (`src/relation_resolver.rs:74`);
  `CatalogMap.by_filenode` is `HashMap<(Oid,Oid), _>`
  (`src/backup_page_walk.rs:96`)
- the design comment spelling out why lives at
  `src/shadow_catalog.rs:939`: *relfilenode is unique per database
  regardless of tablespace*

That assumption is sound on supported versions (PG 16+).
`GetNewRelFileNumber` (`postgresql/src/backend/catalog/catalog.c:542`)
draws the relfilenumber from the cluster-wide OID counter and, on the
CREATE path, checks it unused in the database's `pg_class`; the rewrite
path additionally rejects a colliding on-disk file. So within one
database no two live relations share a `relNode`, across tablespaces or
not. `spc_node` is redundant for identity, and the heap decoder never
needs it — tuple layout doesn't depend on tablespace.

**Consequence:** steady-state streaming of a table sitting in a
non-default tablespace already works. The heap WAL record carries the
concrete physical `spcOid`, the filter ignores it, the decoder emits
rows. No code change needed for the decode path. *Verify, don't assume*
— there is no test pinning this; see §4.

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

1. Teach the path classifier the `pg_tblspc/...` shape. Parse `dbOid`
   and `relNode` out of the deeper path, ignore `spcOid` and the
   version dir for keying (consistent with `(db,rel)` identity).
   Catalog lookup (`self.catalog.get(db, rel)`) is unchanged because
   the catalog seed already records the right `(db,rel)` for these
   relations
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
at route time. The `RelDescriptor.rfn.spc_node` built from the catalog
(`src/shadow_catalog.rs:1007`, `src/backfill_bootstrap.rs:312`) carries
`pg_class.reltablespace`, which is **`0` for the database-default
tablespace** and the real OID otherwise — distinct from the *physical*
`spcOid` in heap WAL (always concrete). `0` is a convenient "default"
sentinel for routing predicates. Mapping `spcOid -> spcname` for
human-readable config uses `pg_tablespace` (already on shadow);
`reltablespace = 0` resolves to the database's `dattablespace`. This
resolution is the only catalog read destination routing needs;
cache it on the `RelDescriptor` (rare-change), do not query per row.

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
