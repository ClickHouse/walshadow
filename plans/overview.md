# walshadow design overview

walshadow turns source Postgres physical WAL into ClickHouse Native blocks
without a logical-decoding plugin. Two consumers share one wire: per-record WAL
filter feeds co-located shadow Postgres running schema-only catalog replay, and
in-tree heap decoder emits user rows to ClickHouse, using shadow as live catalog
oracle for each relation lookup

User topology, requirements, and limits live in
[`docs/`](../docs/README.md). Component implementation state lives in code and
tests

## Why a shadow Postgres

Static catalog snapshot would force three concessions:

1. Operator coordinates every DDL
2. Relfilenode rewrites (`VACUUM FULL`, `CLUSTER`, `REINDEX`, `SET TABLESPACE`)
   stay invisible without an external signal
3. Decoder has no in-tree oracle when it disagrees with PG on Tier 3 values

Second Postgres beside wal-rus, schema only with WAL-driven catalog, removes all
three. Source DDL writes catalog heap records, replay keeps shadow `pg_catalog`
current without operator coordination. Relfilenode rewrites ride same WAL.
`typsend` and `typoutput` on shadow provide differential oracle over libpq

Cost is one extra `postgres` process, schema-sized data directory, and CPU for
catalog-WAL filtering plus CRC rewrite. Catalog WAL is a small fraction of
total, so steady state is DDL-rate-bound rather than data-rate-bound

## Filter contract

| rmgr | kept records | reason |
|---|---|---|
| `RM_HEAP_ID`, `RM_HEAP2_ID` | record `RelFileLocator` in catalog set | DDL writes catalog rows |
| `RM_BTREE_ID` | relation is catalog index | catalog SELECT plans |
| `RM_RELMAP_ID` | all | shared-catalog relfilenode rewrites |
| `RM_XACT_ID` | all | commit and abort visibility |
| `RM_CLOG_ID`, `RM_MULTIXACT_ID` | all | catalog tuple transaction status |
| `RM_STANDBY_ID` | all | recovery housekeeping |
| `RM_XLOG_ID` | checkpoint, nextoid, parameter change | recovery plumbing |
| `RM_SMGR_ID`, `RM_DBASE_ID`, `RM_TBLSPC_ID` | all | file, database, and tablespace lifecycle |
| `RM_COMMIT_TS_ID`, `RM_REPL_ORIGIN_ID` | all | transaction metadata replay |

Everything else drops. Catalog set starts from `pg_class WHERE oid <
FirstNormalObjectId` on freshly initialized shadow, then `CatalogTracker`
follows `RM_RELMAP_ID` and `pg_class` heap writes so rewrites stay in whitelist.
Shared catalogs under `global/` stay unconditional

### Rewrite over fork

For each record, parse header, walk block references, then choose keep, drop, or
placeholder. A record with catalog blocks becomes a synthesized record carrying
only kept blocks and a recomputed CRC32C. Every other record becomes
same-length `XLOG_NOOP`, preserving subsequent `xl_prev` chain

Shadow runs as standby pointed at filter output through walsender wire plus
`restore_command` archive fallback, using an unmodified upstream PostgreSQL
binary

Patching recovery dispatcher with a relfilenode whitelist would create a
permanent PostgreSQL fork. Record rewrite localizes compatibility work in
walshadow and remains preferred until measurement disproves its cost

## Ordering invariants

1. **Shared catalogs stay unconditional.** `pg_database`, `pg_authid`,
   `pg_tablespace`, and `pg_shdepend` carry `dbNode = 0`; shadow cannot start
   without them
2. **CLOG and multixact stay wholesale.** Catalog replay needs transaction
   status records, volume is too small to justify finer filtering
3. **Catalog vacuum replays.** Shadow autovacuum stays off, while source catalog
   prune, vacuum, freeze, and index-cleanup records replay. Shadow catalog bloat
   therefore tracks source within replay lag
4. **DDL rewrites resolve at commit boundary.** Descriptor capture holds shadow
   exactly through catalog-mutating commit, then writes interval-scoped answers
   to descriptor log. Heap record always decodes against shape which produced
   it. Read-time `attmissingval` covers fast-path `ADD COLUMN` without rewrite
5. **Catalog cache may over-invalidate, never under-invalidate.** Any
   `pg_class` write bumps one generation. Finer invalidation stays deferred
   until measurement justifies complexity

## Related design notes

- [filter](filter.md), record classification and rewrite
- [source](source.md), physical WAL ingestion and shadow walsender
- [shadow](shadow.md), shadow lifecycle and catalog reads
- [descriptor log](desc_log.md), layout timeline
- [decoder](decoder.md), heap and type decoding
- [transaction buffer](xact.md), commit ordering and spill
- [emitter](emitter.md), ClickHouse pipeline and DDL barriers
- [bootstrap](bootstrap.md), initial shadow and row load
- [operations](ops.md), durable floor and retention
- [source timeline crossing](failover.md), planned branch transition proof
