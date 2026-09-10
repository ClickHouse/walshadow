# Non-default tablespaces

Backup page walk currently recognizes default-tablespace heap paths. Other
tablespaces can lose initial rows, and shadow recovery can encounter source
paths that do not exist locally. See [page walk](../src/backfill/backup_page_walk.rs)
and [shadow backup sink](../src/backfill/backup_sink.rs)

First reject unsupported layouts before bootstrap changes state. Check database
default tablespace and backup tablespace metadata, including tablespaces needed
by managed shadow even when their user tables are not selected. Apply equivalent
checks to direct and object-store loads. Define rejection for tablespace changes
arriving during streaming

## Complete support

Carry tablespace identity through file enumeration and lookup. Physical relation
identity includes tablespace, database, and filenode. Descriptor history already
uses all three; audit catalog tracker and backup maps that still omit tablespace
Two tablespaces may contain equal database and filenode numbers

Verify direct backup forwards every tablespace archive to its sink. Then teach
page walk to recognize those files and equivalent object-store paths. A path
parser alone cannot recover files discarded by backup transport

Choose a shadow-local directory mapping and use it consistently during backup
restore, CREATE TABLESPACE replay, and restart. Any WAL rewrite must preserve
record length and subsequent LSNs; replacing an arbitrary path with a longer
local path is not automatically safe. Prove a remapping strategy before choosing
record rewrite or a PostgreSQL recovery setting

Handle ALTER DATABASE SET TABLESPACE as well as table moves. Database moves can
change physical identity of default-placed relations without a pg_class update
Recapture affected descriptors when database default changes

## Identity and restore seams

Normalize `pg_class.reltablespace = 0` through `pg_database.dattablespace`
before constructing descriptor keys. WAL locators contain physical tablespace
OID; zero sentinel is not that identity. Preserve explicit shared-catalog
tablespace identity. Audit every `CatalogMap` and tracker lookup using only
`(db_node, rel_node)`, including pending-relation and bootstrap completion maps

Recognize default `base/<db>/<rel>` paths and tablespace archive entries under
versioned `PG_<major>_<catalog-version>/<db>/<rel>` directories, accounting for
`pg_tblspc/<oid>` indirection. Preserve relation segments and fork suffixes
Carry archive's tablespace OID alongside path instead of guessing from basename
Audit backup client transport before sink, then parser, map, and page walk

Prototype shadow-local `pg_tblspc` mapping with backup extraction and live
`XLOG_TBLSPC_CREATE`/drop replay together. Check major-specific path layout and
same-length rewrite restrictions. Test source paths unavailable on shadow host
and restart after a tablespace appears mid-stream

[Shadow TOAST](shadow_toast.md) needs same mapping for heaps and indexes
Optional [destination routing](extensions.md#multiple-clickhouse-destinations)
can match resolved tablespace OID after correctness lands. Cache OID/name mapping
and compare database default explicitly; do not query names for each row
Multiple source databases remain a separate routing/catalog design

## Completion

Test pre-existing rows, INSERT/UPDATE/DELETE, table moves, database moves,
tablespace creation, and shadow restart on supported PostgreSQL majors. Cover
direct and object-store initial loads, and distinct relations with colliding
database/filenode pairs. Remove rejection guards only for tested layouts
