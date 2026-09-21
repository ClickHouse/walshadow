# Shadow TOAST storage

![Shadow TOAST data and replay paths](shadow-toast.svg)

Shadow value mode keeps PostgreSQL TOAST heaps and indexes in shadow instead of
mirroring chunks into ClickHouse. Chunks from same transaction still resolve
from decoded WAL without a shadow lookup

## Bootstrap and replay

Bootstrap copies selected TOAST files into shadow data directory. Shadow then
replays filtered backup WAL through backup boundary, adding concurrent writes
and repairing copied pages through normal PostgreSQL recovery

walshadow keeps original WAL for decoding and sends retained physical records
to shadow. Existing TOAST relations are seeded explicitly. After bootstrap, a
relation begins shadow routing only when its file creation record proves shadow
has physical base file; later records for that relation follow same route. This
also retains ordinary relations created after bootstrap

Adding a relation later requires shadow to already hold its TOAST heap.
Relations created after bootstrap qualify through their file creation records.
Older relations must be included in bootstrap, since a running standby cannot
be seeded later. Admission rejects relations with missing TOAST heaps, whether
added by opt-in or config. Re-bootstrap with relation included, or use
ClickHouse mirror mode

## Reads and retention

Resolver waits until shadow has replayed referring record, then asks extension
to read and validate stored chunks. Rust side decompresses value and checks raw
size. Shadow is read-only from resolver perspective; decoded chunk writes never
go back into PostgreSQL

PostgreSQL can reclaim chunks through prune, vacuum, truncate, drop, or rewrite
before slower ClickHouse work reads them. Current mode reports such values as
superseded and emits NULL or column default. To detect reused value IDs,
extension reports newest chunk xmin and daemon compares it with highest xid
assigned at referring record. Newer chunks indicate a replacement, whether
complete or partial. A short run with an older xmin still indicates partial
reclamation of original value.
Planned work prevents source reclamation where possible and saves values before
shadow reclaims them, see [safe reclamation plan](../plans/shadow_toast.md)

## Implementation

Start in [WAL routing](../src/filter/engine.rs),
[bootstrap landing](../src/backfill/backup_sink.rs),
[admission](../src/toast/shadow_landing.rs),
[shadow reader](../src/toast/shadow_store.rs), and
[PostgreSQL extension](../pgext/toast.c)
