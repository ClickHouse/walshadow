# descriptor log

Durable append-only relation-shape history; the decode-side catalog
oracle. Owned by `src/catalog/desc_log.rs`, populated by
`src/source/catalog_capture.rs` at catalog-commit boundaries, read
wait-free by every decode site.

## Why a log, not a live cache

Decoders ask a point-in-time question — `descriptor(rfn, L)` for a record
at LSN `L` — while a live shadow-PG query answers "descriptor now".
Bridging that gap with caches, invalidation epochs, baseline ledgers, and
drop sweeps re-implements catalog time travel piecemeal, each piece with
its own race window. The log stores the history itself: per-key version
chains with `valid_from` bounds, so both bounds of every interval hold by
construction and lookups are a binary search, no locks, no SQL, no replay
gate.

## Capture at boundaries

The filter classifies any write below `FirstNormalObjectId` (or a tracked
relocated catalog filenode) as catalog and marks the writing xid dirty
([filter.md](filter.md)); mid-xact `XLOG_XACT_INVALIDATIONS` records
re-dirty an xid whose catalog writes precede the restart resume floor. At
that xact's commit it builds a `BoundaryInfo`: the drain xid (prepared
xid for COMMIT PREPARED), affected user oids (pump-side pg_class decodes
∪ the commit record's relcache invalidations), the xact tree's first
catalog-touch LSN, and a capture-all flag (whole-relcache inval,
pg_namespace catcache / whole-catalog inval, or a write to a catalog
whose effects invals don't enumerate — see
[future/catalog_capture_completeness.md](future/catalog_capture_completeness.md)).
Classification off the commit record alone is restart-safe: it carries
the xact tree's full inval set, so a boundary is recognized even when
every catalog record replayed before a crash.

`BoundaryHoldSink` sequences the boundary inside the publication hold
([source.md](source.md)):

```text
flush predecessors → hold (shadow applies through next_lsn) →
capture (SQL fan-out → entries + events → append + fdatasync →
index publish → events into XactBuffer) → forward commit record
```

Nothing past the commit exists on wire, archive, or worker queue during
capture, so the SQL snapshot *is* the commit's catalog state
(`pg_last_wal_replay_lsn() == next_lsn`, enforced fatal), and any record
reaching a decoder already has coverage.

## Entries, events, intervals

Per captured oid, capture diffs the fresh descriptor against the oid's
log predecessor:

- none/Dropped predecessor → `Present` entry + `Added` event
- shape change → `Present` entry (+ `Changed` when `compute_schema_diff`
  is non-empty; the diff is attribute-based, renames recapture silently)
- filenode rotation (rewrite/TRUNCATE/SET TABLESPACE) → `Retired` entry
  closing the old rfn chain, no event — AccessExclusiveLock means no
  decode query lands past the rotation; the entry exists so GC drops the
  chain and buggy callers fail closed
- absent from capture with a Present predecessor → `Dropped` tombstone +
  event at `next_lsn`

`valid_from` biases early — a descriptor is a backward-compatible reader
of older tuples (missing attrs → default/NULL; dropped columns keep their
physical slots), never the reverse. Sources in preference order: the
rfn's `XLOG_SMGR_CREATE` marker (pump-side map, before any page write),
the oid's first pg_class touch in the xact, the tree's first catalog
touch. Events enter the drain keyed at `valid_from`, sorted with config
events at drain open.

Toast rels ('t') capture entries and `Dropped` events only (the retire
ledger consumes those); indexes are excluded entirely.

## Replay-from-log

Every boundary appends a batch keyed `captured_at = next_lsn` — a
zero-entry stub when nothing changed. Boot loads ckpt + tail, then the
WAL re-read finds each boundary's batch already stored and derives events
from the stored entries against `predecessor_before(oid, captured_at)`
(the historical predecessor, never the loaded head) — no SQL, identical
events every replay. A miss with shadow replayed past the boundary means
the log lost coverage: fatal, remedy `--ignore-cursor` (which deletes the
log) or re-bootstrap. The manifest version gates pre-log spill dirs the
same way ([ops.md](ops.md)).

## Seed + coverage horizon

An empty log seeds one batch from `fetch_all_descriptors` (every rel
`relkind IN ('r','p','m','t')`, oid ≥ 16384) at the raw resume position,
entries valid from the aligned start, persisting `covered_through` in the
ckpt. The aligned-prefix re-read decodes against the seed; boundaries at
or below `covered_through` skip capture and event replay (baked into the
snapshot); `NotCovered` at or below it is a counted row skip (rel died
pre-snapshot). Every boot also runs a boot-`Added` pass over the log's
active Present set — auto-create namespaces and opted-in mapped rels get
their idempotent `CREATE TABLE IF NOT EXISTS` at attach, and newly
enabled config picks up existing rels without log mutation.

## Decode reads

`descriptor_at(rfn, lsn)` / `descriptor_by_oid_at(oid, lsn)` return
`Present | Dropped | Retired | NotCovered | ForeignDb`:

- worker buffering: Present decodes; ForeignDb and horizon/xid-0
  NotCovered are counted skips; NotCovered/Dropped with a live xid stash
  for commit-time resolution; Retired skips (rows can't outlive the
  rotation)
- stash resolution at commit `next_lsn`: Present toast → chunk decode
  behind its marker barrier, Present ordinary → fenced (stash item 5,
  [future/xact_stash.md](future/xact_stash.md)), tombstones discard
- decode pool: per-job memo over the log (mapping writes land inside the
  barrier fence, between jobs); anything but Present on a drained record
  is fatal except ForeignDb / pre-horizon skips
- TRUNCATE fan-out resolves by oid; the barrier apply falls back to the
  rfn chain's last Present when the truncating commit itself retired the
  rfn (rotation's `Retired` lands before the truncate record)

## Storage

`desc_log.ckpt` + `desc_log.tail` under the spill dir. Shared binary
header binds pg major, system id, timeline, db oid, and segment size —
mismatch fatal, mirroring the manifest's foreign-source gate. Frames are
`[len u32][crc32c][body]`; the ckpt (written via `fs::write_atomic`)
carries a meta frame (`covered_through`, `floor_at_write`) plus compacted
batches; the tail is fdatasynced per boundary. A torn final frame
truncates durably at load; interior CRC failure is fatal. One writer
mutex serialises append and GC; readers take an RwLock'd index snapshot
published only after fsync.

GC runs after each manifest persist against the same resolved floor
([ops.md](ops.md)): per key the entry active at the floor survives when
Present; a Dropped/Retired there drops the whole at-or-below chain
(nothing above can reference it — records predate the drop and the floor
never exceeds the re-read start); batches above the floor survive whole,
stubs included. Thresholds: ≥512 droppable entries or an 8 MiB tail.

Identity keys the full physical `RelFileNode`: relfilenumbers are unique
only per database of one tablespace
([future/TABLESPACES.md](future/TABLESPACES.md) §0), so `(db_node,
rel_node)` alone can alias two live relations. Capture resolves the
`pg_class.reltablespace` 0 sentinel to the database's `dattablespace`,
making stored rfns directly comparable to WAL locators' physical spcOid.

## Metrics

`walshadow_desc_capture_*` (sql / log_replay / skipped_covered /
capture_all / rels / seconds), `walshadow_desc_events_*`,
`walshadow_desc_log_*` gauges + GC counters, `walshadow_desc_lookups_*`
by result. Capture time counts inside the boundary-hold duration.
