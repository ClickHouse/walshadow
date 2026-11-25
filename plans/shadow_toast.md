# Safe reclamation for shadow TOAST

Shadow value mode can lose a value when PostgreSQL removes its chunks before
walshadow reads them for ClickHouse. The [source horizon
pin](../architecture/shadow-toast.md#source-horizon-pin) tells PostgreSQL to
keep chunks walshadow may still need. Start protecting them earlier, limit
that protection, and save any needed values before shadow replays a record
that removes them.

## Pin from slot creation

Today the pin starts when live streaming begins. During bootstrap, source
vacuum can remove chunks referenced by rows from the start of the backup.
Walshadow reads some of those values only after replaying WAL from the backup
window. The oldest transaction still running at stream startup can be too
recent to protect those values.

Right after creating the physical slot and before `BASE_BACKUP`, read a source
snapshot's `xmin`. Start a short `START_REPLICATION` session on that slot and
send `xmin` as feedback. The slot keeps it after the session ends. Keep sending
the same `xmin` while streaming WAL from the backup window, then resume from
the slot's `xmin` when live streaming begins. Bootstrap without a slot still
runs without a pin.

## Bounded pin

The pin affects every table in the source cluster. A long outage holds back
vacuum, including transaction ID freezing. Release the pin once its age in
transactions or its WAL lag exceeds a limit, then rely on saved values.
Publish the held `xmin`, its age, and whether the pin was released as metrics.
Today operators can only read the slot's `age(xmin)` on source.

## Value spill

Save needed chunks before shadow replays WAL that removes them. This covers
operations the pin cannot prevent and lets walshadow run with the pin disabled
or released.

Persist spilled values before publishing WAL. `WalStream` sends record bytes
before awaiting record sink, so copying in sink can race shadow replay. Add a
hook before sending bytes, alongside hook proposed for
[replay callback](custom_rmgr.md). Publish only after spill entry is fsynced.
This also protects against daemon outages: shadow can replay only records whose
affected values are already saved

Cover every path that publishes to shadow: live stream, filtered archive, and
backup-window replay during bootstrap. Any uncovered path can destroy values
before they are saved

One TOAST heap saw over 3,000 `PRUNE_ON_ACCESS` records per second across
thousands of pages, with no vacuum records. Pruning in the user table was
several times more frequent and touched no TOAST chunks. See the
[churn benchmark](../benches/shadow_toast_horizon.rs).

Use the relation file number (`relfilenode`) to identify records that touch
TOAST. Each prune record lists positions on a page. Looking up the value IDs
and saving chunks for every record would require thousands of shadow queries
and disk syncs per second before WAL could reach shadow.

### Held reads

Read-ahead already fetches TOAST values when the WAL scan finds rows that carry
a pointer in place of a value. Have the extension return each chunk's bytes
and its location in the heap (TID). Keep a map from `(filenode, block, offset)`
to value while the row's WAL position is at or above the resolved floor.

At a destructive record, intersect its removed offsets with held TIDs from WAL
record alone. Persist only hits, then publish. Records without a hit publish
without a shadow round trip or fsync. Whole-relation destruction persists every
held value of that relation, bounding work by held set instead of TOAST heap
size

Gate shadow publication of a destructive record until every pointer decoded
before it has a completed read. Gate depends on shadow replay below held
position, never on ClickHouse or floor, so it adds read latency to catalog
boundaries without deadlock described below

Held set lives in memory under `[memory]` budget and drops entries once floor
passes their referring record. Crash loses only unhit entries, whose chunks
shadow still holds, so restart rereads them. Hit entries are fsynced before
publication and survive

Work grows with the number of rows whose values walshadow needs to read. With
the pin off, the benchmark lost values for about 0.1% of rows carrying only a
TOAST pointer with read-ahead on and about 0.5% with it off. Waiting for reads
to finish covers cases where shadow rejected an early read, then removed the
chunks before walshadow retried.

Shadow replay already limits how fast walshadow catches up. While draining a
backlog, shadow's startup process uses almost a full core and the queue
worker spends part of its time waiting on reads. Keep the extra work out of
PostgreSQL's replay process.

### Batched spill

Fallback for pointers without a held read, such as gate disabled or budget
exhausted. Hold shadow publication at first destructive record of a window,
read every pending-referenced value once at that position, and publish window.
Chunks inserted inside window come from decoded WAL, not shadow

### Entries

Key entries by tablespace, database, filenode, value ID, and destroying LSN.
Resolve reads using referring position to distinguish reused IDs

Release entries using resolved floor, not current pending work. Restart resumes
below durable ack and can repeat reads completed before crash

Stop if an unclassified record touches retained TOAST storage. Spilling requires
identifying every destructive operation so its affected chunks can be saved

Bound spilled bytes and define behavior at limit. Expose retained bytes,
oldest retained LSN, and release lag to operators

For whole-relation destruction, copy only values referenced above floor, or
stop. Copying a whole TOAST heap during WAL processing is unbounded work

## Prove reclamation behavior

Classify every supported PostgreSQL WAL operation that can destroy or detach
TOAST chunks: pruning, vacuum, index cleanup, rewrite, truncate, relation
replacement, and relation, database, or tablespace drop. Test each supported
major with insert, delete, cleanup, value-ID reuse, and physical rewrite

Use original pointer metadata and referring WAL position to distinguish an
exact historical value from a newer generation. Reads compare chunk `xmin`
with highest xid assigned at referring record. This can detect reuse but
cannot prove a value is original: frozen chunks provide no evidence, and
samples round up. Investigate page LSN at or below referring record as stronger
evidence that a value is original

## Why delaying replay can deadlock

Holding destructive WAL until older reads finish requires a release condition
that current progress tracking cannot provide

Resolved floor is `align_down(resume-safe ack)` capped at fsynced sealed-archive
end. It advances only while pump keeps reading, sealing, and acknowledging WAL
past held position. Stopping pump also stops progress needed to release it.
Resume-safe ack cannot pass any in-flight transaction's first record, so a
transaction spanning held position also prevents floor from advancing

Blocking archive writes has same problem. Sealed-segment watermark caps floor
and supplies `flush_lsn` advertised to source. Blocking writes freezes both
floor and source slot progress, so any delay must apply to shadow publication
only

Delaying shadow publication conflicts with catalog capture. At every commit
that changes catalogs, capture stops pump until shadow replays through it.
First catalog boundary past a held position causes pump to hit source
`wal_sender_timeout`. VACUUM's own in-place `pg_class` update creates such a
boundary immediately after prune records. Physical redo is serial, so shadow
cannot replay newer catalog changes while preserving older TOAST pages

## Completion

Test horizon pin and value spill with live and archived WAL, covering prune,
vacuum, rewrite, truncate, drop, value-ID reuse, and transactions spanning
bootstrap. Cover source and shadow restart, crashes around each persistence
and publication boundary, and pin loss through slot invalidation and promotion

Compare emitted values with ClickHouse value mode and source PostgreSQL. Once
pin or spill covers every destructive operation, any superseded fill indicates
a lost value. Assert those counters stay at zero

Add a recovery procedure for missing module, incomplete physical seed, lost
spill state, and disk exhaustion. Remove reclamation limitation only after
these tests pass on every supported PostgreSQL major
