# Safe reclamation for shadow TOAST

Shadow value mode can lose a value when PostgreSQL reclaims its chunks before
ClickHouse finishes older row work. Prevent source reclamation where possible,
and save values before shadow reclaims them otherwise. Neither approach delays
shadow replay. See [shadow TOAST architecture](../architecture/shadow-toast.md)
for current data flow

## Source horizon pin

walshadow already streams as a physical standby using a slot. Optional
hot-standby feedback can hold source xmin at oldest transaction pending work
may still read. This prevents reclamation from entering WAL, so shadow has
nothing to delay or stage

Derive advertised xmin from resolved floor, already used for retention and
resume. Sample `oldestRunningXid` from `xl_running_xacts`. Unlike generation
checks, round down: keep newest sample at or below floor, so rounding retains
more history

Include xid epoch: walsender silently rejects feedback whose xmin is not in
recent past. A slot's xmin only moves forward, so hold pin continuously from
stream start. Releasing and restoring it cannot protect values reclaimed
while it was absent

Pin covers removal governed by transaction horizon: prune, vacuum, dead line
pointers, and tail truncation. It also prevents value-ID reuse, because
PostgreSQL reissues an ID only after all its chunks are gone

Pin does not cover file removal or replacement: `TRUNCATE`, `DROP TABLE`, and
rewrites through `VACUUM FULL`, `CLUSTER`, or `ALTER TABLE`. Slot invalidation
under `max_slot_wal_keep_size` also loses pin, as does source promotion, which
leaves new primary unprotected. Source bloat grows with ClickHouse lag, so
document when to release pin and rely on spilling values instead

## Value spill

Copy affected chunks out of shadow before it replays records that destroy them.
This covers operations pin cannot prevent and deployments that disable it.
Missing any requirement below can lose values

Persist spilled values before publishing WAL. `WalStream` sends record bytes
before awaiting record sink, so copying in sink can race shadow replay. Add a
hook before sending bytes, alongside hook proposed for
[replay callback](custom_rmgr.md). Publish only after spill entry is fsynced.
This also protects against daemon outages: shadow can replay only records whose
affected values are already saved

Cover every path that publishes to shadow: live stream, filtered archive, and
backup-window replay during bootstrap. Any uncovered path can destroy values
before they are saved

Read affected chunks just before destructive record. Wait for shadow to replay
up to that position using bytes already sent, without waiting for ClickHouse

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
