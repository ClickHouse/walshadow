# Safe reclamation for shadow TOAST

Shadow value mode can lose a value when PostgreSQL reclaims its chunks before
ClickHouse finishes older row work. Add a reclamation fence before treating this
mode as safe under lag or restart. See
[shadow TOAST architecture](../architecture/shadow-toast.md) for current data
flow

## Prove reclamation behavior

Classify every supported PostgreSQL WAL operation that can destroy or detach
TOAST chunks: pruning, vacuum, index cleanup, rewrite, truncate, relation
replacement, and relation, database, or tablespace drop. Reject unclassified
destructive operations on retained storage

Test each supported PostgreSQL major with insert, delete, cleanup, value-ID
reuse, and physical rewrite. Use original pointer metadata and referring WAL
position to distinguish an exact historical value from a newer generation

## Define safe release

Define a durable boundary below which no queued, active, spilled, deferred, or
restartable work can request an older value. Release destructive WAL only when
that boundary covers it. Live WAL and archived segments must obey same gate

Persist enough staged WAL and release state to recover after crashes. Reject
startup if shadow replay has already crossed durable safety evidence. Bound
staged bytes and expose stalled boundary and retained storage to operators

Include pending bootstrap rows and future destination queues in safety proof
Acknowledged ClickHouse progress alone is insufficient when restart can replay
older source work

## Break catalog dependency cycles

A commit that combines DML with `DROP TABLE`, `TRUNCATE`, or rewrite can create
a cycle:

1. row decoding waits for commit-time catalog capture
2. capture waits for shadow to replay commit
3. reclamation gate waits for older rows to reach ClickHouse

Define separate catalog and reclamation progress or preserve required value
data before allowing catalog replay. Prove no equivalent cycle exists for
command-boundary capture, bootstrap deferral, or archive publication

## Completion

Test live and archived replay with prune, vacuum, rewrite, truncate, drop,
value-ID reuse, and transactions spanning bootstrap. Cover source and shadow
restart plus crashes around each persistence and publication boundary

Compare emitted values with ClickHouse value mode and source PostgreSQL. Add a
recovery procedure for missing module, incomplete physical seed, lost staging
state, and disk exhaustion. Remove reclamation limitation only after these tests
pass on every supported PostgreSQL major
