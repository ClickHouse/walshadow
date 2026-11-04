# Separate bootstrap ordering from WAL position

Walked rows keep backup start as row version and sequence position. Greenfield
bootstrap resolves deferred TOAST values at heap page LSN or backup start,
whichever is later. Drop unreadable rows only when window replay emitted a
tombstone or newer version for that tuple's slot. Retry other unreadable rows
at backup start; values created and deleted within backup window remain fatal.
See [bootstrap producer](../src/emit/pipeline/bootstrap.rs),
[window replay](../src/backfill/wal_replay.rs), and
[bootstrap visibility](bootstrap.md)

## Per-table and pending loads

Per-table backup loads still read at load version. Object-store loads resolve
deferred values before gap replay recovers chunks or records replacements.
Resolve values after gap replay without breaking checkpoint resume, and collect
replacement evidence during replay. `initial_load = "base_backup"` has no
replay leg: live stream supplies chunks written during backup at positions
above load version. It would also need to supply replacement evidence

Pending rows read values at load version without window coverage. Transactions
still open at handoff can write chunks that committed replay has not mirrored.
Coordinate with [bootstrap visibility](bootstrap.md)

## Evidence limits

Replacement evidence records heap slots deleted or updated by committed window
transactions on routed relations that keep deletes. Append-only routes collect
none, so unreadable values there remain fatal. Keep evidence in memory during
window replay and deferred resolution; rebuild it on resume by replaying window
WAL. Memory grows with window deletes and updates on relations with TOAST

Window page images mirror every tuple at page LSN regardless of `xmax`. Images
taken after a delete commits restore dead chunks above their tombstones.
Replayed replacements outrank affected rows, but after OID wrap a reused value
ID can pick up restored chunks and report a size mismatch. Decide whether to
tombstone or skip image tuples whose deleting transaction committed

Shadow value mode reads at load version. Shadow treats read bound as a replay
floor and replays only through backup end before WAL pump starts. Values
created during backup window are classified as later generations and replaced
with fill values; window replay must emit rows that outrank them. Reading
shadow at backup end needs a ceiling that does not wait for replay past it

Coordinate retention with [TOAST reclamation](shadow_toast.md)

## Completion

Extend [torn-backup coverage](../tests/bootstrap_torn_toast_ch.rs) with
heap slot reuse, TOAST value ID reuse after OID wrap, forced mirror merges
before deferred reads, rows with several external values, and transactions crossing
handoff. Repeat across direct bootstrap, object-store bootstrap, and per-table
backup loads. Restart around deferred resolution and replacement
acknowledgement. Assert exact values, deletion state, replacement priority, and
durable resume position
