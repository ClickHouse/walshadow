# 05 — Subxact lineage + `ROLLBACK TO SAVEPOINT`

Closes [PLAN.md §"Known correctness gaps" #2](../PLAN.md#known-correctness-gaps).
Phase 6 ships top-level-xact-only; `XLOG_XACT_ASSIGNMENT` is ignored.
`ROLLBACK TO SAVEPOINT` mid-xact still lands every pre-savepoint
write in CH at commit. ORMs that wrap each statement in an implicit
savepoint (Django's atomic, Hibernate, Rails) emit ghost rows under
exception paths

## Why

PG subxacts (savepoints) get their own xids. Heap writes inside the
savepoint carry the subxid; the assignment record
`XLOG_XACT_ASSIGNMENT` (rmgr `RM_XACT_ID`, info `0x50`) tells the
standby "subxid S belongs to top xid T". At commit, PG either:

- `XLOG_XACT_COMMIT` carrying the list of committed subxids — all
  writes apply
- `XLOG_XACT_ABORT` carrying the list of aborted subxids — only
  those subxids' writes discard, the top xact's other writes still
  apply
- `ROLLBACK TO SAVEPOINT` writes an `XLOG_XACT_ABORT` for the
  rolled-back subxid(s) while the top xact remains open; pre-
  savepoint subxids' writes survive

Today's [`xact_buffer.rs`](../../src/xact_buffer.rs) keys on
`xact_id` from the heap record header. For a write inside subxact S
of top T, the buffer creates an entry keyed on S, separate from T.
At commit, `XLOG_XACT_COMMIT` arrives keyed on T with a subxids
list — Phase 6's decoder ignores the subxids list and only flushes
T's buffer, leaving S's entries orphaned. They eventually evict via
the largest-xact-first eviction path, silently losing the writes

The mirror failure: standalone `ROLLBACK TO SAVEPOINT` emits
`XLOG_XACT_ABORT` for S. Today's decoder calls `buffer.abort(S, lsn)`
which drops S's buffer. So far correct — except T's buffer doesn't
know that S aborted, so any pre-savepoint writes T issued before
the savepoint also stay buffered. (For top-xact ABORT only, the
discard path is correct.)

Two pieces to fix:

1. **Subxid → top-xid mapping.** Build it from `XLOG_XACT_ASSIGNMENT`
2. **Commit/abort drain crosses both maps.** Commit T flushes T's
   buffer plus every S ∈ subxids; abort S drops only S's entries

## Surface

New `SubxactTracker` keyed on `subxid -> top_xid`
([`src/xact_buffer.rs`](../../src/xact_buffer.rs)):

```rust
struct SubxactTracker {
    parent: HashMap<u32, u32>,           // subxid -> top_xid
}

impl SubxactTracker {
    fn assign(&mut self, top_xid: u32, subxids: &[u32]);
    fn top_for(&self, xid: u32) -> u32;  // returns xid itself if no parent
    fn forget_tree(&mut self, top_xid: u32);  // on top commit/abort
}
```

`XactRecordSink::on_record`
([`xact_buffer.rs:706-749`](../../src/xact_buffer.rs)) gains
`XLOG_XACT_ASSIGNMENT` (info `0x50`) handling:

```rust
const XLOG_XACT_ASSIGNMENT: u8 = 0x50;

// In on_record match:
XLOG_XACT_ASSIGNMENT => {
    let subxids = parse_xact_assignment(&record.parsed.main_data);
    self.subxact_tracker.lock().await.assign(record.parsed.header.xact_id, &subxids);
}
```

`xl_xact_assignment` payload (`access/xact.h`):

```c
typedef struct xl_xact_assignment {
    TransactionId xtop;        // top xid this assignment applies to
    int           nsubxacts;
    TransactionId xsub[FLEXIBLE_ARRAY_MEMBER];
} xl_xact_assignment;
```

Note that `xtop` overrides the record header's `xact_id` — PG writes
the assignment record under the top xact, so the header xid is already
the top. Either is fine; the payload's `xtop` is canonical

`BufferingDecoderSink::on_record` keys per-tuple buffering on the
header's `xact_id` (unchanged). The collapse happens at commit time

`XactBuffer::commit(xid, ...)` switches to:

```rust
pub async fn commit(
    &mut self,
    top_xid: u32,
    commit_ts: i64,
    commit_lsn: u64,
    subxids: &[u32],     // from xl_xact_commit
    catalog: &ShadowCatalog,
    observer: &mut dyn TupleObserver,
) -> Result<()> {
    // Drain the top_xid's buffer plus every subxid's buffer in WAL
    // order across them all. New behaviour: walk a merge-iterator
    // across (top_xid + subxids) buffers, yielding tuples in
    // commit-WAL-LSN order
    ...
}
```

`xl_xact_commit` payload carries the subxact list inline. Today's
[`parse_xact_time`](../../src/xact_buffer.rs) only reads the
timestamp; extend it to a full `XactCommitPayload` parse covering:

- `xact_time` (i64 — already extracted)
- `nsubxacts` + `subxacts[]` (gated on `XACT_XINFO_HAS_SUBXACTS` flag
  in the record's `xinfo`)
- (other fields — invals, relfilenodes-to-drop, gid for prepared —
  remain ignored for now)

`XactBuffer::abort(xid, abort_lsn, subxids)`: top abort drops the
top's buffer + every subxid's buffer; standalone subxact abort
(`XLOG_XACT_ABORT` carrying a subxid that is not the header's
xact_id — distinguished by the `xinfo` flag layout) drops only the
named subxid's buffer

`xl_xact_abort` carries the subxact list under the same `xinfo` gate
(`XACT_XINFO_HAS_SUBXACTS`); reuse the commit parser's subxact
extractor

## Drain ordering

Per-subxact entries land in their own buffer slot, each carrying the
WAL LSN at which the heap record arrived. At top-xact commit, the
drain must walk top + every named subxact in *commit-WAL-order* —
otherwise a tuple from subxact S1 (LSN 100) would emit after a tuple
from top T (LSN 200) when the operator intent is "100 came first"

Simple shape: collect all per-xid `Vec<CommittedTuple>` references,
merge-sort by source_lsn at drain start, emit in sorted order. The
buffers are already ordered internally; merge is k-way over k
buffers (k = 1 + nsubxacts). For the typical k <= 4 case, a simple
linear-scan merge is cheap

## Spill interaction

[`spill.rs`](../../src/spill.rs)'s per-xid `xid-<xid>-<first_lsn>.bin`
already names files by xid — subxacts spill independently. Drain
walks each xid's spill file in turn (top + subxids), same merge-sort
shape but with disk reads. Largest-xact-first eviction stays keyed
on individual xid sizes; this is correct for the orphan-prevention
goal since the top xact's commit will pull from spill files for
every subxid

## Tests

Unit:
- `SubxactTracker` round-trip: assign(T, [S1, S2]), top_for(S1) ==
  T, forget_tree(T) clears
- `parse_xact_commit_payload` extracts subxacts under
  `XACT_XINFO_HAS_SUBXACTS`
- `XactBuffer::commit` with one subxact: tuples drain in source_lsn
  order across the two buffers

Integration (`tests/phase14_subxact.rs`):
- `BEGIN; INSERT R1; SAVEPOINT s; INSERT R2; ROLLBACK TO SAVEPOINT
  s; INSERT R3; COMMIT;` — CH must show R1 + R3, no R2
- `BEGIN; INSERT R1; SAVEPOINT s; INSERT R2; RELEASE SAVEPOINT s;
  COMMIT;` — CH must show R1 + R2 (release commits the subxact into
  the top)
- ORM-style: `BEGIN; INSERT R1; SAVEPOINT s1; INSERT R2; ROLLBACK
  TO SAVEPOINT s1; SAVEPOINT s2; INSERT R3; ROLLBACK TO SAVEPOINT
  s2; INSERT R4; COMMIT;` — CH shows R1 + R4
- Top abort with subxacts: `BEGIN; INSERT R1; SAVEPOINT s; INSERT
  R2; ROLLBACK;` — CH shows neither

## Size

~320 LOC product + ~160 LOC test. The bulk lives in
`xl_xact_commit` / `xl_xact_abort` payload parsing (which carries
more than just the subxact list — invals, relfilenodes-to-drop, gid
for prepared) and the merge-iterator drain

## Risks

- **`XACT_XINFO_HAS_SUBXACTS` layout drift across PG majors.** Pin
  unit tests against fixtures from PG 16 / 17 / 18, same fixture
  shape as item 02's MULTI_INSERT cross-major check
- **Subxact assignment **after** the first heap write of the
  subxact.** PG batches assignment records — the first 64 (per
  `PGPROC_MAX_CACHED_SUBXIDS`) subxacts may not have an explicit
  ASSIGNMENT record at all; instead the commit/abort record's
  subxacts list is authoritative. Walshadow can't tell at heap-
  write time that subxid S belongs to top T; the per-tuple xact
  buffer keys on S regardless, and the collapse happens at commit.
  No mid-stream mapping needed — the parent map is a hint for early
  collapse / eviction, not a correctness gate
- **Long-lived top xact with many subxacts.** Eviction is per-xid;
  if memory pressure forces eviction of a subxact's buffer while
  the top is still open, the subxact's writes spill to disk and
  drain from there at top commit. No change to eviction policy
- **Implicit savepoints from `BEGIN` + `EXCEPTION WHEN` in PL/pgSQL.**
  Each EXCEPTION-protected block runs in an implicit subxact. Same
  shape as ORM savepoints; covered by the test surface above
