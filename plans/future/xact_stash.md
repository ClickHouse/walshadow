# xact stash — generic commit-time raw-record decode

Status: future, extends the commit-time stash in [xact.md](../xact.md) and
[TOAST.md](../TOAST.md)

## Decision

Promote raw-record stash from toast-only decode to a generic xact-buffer
capability: any record on an MVCC-invisible filenode decodes at commit
against a commit-accurate descriptor. Substrate already exists and is
kind-agnostic — `SpillEntry::Raw` (spill v4), the `XLOG_SMGR_CREATE`
marker map, `resolve_stash` — only verdicts, descriptor fidelity, and
naming are toast-shaped. Two new mechanisms are required before ordinary
heaps can decode: a shadow publication fence and durable descriptor
snapshots. Both specified here

## Current gap

`StashOutcome::Skip` fences every non-toast resolution: committed rows
never reach ClickHouse, counted `toast_stash_skipped`. Lost classes:

- `BEGIN; CREATE TABLE; COPY; COMMIT` — entire initial load
- same-xact `TRUNCATE` + reload — every reloaded row (toast side already
  decodes; main tuples skip)

Root cause: `relation_at(rfn, commit_lsn)` imposes a lower replay bound
only. `QueueingRecordSink` decouples decoder workers from shadow replay,
so shadow can apply a later same-filenode `ALTER` before the worker
resolves this commit — lookup then returns future `pg_attribute` shape,
and an `Added` publication would carry future columns. Toast tolerates
this because chunk shape is fixed (`chunk_id, chunk_seq, chunk_data`);
ordinary heaps cannot

## Invariants

1. Shadow may replay past a stash-carrying commit only after that
   commit's descriptor snapshot is durable
2. Decode verdict is a pure function of (stashed records, snapshot);
   re-decode after restart yields byte-identical rows, preserving `_lsn`
   dedup as pure dedup
3. Abort discards stash and tracking; no speculative descriptors are
   minted from catalog WAL
4. Markerless generations fail closed for replicated rels — a partial
   decode is silent row loss, worse than an explicit resnapshot demand
5. Fence holds are bounded by replay catch-up plus one catalog read;
   never coupled to ClickHouse ack

## Fence

Pump-side hold on shadow publication at stash-carrying commits:

- classifier already parses `XLOG_SMGR_CREATE` (Route::ToShadow) and
  heap block filenodes pump-side; mirror the marker set there, plus the
  set of xids whose heap records touch a marker filenode — same
  admission rule as `is_stash_candidate`
- at a tracked xid's commit: forward the commit record's wire bytes,
  queue the record, then park before forwarding successor bytes. Wire
  and archive segments are written by the same pump task, so one hold
  point covers both delivery paths (a parked pump completes no segment,
  so `restore_command` cannot bypass the hold on reconnect)
- decoder worker drains its queued backlog through the commit;
  `resolve_stash`'s `relation_at` waits for replay ≥ commit (bytes
  already delivered pre-park), snapshot is taken and persisted, release
  signal unparks the pump
- abort clears tracking without a park
- deadlock audit: release needs only records ≤ commit, all queued before
  the park; shadow walreceiver applies delivered bytes independent of
  the park
- crash while parked: shadow never received bytes past the commit, so
  restart re-decodes, re-parks, resolves identically
- markers are consumed at resolution (`forget_markers`), so later xids
  touching the same filenode never park — false-positive parks are
  limited to bare CREATEs with no stashed writes, one replay-wait each

Fence lives at the pump feeding shadow, shared by both drain consumers
(pipeline reorder and serial `XactRecordSink`)

## Descriptor snapshots

Ordinary-kind resolutions persist the resolved `RelDescriptor` (plus
relkind and destination mapping identity) keyed
`(top_xid, commit_lsn, rfn)`, fsynced before fence release:

- stored outside the spill dir — spill wipes on boot
  (`SpillStore::clear`), exactly when a re-decode needs the snapshot;
  pruned once the persisted floor passes the commit (re-decode
  impossible past it)
- re-decode path loads the snapshot instead of calling `relation_at`:
  shadow does not rewind across restart, so post-release lookups can see
  arbitrarily later shape
- crash windows: before release, shadow never passed the commit and a
  fresh resolution is accurate; after release, the snapshot file exists
  by invariant 1. A missing/corrupt snapshot when shadow is already past
  the commit is unprovable shape — fail closed, fresh snapshot required
- toast resolutions persist nothing: fixed shape, and the
  rotation/drop discard proof covers lifecycle races

This is the cheap sibling of the durable rule in
[shadow_toast.md](shadow_toast.md): gate shadow on descriptor
durability, not on decoder-work durability — no ack coupling, hold
duration stays one catalog round trip

## Drain decode

- Raw ordinary-heap records decode inside the k-way merge like live
  records: descriptor from `StashOutcome::Heap`, `DecodedHeap` into the
  committed-tuple path, record-LSN ordered — the same-xact TRUNCATE wipe
  barrier orders correctly for free
- resolve toast verdicts before ordinary heap decode: a CREATE that
  makes both a main rel and its toast rel stashes both generations, and
  main-tuple detoast reads the in-xact chunk map the toast decode fills
- insert records whose tuple rides only the FPI decode from the restored
  image via the existing on-page decoder (checkpoint mid-COPY)
- resolution-surfaced `Added` events are stamped at the filenode's
  marker LSN, not commit LSN: the marker precedes every stashed record
  by construction, so catalog-before-tuple ordering creates the CH table
  ahead of its rows. Commit-LSN stamping would sort the event after the
  tuples it gates

## Verdicts

`StashOutcome::{Toast(desc), Heap(desc), Discard}`:

- `Heap` only for rels mapping to a CH destination (pinned/auto-create
  config); non-replicated relkinds and unmapped rels discard, counted by
  reason. Fence still parks for them (pump cannot know mapping); release
  happens at resolution regardless
- markerless resolution of a replicated rel fails closed — converts
  today's silent skip into an explicit fresh-snapshot demand, matching
  the toast posture (`IncompleteToastGeneration`)
- unresolvable post-commit filenodes keep the existing discard proof
  (dropped or rotated away, end-state-neutral under AEL supersession)

## Consumers

- current: toast generations (rewrites, same-xact toast churn)
- this plan: ordinary-heap same-xact CREATE+INSERT and TRUNCATE+INSERT
- prospective: rewrite main-heap FPI walk — `log_newpage` pages of a
  rewriting ALTER carry post-rewrite tuples; decoding them would re-emit
  exact bytes instead of relying on the applicator's CH-side type
  conversion. Needs FPI admission into the stash (today only heap-rmgr
  records stash); out of scope here
- prospective: [two_phase_commit.md](two_phase_commit.md) — a
  PREPARE-durable buffer is the same spill substrate; the
  PREPARE-to-COMMIT-PREPARED restart gap is inherited unchanged

## Observability

- rename `toast_stash_*` → `stash_*`; `stash_decoded` splits by kind,
  `stash_skipped` retires with the fence, discards gain a reason label
- new: fence parks + hold duration, snapshots persisted/loaded/pruned,
  fail-closed count

## Phases

1. Fence + snapshot persistence, ordinary verdict still skips: assert
   snapshot equals `relation_at` under a forced worker-lag ALTER race —
   proves the fence with zero row-path change
2. `Heap` decode at drain: marker-LSN `Added` ordering, chunk-map
   interplay, markerless fail-closed, TRUNCATE barrier ordering
3. Surface generalization: metric renames, promote the
   [xact.md](../xact.md) stash section to kind-neutral wording with
   toast as one consumer, discard-reason taxonomy

## Acceptance

- `BEGIN; CREATE TABLE; COPY` (toasted + plain) `; COMMIT` — table,
  rows, and toast values land; one xact end to end
- same-xact TRUNCATE + reload — exactly the reloaded rows survive the
  wipe barrier
- CREATE + INSERT + `ALTER ADD COLUMN` + INSERT + COMMIT — both batches
  decode at commit shape (tuple natts handles the pre-ALTER batch)
- commit followed immediately by a next-xact same-filenode ALTER under
  forced worker lag — snapshot reflects the commit, ALTER applies
  afterwards as its own event
- crash while parked — restart re-parks and resolves, no divergence
- crash after release, before floor passes the commit — re-decode from
  snapshot is byte-identical
- snapshot removed with shadow past the commit — fail closed
- top-level and subxact abort — no rows, tracking cleared
- checkpoint mid-COPY — FPI-carried tuples decode from the image
- attach mid-xact (markerless) — fail closed for a replicated rel
- unmapped rel CREATE+INSERT — discard counted, fence released at
  resolution

## Rejected shortcuts

- versioned descriptors from catalog WAL decode:
  `XLH_UPDATE_PREFIX_FROM_OLD` elision (PG
  `src/include/access/heapam_xlog.h`) breaks identity on same-page
  pg_class updates, pg_attribute layout is version-sensitive, and the
  overlay duplicates shadow's whole catalog job
- speculative descriptors minted at record time: xid ownership, abort
  discard, and worker-ordering surface — the observe-versus-apply class
- durability-coupled fence (hold shadow until the CH floor passes the
  commit): couples all decode progress to CH latency and outages;
  snapshot durability is the sufficient condition at file-fsync cost
- per-record exact replay gate: reintroduces the pump lockstep
  `QueueingRecordSink` exists to break
- snapshots inside the spill dir: wiped on boot, precisely when needed
- snapshots in ClickHouse: fence release would block on a remote round
  trip and resume would grow a CH read dependency

## Open questions

- snapshot encoding: reuse decoder `RelDescriptor` serialization or a
  minimal `(attnum, name, type oid, attisdropped)` projection?
  [xact.md](../xact.md) rejects spilling descriptors to avoid shape
  duplication; one file per fenced commit changes the cost but not the
  duplication
- `REFRESH MATERIALIZED VIEW` is rewrite-shaped with relkind `m` —
  replicate or discard?
- rewrite main-heap FPI consumer: is applicator-cast versus PG-rewrite
  divergence observable in practice (timestamp/text formatting), enough
  to justify FPI admission?
