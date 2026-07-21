# xact stash — generic commit-time raw-record decode

Status: future, extends the commit-time stash in [xact.md](../xact.md) and
[TOAST.md](../TOAST.md)

## Decision

Promote raw-record stash from toast-only decode to a generic xact-buffer
capability: any record on an MVCC-invisible filenode decodes at commit
against a commit-accurate descriptor. Substrate already exists and is
kind-agnostic — `SpillEntry::Raw`, the `XLOG_SMGR_CREATE` marker map,
`resolve_stash` — only verdicts, descriptor fidelity, and naming are
toast-shaped

Three mechanisms carry the promotion:

1. shadow publication fence at commit record *end*: result-bearing,
   force-flushed, live-wire only
2. durable per-commit `ResolutionBundle`: one artifact both fence
   release and crash re-decode depend on
3. resolve-once dataflow: every heap reaching the emitter travels as
   `ResolvedHeap` carrying descriptor and route; nothing downstream
   re-resolves. Live and stash paths converge on the same envelope, so
   future producers (rewrite FPI, [two_phase_commit.md](two_phase_commit.md))
   plug in without new plumbing

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

A second, quieter gap: today resolution context evaporates immediately.
`DecodedHeap` carries only rfn/xid/LSN/op/tuples, so pipeline decode
re-resolves the relation from the live catalog, re-reads the live
mapping, and detoast re-resolves again. Each re-resolution is a window
where a later `ALTER`, cache eviction, or restart substitutes future
shape — a commit-time snapshot that protects only the first parse
protects nothing. Fixing the descriptor race requires killing the
re-resolutions, not adding one more lookup

## Dataflow

```
source wire ─▶ pump/classifier ──bytes──▶ shadow (wire + archive segs)
                    │                        ▲ hold at tracked commit END
                    │ FenceRequest           │ release on Ok(bundle)
                    ▼                        │
              resolver lane ──▶ ResolutionBundle (fsync) ─┘
                    │                 durable, per commit
                    ▼
              (bundle indexed by top_xid)
                    │
decoder workers ─▶ xact buffer ─▶ drain merge ─▶ ResolvedHeap ─▶ emit
                   (spill v5)      raw decode     {decoded,
                                   via bundle      descriptor: Arc,
                                                   route}
```

Each boundary has one typed artifact; each artifact is produced once
and only consumed downstream:

- `Record` carries `end_lsn` alongside start LSN
- `FenceRequest { top_xid, subxids, commit_start, commit_end,
  candidates }` from pump to resolver
- `ResolutionBundle` from resolver to fence release, drain decode, and
  crash replay
- `ResolvedHeap` from drain decode to encoder/emitter

## Invariants

1. Shadow may replay through a fenced commit, but receives no bytes
   past the commit record end until that commit's `ResolutionBundle`
   is durable
2. Decode verdict is a pure function of (stashed records, bundle);
   re-decode after restart yields byte-identical rows, preserving
   `_lsn` dedup as pure dedup
3. Abort discards stash and tracking; no speculative descriptors are
   minted from catalog WAL
4. Generations without positive resolution proof fail closed for
   replicated rels — a partial decode is silent row loss, worse than
   an explicit resnapshot demand
5. Fence holds are bounded by replay catch-up plus one batched catalog
   read on a dedicated lane; never coupled to ClickHouse ack, drain
   backlog, or queued barrier work
6. Downstream of resolution, no live catalog or mapping read for a
   bundle-backed heap; descriptor and route travel with the row

## Record end LSN

`Record::source_lsn` is record start. `pg_last_wal_replay_lsn()`
reports `lastReplayedEndRecPtr` — end of last applied record (PG
`src/backend/access/transam/xlogrecovery.c`). Comparing replay against
commit *start* releases early: previous record can end exactly at
commit start before the commit applies, snapshot lookup still sees the
creating xact as invisible

`Record` grows `end_lsn`; every fence-side comparison uses it:

- replay wait: `replay >= commit_end`
- publication boundary: bytes ≤ commit_end delivered, successor bytes held
- crash test "safe to fresh-resolve": `shadow replay <= recorded fence end`
- missing-bundle fail-closed comparison

Record-start stays `commit_lsn` for row versioning and cursor semantics

## Fence

Pump-side hold on shadow publication at stash-carrying commits:

- classifier already parses `XLOG_SMGR_CREATE` (Route::ToShadow) and
  heap block filenodes pump-side; mirror the marker set there, plus the
  set of xids whose heap records touch a marker filenode — same
  admission rule as `is_stash_candidate`
- protocol at a tracked xid's commit, in order:
  1. register a result-bearing fence waiter (before the commit can
     reach any worker)
  2. forward the commit record's wire bytes, append the commit to the
     decoder queue
  3. force `QueueingRecordSink::flush()` — normal flush runs only
     after the whole source chunk returns, so parking without a forced
     flush strands the commit in the pump buffer and deadlocks
  4. submit `FenceRequest` to the resolver lane, await the fence
     `Result`
  5. on `Ok`, resume successor-byte publication; on `Err`, terminate
     the pump — worker error, panic, catalog timeout, bundle-write
     failure, and channel closure all wake the waiter with `Err`
     (a parked pump has no next call for deferred error surfacing)
- abort clears tracking without a park
- wire and archive segments are written by the same pump task, so one
  hold point covers both delivery paths; a parked pump completes no
  segment, so `restore_command` cannot bypass the hold on reconnect
- crash while parked: shadow never received bytes past commit end, so
  restart re-decodes, re-parks, resolves identically
- markers are consumed at resolution (`forget_markers`), so later xids
  touching the same filenode never park — false-positive parks are
  limited to bare CREATEs with no stashed writes, one replay-wait each

Fence lives at the pump feeding shadow, shared by both drain consumers
(pipeline reorder and serial `XactRecordSink`)

### Live wire required

Archive segments publish whole, after drain and segment flush; a fence
cannot hold mid-segment and later amend the segment with original
successor bytes. Fenced stash therefore requires an active walreceiver:
reject archive-only configuration (`--walsender-connect-timeout=0`) at
startup. Archive remains a post-release fallback only. Operational
consequence: shadow wire loss during a fence recovers by live
reconnect, not by the archive segment containing that commit

## Resolver lane

Resolution runs on a dedicated pre-drain lane, not inside drain commit:
the drain path runs `flush_due_retires` (which can call ClickHouse via
`retire_mirror`) and sits behind all older queue entries — DDL,
truncate barriers, downstream backpressure. Routing resolution through
it would couple fence holds to ClickHouse outages, violating
invariant 5

The lane consumes `FenceRequest`s:

1. wait `shadow replay >= commit_end` on a dedicated shadow connection
   (or cache-bypassing fetch — worker-position invalidation state can
   make the shared cache stale)
2. resolve all candidates in one batched catalog read: single query
   over the candidate rfn set joining pg_class/pg_attribute, not the
   serial per-candidate class + replident + attributes round trips of
   `relation_at` today
3. build the `ResolutionBundle`, write durable, release the fence

Drain-side `resolve_stash` becomes a bundle lookup by top_xid — no
catalog access at drain time

## ResolutionBundle

One versioned, checksummed file per fenced commit — write temp, fsync,
rename, fsync directory. Single atomic unit avoids partially persisted
per-rfn sets and gives fence release exactly one durable condition.
Contents:

- top xid, commit start LSN, commit end LSN
- per-candidate outcome, keyed by rfn:
  - ordinary: full `RelDescriptor` (see fidelity below), marker LSN
  - toast: toast relation OID, marker LSN — fixed chunk shape removes
    the descriptor need, not the identity need. Crash after release but
    before chunks drain, followed by shadow replaying a later
    drop/rewrite, must still resolve chunk ownership from the bundle,
    or the main row cannot detoast
  - discard: reason, with the positive proof that justified it
- schema-event intents: first resolution of a new rel emits
  `SchemaEvent::Added` through a volatile channel today; crash after
  release but before apply loses it, and bundle-based reload bypasses
  `relation_at` so nothing regenerates it. The intent is part of the
  bundle; drain replays it idempotently at the marker LSN
- routing input: config-version reference for the commit position
  (see verdicts)
- completion marker

Bundle lives beside the durable cursor and toast-retire ledger under a
nonvolatile filename prefix: `SpillStore::clear` removes only
`xid-*.bin` and `toastbody-*.bin`, so boot wipe does not touch it, and
cursor + bundle share one filesystem for ordering

Load rejects unknown versions and checksum failures — fail closed, not
best-effort parse

### Retention

Restart begins at `align_down(emitter_ack_lsn, WAL_SEG_SIZE)`, not at
the exact persisted commit LSN, so "floor passed the commit" is not
prune-safe — the restart still rereads the commit's whole segment.
Same rule the toast-retire ledger already uses:

```
cut = align_down(durably persisted resume-safe emitter floor)
prune bundle iff commit_start_lsn < cut
```

Read the floor only after the crash-safe cursor write succeeds;
in-memory ack advancement is insufficient

Missing bundle on restart: fresh resolution is safe iff shadow replay
≤ recorded fence end and no durable release evidence exists (crash
while parked). Shadow past the fence end with no bundle is unprovable
shape — fail closed. Explicit `--start-lsn` rewind into pruned range:
reject unless bundles are retained

## ResolvedHeap

The envelope every heap travels in from drain decode to emit:

```rust
struct ResolvedHeap {
    decoded: DecodedHeap,
    descriptor: Arc<RelDescriptor>,
    route: Option<Arc<TableMapping>>,
}
```

Exact shape may evolve; requirements are fixed:

- raw tuple parse, detoast, DDL event, routing, and encoder-plan
  construction all read the same envelope
- no catalog or live-mapping re-resolution downstream — the current
  hops (pipeline decode re-resolving relation and mapping, xact-buffer
  detoast re-resolving descriptor) are removed, not bypassed
- spill and batch memory accounting includes retained context

Live path constructs the envelope at decode time from the
epoch-validated cache; stash path from the bundle. Downstream is
source-blind — one code path, and future producers (rewrite FPI pages,
prepared xacts) join by constructing the same envelope

Per-filenode caches (`relation_at_pooled`) are not a substitute: a
later inline lookup can replace the same-filenode entry before a
pooled worker reads it. Ownership travels with the row or not at all

## Raw substrate repairs

Two structural changes before ordinary heaps decode correctly:

- spill v5 persists xid in `RawRecord`: `to_xlog_record` leaves
  `xact_id` zero today, and the emitter writes header xid into `_xid` —
  every generic raw-decoded row would emit `_xid = 0`, breaking
  byte-identical replay and user-visible metadata
- pending decoded-heap queue in `MergedDrain`: one raw
  `XLOG_HEAP2_MULTI_INSERT` yields multiple heaps, `fold_raw` currently
  folds one entry with no queue. All tuples of one raw record emit
  before the merge advances past that record LSN; event-before-heap
  tie-break at equal LSN preserved; queued decoded memory accounted

## Drain decode

- Raw ordinary-heap records decode inside the k-way merge like live
  records: descriptor from the bundle, `ResolvedHeap` into the
  committed-tuple path, record-LSN ordered — the same-xact TRUNCATE
  wipe barrier orders correctly for free
- resolve toast verdicts before ordinary heap decode: a CREATE that
  makes both a main rel and its toast rel stashes both generations, and
  main-tuple detoast reads the in-xact chunk map the toast decode fills
- bundle-carried `Added` intents are stamped at the filenode's marker
  LSN, not commit LSN: the marker precedes every stashed record by
  construction, so catalog-before-tuple ordering creates the CH table
  ahead of its rows. Commit-LSN stamping would sort the event after
  the tuples it gates

### TRUNCATE

Existing-table TRUNCATE + reload works once new-filenode raw rows
decode: the truncate barrier uses the old, visible descriptor

Same-xact CREATE + INSERT + TRUNCATE + INSERT does not: the relation is
invisible when `handle_truncate` resolves by OID, and the TRUNCATE
record carries OIDs, not filenodes, so marker admission never sees it.
Defer unresolved TRUNCATE OIDs to commit-time resolution via the
bundle, preserving the truncate record LSN for barrier ordering

### Image-only decode

Ordinary INSERT/COPY retains tuple bytes with `REGBUF_KEEP_DATA` under
`wal_level=logical` ([decoder.md](../decoder.md)); a checkpoint
mid-COPY attaches an FPI but block data still carries the tuple, so
that case exercises the normal path. The genuine image-only source is
`HEAP_INSERT_NO_LOGICAL` — the rewrite path (PG
`src/backend/access/heap/rewriteheap.c`), out of scope here. Until
rewrite FPI admission lands, an image-only ordinary record is a
fail-closed error, tested with a synthetic record carrying an image
and no block data. The eventual ordinary image helper (distinct from
the toast-specific one returning `StashedToastOp`) constructs
`DecodedHeap` preserving original xid and operation, malformed line
pointer fails closed

## Verdicts

`StashOutcome::{Toast(desc), Heap(desc), Discard(proof)}`:

- `Heap` for every supported ordinary relkind, independent of current
  mapping. Routing is not a resolution-time property: config events
  apply inside drain by WAL position ([config.md](../config.md)), so a
  config-table row in the same xact can add the mapping ahead of
  trailing stashed rows, opt-in mid-xact splits route/discard by
  record position, and auto-created rels gain their mapping only when
  the `Added` event applies. A per-rfn mapped/unmapped verdict cannot
  represent any of these. Drain barriers decide the route per record;
  unmapped-at-position rows discard there, counted by reason. Fence
  still parks for unmapped rels (pump cannot know mapping); release
  happens at resolution regardless
- byte-identical restart requires routing input be replay-stable: the
  bundle records a config-version reference, and replayed drain applies
  the same ordered config stream. Non-WAL config changes (SIGHUP, file
  edit) landing between dispatch and crash replay are the residual
  hazard — see open questions
- markerless resolution of a replicated rel fails closed — converts
  today's silent skip into an explicit fresh-snapshot demand, matching
  the toast posture (`IncompleteToastGeneration`)
- unresolvable-at-commit candidates discard only on positive proof:
  created-and-dropped within the xact, or the superseding generation
  is itself captured completely. Rotated-away is not neutral — with
  main-heap rewrite FPI out of scope, `BEGIN; TRUNCATE t; INSERT;
  ALTER TABLE t ... TYPE ...; COMMIT` leaves the intermediate
  generation unresolvable, the final generation uncaptured, and a
  discard would apply the CH truncate while emitting nothing. Without
  proof: fail closed
- mapped relkind `m` is unsupported, fail closed: `REFRESH
  MATERIALIZED VIEW` is replacement-shaped, a discard leaves a stale
  destination

## Descriptor fidelity

Bundles serialize the full `RelDescriptor`/`RelAttr`, versioned — not
a minimal `(attnum, name, type oid, dropped)` projection. Tuple decode
and DDL/routing reproduction need type len/align/byval/storage, typmod,
missing value, dropped flag, nullability, type name, replica identity
and key attnums, persistence, relkind, namespace and relation identity.
Anything less re-derives shape from a live catalog, which is the race
this plan exists to close

Dropped columns expose a live fidelity gap to fix on the way: PG keeps
`attlen`/`attalign` on dropped attributes while zeroing `atttypid`
(PG `src/backend/catalog/heap.c`), but the catalog query inner-joins
`pg_type`, so dropped attributes vanish from the descriptor instead of
consuming their physical tuple bytes. Fetch dropped attributes without
requiring `pg_type`, read physical fields from `pg_attribute`
directly, keep one descriptor element per positive attnum including
dropped positions

## Consumers

- current: toast generations (rewrites, same-xact toast churn)
- this plan: ordinary-heap same-xact CREATE+INSERT and TRUNCATE+INSERT
- prospective: rewrite main-heap FPI walk — `log_newpage` pages of a
  rewriting ALTER carry post-rewrite tuples; decoding them would
  re-emit exact bytes instead of relying on the applicator's CH-side
  type conversion, and would give unresolvable-generation candidates
  their positive proof. Needs FPI admission into the stash (today only
  heap-rmgr records stash)
- prospective: [two_phase_commit.md](two_phase_commit.md) — a
  PREPARE-durable buffer is the same spill substrate producing the same
  `ResolvedHeap` envelope; the PREPARE-to-COMMIT-PREPARED restart gap
  is inherited unchanged

## Observability

- rename `toast_stash_*` → `stash_*`; `stash_decoded` splits by kind,
  `stash_skipped` retires with the fence, discards gain a reason label
- fence: parks, hold duration, releases by result (ok/error)
- resolver lane: queue depth, batched-read duration
- bundles: persisted/loaded/pruned counts and bytes
- fail-closed count by reason (markerless, unresolvable-no-proof,
  missing-bundle, checksum, image-only, relkind)

## Phases

0. Transport and fence, no row-path change: `end_lsn` on `Record`,
   force-flush result-bearing fence protocol, live-wire startup
   requirement, resolver lane, bundle persist/load/prune. Ordinary
   verdict still skips; assert bundle descriptor equals `relation_at`
   under a forced worker-lag ALTER race — proves fence and bundle with
   zero row-path change
1. Ordinary INSERT and MULTI_INSERT decode: `ResolvedHeap` end to end,
   spill v5 xid preservation, multi-insert pending queue, replayable
   `Added` at marker LSN
2. Interplay: toast/main chunk-map ordering, ordered-config routing at
   drain, existing-table TRUNCATE, deferred TRUNCATE-OID resolution
3. Hardening: fail-closed unresolved generations, dropped-column
   descriptor fidelity, crash matrix
4. Surface generalization: metric renames, promote the
   [xact.md](../xact.md) stash section to kind-neutral wording with
   toast as one consumer, discard-proof taxonomy

Separate future work: main rewrite FPI admission, materialized-view
refresh

## Acceptance

Fence protocol:

- decoder_batch_size > 1; commit mid source CopyData chunk
- worker error before commit; error during bundle write; worker panic —
  each wakes the fence waiter with `Err`, pump terminates
- false-positive tracked commit (bare CREATE, no stashed writes) —
  parks once, releases at resolution
- archive-only configuration rejected at startup when fence enabled

Retention:

- floor one byte past commit, same segment — bundle retained
- floor at next segment boundary — bundle prunable
- crash between cursor fsync and bundle deletion; between deletion and
  directory fsync
- `--start-lsn` rewind into pruned range rejected

Decode:

- `BEGIN; CREATE TABLE; COPY` (toasted + plain) `; COMMIT` — table,
  rows, toast values land, `_xid` matches source; one xact end to end
- same-xact TRUNCATE + reload — exactly the reloaded rows survive the
  wipe barrier
- CREATE + INSERT + TRUNCATE + INSERT in one xact — only post-truncate
  rows land
- CREATE + INSERT + `ALTER ADD COLUMN` + INSERT + COMMIT — both batches
  decode at commit shape (tuple natts handles the pre-ALTER batch)
- multi-insert raw record — every tuple emits before merge advances
- commit followed by next-xact same-filenode ALTER under forced worker
  lag — bundle reflects the commit, ALTER applies afterwards as its own
  event
- descriptor with dropped columns — physical bytes consumed, values
  correct

Crash matrix:

- crash while parked — restart re-parks and resolves, no divergence
- crash after release, before floor passes the commit — bundle
  re-decode is byte-identical, including `Added` replay and toast
  identity
- bundle removed with shadow past fence end — fail closed
- bundle checksum failure — fail closed

Fail closed:

- attach mid-xact (markerless) for a replicated rel
- TRUNCATE + reload + rewriting ALTER in one xact — no discard, no
  partial emit
- mapped matview refresh
- synthetic image-only ordinary record

Routing:

- unmapped rel CREATE+INSERT — rows discard at drain position, counted
- config-table opt-in mid-xact — pre-row discards, post-row routes
- top-level and subxact abort — no rows, tracking cleared

## Rejected

- fence keyed on commit record start: `pg_last_wal_replay_lsn` reports
  record end; a start comparison releases before the commit applies
- park inside `on_record` without forced flush: commit strands in the
  pump buffer, decoder never sees it, deadlock
- notification-only fence release: worker errors surface on the next
  pump call, which a parked pump never makes
- resolution inside drain commit: sits behind `flush_due_retires` and
  arbitrary queue backlog — couples shadow publication to ClickHouse
- archive-only delivery under fence: segments publish whole, cannot
  hold mid-segment
- per-rfn mapped/unmapped verdict at resolution: contradicts ordered
  config apply, cannot express mid-xact opt-in or auto-create
- discard of unresolvable generations without proof: silent loss when
  the superseding generation is uncaptured (rewrite FPI out of scope)
- minimal descriptor projection: cannot decode tuples (len/align/byval,
  missing values, dropped positions) nor reproduce DDL/routing
- checkpoint-mid-COPY as the image-only acceptance: `REGBUF_KEEP_DATA`
  keeps tuple bytes in block data, the image path never executes
- versioned descriptors from catalog WAL decode:
  `XLH_UPDATE_PREFIX_FROM_OLD` elision (PG
  `src/include/access/heapam_xlog.h`) breaks identity on same-page
  pg_class updates, pg_attribute layout is version-sensitive, and the
  overlay duplicates shadow's whole catalog job
- speculative descriptors minted at record time: xid ownership, abort
  discard, and worker-ordering surface — the observe-versus-apply class
- durability-coupled fence (hold shadow until the CH floor passes the
  commit): couples all decode progress to CH latency and outages;
  bundle durability is the sufficient condition at file-fsync cost
- per-record exact replay gate: reintroduces the pump lockstep
  `QueueingRecordSink` exists to break
- bundles inside the spill dir wipe set or in ClickHouse: wiped exactly
  when needed, or fence release blocks on a remote round trip

## Open questions

- routing replay stability for non-WAL config: persist the effective
  route projection per dispatched segment, or pin a durable config
  version across the replayed range and defer SIGHUP-applied mapping
  changes to a recorded LSN? Config-table-sourced changes are already
  WAL-ordered and safe
- rewrite main-heap FPI consumer: is applicator-cast versus PG-rewrite
  divergence observable in practice (timestamp/text formatting), enough
  to justify FPI admission? It is also the missing positive proof for
  rewrite-superseded generations
