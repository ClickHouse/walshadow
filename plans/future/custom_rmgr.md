# Custom resource manager replay interpose

Status: future throughput optimization

Refines redo-interpose phase in
[PLAN_EXTENSION.md](../../PLAN_EXTENSION.md). Depends on
[PLAN_PENDING_XACT.md](../../PLAN_PENDING_XACT.md) command-boundary capture,
one descriptor assembler, durable descriptor-log promotion at commit, and
unified resume floor

## Objective

Replace transport starvation at `XLOG_XACT_INVALIDATIONS` boundaries with an
exact redo callback:

1. arm boundary before filtered bytes reach shadow
2. rewrite only record's resource manager id, preserving length and body
3. let WAL receipt and filtering continue
4. stop startup process inside custom redo callback at boundary
5. push boundary arrival to daemon, capture pending catalog state, publish
   pending coverage, then release redo

Keep current successor-byte hold as fallback. Treat custom redo as latency
optimization, never sole correctness mechanism

Commit and abort records stay under built-in transaction resource manager.
Their redo changes transaction state and cannot be replaced. Existing commit
hold remains final durability boundary for timeline promotion

## Why

Pending capture holds once per dirty transaction command boundary. Current
hold sends boundary record, withholds every successor byte, polls standby
status at 20 ms cadence, captures, then resumes pump. Sequence is exact
because replay cannot advance past bytes shadow never received, but every
boundary stalls:

- source WAL consumption
- source keepalive handling
- shadow WAL receipt
- filtered archive assembly
- decoder queue ingress

A transaction with hundreds of DDL commands pays poll and status propagation
hundreds of times. Custom redo removes fixed transport delay and lets WAL
queue behind startup process while catalog capture runs

P3 improves ingress throughput and liveness, not catalog correctness. Current
hold already pins replay exactly. Stale status can delay release but cannot
release early while successor WAL remains absent

## Invariants

1. No decoder record after boundary `L` becomes visible to decode worker until
   pending coverage or explicit degradation for `L` is installed
2. No commit drains until pending entries are promoted into durable descriptor
   batch or existing fail-closed path wins
3. Resume floor never passes unresolved boundary or uncommitted transaction
4. Filter never emits custom rmgr record unless shadow acknowledged matching
   live arm
5. Unarmed custom rmgr replay is a no-op, covering crash recovery and retained
   archive replay after original capture
6. Missing, stale, failed, or timed-out interpose state selects byte hold or
   pending-capture degradation, never guessed catalog state
7. Ahead WAL, armed boundaries, queued records, and archive publication stay
   bounded
8. `ShadowStart::External` never receives custom record by default

## Scope

Interpose only `XLOG_XACT_INVALIDATIONS` records emitted at command end under
`wal_level=logical`

PostgreSQL transaction redo ignores these records. Previous catalog heap
records have already applied when callback starts, while invalidation record
itself has no physical redo effect. Blocking inside callback therefore exposes
catalog page state at command boundary without replacing required PostgreSQL
redo

Do not interpose:

- `XLOG_XACT_COMMIT`
- `XLOG_XACT_ABORT`
- prepared transaction records
- assignment records
- clean-xact invalidation records with no pending capture work
- any record sent to shadow lacking negotiated capability

## Record rewrite

Preserve:

- `xl_tot_len`
- `xl_xid`
- `xl_prev`
- `xl_info`
- resource-manager body
- page layout and record alignment

Replace `xl_rmid` with reserved walshadow custom id, then recompute record CRC
using existing rewrite machinery. Same length preserves every following LSN

Validate before rewrite:

- built-in rmgr is transaction
- op is `XLOG_XACT_INVALIDATIONS`
- record has no block references
- parsed body is structurally valid for negotiated PostgreSQL major
- filter classified boundary as pending-capture command

Filtered manifest keeps original rmgr and info and marks entry
`Interposed`. Archive reader must recover original classification from
manifest. Custom record without matching manifest entry is corruption for
daemon replay, not an ignorable unknown record

Reserve stable custom rmgr id before default enablement. Experimental id is
development-only

## Capability and activation

Register custom rmgr from `_PG_init` whenever module appears in
`shared_preload_libraries`, independent of socket enablement. Retained custom
WAL may need callback even when live interpose is disabled

Extend `HELLO` with:

- `WS_CAP_REDO_INTERPOSE`
- registered custom rmgr id
- shared-state protocol version
- maximum arm-ring capacity

Activate per shadow session only after exact protocol, projection, PostgreSQL
major, custom id, and capability match

For daemon-owned shadow:

- use interpose after successful session open
- fall back to current byte hold on any arm failure
- keep module preloaded until every retained custom record ages out

For external shadow:

- default to no rewrite
- require explicit operator opt-in plus successful capability negotiation
- never infer support from socket reachability alone

Preflight refuses config removing module while archive or manifest reports
retained `Interposed` entries

## Shared memory

Request fixed-size addin shared memory during preload. Keep one session header
and bounded ring of boundary slots

Session header:

```text
protocol
session_generation
owner_nonce
state: closed | open | draining
head
tail
capacity
worker_proc
startup_proc
```

Boundary slot:

```text
generation
timeline
record_start_lsn
record_end_lsn
xid
state: free | armed | reached | scanning | released | aborted | timed_out
result
```

Use generation plus exact LSN to prevent ABA after reconnect, ring reuse, or
WAL resend. Protect state transitions with extension LWLock or spinlock, wake
processes through latches or condition variables. Never sleep while holding
state lock

One startup process consumes slots in LSN order. Multiple slots let pump arm
future command boundaries while replay waits at first. Full ring applies
backpressure to pump

## Control channel

Use dedicated bridge connection for bidirectional interpose control. Existing
request connections continue serving `SCAN`, `DECODE`, and `REPLAY_LSN`

Control messages:

| direction | message | purpose |
|---|---|---|
| daemon → worker | `INTERPOSE_OPEN` | claim single session, establish generation |
| daemon → worker | `INTERPOSE_ARM` | append boundary slot before wire publication |
| worker → daemon | `INTERPOSE_REACHED` | push startup arrival, no poll |
| daemon → worker | `INTERPOSE_RELEASE` | allow successful boundary callback to return |
| daemon → worker | `INTERPOSE_ABORT` | allow failed or degraded boundary callback to return |
| either | `INTERPOSE_CLOSE` | retire session and wake waiter |

Tag every frame with session and boundary generation. Dedicated channel avoids
unsolicited `REACHED` frames interleaving with request-response bridge traffic

Worker event loop watches control socket plus latch set by redo callback.
Callback only mutates shared state and wakes worker. It performs no socket I/O,
catalog access, allocation-heavy work, or daemon protocol parsing

Control disconnect marks live slots aborted and wakes startup. Worker restart
reattaches to shared state. Postmaster restart clears volatile state, making
retained records unarmed no-ops

## Pre-wire arm

Arm must complete before rewritten record reaches `RecordBytesSink`. Current
record sink callback runs after wire delivery, too late for this guarantee

Add pre-wire interposer seam in `WalStream::drain_records` after filter verdict
and before in-place rewrite or `on_wire_chunk`:

```text
parse original record
classify command boundary
request INTERPOSE_ARM
  acknowledged:
    rewrite rmid + CRC
    mark manifest Interposed
  unavailable, full, stale, or rejected:
    keep original xact record
    select current byte hold
publish wire bytes
enqueue original parsed record for daemon processing
```

Never rewrite first and arm later. Never retry arm after any byte from record
may have reached shadow

If source disconnects after arm but before wire publication, close or cancel
slot only when byte sink proves record was not published. Otherwise leave slot
for exact-LSN match or session cleanup

## Redo callback

On custom record:

1. validate info, xid, record start, and record end against ring head
2. if no exact armed slot exists, return with no effect
3. transition `armed → reached`
4. publish startup process identity
5. wake bridge worker
6. wait interruptibly for `released`, `aborted`, session loss, shutdown, or
   deadline
7. clear process identity and return

Unarmed no-op is required:

- already captured record may replay after restart
- startup may encounter retained archive before bridge worker starts
- worker starts only at consistent state because catalog scan needs database
  connection
- postmaster crash clears shared arm state

Callback timeout returns after marking slot `timed_out`. It must not wedge
recovery indefinitely. Daemon-side publication gate makes fail-open replay
safe: scan loses exact position, command boundary degrades, and successor
records remain unpublished until fallback verdict exists

Unknown custom rmgr remains hard operational failure. Keep module preload
dependency until retained custom WAL disappears

## Replay position

Current `SCAN` reads `GetXLogReplayRecPtr()`. That reports
`lastReplayedEndRecPtr`, updated only after redo callback returns, so it points
to previous record while interpose is active

PostgreSQL sets `replayEndRecPtr` before calling rmgr redo.
`GetCurrentReplayRecPtr()` therefore reports interposed record end while
callback waits

Do not globally replace scan checks with `GetCurrentReplayRecPtr()`. Another
rmgr may be executing outside interpose

Interposed scan validates:

- shared slot is `reached` or `scanning`
- session and boundary generation match request
- expected LSN equals slot `record_end_lsn`
- `GetCurrentReplayRecPtr()` equals expected LSN
- slot remains same generation before and after scan
- `GetXLogReplayRecPtr()` has not advanced through boundary

Normal pinned scan keeps current `GetXLogReplayRecPtr()` checks

## Capture and publication

Move command-boundary capture behind bounded queue rather than blocking pump
thread

For interposed boundary:

1. pump arms, rewrites, sends bytes, and enqueues original parsed record
2. startup reaches custom callback and pushes `REACHED`
3. capture lane waits for matching push
4. worker scans projections while callback holds redo
5. daemon assembles descriptors through existing Rust assembler
6. daemon installs pending timeline entry or explicit degradation
7. daemon releases callback
8. decoder publication gate forwards boundary, then queued successor records

Pending entries remain speculative until transaction outcome:

- commit folds entries into durable descriptor batch before commit publication
- abort drops entries
- restart before commit replays from unified floor and may degrade if shadow
  already passed command boundary

Do not wait for descriptor-log fsync at every command boundary. Commit remains
durability point. Requiring per-command fsync would exchange poll stall for
storage stall

Archive may write and fsync bytes ahead of command boundary, but resume floor,
decoder ack, retention cut, and commit publication cannot pass unresolved
transaction. Existing unified floor remains recovery anchor

## Bounded flow

Keep independent bounds:

- shared arm-ring slots
- pump-to-capture record channel
- queued decoder records behind earliest unresolved boundary
- filtered archive bytes not yet eligible for resume-floor advancement
- shadow `pg_wal` growth while redo waits

Ring or channel saturation parks pump and lets source slot retain WAL. P3
removes fixed per-boundary stop, not all backpressure

One long transaction can place substantial DML between command boundary and
commit. Spill queued records through existing transaction-buffer machinery
rather than grow memory without bound

## Failure semantics

| event | action |
|---|---|
| capability absent or `OPEN` fails | never rewrite, use byte hold |
| `ARM` rejected before wire | keep original record, use byte hold |
| daemon dies after arm, before wire | session cleanup aborts slot |
| daemon dies after wire, before scan | callback aborts or times out; restart reprocesses at floor, pending capture degrades if replay moved |
| worker dies before callback | arm remains in shmem; restarted worker resumes session or timeout aborts |
| worker dies during scan | request fails, daemon installs degradation, callback aborts or times out |
| control socket closes while callback waits | mark session draining, abort slots, wake startup |
| postmaster crashes while callback waits | shared state disappears; replayed custom record is unarmed no-op; daemon loses bridge session and degrades unresolved boundary |
| catalog lock timeout | scan errors, daemon installs degradation, sends `ABORT` |
| replay position differs | reject scan, install `ReplayMismatch` degradation, send `ABORT` |
| stale custom record during archive replay | unarmed callback returns immediately |
| ring full | stop arming until head retires, bounded pump backpressure |
| module missing with retained custom WAL | startup fails; operator restores preload or rebuilds shadow |

Failure may cost pending coverage and throughput. It must not publish descriptor
from wrong replay position or strand startup indefinitely

## Diagnostics

Stock `pg_waldump` supports custom rmgr ids on PostgreSQL 16 and later. It
prints numeric `customNNN` name without walshadow-specific description.
Filtered segments remain structurally readable but lose transaction
invalidation description

Manifest tooling should render `Interposed` entry as original transaction
invalidation record plus custom id and boundary generation where available

Expose current session and ring state in worker logs and daemon status without
printing catalog row data

## Metrics

- `custom_rmgr_session_up`
- `custom_rmgr_armed_total`
- `custom_rmgr_reached_total`
- `custom_rmgr_released_total`
- `custom_rmgr_aborted_total{reason}`
- `custom_rmgr_unarmed_replay_total`
- `custom_rmgr_wait_seconds`
- `custom_rmgr_ring_depth`
- `custom_rmgr_ring_full_total`
- `custom_rmgr_scan_errors_total{reason}`
- `custom_rmgr_fallback_total{reason}`
- `custom_rmgr_ahead_bytes`
- `custom_rmgr_callback_timeouts_total`

Compare against:

- `pending_holds`
- `pending_hold_nanos`
- source keepalive failures
- boundary capture latency
- pending degradation rate

Expected result after default enablement: command-boundary byte holds trend to
zero on daemon-owned shadow, while commit holds remain

## Phases

### P0: direct replay read

Use bridge `REPLAY_LSN` for current byte hold instead of forced standby-status
round trip. Keep successor withholding and poll. This is independent,
low-risk latency reduction

### P1: protocol and shared state

Register custom rmgr, add capability handshake, control channel, shared ring,
generation checks, push notification, and callback timeout. Exercise callback
against synthetic custom records without enabling filter rewrite

### P2: pre-wire rewrite and fallback

Add pre-wire arm seam, `Interposed` manifest kind, CRC rewrite, archive-reader
interpretation, and exact fallback to original record plus byte hold on every
unarmed path. Keep feature off by default

### P3: queued capture

Move command-boundary capture behind bounded publication gate. Install pending
coverage before successor decoder records, preserve commit-time durability,
and add bounded spill/backpressure

### P4: fault rollout

Enable opt-in on daemon-owned shadows after cross-major and crash matrix passes.
Compare hold metrics and degradation rate against byte-hold baseline. Make
default only when interposed path shows no correctness-only failure mode and
fallback remains continuously exercised

## Acceptance

- capability mismatch emits original xact record byte-for-byte and takes
  current hold
- acknowledged arm precedes first rewritten wire byte
- rewritten record preserves length, xid, prev pointer, info, body, alignment,
  and valid CRC
- shadow with module replays custom record; shadow without live arm treats it
  as no-op
- callback reports exact boundary through shared generation and
  `GetCurrentReplayRecPtr`
- normal `SCAN` continues using `GetXLogReplayRecPtr`
- successor WAL reaches shadow while callback waits, but successor decoder
  record does not pass publication gate
- pending coverage publishes before first post-boundary decoder record
- commit promotion remains durable before commit drain
- abort drops every speculative command entry
- two hundred command boundaries reuse bounded ring without status polling or
  unbounded memory
- ring saturation backpressures pump without losing arm or record order
- catalog `VACUUM FULL` lock conflict hits worker lock timeout, degrades, and
  releases callback
- worker kill during scan does not wedge startup past callback deadline
- daemon kill at every transition restarts from floor with either captured
  timeline or explicit degradation
- postmaster kill during callback replays retained custom record unarmed and
  daemon refuses stale scan
- old interposed archive replays after feature disable while module remains
  preloaded
- external shadow receives no custom record without explicit opt-in and
  negotiated capability
- stock `pg_waldump` walks filtered segment and labels custom record numerically
- PostgreSQL 16, 17, and 18 pass same protocol and recovery matrix

## Promotion criteria

1. pending command-boundary capture and commit promotion are landed
2. byte-hold path has production metrics establishing boundary frequency and
   stall cost
3. every acceptance case above runs in CI or dedicated fault suite
4. custom rmgr id is stable
5. retained-WAL preload dependency has preflight and operator recovery path
6. fallback byte hold remains available per boundary

Do not fold into pending-capture correctness work. Do not defer after these
criteria hold and metrics show command-boundary stall is material
