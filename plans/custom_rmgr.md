# Reduce command-boundary capture stalls

Current pending-descriptor capture holds successor WAL bytes so shadow cannot
replay past a command boundary while catalog is read. This already provides
correctness. Evaluate a PostgreSQL redo callback only when measurements show
these holds limit throughput or responsiveness

See [catalog architecture](../architecture/catalog.md) and
[capture implementation](../src/source/catalog_capture.rs)

## Proposed behavior

Arm a boundary before sending its filtered record. Replace only an eligible
command-end invalidation record with a same-length custom resource-manager
record. Startup process stops inside its callback, reports arrival, and waits
while daemon captures pending catalog state. WAL receipt may continue into
bounded buffers while replay waits

Keep commit, abort, prepared-transaction, and assignment records under built-in
resource managers. Their physical effects cannot be replaced by this callback
Keep existing commit hold and durable descriptor promotion

## Required constraints

Negotiate capability, PostgreSQL major, protocol, and custom ID before rewriting
Never send custom records to an unprepared shadow. External shadows require
explicit opt-in. Preserve record length, body, alignment, and following LSNs,
and recompute CRC

Keep original record classification recoverable when reading filtered archives
A custom record without required provenance must not disappear as an unknown
operation during daemon replay

An unarmed callback during crash or archive replay must return without waiting
Active waits need bounded timeouts and cancellation on session loss. Stale
sessions must never release or capture another boundary

Do not publish successor decoder work before pending coverage or explicit
degradation is installed. Resume floor cannot pass unresolved work. Bound armed
records, ahead WAL, decoder queues, and archive staging independently

If negotiation or arming fails before rewrite, use existing byte hold. If
failure happens after publication, preserve current rejection/degradation
behavior without guessing catalog state or leaving startup stuck

Retained custom WAL creates a module dependency across restarts, even after
feature is disabled. Keep module preloaded until those records age out; add
preflight and recovery instructions before deployment

## Interposer and control protocol

Introduce pre-wire seam after full-record classification and before rewrite or
byte-sink publication. Validate built-in transaction rmgr, eligible invalidation
opcode, no block references, and body layout for negotiated major. Request arm
first; only acknowledged arm permits custom rmgr plus CRC rewrite. Enqueue
original parsed record for daemon processing and persist original rmgr/info in
filtered manifest. Cancel unpublished arm only when byte sink proves no bytes
escaped, otherwise retire through session cleanup

Use dedicated bidirectional control connection alongside request/response bridge
Proposed messages: `OPEN`, `ARM`, pushed `REACHED`, `RELEASE`, `ABORT`, `CLOSE`
Keep arrival notifications out of scan/decode response stream. Shared bounded
ring records session generation, owner nonce, timeline, record start/end, xid,
and slot generation. Transition slots through armed, reached/scanning, and
released/aborted/timed-out states; match exact boundary to prevent stale release

Redo callback validates head slot, signals worker through latch, then waits
interruptibly without holding shared-state lock. Perform no catalog scan or
socket I/O inside redo. Worker disconnect aborts slots and wakes startup;
postmaster restart clears arms, so archived custom records replay without waits
Register rmgr during preload even when live control socket is disabled

## Exact scan and publication

While redo callback runs, `GetXLogReplayRecPtr()` still names previous completed
record. An interposed scan must verify `GetCurrentReplayRecPtr()` against armed
record end plus matching shared slot before and after scan. Preserve completed
replay-pointer checks for ordinary scans, another rmgr may be executing outside
interpose. Validate behavior against each supported PostgreSQL major

Use bounded capture lane: arm and publish bytes, await matching arrival, scan,
assemble descriptor through existing assembler, install pending coverage or
degradation, release callback, then publish successor decoder records. Promote
pending entries durably at commit and discard on abort, no per-command fsync
requirement. Timeout may release redo but cannot authorize a stale catalog scan

Filtered archive may run ahead of pending capture only under descriptor recovery
rules. When combined with [shadow TOAST](shadow_toast.md), archive publication
also obeys reclamation gate. Include both gates in [wait-dependency audit](coordination.md)

## Implementation sequence

1. Measure current holds; evaluate direct bridge replay-position reads as smaller
   latency improvement while retaining successor-byte withholding
2. Prove protocol, shared ring, callback placement, timeout, and unarmed replay
   with synthetic records before filter can rewrite live traffic
3. Add pre-wire arm, original-classification manifest, archive interpretation,
   and byte-hold fallback, disabled by default
4. Add bounded capture lane and successor publication gate, preserving commit
   promotion and spill/restart floor
5. Run cross-major fault matrix before opt-in rollout, compare hold cost and
   degradation rate against current path

## Completion

Measure boundary frequency and stall cost first. Prove callback placement with
same-transaction DDL/DML and catalog reads. Inject daemon, worker, socket, and
postmaster failure before and after arm, receipt, capture, and release

Test ring exhaustion, stale sessions, timeout, archive replay, missing module,
and restart with retained custom records. Keep fallback available per boundary
Adopt a stable custom ID and demonstrate performance benefit without weakening
descriptor coverage or durable restart rules
