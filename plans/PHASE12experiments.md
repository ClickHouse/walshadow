# PHASE12experiments — backfill bridge prototype evaluation

Eleven prototype combinations of the PHASE12 backfill bridge and the
[BASEBACKUP](BASEBACKUP.md) Phase 6.5 axes, built in parallel under
`.claude/worktrees/agent-*`. Each builds clean, none has landed on
`main`. This doc reads the prototypes against each other, names what
they teach, and proposes synthesis paths.

Companion docs:
- [PLAN.md §"Phase 12"](PLAN.md#phase-12--backfill-bridge) — the brief
  the prototypes target.
- [BASEBACKUP.md](BASEBACKUP.md) — the evaluation that decomposed the
  problem into UC1 (filtered fetch → shadow data_dir) and UC2 (CH
  initial load: 2A page-walk vs 2C source COPY).

## Inventory

| # | Combo | Worktree | LOC src | LOC test | wal-rs | e2e status |
|---|---|---|---:|---:|---:|---|
| A | 2C per-relation only | `agent-a8f0a265dd85b2dac` | ~1100 | 693 | 0 | green |
| B | 2A object_store (CH-side only) | `agent-a623236fc3e021f1a` | ~1320 | 430 | 0 (stubbed) | unit-only |
| C | UC1 + 2C unified | `agent-a3d0c4d4e910ac79a` | ~1580 | 556 | ~193 | partial |
| D | UC1 + 2A unified | `agent-a2cac814dd0cdb036` | ~1700 | 300 | ~326 | unit-only |
| E | CTID partition (PeerDB-style) | `agent-afd6cc563fb5d6e10` | ~1700 | (incl.) | 0 | green |
| F | Hybrid 2A+2C size-routed | `agent-a43f448feb0dcac47` | ~1510 | (incl.) | 0 | unit-only |
| G | Resumable per-relation cursor | `agent-ac684ca30ca43ea8f` | ~1030 | (incl.) | 0 | green (NoopCopier) |
| H | UC1-only Phase 6.5 baseline | `agent-ae7bb3c06ea277c42` | 615 | 415 | 0 | green |
| J | Streaming pipeline 2C | `agent-a75244b1a8830db24` | ~1956 | (incl.) | 0 | green |
| K | pg_dump --data-only bridge | `agent-ab2fe5a9c4f7594f8` | ~940 | (incl.) | 0 | discard |
| L | Read-from-standby | reverted | — | — | 0 | structural insight only |
| I | Logical-slot snapshot | not built | — | — | — | dead end |
| M | All-logical (pgoutput) | not built | — | — | — | dead end |

`wal-rs` column counts only **new** lines in `~/s/wal-rs/`. C and D
introduced the shared surface (`SYSTEM_DIRS_DENYLIST`,
`EntryAction{Keep,Skip,Tap}`, `EntryFilter`, `TapSink`,
`FetchArgs::entry_filter`/`entry_tap`,
`ReplicationConn::create_physical_slot`); subsequent worktrees consume
that surface with zero additional wal-rs edits.

## What got built

### A — 2C per-relation source-direct COPY

PLAN.md §"Phase 12" default. Single source xact under REPEATABLE READ,
`pg_export_snapshot()` exports id `S`, `pg_current_wal_lsn()` captures
`B`. N parallel libpq sessions each `SET TRANSACTION SNAPSHOT 'S'` then
run `COPY (SELECT * FROM <rel>) TO STDOUT BINARY` over one assigned
relation. Decoded rows feed `Emitter::push_backfill_row`, bypassing
`relation_at`'s replay-LSN gate (shadow isn't replayed past B during
backfill). All rows tagged `_lsn = B`; WAL pump rebinds to
`--start-lsn = align_down(B, WAL_SEG_SIZE)` after backfill drains.

Pros:
- Simplest correct shape. 1:1 with PLAN.md's default.
- Predictable source-CPU bound at N parallel COPYs.
- No wal-rs surface; no shadow-side bootstrap changes.
- Binary COPY wire form is closer to WAL on-disk than text COPY — codec
  drift is small (see Cross-cutting findings).

Cons:
- Per-relation parallelism stalls on skewed tables. A single 100 GB
  table holds one worker while N-1 sit idle.
- COPY BINARY needs per-OID `FromSql` adapters. tokio-postgres covers
  Tier 1 OIDs but `uuid`/`date`/`time`/`timestamp` need newtype wrappers
  (`UuidBytes`, `PgDate`, `PgTime`, `PgTimestamp`) inside the prototype.
- `Emitter` is `!Send` (raw `chc_client` fd). Coordinator runs
  `Arc<Mutex<Emitter>>` with one drain task fed via mpsc from workers.
  Single CH socket caps throughput.
- No resume. Single-shot per PLAN.md out-of-scope.

### B — 2A page-walk via object_store (CH-side only)

BASEBACKUP.md UC2A in isolation: page-walk decoder against
BASE_BACKUP tar entries fetched from S3. No UC1 shadow extraction in
this worktree (sister worktree C/D land that). `PageWalker` adapts the
Phase 5 decoder by exposing `decode_tuple_payload(buf, header_off,
rel, 0, 0)` as `pub(crate)`; setting `header_off = 18` (past
`HeapTupleHeaderData`'s 12-byte `t_choice` + 6-byte `t_ctid`) and zero
prefix/suffix lets the same column-projection logic run against
on-disk 8 KiB pages instead of WAL block-data. `ToastBuffer` keys
chunks by `(toast_relid, chunk_id, chunk_seq)`. `FpiReplayer` is
in-memory page-patch primitive; `apply_window(start_lsn, end_lsn)` is
stubbed.

Pros:
- **Zero codec drift**. Page bytes are the same bytes the WAL decoder
  reads. Backfill rows and post-attach WAL rows agree on every
  type's physical encoding by construction.
- No source touches across the whole bootstrap window. Bytes flow S3 →
  daemon → CH.
- Reuses the Phase 5 type matrix — no new codec surface for backfill.

Cons:
- Catalog-before-heap ordering: pass 1 lands catalogs to shadow,
  shadow's `pg_class` is read, pass 2 routes user heap. The prototype
  leaves the pass-1 catalog land step to sister worktrees C/D.
- Torn pages: BASE_BACKUP captures pages mid-write; FPI replay between
  `start_lsn` and `end_lsn` is required. `FpiReplayer::apply_window` is
  a stub; `wal_rs::pg::wal::fetch` integration is a TODO.
- TOAST cross-archive bookkeeping is sketched but not implemented
  end-to-end.
- Object-store driver (`fetch_to_scratch`) carries a TODO at the
  wal-rs `handle_with_args` boundary — wiring wal-g `Settings::from_env`
  + `DynStorage` construction is straightforward but expands the
  prototype LOC budget significantly.

### C — UC1 + 2C unified

BASEBACKUP.md's recommended default. UC1 drives wal-rs's
`run_base_backup` (direct) or `fetch::handle_with_args` (object_store)
with `CatalogOnlyFilter`: catalog files (`oid < 16384` or in
`catalog_tracker_seed` whitelist) land on shadow's data_dir; user heap
drops on the floor; system-dirs denylist drops contents but keeps
empty dir entries (PG needs the empty dirs at standby start). UC2C
opens a separate `SnapshotAnchor` libpq session under REPEATABLE READ
READ ONLY, exports the snapshot via `pg_export_snapshot()`, holds the
xact open across the whole backfill. Workers `SET TRANSACTION SNAPSHOT`
and run COPY BINARY.

Pros:
- Closes BASEBACKUP.md's Gap 1 (mapped + non-mapped catalog filenode
  skew). Shadow's `pg_class` lands at source's post-`VACUUM FULL`
  filenode without a separate seed step.
- Closes Gap 2 (CH initial load) in the same daemon boot pass.
- Defines the wal-rs surface (`EntryFilter` + `SYSTEM_DIRS_DENYLIST` +
  `create_physical_slot`) every subsequent variant consumes.
- Real PG 18.4 end-to-end test exercises UC1's tar extraction: logs
  `start_lsn=X end_lsn=Y dropped=7`, asserts no user heap under `base/`.

Cons:
- UC2C path's snapshot anchor lives on a separate session from the
  BASE_BACKUP issuer. Snapshot is at-or-after `start_lsn`, but the
  exact LSN tracking requires explicit operator co-ordination (the
  worktree's `path (a)` choice rather than wal-rs's
  `start_backup_with_snapshot` lift).
- Pitfall #10 (replay deadline) deferred — shadow stalls in recovery
  waiting for WAL between `B` and `E` without a `restore_command`. Test
  surfaces this gracefully but daemon doesn't drive the catch-up.
- Same COPY-BINARY codec drift surface as A.
- Same `!Send` Emitter constraint as A.

### D — UC1 + 2A unified

Single BASE_BACKUP fetch feeds both consumers. `CatalogOnlyFilter`
returns `EntryAction::Keep` for catalogs (write to shadow data_dir),
`Skip` for denylist dirs, `Tap` for user heap. `PageWalkTap` extends
`EntryAction::Tap`: tap buffers entry body in 64 KiB chunks, drives
`sink.begin/chunk/end` under `Arc<Mutex<dyn TapSink>>` because tar
unpack runs on `spawn_blocking`. Two-pass walk solves catalog-ordering:
pass 1 = `tap_user_heap = false` (catalogs land), pass 2 =
`tap_user_heap = true` (page-walk emits).

Pros:
- **Zero codec drift on CH side** (same as B's page-walk shape, but
  unified with UC1 so one fetch feeds both consumers).
- One tar stream consumed twice for object_store; one stream + one
  re-issued BASE_BACKUP for direct sourcing.
- wal-rs surface (`EntryAction::Tap`, `TapSink`,
  `FetchArgs::entry_tap`) is the natural shape; both walshadow's
  filter and wal-rs's unpack benefit from the same `is_system_dir_path`
  helper.
- Reuses Phase 5 decoder via `decode_block_data_for_test` shim.

Cons:
- Object-store driver still TODO (`fetch_to_scratch` returns explicit
  error). Worktree provides architecture, not the wal-g
  `Settings`/`DynStorage` wiring.
- FPI replay (`FpiReplayer::apply_window`) stub returns `Ok(0)`. Full
  implementation needs `wal_rs::pg::wal::fetch` across the
  `[start_lsn, end_lsn)` segment range plus FPI extraction.
- TOAST spill-to-scratch sketched (tracks `would_spill_bytes`) but no
  file emission.
- TOAST page-decoder variant not implemented — `pg_toast_<relid>`
  pages are recognized but `(chunk_id, chunk_seq, chunk_data)`
  projection is TODO.
- Pass-2 emitter handoff in `bin/stream.rs` stubbed — the
  `PageWalkTap` output → Emitter wire needs a live BASE_BACKUP smoke
  env.
- BASEBACKUP.md estimate was ~650 LOC additional on the 2A side; the
  prototype lands at ~1370 walshadow-side (911 + 463) because
  page-walk + reshaper + tap state machine + two-pass orchestrator
  don't fit the plan's flat estimate.

### E — CTID partition parallelism (PeerDB-style)

Sub-relation parallelism. Per relation, probe
`pg_relation_size(rel) / block_size` for an authoritative page count
(ignoring stale `pg_class.relpages`); compute chunks at
`ceil(pages × block_size / chunk_target_bytes)` (default 64 MiB).
Workers grab `(relation, ctid_lo, ctid_hi)` tuples off one queue and
run:

```sql
COPY (SELECT * FROM <rel>
      WHERE ctid >= '(lo,0)'::tid AND ctid < '(hi,0)'::tid)
TO STDOUT (FORMAT BINARY)
```

Tail chunk uses `ctid <= '(max_page,65535)'::tid` to absorb partial
pages. Semaphore caps in-flight workers at `parallelism` (default 8).
Same snapshot session shape as A.

Pros:
- Linearizes wall-clock on skewed workloads. A single 100 GB table at
  64 MiB chunks splits into ~1600 work items consumed by N workers.
- Matches PeerDB's established initial-load shape — operator
  precedent.
- Stale `relpages` defended against by always querying
  `pg_relation_size` at plan time.
- Real benchmark on the test workload (10k×80B = 800 KiB, 134 pages):
  16 KiB chunks → 67 chunks in 292 ms at parallelism=8; degenerate
  4 KiB → 134 chunks in 395 ms. Chunk-setup overhead dominates at
  small scale; benefits show at GB-scale.

Cons:
- Codec drift surface widens: agent added
  `decode_numeric_pgcopy_binary` and `decode_inet_pgcopy_binary`
  because binary COPY's typsend wire form differs from on-disk for
  `numeric` (i16/i16/i16/i16/digits[]) and `inet` (4-byte
  family/bits/is_cidr/nb prefix). Per-OID adapters will grow as type
  coverage expands. This is the second copy of every type's codec.
- Partitioned parent tables collapse to one chunk — requires
  `pg_inherits` walk to split per-partition; deferred.
- Per-chunk session overhead (`BEGIN; SET TRANSACTION SNAPSHOT; COPY;
  COMMIT`) is non-trivial for small chunks. Transition point: when
  *largest relation's bytes > sum(other relations' bytes) × small
  factor*, CTID wins; otherwise per-relation wins.
- Snapshot session must stay alive for the longest chunk's duration
  (worst case: one worker on the last 100 GB chunk).
- No resume.

PeerDB-shape comparison: walshadow keeps the coordinator's
snapshot-holding xact open for the full backfill; PeerDB records the
snapshot id, lets the originating xact commit, every worker re-opens.
PeerDB's shape is more crash-tolerant but exposes a "snapshot id
exists but xact gone" race window. walshadow's shape is simpler at
the cost of one long-held session.

### F — Hybrid 2A+2C size-routed

Per relation: probe `pg_relation_size`; if `>= 1 GiB` route to 2A
(page-walk over cached BASE_BACKUP tar), else route to 2C (parallel
COPY under snapshot). Both routes feed the same `Emitter`. 2C rows
tag `_lsn = B`; 2A rows tag `_lsn = E`. Routing is all-or-nothing per
relation (no same-PK race between paths because routing pins each
relation to exactly one path).

Pros:
- Bimodal sources (TB facts + small dims, canonical OLTP shape) shed
  source-CPU on the facts (2A from S3) while avoiding 2A's TOAST/FPI
  complexity for the dims. A 10 TB cluster with 5 TB in one fact +
  5 GB across 200 dim tables would have **pure 2C** scan 10 TB on
  source; hybrid scans 5 GB on source.
- Reuses wal-rs surface from C+D entirely — zero new wal-rs LOC.
- Inclusive threshold (`size >= threshold → 2A`) avoids off-by-one at
  exact-1-GiB tables.
- Routing decision frozen before any worker spawns; logged as one
  banner `plan = "2A: N rels / X GiB, 2C: M rels / Y GiB"`.

Cons:
- 2A page-walk path is the stubbed-in-D shape: the page walker is
  stubbed in this prototype too (returns 0 rows), with a TODO listing
  the 5 sub-steps. Real 2A LOC is on top.
- Direct-from-source UC1 not implemented; object-store required.
- Boundary-flip across runs (relation grows past threshold) is
  surfaced in the banner log; same-bootstrap routing is stable.
- `pg_total_relation_size` is wrong here (it sums TOAST, which 2A
  doesn't decode); must use `pg_relation_size` (heap-only). Easy
  surprise.
- Same codec drift on the 2C arm as A/E. 2A arm is zero-drift.

### G — Resumable per-relation cursor

Borrows `src/cursor.rs`'s atomic-rename + CRC32C shape for a new
`backfill-cursor.bin` carrying `(snapshot_id, backfill_lsn,
completed_rels, in_flight_rels)`. Three on-disk artefacts under
`{spill_dir}`: cursor (rewritten on every relation completion),
`backfill-progress.log` (append-only TSV, audit), `backfill-done.lsn`
(terminal sentinel).

Resume check order: `--restart-from-scratch` flag → `done.lsn` present
(skip backfill entirely) → cursor unreadable/CRC-bad (greenfield with
log) → cursor valid (snapshot validity test via
`SET TRANSACTION SNAPSHOT '<id>'`; SQLSTATE 22023 = expired; fail loud
unless `--snapshot-keepalive-cmd` set) → slot retention check (slot
present + active + `restart_lsn <= B`).

Pros:
- Per-relation granularity covers the common crash points at minimal
  cost: ~1 KB cursor per 100 relations, one fsync per relation
  completion (bounded human-cadence rate).
- CH dedup correctness preserved trivially: re-emit of completed
  relation's rows produces the same `(PK, _lsn=B)` rows; CH's
  `ReplacingMergeTree(_lsn)` collapses identical pairs. No truncate
  needed on resume.
- Resume cleanly aborts when the snapshot is genuinely gone
  (daemon-owned snapshot dies with daemon) — operator runs
  `--restart-from-scratch`. The operator-sidecar (`--snapshot-keepalive-cmd`)
  is exposed for completeness but deliberately ugly.
- Failure modes surface loud (`SnapshotExpired`, `SlotMissing`,
  `Crc`) with concrete operator instructions.

Cons:
- **Row-shipping path is stubbed** — `NoopCopier` placeholder. The
  cursor/resume harness is real but the COPY → Emitter wire is
  separate work. Pairs naturally with A/E for the actual transfer.
- Per-relation granularity doesn't help a single 100 GB relation
  crashing 99% through; re-COPY the whole relation. Per-chunk
  resumability is the v2 (combines with E's CTID chunks).
- Snapshot expiry on resume is hard fail; operator must wipe CH dest
  rows for in-flight relations and pass `--restart-from-scratch`.
- WAL slot retention during a multi-hour paused backfill is operator
  concern; surfaced via a pre-resume slot-state probe.

### H — UC1-only Phase 6.5 baseline

UC1 with no CH backfill — replaces `Shadow::apply_schema_dump` only.
Smallest variant: 615 LOC walshadow src + 415 LOC test, zero new
wal-rs LOC (pure consumer of C+D's surface).

Pros:
- Closes BASEBACKUP.md's Gap 1 (catalog filenode skew on both mapped
  and non-mapped catalogs) without changing the CH-side data shape.
- Sits alongside the existing `apply_schema_dump` rather than
  replacing it; operators with neither REPLICATION grant nor
  storage credentials fall back to schema-only.
- Real correctness test: `VACUUM FULL pg_class` on source in a loop
  until filenode crosses 16384; UC1 runs; asserts shadow's `pg_class`
  filenode matches source's post-rotation value. Schema-only path
  cannot pass this test.
- Marginal LOC vs the combined UC1+2C is small per BASEBACKUP.md's
  *plan estimate* (~80 LOC for the 2C bolt-on). Actual worktree C
  shipped 645 LOC for the backfill module — the plan estimate
  underestimated.
- All BASEBACKUP.md Pitfalls #1, #3, #5, #9, #10 implemented;
  #2 (pg_control ordering on direct path) implicit but unenforced;
  #6 (fast vs spread checkpoint) exposed as `BaseBackupOpts` knob with
  no walshadow policy.

Cons:
- CH initial-load gap remains open by design. Pre-existing source rows
  invisible to CH until operator-driven backfill (or PHASE12) lands.
- Acceptance criterion §1 (`pgbench -i` pre-populated source) still
  fails — needs CH backfill.
- Daemon orchestration of UC1 deliberately untouched in the prototype;
  CLI knobs (`--bootstrap-mode`, `--base-backup-source`,
  `--base-backup-name`) exist but greenfield boot doesn't drive UC1
  end-to-end. That's a separate landing.

### J — Streaming pipeline 2C

Three-stage pipeline: N COPY producers → bounded mpsc → M decoders →
bounded mpsc → single emitter task owning multiple `TableEncoder`s on
an LRU keyed by `<namespace>.<relname>`. Each open encoder flushes on
its own size/time threshold (default 16 MiB / 1000 ms / LRU cap 16).

Pros:
- Latency-to-first-row in CH is bounded by the producer-to-CH path
  length (sub-second measured on the 1600-row test), not the
  longest-relation duration. Operators watching dashboards see CH
  populating in real time.
- Stage 1 backpressure propagates naturally: not polling the
  `CopyOutStream` blocks libpq's server-side `pq_putmessage` via TCP
  window-close. No special handling.
- Bounded memory: K open encoders × block budget. LRU evicts the
  oldest open encoder when distinct-relation count exceeds K.

Cons:
- Stage 3 is single-task (Emitter is `!Send`). Throughput cap is one
  CH socket. M (decode parallelism) saturates around 4-8 unless
  per-row decode is unusually heavy.
- LOC almost doubles vs A's batch-by-relation (~1956 vs ~890).
- Five new knobs (`copy_parallelism`, `decode_parallelism`,
  `block_bytes`, `block_ms`, `active_relations`) vs A's two. Two of
  them (`block_ms`, `active_relations`) are real foot-guns:
  - `block_ms` too low thrashes flushes
  - `active_relations` too low evicts hot relations on every miss
- LRU eviction cost dominates when active relations > K. With 50
  relations and K=16, eviction fires every K+1th distinct relation.
- Same codec drift as A.

Verdict: pipeline wins on latency-to-first-row and many-small-relations
workloads (interleaved rows, no idle producers); batch wins on
throughput-only KPIs (no per-row LRU bookkeeping) and
connection-constrained sources (N=1).

### K — pg_dump --data-only bridge (DISCARD)

Spawn `pg_dump --data-only --format=directory -j N --snapshot=<id>` as
subprocess, parse the TOC via `pg_restore --list`, decode `.dat` files
through a hand-rolled text-COPY parser, feed `Emitter` with
`_lsn = B`.

Pros:
- Operational familiarity: pg_dump is universal.
- Snapshot binding is a single `--snapshot=` flag.
- Parallel job orchestration via `-j N` is free.
- ~940 LOC, smaller than A.

Cons (structural, fatal):
1. **Not streaming**. pg_dump completes the entire dump to
   `<scratch>/pg_dump.d/*.dat` *before* walshadow opens a byte. TB
   source = TB scratch.
2. **Text-COPY codec drift across the backfill/WAL boundary**. pg_dump
   emits PG's `typoutput` text form. WAL path emits `ColumnValue` from
   on-disk bytes. Drift cases:
   - `timestamp`/`timestamptz`: text `"2026-05-18 10:30:45+00"` vs
     WAL → `Timestamp(i64 µs)` → CH `DateTime64(6)`. Different CH
     types per path — CH rejects the cross-path insert, or operator
     picks one shape and the other drifts.
   - `bytea`: pg_dump hex `\x...` vs WAL raw bytes.
   - `uuid`, `numeric`, `inet`, `interval`: codec-by-codec coincidence.
   `ReplacingMergeTree(_lsn)` cannot collapse a backfill row's
   text-form value against a post-attach WAL row's binary-form value
   for the same logical PK. **Dedup is structurally broken.**

The variant ships with a working test but the test only covers types
where text matches binary by accident. Discard.

### L — Read-from-standby (self-reverted)

Built and tested, then reverted by the agent. Structural insight:
standby-COPY is properly a **knob on the 2C orchestrator**, not a
parallel implementation. `--backfill-source-standby <conninfo>` flips
the COPY endpoint; the WAL pump still goes to primary at
`--start-lsn = B` where `B = pg_last_wal_replay_lsn()` from the
standby. Standby-promotion watchdog (2 s poll on
`pg_is_in_recovery()`) and preflight (`hot_standby = on`, lag bound)
fit naturally inside the 2C orchestrator.

The prototype also picked **text COPY**, hitting the same drift defect
as K. Switching to binary COPY is mechanical and required.

Treat L as a feature spec for whichever 2C path lands, not a separate
worktree.

### I — Logical-slot snapshot coordination (NOT BUILT)

`CREATE_REPLICATION_SLOT walshadow_init LOGICAL pgoutput EXPORT_SNAPSHOT`
to mint the snapshot id and `consistent_point` LSN atomically, then
drop the logical slot after backfill drains and use a physical slot
for the WAL pump.

Discard rationale:
- `wal_level = logical` is a hard floor walshadow cannot enforce on
  source. With a `pg_export_snapshot()` fallback, this variant becomes
  a strict superset of A — the logical path is dead branches operators
  never hit.
- Two-slot operational shape (logical alive until drop, physical alive
  forever) adds visibility cost without buying tighter LSN coupling.
  `pg_export_snapshot()` already gives atomic snapshot-LSN coupling
  inside a single xact.
- `pgoutput` plugin negotiation for bytes we never read is pure
  protocol cargo.

### M — All-logical (pgoutput) bootstrap (NOT BUILT)

Use pgoutput logical decoding for both initial sync *and* steady-state
stream, abandoning walshadow's physical-WAL path.

Discard rationale:
- Walshadow's identity is the physical-WAL filter + shadow-PG decode
  oracle. Trading both for pgoutput reproduces Debezium and loses the
  differentiator.
- DDL handling regresses: pgoutput does not emit DDL. Walshadow's
  physical path detects `VACUUM FULL`/`REINDEX`/`ALTER TABLE` through
  shadow replay. All-logical breaks this.
- PG-version coupling to pgoutput's wire-format evolution tightens.

## Cross-cutting findings

### Codec drift across the backfill/WAL boundary

Three encodings of the same row exist on the source side:

1. **On-disk heap bytes** — what the WAL decoder reads. Walshadow's
   `ColumnValue` codecs are written for this form.
2. **typsend wire form** — what `COPY ... TO STDOUT BINARY` emits.
   Close to on-disk for fixed-width types, divergent for
   `numeric` (i16/i16/i16/i16/digits[] vs on-disk short/long form),
   `inet` (different prefix layout), arrays/ranges/`jsonb`.
3. **typoutput text form** — what `COPY ... TO STDOUT` (default) and
   pg_dump emit. Diverges from on-disk for nearly every non-trivial
   type.

CH's `ReplacingMergeTree(_lsn)` collapses same-PK rows only when their
physical encoding matches. A backfill row's value must encode
**bit-identical** to the post-attach WAL row's value for the same
logical PK, else CH stores both.

Drift inventory by path:
| Path | Drift surface | Mitigation |
|---|---|---|
| 2A page-walk (B, D) | none | reads on-disk bytes through WAL decoder |
| 2C binary COPY (A, C, E, F-2C, G, J) | partial | per-OID adapter per type (`decode_numeric_pgcopy_binary` etc); list grows as type coverage expands |
| 2C text COPY (K, L) | full | structural — discard |

The 2A path is the only one with zero drift by construction. The 2C
binary path has bounded drift but the adapter list is a second codec
to maintain — every codec fix has to land twice. Pure 2C-binary
deployments must own this drift inventory explicitly.

### wal-rs surface stabilized after C+D

What landed at `~/s/wal-rs/`:

| File | Surface |
|---|---|
| `pg/backup/mod.rs` | `SYSTEM_DIRS_DENYLIST: &[&str]`, `EntryAction { Keep, Skip, Tap }`, `EntryFilter` trait, `TapSink` trait, `is_system_dir_path` helper |
| `pg/backup/fetch.rs` | `FetchArgs::entry_filter: Option<Arc<dyn EntryFilter>>`, `FetchArgs::entry_tap: Option<Arc<Mutex<dyn TapSink>>>`, plumbing through `unpack_part`/`unpack_manual` |
| `pg/replication/conn.rs` | `ReplicationConn::create_physical_slot(slot, reserve_wal) -> Result<Lsn>` |

Existing callers unaffected: `entry_filter` and `entry_tap` default to
`None`; `create_physical_slot` is additive.

This surface is load-bearing for E/F/G/H/J — none of them add to wal-rs.
The right move is land it on wal-rs `main` as walshadow-side decisions
are made.

### `Emitter::!Send` constraint

`clickhouse_c_rs::Client` owns a raw socket fd and is not `Send`.
Every variant that wants multiple producer tokio tasks lands on
`Arc<Mutex<Emitter>>` with one drain task fed by mpsc from N workers.
J's three-stage pipeline locks this shape in: Stage 3 is necessarily
single-task; M (Stage 2 parallelism) ceilings around 4-8 because Stage
3's lock contention bounds the speed-up.

This is structural for the prototype. Loosening would require either
(a) a CH client pool with one Native socket per worker (lots of
sockets, blocks Hop attempts) or (b) a `Send`-capable wrapper around
`clickhouse-c-rs::Client` (substantial upstream work). Neither is in
scope for PHASE12.

### Snapshot-session lifetime

Daemon-owned (A, C, E, G, J) — snapshot dies when daemon dies.
Operator-sidecar (G's `--snapshot-keepalive-cmd` escape hatch) —
operator holds a long-running psql session running `pg_export_snapshot()`
out-of-band.

Consensus across worktrees: daemon-owned + fail-loud is the right
default. The sidecar shape is real but exposes a second moving part
operators have to manage. Worktree G makes the fail-loud explicit:
`SnapshotExpired` on restart, operator passes `--restart-from-scratch`
to wipe and re-snapshot.

UC1's BASE_BACKUP doesn't need a snapshot — `pg_control` carries the
LSN pair. UC2A doesn't need one either — the tar stream is the
snapshot. Only UC2C and pure-2C variants pay the snapshot lifetime
cost.

### LSN handoff: `B` vs `E`

PHASE12 default (A, E, G, J): `B = pg_current_wal_lsn()` captured in
snapshot xact. All rows tag `_lsn = B`. WAL pump rebinds to
`--start-lsn = align_down(B, WAL_SEG_SIZE)`.

UC1 + UC2A (D): `(B, E) = BASE_BACKUP outcome`. UC2A rows tag
`_lsn = E`. WAL pump at `E`.

UC1 + UC2C unified (C): UC1 yields `(B, E)`. UC2C's snapshot anchor
exports a snapshot at-or-after `B`. UC2C rows tag `_lsn = B`. WAL
pump at `E`. Race window between `B` and `E` is collapsed by
`ReplacingMergeTree(_lsn)`: any same-PK WAL row in that window
carries `_lsn > E` and wins.

Hybrid (F): 2C rows tag `_lsn = B`, 2A rows tag `_lsn = E`. Routing
is all-or-nothing per relation so no same-PK race between paths.

Standby (L's insight): `B = pg_last_wal_replay_lsn()` on standby. WAL
pump on primary at `B`. Primary has WAL ≥ B by construction (slot
pins retention).

### Source vs. object_store sourcing

| Bootstrap path | Source CPU | Source IO | Wire load |
|---|---|---|---|
| 2C pure (A/E/G/J) | high (N parallel COPYs) | high | source → daemon → CH |
| 2A pure (B/D) | none | none (object_store) or full BASE_BACKUP duration (direct) | S3 → daemon → CH; or source → daemon → CH for direct |
| UC1 + 2C (C) | high during 2C window | low (BASE_BACKUP doesn't re-read after) | mixed |
| UC1 + 2A (D, object_store) | none | none | S3 → daemon → both |
| Hybrid (F) | low (2C only for small rels) | mixed | mostly S3 for big rels |

Object-store sourcing wins on every metric except deployability
(requires existing wal-g infrastructure). Direct sourcing is the
greenfield fallback.

### Resume granularity

| Path | Crash recovery | Cost on disk | Cost on hot path |
|---|---|---|---|
| Single-shot (PLAN.md default) | re-COPY everything | none | none |
| Per-relation cursor (G) | re-COPY one relation | ~1 KB / 100 rels + audit log | one fsync per relation done |
| Per-chunk cursor (E + G synthesis) | re-COPY one chunk | ~1 KB / 100 chunks | one fsync per chunk done |
| BASE_BACKUP end-LSN marker (UC1 in C/D/H) | re-fetch BASE_BACKUP | single LSN | atomic post-fetch |

Per-chunk cursor is the natural endpoint. Per-relation is a 90%
shortcut that doesn't cover the single-large-relation crash mode.

## Synthesis matrix

Which pairs compose, which conflict:

| Pair | Composable? | Notes |
|---|---|---|
| A + E | yes | E supersedes A's per-relation parallelism. CTID chunks are A's "relations" reinterpreted. |
| A + G | yes | G's per-relation cursor wraps A's worker pool. Drop the `NoopCopier` and wire A's COPY → Emitter inside G's worker closure. |
| A + J | yes | J's pipeline replaces A's batch-by-relation drain shape. Stage 1 = A's per-rel COPY producer; Stage 3 = A's emitter drain. |
| E + G | **synthesis target** | per-chunk cursor. Cursor `completed_rels` becomes `completed_chunks: Vec<(rel, ctid_lo, ctid_hi)>`. Crash on chunk K only re-COPYs chunk K. The natural v2 of both. |
| E + J | yes | CTID chunks as Stage 1 work items. Stage 3 LRU still keyed on relation (chunks of the same relation accumulate into the same `TableEncoder`). |
| G + J | partial | per-relation cursor doesn't map cleanly to pipelined emit — "relation done" requires all chunks of that relation drained through Stage 3, plus a barrier. Better paired with E's per-chunk cursor. |
| C + E | yes | UC1 lands shadow data_dir; UC2C path runs as CTID-partitioned 2C. The default greenfield composite. |
| C + G | yes | UC1 + resumable 2C. UC1's end-LSN marker is the BASE_BACKUP-side resume primitive; G's cursor handles UC2C restart. |
| D + E | no | E is 2C-shaped; D is 2A. Mutually exclusive on CH-side. |
| D + F | yes | F's 2A arm is D's page-walk shape. F's 2C arm is A/E. |
| H + E | yes | UC1-only shadow bootstrap + CTID-partitioned 2C. Same as C+E but with H's UC1 isolated implementation. |
| H + G | yes | UC1 + resumable per-relation 2C. Layering. |
| A/E/G/J + L's knob | yes | `--backfill-source-standby` toggle on whichever 2C orchestrator lands. Adds promotion watchdog and lag preflight. |
| K + anything | no | discard. |
| I/M + anything | no | dead end. |

Mutually exclusive primary paths: 2A (page-walk) vs 2C (COPY) for any
given relation. F's per-relation routing is the explicit composition
of both.

## Recommended composite path

Three deployment shapes cover the realistic operator surface. All
three share H's UC1 (shadow data-dir bootstrap) and the wal-rs surface
from C+D.

### Default (greenfield, object_store available)

**H (UC1) + E (CTID 2C) + G (per-chunk cursor synthesis)**

- UC1 closes catalog filenode skew on shadow.
- CTID partitioning linearizes wall-clock on skewed relation size
  distributions (the PeerDB shape).
- Per-chunk resume covers the single-large-relation crash mode.
- 2C binary COPY codec drift surface is bounded; per-OID adapter list
  is maintained as a known artefact.

Estimated combined LOC: ~2200 walshadow (H's 615 src + E's 1066 src
+ G's cursor 588 src trimmed of redundancy + small integration glue).
wal-rs surface lands as-is from C+D.

### Constrained source (operator opt-in, wal-g infra)

**H (UC1) + D (UC2A page-walk via object_store)**

- Zero source touches across the whole bootstrap window.
- Zero codec drift everywhere — single decoder on backfill and WAL.
- Pays D's deferred LOC: FPI replay
  (`FpiReplayer::apply_window` calling `wal_rs::pg::wal::fetch`),
  TOAST cross-archive bookkeeping with spill, object-store wiring at
  the `fetch_to_scratch` boundary.

Estimated extra LOC over D's current state: ~600 (FPI replay ~300,
TOAST chunk decoder ~150, object-store wiring ~150).

### Hybrid bimodal (large heterogeneous source)

**H (UC1) + F (per-relation routing) + E (CTID 2C arm) + D (2A arm)**

- Big relations (≥ 1 GiB) go through D's 2A path — zero drift, zero
  source pressure.
- Small relations go through E's CTID 2C — but each relation likely
  collapses to one CTID chunk because it fits under chunk_target. So
  the 2C arm acts as per-relation parallelism for the small list.
- Sweet spot: TB facts + many small dims. 10 TB source becomes ~5 GB
  source-side COPY load.
- Pays F's routing complexity on top of D's 2A LOC and E's CTID 2C
  LOC.

Useful for production where source primary is QoS-constrained and
wal-g exists. Operationally heavier; defer until measurement proves
the bimodality.

### Read-from-standby as a knob

`--backfill-source-standby <conninfo>` toggle on the 2C orchestrator
of whichever deployment shape lands. Adds the promotion watchdog
(`pg_is_in_recovery()` poll on a sidechannel) and lag preflight
(`--backfill-max-lag-bytes`, default 64 MiB). Common in
Debezium/pglogical so the operator surface isn't unfamiliar.

## What's still missing

Even with the composite path picked, a few things land separately:

- **Per-OID binary COPY codec coverage**. E surfaces `numeric` and
  `inet`; the rest of the Tier 1/2 matrix needs adapters too. Pin in
  a `src/copy_binary.rs` so the drift inventory has one home.
- **TOAST page decoder for 2A**. `pg_toast_<relid>` page-decoder
  variant: extract `(chunk_id oid, chunk_seq int4, chunk_data bytea)`
  triples per slot. Maintains `ToastBuffer` across the tar walk so
  main-heap rows resolving `va_valueid` references find their chunks.
- **FPI replay window for 2A**. `FpiReplayer::apply_window` driving
  `wal_rs::pg::wal::fetch` across `[start_lsn, end_lsn)`.
- **Spill-to-scratch for TOAST buffer**. D tracks
  `would_spill_bytes`; implementation emits files under
  `{spill_dir}/toast/<rfn>/<vid>/<seq>` and drains them in-order at
  reassembly time.
- **BASE_BACKUP retry on interrupted fetch**. Pitfall #9 marker
  exists; what's missing is the boot-time decision: marker present →
  skip BASE_BACKUP, resume at standby start; marker absent → wipe
  data_dir and re-fetch. No partial-tar resumption attempted.
- **Direct-from-source UC1 path**. F + D both stub it; only object_store
  is wired end-to-end.
- **Daemon orchestration end-to-end**. H wires the CLI but not the
  greenfield boot path that actually drives UC1 + Backfill. C does
  the orchestration but on top of UC1+2C only; the synthesis composite
  needs the orchestration extracted.
- **pg_export_snapshot anchor co-ordination with UC1 start_lsn**.
  Worktree C went path (a) — separate snapshot session. Path (b) —
  wal-rs's `start_backup_with_snapshot(label) -> (StartInfo, String)`
  — is the cleaner shape; ~80 LOC on the wal-rs side.

## Acceptance criterion (carried from BASEBACKUP.md §"Recommendation")

For PHASE12 commit:

> A source pre-populated with `pgbench -i -s 10` is fully reflected in
> CH after a single `walshadow bootstrap` followed by steady-state
> replication, with row counts and checksums matching, and shadow
> `data_dir` stays under a configurable ceiling (catalog-scale,
> MiB-order) across the whole bootstrap.

Adding from the experiment review:

> Re-run with `kill -9` mid-backfill must produce CH end-state
> identical to the uninterrupted run, with shadow `data_dir`
> unchanged. (Per-chunk cursor recovery; combines PHASE11 §5 + PHASE12
> resume.)

> Codec drift inventory documented: every type in the Tier 1/2 matrix
> has either (a) on-disk-bit-identical encoding to backfill wire form
> or (b) a per-OID adapter, with the adapter pinned by a round-trip
> test against a known-value source row.

## Notes carrying forward

- C and D's wal-rs surface is the consensus base; land it on wal-rs
  `main` regardless of which walshadow path commits.
- L's insight (standby-COPY is a knob, not a variant) is a feature
  spec for whichever 2C path lands.
- K and I/M are recorded discards; do not re-litigate without new
  evidence.
- Worktree A's "binary COPY type drift" assumption (that
  `FromSql`-via-tokio-postgres is enough) is broken by E's adapter
  additions. Plan for the adapter list to grow as type coverage
  expands.
