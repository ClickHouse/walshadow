# Phase 14 — close v1.0 product gaps, lift integration coverage

Phase 13 (streaming-fed shadow) + POST13zerocopy (allocation audit) +
Phase 12 (backfill bridge) lit up the v1.0 ship target. What remains
is a focused gap-closure pass plus the integration tests that exercise
the v1.0 acceptance criteria end-to-end against a live PG + shadow +
CH topology

Each item ships as its own sub-plan + commit, mirroring the
[`pre5/`](../pre5/) layout. The overview here covers sequencing,
deferrals, and the cross-item acceptance contract

## Scope

Five product gaps + three integration tests. Ordered loosely by
acceptance-gate weight, but each is independently shippable

| # | Title | Plan | Size (LOC) |
|---|---|---|---:|
| 1 | Read-time defaults (`atthasmissing` / `attmissingval`) | [01-read-time-defaults.md](01-read-time-defaults.md) | ~300 |
| 2 | `XLOG_HEAP2_MULTI_INSERT` per-tuple fan-out | [02-multi-insert.md](02-multi-insert.md) | ~270 |
| 3 | `TRUNCATE` propagation | [03-truncate.md](03-truncate.md) | ~330 |
| 5 | Subxact lineage / `ROLLBACK TO SAVEPOINT` | [05-subxact-rollback.md](05-subxact-rollback.md) | ~480 |
| 6 | `walshadow_shadow_apply_lag_*` metrics | [06-apply-lag-metrics.md](06-apply-lag-metrics.md) | ~120 |
| 7 | Kill -9 + restart integration test (§5) | [07-kill-restart-test.md](07-kill-restart-test.md) | ~250 |
| 8 | pgbench acceptance integration test (§1) | [08-pgbench-acceptance-test.md](08-pgbench-acceptance-test.md) | ~300 |
| 9 | Phase 12 direct + CH-backed bootstrap e2e | [09-bootstrap-ch-e2e.md](09-bootstrap-ch-e2e.md) | ~400 |

Item 4 (`DROP TABLE` propagation) moved to
[PHASE15.md §6](../PHASE15.md). DROP TABLE rides the same
`SchemaEvent` channel + `DrainEntry::Catalog` xact-buffer wiring as
the shape-mutating DDLs PHASE15 ships, so co-locating them
collapses one round of duplicated plumbing. Sub-plan IDs stay
contiguous (1, 2, 3, 5, 6, …) so existing cross-references keep
resolving

Totals: ~1500 product LOC + ~830 test LOC. Bulk-in-tests matches
phase 14's gap-closure-plus-coverage focus — most source deltas are
mechanical against landed Phase 5/6/7 surfaces

## Sequencing

Items 1–6 are decoder / emitter / metrics additions. Items 7–9 are
integration-test additions. Natural land order:

1. **Item 1 (read-time defaults)** lands first. Unblocks item 8's
   `ALTER TABLE ... ADD COLUMN ... DEFAULT k` checksum + PHASE15's
   type-bridge default rendering
2. **Items 2, 3, 6** in parallel — independent code paths beyond
   the shared `HeapOp` enum extension that items 2/3 land
3. **Item 5 (subxact rollback)** lands after item 2 because the
   MULTI_INSERT fan-out lifts `BufferingDecoderSink::on_record` into
   the batch shape subxact tracking also needs
4. **Item 7 (kill-restart)** lands after items 2/3/5 so the workload
   it kills exercises the new decoder branches
5. **Items 8–9 (pgbench / bootstrap-CH)** land last as the acceptance
   gate. Item 8 gated on item 1; item 9 is independent

## Out of scope (deferred to v1.1+)

Each is genuinely independent of v1.0's gating criteria. Deferring
each individually keeps Phase 14 from sprawling

- **Two-phase commit** ([PLAN.md gaps #7](../PLAN.md#known-correctness-gaps)).
  PREPARE + COMMIT PREPARED needs a separate buffer keyed on gxid
  plus cursor handling for prepared xacts surviving restart. Sized
  similar to Phase 6; warrants its own phase
- **Sequence state replication**
  ([PLAN.md gaps #5](../PLAN.md#known-correctness-gaps)). Synthetic
  CH-side column needs a config-surface decision; downstream
  consumers haven't asked for it yet
- **Cross-table WAL ordering inside an xact**
  ([PLAN.md gaps #6](../PLAN.md#known-correctness-gaps)). `_lsn`
  dedup keeps end-state consistent; reader-mid-drain visibility is
  a CH semantics question, not a walshadow correctness one
- **CH-server-bounce recovery**
  ([PLAN.md gaps #8](../PLAN.md#known-correctness-gaps)). Phase 10's
  bounded retry covers the operational case; the deeper "re-emit
  from spill on retry-budget exhaustion" needs Phase 11's cursor +
  emitter cooperation this phase doesn't otherwise touch
- **TLS / SCRAM on walsender**
  ([PHASE13.md §"What didn't land"](../PHASE13.md#what-didnt-land-in-phase13)).
  Trust-over-loopback is the only auth path today; production
  deployments outside a single-container topology need this. Sized
  as its own phase against wal-rs's auth machinery
- **`hot_standby_feedback`** handling on the walsender. Silently
  dropped today, documented behaviour, no operator complaint
- **POST13zerocopy criterion benchmark**
  ([POST13zerocopy §"Validation"](../POST13zerocopy.md#validation)).
  Allocation-count + RSS measurement is post-hoc validation, not a
  ship gate. Land as a `benches/` crate when measurement is actually
  contested
- **Walsender keepalive-timeout unit test**
  ([PHASE13 retro §"Tests + acceptance"](../PHASE13.md#acceptance-items-audited)).
  Indirectly covered by the libpq + PG-walreceiver round-trips;
  explicit unit is polish, not load-bearing
- **`XLogRecord.blocks` smallvec / header-walk single-pass**
  ([POST13zerocopy §9](../POST13zerocopy.md#other-wal-rs-allocation-review)).
  Allocation polish below the byte-traffic wins POST13zerocopy
  already booked. Re-evaluate post-bench

## Acceptance — phase 14 closes when

- `cargo test --workspace` green, including items 7–9's new
  integration tests
- v1.0 acceptance §1 ([item 8](08-pgbench-acceptance-test.md)) green
  against PG 16, PG 17, PG 18 in CI
- v1.0 acceptance §5 ([item 7](07-kill-restart-test.md)) green for
  all three pinned cutoff points across 5 runs (probabilistic but
  reproducible-enough for CI signal)
- [PLAN.md §"Known correctness gaps"](../PLAN.md#known-correctness-gaps)
  items #1 (MULTI_INSERT), #2 (subxact), #3 (TRUNCATE), #4
  (read-time defaults) all struck from the list. Remaining gaps
  (#5, #6, #7, #8) cross-referenced to the deferral rationale above.
  The PHASE8.md DROP TABLE followup is owned by
  [PHASE15.md §6](../PHASE15.md), tracked there
- `walshadow_shadow_apply_lag_bytes` +
  `walshadow_shadow_apply_lag_seconds` visible on the metrics
  endpoint
