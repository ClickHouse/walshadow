# risks — measurement-deferred risks and open questions

Risks and open questions, not gaps. Each is either "yes, deferred
until measured" or "live limit, documented"

## Single-threaded recovery on shadow

PG 18 ships parallel-recovery for *hot-standby* mode only. Walshadow
runs shadow in `restore_command` archive-recovery shape so recovery
stays single-threaded. DDL replay is the load-bearing case and DDL
WAL volume is small (catalog WAL is a fraction of total), so steady
state is comfortably under one core

Document as a limit. Risk: catalog WAL volume from a pathological
workload (long-running schema-rewriting migration on a wide table)
could saturate one core. Mitigation if it surfaces: bisect catalog
WAL by namespace, fan out across multiple shadow instances. Not
sized

## Catalog cache invalidation granularity

`ShadowCatalog`'s single generation counter bumps on any `pg_class`
write — see [`src/catalog/shadow_catalog.rs`](../../src/catalog/shadow_catalog.rs).
Over-invalidates: an unrelated `ALTER TABLE t1 ADD COLUMN` evicts
`t2`'s cached descriptor. Cache hit-rate hit is real but
typically benign because catalog writes are rare relative to heap
record dispatches

PG 17's broader pg_class writes hit this granularity harder than
PG 16. Decoder fidelity is unaffected; cache freshness churn is

Defer finer scheme (per-relation invalidation keyed on relOid +
relfilenode) until measurement says cache miss rate is hurting
streaming throughput

## Filter ↔ decoder ordering near boundaries

Decoder gates on `ShadowCatalog::wait_for_replay(source_lsn)`. Shadow
PG stall (autovacuum, long checkpoint, recovery process unresponsive)
stalls the decoder

Metrics surface `walshadow_shadow_apply_lag_bytes` /
`walshadow_shadow_apply_lag_seconds` (gauges) plus
`walshadow_shadow_stream_active_connections` /
`walshadow_shadow_stream_dropped_connections_total`, so the gap
(filter LSN − shadow replay LSN) is observable. Operator alert on
sustained `shadow_apply_lag_seconds > N` catches stalls

Risk remaining: silent stall that doesn't trip the alert (e.g.
shadow slowly bleeding apply rate). Acceptance §3 budgets <1 s WAL
at steady state; alarm threshold should match

## Differential oracle false positives

[`src/ops/oracle.rs`](../../src/ops/oracle.rs) compares decoder text against
shadow's `SELECT $1::bytea::<typ>::text`. Sensitive to locale-bound
output: numeric thousands separator, timestamp formatting, money

Pinned at bootstrap today: shadow `initdb` forces `lc_numeric=C`
and `lc_time=C`. Documented in [`src/catalog/shadow.rs`](../../src/catalog/shadow.rs)
init path. Closes the locale axis

Remaining risk: timezone DB skew. Shadow's `pg_timezone_names` and
`pg_timezone_abbrevs` are populated from the host's tzdata files,
not from source. Source on tzdata 2023d, shadow's host on tzdata
2024a, divergence on rare-but-real timestamp tzname output.
Mitigation: pin tzdata version at deploy time. Not enforced

## Cross-major commit-record tail walk

A commit record's tail is variable-shape: `xl_xact_commit` appends
subxact array, dropped-stats items, relfilelocators, invalidation
messages, twophase gid and origin, each behind its `xl_xact_xinfo` bit.
Reaching a later field means skipping every earlier one at the right
width, so a wrong width reads a valid-looking record wrong and CRC
catches nothing: the bytes are intact, the interpretation is not

The shape does move. `xl_xact_stats_item` went 12 → 16 bytes in PG 18
(PostgreSQL commit `b14e9ce7d55`, `PgStat_HashKey.objid` widened to 8
bytes carried as two `uint32`), and the `SysCacheIdentifier` values the
invalidation walk matches on shifted 35/36 → 37/38 in the same major.
[`src/decode/wal_xact.rs`](../../src/decode/wal_xact.rs) branches on
`page_magic` for both. `MULTI_INSERT` carries the same class of exposure,
its per-tuple walk reads header flags rather than a fixed stride

Risk is a per-major branch that no test pins, so the next widening or id
shift is silent on exactly one major. Mitigation is fixture pinning
through [`tests/classify_fixture.rs`](../../tests/classify_fixture.rs):
capture a commit record with every `xinfo` bit set (subxacts, dropped
stats, relfilelocators, `XACT_XINFO_HAS_INVALS`, origin) plus a
`MULTI_INSERT` batch, on 16 / 17 / 18, and assert each major's walk lands
on the same fields. Capture is already scripted per major; the snapshots
are what is missing

## Path A CRC at >1 GB/s WAL

Filter rewrites every kept record's CRC32C; today single-threaded.
SSE4.2 CRC32C is ~1 ns/byte → 1 s of CPU per 1 GB of WAL on one
core. Source workloads >1 GB/s WAL saturate one core

Record-level parallelism is trivial (records are independent post-
classification). Defer thread pool until measurement demands.
overview.md pitfall #8 flagged this

Zero-copy framing already cut allocator pressure off the hot path;
CRC is the next bottleneck once a bench surfaces it, see
[perf_regression.md](perf_regression.md)

## PG fork temptation

Path B (patch PG recovery dispatcher with a relfilenode whitelist)
keeps surfacing because Path A's CRC rewrite "feels heavy". Resist
until measurement demands. Path A spend is one-time (CRC32C is
mature, the rewrite is mechanical); Path B spend is permanent
(maintain a fork against every PG release)

Reconsider only when Path A's measured CPU + latency cost exceeds
the operator's tolerance and parallelism doesn't close the gap

## Source primary promotion

Operator-driven switchover continues across the fork
([../failover.md](../failover.md)); the deployment preconditions are what
remains a risk

* **Slot doesn't follow.** Physical slots are never synchronized to a
  standby, at any PostgreSQL version; PG 17 failover slots cover logical
  slots only. Slot mode needs the slot pre-created on the promotion target
  under the configured name; walshadow proves it rather than creating one,
  so a missing or too-new slot refuses the repoint by name
  ([../failover.md](../failover.md) §Slot). `max_slot_wal_keep_size` on the
  target can still invalidate a pre-created slot during a long pause, so the
  pause window lives inside that budget
* **Unplanned promotion.** Loss of the primary without a pause window
  fails closed rather than continuing; the paths that convert those
  refusals are [failover.md](failover.md)
* **Re-bootstrap remains the escape.** Walshadow re-attaches against the
  promoted primary at a fresh LSN; the backfill bridge
  ([../bootstrap.md](../bootstrap.md)) reseeds anything between the old
  slot position and the new attach LSN

Catalog on shadow is preserved across a crossing and across re-attach; no
schema replay needed. Slot positioning is the failure mode, not catalog
state
