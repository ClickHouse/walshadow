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
write — see [`src/shadow_catalog.rs`](../src/shadow_catalog.rs).
Over-invalidates: an unrelated `ALTER TABLE t1 ADD COLUMN` evicts
`t2`'s cached descriptor. Cache hit-rate hit is real but
typically benign because catalog writes are rare relative to heap
record dispatches

Cross-link: [memory note "PG version WAL skew"][mem-pg-skew] —
PG 17's broader pg_class writes hit this granularity harder than
PG 16. Decoder fidelity is unaffected; cache freshness churn is

Defer finer scheme (per-relation invalidation keyed on relOid +
relfilenode) until measurement says cache miss rate is hurting
streaming throughput

[mem-pg-skew]: /home/erpre/.claude/projects/-home-erpre-s-walshadow/memory/feedback_pg_version_wal_skew.md

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

[`src/oracle.rs`](../src/oracle.rs) compares decoder text against
shadow's `SELECT $1::bytea::<typ>::text`. Sensitive to locale-bound
output: numeric thousands separator, timestamp formatting, money

Pinned at bootstrap today: shadow `initdb` forces `lc_numeric=C`
and `lc_time=C`. Documented in [`src/shadow.rs`](../src/shadow.rs)
init path. Closes the locale axis

Remaining risk: timezone DB skew. Shadow's `pg_timezone_names` and
`pg_timezone_abbrevs` are populated from the host's tzdata files,
not from source. Source on tzdata 2023d, shadow's host on tzdata
2024a, divergence on rare-but-real timestamp tzname output.
Mitigation: pin tzdata version at deploy time. Not enforced

## Path A CRC at >1 GB/s WAL

Filter rewrites every kept record's CRC32C; today single-threaded.
SSE4.2 CRC32C is ~1 ns/byte → 1 s of CPU per 1 GB of WAL on one
core. Source workloads >1 GB/s WAL saturate one core

Record-level parallelism is trivial (records are independent post-
classification). Defer thread pool until measurement demands.
overview.md pitfall #8 flagged this

Zero-copy framing already cut allocator pressure off the hot path;
CRC is the next bottleneck if `criterion` benchmarks land and
surface it. Bench is itself deferred, see [parked.md](parked.md)

## PG fork temptation

Path B (patch PG recovery dispatcher with a relfilenode whitelist)
keeps surfacing because Path A's CRC rewrite "feels heavy". Resist
until measurement demands. Path A spend is one-time (CRC32C is
mature, the rewrite is mechanical); Path B spend is permanent
(maintain a fork against every PG release)

Reconsider only when Path A's measured CPU + latency cost exceeds
the operator's tolerance and parallelism doesn't close the gap

## Source primary failover

Walshadow's physical slot lives on source primary. Source loss
loses the slot; promoting source's standby loses walshadow's WAL
position

Two operator options
(overview.md pitfall #9):

* **Pre-create slot on the standby.** Failover-aware replication
  slots (PG 17+) follow; pre-PG-17 needs manual operator script
  before failover
* **Re-bootstrap from new LSN.** Walshadow re-attaches against the
  newly-promoted primary at a fresh LSN; backfill bridge
  (see [bootstrap.md](../bootstrap.md)) reseeds anything between old
  slot position and new attach LSN

Catalog on shadow is preserved across re-attach; no schema replay
needed. Slot positioning is the failure mode, not catalog state

Risk: operator doesn't pre-create slot and doesn't tolerate
re-bootstrap window. Document as deployment precondition
