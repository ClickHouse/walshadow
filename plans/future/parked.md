# parked — small operational debt and follow-up polish

Operational debt collected from retros plus follow-ups from the
allocation-audit pass. One-line per item

## v1.0 operational polish

* **Deduplicate `ChServer` fixture across `tests/pipeline_e2e.rs` +
  tests/common.** Two callers using one vendored ChServer is fine;
  third caller lifts to shared. Already lifted `bootstrap_ch_fixture`
  + `http_get` / `parse_metric`
* **OnceCell shared CH-server fixture.** Five acceptance tests each
  spawn own CH (~5 s × N startup). Total CI cost ~25 s of unique
  boot time. Flag if test count doubles

## Cross-major fixture pinning

* **MULTI_INSERT + xl_xact_commit fixtures against PG 16/17/18 via
  `tests/classify_fixture.rs`.** Cross-major drift in tail-walk
  semantics would surface as silent decoder mismatch under one
  specific major. cross-major snapshot fixtures called for snapshot
  fixtures across majors. Not done

## Drive currently-skipped tests

Acceptance tests ship with runtime skip-gates checking for `initdb` /
`pg_basebackup` / `clickhouse` on `PATH`; *not* `#[ignore]`. Each
needs source PG + CH + (usually) basebackup-cloned shadow. Drive in
CI when those binaries reliably present:

* `kill_restart`
* `pgbench_acceptance`
* `bootstrap_direct_ch`
* `bootstrap_object_store_ch`
* `truncate`
* `subxact`
* `copy_into`
* `add_column_default`

Each is a one-line un-skip + observation of which fixture path
needs a kick. Acceptance items §1 (pgbench), §5 (kill-restart)
remain unverified against live topology until driven

## Zero-copy follow-ups

* **Criterion benchmark.** Allocation-count + RSS measurement
  post-hoc; land `benches/` crate when measurement contested.
  Targets predicted RSS drop (≈200 MB → ≈0 for 100k-record
  heap-INSERT segment) + 1.5-3× decode throughput from dropped
  allocator pressure
* **`XLogRecord.blocks` smallvec.** Records average 0-2 blocks;
  `SmallVec<[_; 2]>` keeps common case stack-resident. Allocation
  polish below byte-traffic wins already booked via Cow
* **Header-walk single-pass merge.** `record.blocks` walk runs
  twice (once for IDs, once for payloads) in wal-rus parser. Merge
  into single pass since IDs arrive in order. Leftover from wal-g
  port

## Walsender hardening

* **TLS / SCRAM auth.** Trust-over-loopback only today.
  Production multi-host deployments need this. Sized against
  wal-rus's auth machinery — the receive-side already speaks SCRAM,
  send-side mirrors
* **`hot_standby_feedback` (`'h'` frame).** Silently dropped today;
  documented behaviour. Long-running shadow queries that conflict
  with replay still hit `max_standby_streaming_delay`
* **Walsender keepalive-timeout unit test.** Indirectly covered by
  libpq + PG-walreceiver round-trips in `walsender_pg18_walreceiver`;
  explicit unit test is polish

## Decoder follow-ups

* **Subxact `XACT_XINFO_HAS_INVALS` ordering verification fixture.**
  Capture commit record from PG with all `xinfo` bits set; prove
  walk doesn't drift under out-of-the-way ordering on some major.
  Subxact retro flagged this
* **TRUNCATE strategy knob.** v1 emits single `TRUNCATE TABLE <dest>`
  per relation. Per-table `truncate_strategy = "passthrough" |
  "ignore"` knob once downstream consumer asks. Defer-until-asked
* **DROP TABLE propagation polish.** Basic path landed via
  `SchemaEvent` + `DrainEntry::Catalog` channel. Corner cases
  (CASCADE, RESTRICT, dependent objects) need pinning against
  fixture matrix
