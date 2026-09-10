# Reduce generic protocol maintenance

Evaluate dependency replacements when they remove meaningful maintenance work
without changing credential, retry, cancellation, or persistence behavior
Keep adapters small and avoid exposing provider types through domain interfaces

## Object storage

Evaluate a maintained object-storage implementation behind wal-rus Storage
interface. Preserve wal-g archive naming and environment conventions. Explicit
credential handling must not introduce AWS profile discovery or scanning

Prove list pagination, missing-object errors, multipart uploads, checksums,
token refresh, retries, cancellation, and existing archive compatibility before
removing current implementation. Add one provider at a time

If a full adapter cannot preserve credential behavior, evaluate narrower signing
and token-management libraries. Treat this as an alternative to a full backend
replacement, not a reason to stack both approaches

## Metrics and throttling

Consider a standard Prometheus encoder when label, histogram, or escaping
complexity warrants it. Preserve metric names and bound label cardinality
Changing encoder does not require replacing HTTP endpoint

Consider a rate limiter when measured requirements include aggregate budgets,
burst limits, or fairness across concurrent readers. Preserve cancellation and
stream behavior. A smaller local implementation remains acceptable when it
already meets those requirements

## Completion

Review API fit, supported Rust version, license, dependency tree, and failure
behavior. Run existing behavioral tests plus provider-specific edge cases before
deleting replaced code. Keep small config helpers and rate estimators local
unless a concrete maintenance problem justifies moving them

## Candidates and ownership

Evaluate `object_store` behind upstream wal-rus adapter, starting with one S3
path and explicit credentials, then GCS after token mapping is proved. Retain
existing backend during parity tests. Audit retry composition before removing
wrapper, nested retry budgets can change outage and cancellation behavior

If credential conventions prevent full replacement, investigate `aws-sigv4` for
explicit-credential signing and `yup-oauth2` or `gcp_auth` for GCS token lifecycle
Treat names as candidates requiring API review, never enable profile discovery
as part of evaluation. Test adapter with [timeline archive lookup](failover.md)
and [staged publication](shadow_toast.md), including atomic visibility assumptions

## Read budgets: governor

Use a shared governor limiter when introducing aggregate read budgets. Current
`ObjectStoreSource::run` clones Settings for concurrent parts; each
`Settings::throttle_network` call in wal-rus 0.3.2 constructs an independent
`RateLimited` reader. N active parts can approach N times configured network rate
Current pacing tracks bytes since reader creation, allows initial read through,
and accumulates idle credit without an explicit burst bound

Own one `Arc<RateLimiter>` per budget scope, share across parts and concurrent
backfills when scope is process-wide. Keep network and disk budgets separate
Charge network bytes before decrypt/decompress, retain zero as unlimited
Prefer upstream wal-rus ownership for its reader adapter, expose shared budget
handles to walshadow. Keep resident-memory permits in `src/budget.rs` separate

[Governor quotas](https://docs.rs/governor/0.10.4/governor/struct.Quota.html)
support weighted cells and explicit burst capacity. Bound requests to burst
capacity, oversized `check_n` requests fail instead of waiting. Choose cell
granularity explicitly for u64 byte rates, u32 cell counts, and nanosecond
replenishment precision; avoid truncating rates or repeatedly rounding tiny reads

Governor does not supply an AsyncRead adapter or FIFO scheduling. Its
[async wait loop](https://docs.rs/governor/0.10.4/src/governor/state/direct/future.rs.html)
rechecks competing requests using futures-timer. Keep pending read/charge state
across polls, account actual short reads, and define cancellation after admission
Tokens have no RAII refund. Use explicit clock injection for deterministic tests,
Tokio paused time alone does not control governor's clock and timer

Implement only with aggregate and burst behavior specified. Verify concurrent
readers, reader replacement, idle bursts, short reads, EOF, cancellation, rates
below chunk size, and rates above u32 byte range. Measure contention before
claiming fairness or throughput improvement

## Encoding: prometheus-client

Prefer prometheus-client for a planned encoding migration, after resolving sample
name compatibility. `src/ops/metrics.rs::render` contains roughly 950 lines of
declarations and formatting, with another renderer in `src/ops/stages.rs`
Most labels are fixed, so escaping is currently limited exposure; declarations
and domain snapshots remain necessary even after replacing wire formatting

Keep MetricsSnapshot and existing atomic counters. Await snapshot acquisition
before synchronous encoding, expose owned snapshot through a
[Collector](https://docs.rs/prometheus-client/0.25.0/prometheus_client/collector/trait.Collector.html)
with const metrics or direct MetricEncoder calls. Avoid duplicating counters in
a second mutable registry. Preserve integer values without i64 casts and keep
RateEstimator local

[Text encoding](https://docs.rs/prometheus-client/0.25.0/prometheus_client/encoding/text/index.html)
emits OpenMetrics, appends `_total` to counter sample names, and terminates with
`# EOF`. Register conventional counters without their existing `_total` suffix
Resolve these legacy counter names before migration:

- `walshadow_uptime_seconds`
- `walshadow_desc_capture_total_sql`
- `walshadow_desc_capture_total_log_replay`
- `walshadow_xact_plan_bytes`
- `walshadow_xact_plan_rows`
- `walshadow_raw_stash_bytes`

Strict preservation of counter names and types prevents a direct standard
OpenMetrics conversion for these families. Plan explicit name migration or
retain existing encoding; avoid disguising counters as gauges or rewriting
encoded output to remove suffixes

Encode stage metrics before one final EOF marker. Keep HTTP endpoint, update
Content-Type to `application/openmetrics-text; version=1.0.0; charset=utf-8`
Preserve conditional families and bare plus labelled backfill-pending samples
Verify parsed names, labels, values, types, one descriptor per family, infinity,
escaping, stage inclusion, and successful scraping. Existing substring tests
alone cannot establish wire-format compatibility

## Dependency review

Reviewed governor 0.10.4 and prometheus-client 0.25.0
This evaluation changes no runtime dependencies and makes no performance claim

- [Governor manifest](https://github.com/boinkor-net/governor/blob/v0.10.4/governor/Cargo.toml):
  MIT, no declared rust-version. Start with default-features=false and std for
  direct limiter, avoid optional dashmap, quanta, and jitter dependencies unless
  needed. std still brings futures-timer; core includes nonzero_ext and spinning_top
- [Prometheus-client manifest](https://docs.rs/crate/prometheus-client/0.25.0/source/Cargo.toml):
  Apache-2.0 OR MIT, no declared rust-version. Default features empty, text path
  uses dtoa, itoa, parking_lot, and derive encoder. Leave protobuf features off

Repo CI uses stable Rust without declared MSRV. Verify candidate resolution,
actual dependency delta, and build on supported toolchain during implementation
