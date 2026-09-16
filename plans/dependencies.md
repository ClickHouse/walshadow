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

## Throttling

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

## Dependency review

- [Governor manifest](https://github.com/boinkor-net/governor/blob/v0.10.4/governor/Cargo.toml):
  MIT, no declared rust-version. Start with default-features=false and std for
  direct limiter, avoid optional dashmap, quanta, and jitter dependencies unless
  needed. std still brings futures-timer; core includes nonzero_ext and spinning_top
  Reviewing 0.10.4 claims nothing about throughput; no limiter is wired yet

Repo CI uses stable Rust without declared MSRV
