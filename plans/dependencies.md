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

Evaluate `governor` for aggregate read budgets and `prometheus-client` for encoding
when requirements above justify them. MPMC and retry dependencies already exist
as `async-channel` and `backon`, verify remaining maintenance problem before
reviving replacement work. Keep small config parsers and rate estimators local
