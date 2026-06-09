# dependencies — crates.io replacement candidates

Review log for future dependency evaluation. These are candidates for
replacing local generic infrastructure with maintained crates, not
commitments. Evaluate API fit, MSRV, dependency tree, license, feature
flags, test coverage, and operational behavior before landing any swap.

## Highest-leverage candidate: object storage

Evaluate `object_store` as substrate behind existing `wal-rs`
`Storage` trait.

Today:

* `wal-rs/src/storage/s3.rs` owns SigV4 signing, multipart upload,
  ListObjectsV2 pagination, XML parsing, request retry interaction
* `wal-rs/src/storage/gcs.rs` owns service-account JWT/OAuth flow,
  PEM key parsing, REST upload/download/list/delete, token refresh
* `wal-rs/src/storage/retrying.rs` layers generic storage retries

Why replace: S3/GCS auth and request semantics are protocol-heavy,
security-sensitive, and easy to drift from provider behavior. This is
generic object-store work, not WAL-domain logic.

Candidate fit:

* `object_store::ObjectStore` covers put/get/list/delete/head-style
  operations expected by wal-rs storage
* crate includes Amazon S3 and Google Cloud Storage builders
* crate includes retry, multipart, and throttling support
* existing `Storage` trait can stay as compatibility boundary for
  call sites and tests

Evaluation notes:

* Preserve WALG-compatible environment handling where users already
  depend on it
* Do not scan AWS profiles or add implicit profile discovery
* Keep local adapter small: translate paths, map errors, preserve
  existing tests/fixtures, avoid leaking provider crate types upward
* Verify multipart behavior, list pagination, missing-object errors,
  checksum/ETag expectations, and retry observability

Likely order:

1. Add adapter behind feature or new backend variant
2. Port S3 first, because current local code has most hand-rolled
   protocol surface
3. Port GCS after credential mapping is explicit
4. Delete local retrying wrapper only after storage behavior is
   proven equivalent

Fallback if `object_store` credential model conflicts with WALG-compat:

`object_store`'s `AmazonS3Builder`/`GoogleCloudStorageBuilder` carry
their own credential/env conventions and may do implicit discovery,
which fights "preserve WALG environment handling" and "do not scan AWS
profiles" above. If so the swap stalls and yields zero risk reduction.

Incremental path: keep local `Storage` impls and WALG env reading,
replace only security-sensitive crypto internals with audited crates.
Ranked by security value, not line count:

1. SigV4 → `aws-sigv4`. Hand-rolled request signing
   (`derive_signing_key`, canonical request, AWS4-HMAC-SHA256 assembly)
   is the highest-risk crypto here. Feeds explicit creds, sidesteps
   profile discovery, stays WALG-compatible
2. GCS service-account flow → `yup-oauth2` or `gcp_auth`. Replaces JWT
   mint plus token cache/refresh, not just the signing step
3. PEM/DER → `rustls-pemfile`, already in tree (TLS certs only today);
   reuse for GCS key path instead of `pem_to_der`. No new dependency
4. S3 XML → `quick-xml`. Lowest stakes; `parse_list_v2`/`extract_xml_tag`
   are fragile substring matching but response shape is narrow. Last

Items 1-3 are where hand-rolling is actually dangerous; item 4 is
robustness, not security. This path is mutually exclusive with the
full `object_store` swap, not additive to it.

## MPMC queue

Evaluate `async-channel` for `src/pipeline/mpmc.rs`.

Today: local bounded MPMC queue uses `Mutex<VecDeque<T>>`,
`Semaphore`, manual close state, and custom receiver clone semantics.

Why replace: bounded async MPMC with close semantics is generic
concurrency plumbing. `async-channel` already provides bounded
multi-producer/multi-consumer channels, cloneable sender/receiver
handles, async send/recv, close, and length/capacity inspection.

Evaluation notes:

* Preserve existing public wrapper if call sites rely on current names
* Check closed-channel wake behavior against current tests
* Preserve backpressure behavior for decoder/inserter pipeline
* Keep custom queue only if existing semantics intentionally differ
  from `async-channel`

## Retry and backoff

Evaluate `backon` for non-storage retry loops.

Today:

* `wal-rs/src/retry.rs` defines generic retry policy, exponential
  backoff, jitter, and `with_retry`
* `src/pipeline/inserter.rs`, `src/shadow_catalog.rs`, and
  `src/oracle.rs` each contain localized retry/backoff loops

Why replace: exponential backoff with jitter is generic control-flow
logic. Storage retries should likely move into `object_store`; remaining
database/network retry loops can share one crate-backed policy.

Evaluation notes:

* Keep domain-specific retry classification local
* Map existing max-attempt/budget behavior exactly before deleting
  custom helper
* Do not flatten all retry loops if caller-specific logging or cursor
  safety matters

## Byte throttling

Evaluate `governor` if throttling needs precision, fairness, or shared
budgeting across readers.

Today: `wal-rs/src/throttle.rs` implements an `AsyncRead` wrapper with
average-rate sleeping after reads.

Why replace: rate limiting is generic, but current implementation is
small and easy to audit. Replacement is worthwhile only if semantics
need to become aggregate across tasks, burst-aware, or stricter under
concurrency.

Evaluation notes:

* Treat as optional, behind measured need
* Preserve current stream wrapper API if possible
* Compare read throughput, burst behavior, and cancellation behavior

## Metrics exposition

Evaluate `prometheus-client` if metrics surface grows.

Today: `src/metrics.rs` hand-renders Prometheus text format and serves
HTTP from raw Tokio TCP handling.

Why replace: typed metric registration and OpenMetrics/Prometheus
encoding are generic concerns. Current local renderer is tolerable
while metric set stays small, but risk grows with labels, histograms,
escaping rules, and process/runtime metrics.

Evaluation notes:

* Keep existing metric names and label cardinality stable
* Decide separately whether HTTP serving should stay local or move to
  an HTTP crate already in dependency tree
* Prefer crate-backed encoding before adding more label-heavy metrics

## Considered, kept hand-rolled

Recorded so future readers do not re-litigate these:

* Cursor binary codec (`src/cursor.rs` `encode`/`decode`): durable
  on-disk 64-byte format with CRC32C trailer. `bincode`/`postcard`
  would change byte layout and force migration of existing cursor
  files, for a fixed struct already simple and stable
* Metrics HTTP serving (`src/metrics.rs` raw `TcpListener`): no HTTP
  server in tree (`reqwest` is client-only), so a crate means pulling
  `hyper`/`axum` for one endpoint. `prometheus-client` covers encoding
  only; keep serving local until endpoint surface grows
* Env var parse helpers (`wal-rs/src/config` `parse_env_*`): ~30 lines,
  no validation complexity; `envy`/`config` not worth the dependency
* `RateEstimator` rolling window (`src/metrics.rs`): single use site,
  generic but small

## Recommendation

1. `object_store`, highest risk removed and largest local protocol
   surface deleted
2. `async-channel`, low-risk concurrency simplification
3. `backon`, only after storage retry ownership is clear
4. `governor`, only if rate limiting requirements grow
5. `prometheus-client`, when metrics format complexity grows

Keep first implementation review-focused: add crate-backed path, run
existing tests, add behavioral tests for edge cases being delegated,
then delete local replacement code only after parity is visible.
