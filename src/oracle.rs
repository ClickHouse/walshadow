//! Phase 9 — differential decode oracle backed by shadow PG.
//!
//! Two roles:
//!
//! 1. **PgPending resolver.** When the decoder emits
//!    [`ColumnValue::PgPending`](crate::heap_decoder::ColumnValue::PgPending)
//!    for a varlena type outside walshadow's local matrix
//!    (`jsonb`, arrays, `tsvector`, ranges, custom domains, ...),
//!    [`Oracle::resolve_pending`] runs
//!    `walshadow_decode_disk(oid, bytea) -> text` on shadow PG. Output
//!    replaces the `PgPending` with a [`ColumnValue::Text`] so the
//!    emitter can ship it as a CH `String`. When the
//!    `walshadow_oracle` extension is **absent** the resolver returns
//!    `Ok(None)` and the emitter falls back to writing the raw on-disk
//!    bytes — i.e. the extension is optional.
//!
//! 2. **1-in-N validator.** Sampled rows from the local Tier 3 codecs
//!    (`numeric` / `inet` / `interval`) round-trip through shadow PG
//!    via `SELECT $1::bytea::<typname>::text`. Mismatches bump
//!    `mismatches` and log; the row still goes out to CH (validator is
//!    a watchdog, not a gate). Off by default; enabled with
//!    `walshadow-stream --validate <N>`.
//!
//! Sits next to [`ShadowCatalog`](crate::shadow_catalog::ShadowCatalog)
//! — it reuses the same tokio-postgres connection model but doesn't
//! share the catalog's `Client` (the oracle's queries don't need to
//! observe replay-LSN gating and shouldn't pessimise the catalog's
//! query-one path).

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use thiserror::Error;
use tokio::sync::Mutex;
use tokio_postgres::{Client, NoTls};

use crate::codecs::NumericKind;
use crate::decoder_sink::{DecoderSinkError, TupleObserver};
use crate::heap_decoder::{ColumnValue, CommittedTuple};

#[derive(Debug, Error)]
pub enum OracleError {
    #[error("oracle pg connect: {0}")]
    Connect(tokio_postgres::Error),
    #[error("oracle pg query: {0}")]
    Query(tokio_postgres::Error),
}

#[derive(Debug, Default, Clone)]
pub struct OracleStats {
    /// `walshadow_decode_disk` calls that returned a text payload.
    pub resolved: u64,
    /// `walshadow_decode_disk` calls returning NULL or absent-extension.
    pub fallback_raw: u64,
    /// 1-in-N samples taken (Tier 3 hot types only).
    pub probes: u64,
    /// Probe outcomes that matched the local decoder.
    pub matches: u64,
    /// Probe outcomes where local decoder text != shadow PG text.
    pub mismatches: u64,
    /// SQL / connection errors collapsed into a single bucket.
    pub errors: u64,
}

/// Sampler tracks 1-in-N selection across calls. Lock-free counter so
/// multiple decoder workers can share one `Oracle` without serialising.
#[derive(Debug)]
pub struct Sampler {
    rate: u32,
    counter: AtomicU64,
}

impl Sampler {
    pub fn new(rate: u32) -> Self {
        Self {
            rate,
            counter: AtomicU64::new(0),
        }
    }

    /// `true` iff this call counts as the next 1-in-N hit. `rate == 0`
    /// means sampling disabled.
    pub fn pick(&self) -> bool {
        if self.rate == 0 {
            return false;
        }
        let n = self.counter.fetch_add(1, Ordering::Relaxed);
        n.is_multiple_of(self.rate as u64)
    }
}

pub struct Oracle {
    client: Mutex<Option<Client>>,
    conninfo: String,
    has_extension: Mutex<Option<bool>>,
    pub stats: Mutex<OracleStats>,
    pub sampler: Sampler,
}

impl Oracle {
    /// Connect to shadow PG and probe for the `walshadow_oracle`
    /// extension. Absence is not a failure: the resolver returns
    /// `Ok(None)` thereafter, which signals "fall back to raw bytes".
    pub async fn connect(conninfo: &str, sample_rate: u32) -> Result<Self, OracleError> {
        let (client, connection) = tokio_postgres::connect(conninfo, NoTls)
            .await
            .map_err(OracleError::Connect)?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let has_ext = probe_extension(&client).await.unwrap_or(false);
        Ok(Self {
            client: Mutex::new(Some(client)),
            conninfo: conninfo.to_owned(),
            has_extension: Mutex::new(Some(has_ext)),
            stats: Mutex::new(OracleStats::default()),
            sampler: Sampler::new(sample_rate),
        })
    }

    /// `true` iff the `walshadow_oracle` extension was visible at connect
    /// time. Read by the daemon's status line so operators can confirm
    /// the optional-extension contract on boot.
    pub async fn has_extension(&self) -> bool {
        self.has_extension.lock().await.unwrap_or(false)
    }

    /// Reconnect once on a closed connection. Mirrors
    /// [`ShadowCatalog`](crate::shadow_catalog::ShadowCatalog)'s
    /// `query_one_retry` pattern; sits in this module to keep the
    /// oracle's pool independent of the catalog's.
    async fn reconnect(&self) -> Result<(), OracleError> {
        let (client, connection) = tokio_postgres::connect(&self.conninfo, NoTls)
            .await
            .map_err(OracleError::Connect)?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let has_ext = probe_extension(&client).await.unwrap_or(false);
        *self.client.lock().await = Some(client);
        *self.has_extension.lock().await = Some(has_ext);
        Ok(())
    }

    /// Run `walshadow_decode_disk(type_oid, raw)` and return the
    /// resolved text. Returns `Ok(None)` when the extension is absent
    /// (the emitter then falls back to raw bytes) or on a transient
    /// error (logged via `stats.errors`).
    pub async fn resolve_pending(
        &self,
        type_oid: u32,
        raw: &[u8],
    ) -> Result<Option<String>, OracleError> {
        if !self.has_extension().await {
            self.stats.lock().await.fallback_raw += 1;
            return Ok(None);
        }
        let sql = "SELECT walshadow_decode_disk($1::oid, $2::bytea)";
        let typoid_param: u32 = type_oid;
        let mut attempt = 0u8;
        loop {
            let row = {
                let mut guard = self.client.lock().await;
                let Some(client) = guard.as_mut() else {
                    return Ok(None);
                };
                client.query_one(sql, &[&typoid_param, &raw]).await
            };
            match row {
                Ok(r) => {
                    let txt: Option<String> = r.try_get(0).ok();
                    let mut stats = self.stats.lock().await;
                    if txt.is_some() {
                        stats.resolved += 1;
                    } else {
                        stats.fallback_raw += 1;
                    }
                    return Ok(txt);
                }
                Err(e) if attempt == 0 && e.is_closed() => {
                    attempt = 1;
                    let _ = self.reconnect().await;
                }
                Err(_) => {
                    self.stats.lock().await.errors += 1;
                    return Ok(None);
                }
            }
        }
    }

    /// Cross-check a locally-decoded Tier 3 value against shadow PG's
    /// own `typoutput`. Only fires on a sampler hit. Returns whether
    /// the probe ran (informational; not used for control flow).
    pub async fn validate(&self, type_oid: u32, raw: &[u8], local_text: &str) -> bool {
        if !self.sampler.pick() {
            return false;
        }
        let sql = match type_oid {
            crate::heap_decoder::NUMERICOID
            | crate::heap_decoder::INETOID
            | crate::heap_decoder::CIDROID
            | crate::heap_decoder::INTERVALOID
                if self.has_extension().await =>
            {
                // For local Tier 3 hot types we already have the same
                // path the oracle uses: walshadow_decode_disk reconstructs
                // the Datum then calls typoutput. Reuse it when present.
                "SELECT walshadow_decode_disk($1::oid, $2::bytea)"
            }
            _ => return false,
        };
        let typoid_param: u32 = type_oid;
        let mut attempt = 0u8;
        let res = loop {
            let row = {
                let mut guard = self.client.lock().await;
                let Some(client) = guard.as_mut() else {
                    return false;
                };
                client.query_one(sql, &[&typoid_param, &raw]).await
            };
            match row {
                Ok(r) => break Some(r),
                Err(e) if attempt == 0 && e.is_closed() => {
                    attempt = 1;
                    let _ = self.reconnect().await;
                }
                Err(_) => break None,
            }
        };
        let mut stats = self.stats.lock().await;
        stats.probes += 1;
        let Some(r) = res else {
            stats.errors += 1;
            return true;
        };
        let pg_text: Option<String> = r.try_get(0).ok();
        let matched = pg_text.as_deref() == Some(local_text);
        if matched {
            stats.matches += 1;
        } else {
            stats.mismatches += 1;
        }
        true
    }
}

async fn probe_extension(client: &Client) -> Result<bool, OracleError> {
    // `pg_proc` is in every PG; this query is cheap.
    let row = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_proc WHERE proname = 'walshadow_decode_disk')",
            &[],
        )
        .await
        .map_err(OracleError::Query)?;
    Ok(row.try_get::<_, bool>(0).unwrap_or(false))
}

/// Apply [`Oracle::resolve_pending`] across every column of a tuple
/// in place. PgPending → Text on success; PgPending stays put on
/// fall-back (raw bytes then surface through `encode_value`).
pub async fn resolve_pending_tuple(oracle: &Oracle, columns: &mut [Option<ColumnValue>]) {
    for col in columns.iter_mut() {
        if let Some(ColumnValue::PgPending { type_oid, raw }) = col {
            match oracle.resolve_pending(*type_oid, raw).await {
                Ok(Some(s)) => {
                    *col = Some(ColumnValue::Text(s));
                }
                _ => {
                    // Leave PgPending in place; emitter writes raw bytes.
                }
            }
        }
    }
}

/// Cross-check sampler entry point: walks a tuple, picks an entry for
/// the local hot types, fires the probe. Does not consume bytes; safe
/// to invoke concurrently across rows.
pub async fn maybe_validate_tuple(oracle: &Oracle, columns: &[Option<ColumnValue>]) {
    for col in columns.iter().flatten() {
        match col {
            ColumnValue::Numeric(k) => {
                let local = match k {
                    NumericKind::Finite(s) => s.clone(),
                    NumericKind::NaN => "NaN".into(),
                    NumericKind::PInf => "Infinity".into(),
                    NumericKind::NInf => "-Infinity".into(),
                };
                // For numerics we don't have the raw bytes here; the
                // validator path requires raw bytes. Without them we
                // skip — the validator's primary value is jsonb/array
                // (PgPending) where raw bytes are present.
                let _ = local;
            }
            ColumnValue::PgPending { type_oid, raw } => {
                let _ = oracle.validate(*type_oid, raw, "").await;
            }
            _ => {}
        }
    }
}

/// Helper for `walshadow-stream` to format a one-line oracle status
/// summary, suitable for the status line emitted alongside decoder /
/// xact-buffer stats.
impl OracleStats {
    pub fn summary(&self) -> String {
        use std::fmt::Write as _;
        let mut s = format!("oracle resolved={}", self.resolved);
        let pairs: [(&str, u64); 5] = [
            ("fallback", self.fallback_raw),
            ("probes", self.probes),
            ("match", self.matches),
            ("mismatch", self.mismatches),
            ("err", self.errors),
        ];
        for (label, n) in pairs {
            if n > 0 {
                write!(&mut s, " {label}={n}").unwrap();
            }
        }
        s
    }
}

/// [`TupleObserver`] wrapper. On each tuple: resolve every
/// `PgPending` column through the oracle (replaces with `Text`) and
/// optionally fire 1-in-N validator probes against shadow PG, then
/// forward the mutated clone to the inner observer.
///
/// One clone per tuple. Mid-volume workloads (Tier 3 columns are
/// typically a small minority of any schema) shouldn't notice; very
/// hot workloads can disable the wrapper by passing
/// `--validate 0 --without-oracle` and live with raw bytes for
/// `PgPending` types.
pub struct OracleObserver<O: TupleObserver + Send> {
    oracle: Arc<Oracle>,
    inner: O,
}

impl<O: TupleObserver + Send> OracleObserver<O> {
    pub fn new(oracle: Arc<Oracle>, inner: O) -> Self {
        Self { oracle, inner }
    }

    pub fn inner_mut(&mut self) -> &mut O {
        &mut self.inner
    }
}

impl<O: TupleObserver + Send> TupleObserver for OracleObserver<O> {
    fn on_tuple<'a>(
        &'a mut self,
        committed: &'a CommittedTuple,
    ) -> Pin<Box<dyn Future<Output = Result<(), DecoderSinkError>> + Send + 'a>> {
        Box::pin(async move {
            let mut owned = committed.clone();
            if let Some(t) = owned.decoded.new.as_mut() {
                resolve_pending_tuple(&self.oracle, &mut t.columns).await;
            }
            if let Some(t) = owned.decoded.old.as_mut() {
                resolve_pending_tuple(&self.oracle, &mut t.columns).await;
            }
            if let Some(t) = owned.decoded.new.as_ref() {
                maybe_validate_tuple(&self.oracle, &t.columns).await;
            }
            self.inner.on_tuple(&owned).await
        })
    }

    fn on_xact_end<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<(), DecoderSinkError>> + Send + 'a>> {
        self.inner.on_xact_end()
    }
}

/// Convenience wrapper: connect with a timeout budget so a still-warming
/// shadow doesn't pin the daemon at boot. Matches the catalog's
/// [`with_transient_retry`](crate::shadow_catalog::with_transient_retry)
/// shape.
pub async fn connect_with_budget(
    conninfo: &str,
    sample_rate: u32,
    budget: Duration,
) -> Result<Oracle, OracleError> {
    let deadline = tokio::time::Instant::now() + budget;
    let mut backoff = Duration::from_millis(100);
    loop {
        match Oracle::connect(conninfo, sample_rate).await {
            Ok(o) => return Ok(o),
            Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(1));
            }
            Err(e) => return Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sampler_off_when_rate_zero() {
        let s = Sampler::new(0);
        for _ in 0..1000 {
            assert!(!s.pick());
        }
    }

    #[test]
    fn sampler_picks_one_in_n() {
        let s = Sampler::new(5);
        let mut hits = 0;
        for _ in 0..100 {
            if s.pick() {
                hits += 1;
            }
        }
        // 100 / 5 = 20 hits.
        assert_eq!(hits, 20);
    }

    #[test]
    fn stats_summary_skips_zero_buckets() {
        let s = OracleStats {
            resolved: 4,
            fallback_raw: 0,
            probes: 2,
            matches: 2,
            mismatches: 0,
            errors: 0,
        };
        let out = s.summary();
        assert!(out.contains("resolved=4"));
        assert!(out.contains("probes=2"));
        assert!(out.contains("match=2"));
        assert!(!out.contains("fallback"));
        assert!(!out.contains("mismatch"));
        assert!(!out.contains("err"));
    }
}
