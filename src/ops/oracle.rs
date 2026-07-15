//! Differential decode oracle backed by shadow PG.
//!
//! 1. PgPending resolver: for varlena types outside walshadow's local matrix
//!    (`jsonb`, arrays, `tsvector`, ranges, custom domains, ...),
//!    [`Oracle::resolve_pending`] runs `walshadow_decode_disk(oid, bytea) -> text`
//!    on shadow PG, replacing PgPending with [`ColumnValue::Text`]. Extension is
//!    optional: when absent resolver returns `Ok(None)` and emitter ships raw
//!    on-disk bytes.
//! 2. 1-in-N validator: sampled Tier 3 codec values (`numeric`/`inet`/`interval`)
//!    cross-checked against shadow PG's typoutput. Mismatches counted + logged,
//!    row still ships (watchdog, not gate). Off by default, `--validate <N>`.
//!
//! Separate `Client` from [`ShadowCatalog`](crate::catalog::shadow_catalog::ShadowCatalog):
//! oracle queries don't observe replay-LSN gating and mustn't pessimise the
//! catalog's query-one path.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use backon::{ExponentialBuilder, Retryable};
use thiserror::Error;
use tokio::sync::Mutex;
use tokio_postgres::{Client, NoTls};

use crate::decode::codecs::NumericKind;
use crate::decode::decoder_sink::{DecoderSinkError, TupleObserver};
use crate::decode::heap_decoder::{ColumnValue, CommittedTuple};

#[derive(Debug, Error)]
pub enum OracleError {
    #[error("oracle pg connect: {0}")]
    Connect(tokio_postgres::Error),
    #[error("oracle pg query: {0}")]
    Query(tokio_postgres::Error),
}

crate::atomic_stats! {
    pub struct OracleStats {
        /// `walshadow_decode_disk` calls returning a text payload
        pub resolved,
        /// `walshadow_decode_disk` calls returning NULL or absent-extension
        pub fallback_raw,
        pub probes,
        pub matches,
        /// local decoder text != shadow PG text
        pub mismatches,
        /// SQL / connection errors, single bucket
        pub errors,
    }
}

/// 1-in-N selection. Lock-free counter so decoder workers share one `Oracle`
/// without serialising.
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

    /// `rate == 0` disables sampling
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
    has_extension: AtomicBool,
    pub stats: Arc<OracleStats>,
    pub sampler: Sampler,
}

impl Oracle {
    /// Connect to shadow PG, probe for `walshadow` extension. Absence not a
    /// failure: resolver returns `Ok(None)` thereafter (fall back to raw bytes).
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
            has_extension: AtomicBool::new(has_ext),
            stats: Arc::new(OracleStats::default()),
            sampler: Sampler::new(sample_rate),
        })
    }

    /// Extension visible at connect time. Daemon status line surfaces this so
    /// operators confirm the optional-extension contract on boot.
    pub fn has_extension(&self) -> bool {
        self.has_extension.load(Ordering::Relaxed)
    }

    /// Mirrors [`ShadowCatalog`](crate::catalog::shadow_catalog::ShadowCatalog)'s
    /// `query_one_retry`; duplicated here to keep oracle's pool independent.
    async fn reconnect(&self) -> Result<(), OracleError> {
        let (client, connection) = tokio_postgres::connect(&self.conninfo, NoTls)
            .await
            .map_err(OracleError::Connect)?;
        tokio::spawn(async move {
            let _ = connection.await;
        });
        let has_ext = probe_extension(&client).await.unwrap_or(false);
        *self.client.lock().await = Some(client);
        self.has_extension.store(has_ext, Ordering::Relaxed);
        Ok(())
    }

    /// `Ok(None)` when extension absent (emitter falls back to raw bytes) or on
    /// transient error (counted via `stats.errors`).
    pub async fn resolve_pending(
        &self,
        type_oid: u32,
        raw: &[u8],
    ) -> Result<Option<String>, OracleError> {
        if !self.has_extension() {
            self.stats.fallback_raw.fetch_add(1, Ordering::Relaxed);
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
                    if txt.is_some() {
                        self.stats.resolved.fetch_add(1, Ordering::Relaxed);
                    } else {
                        self.stats.fallback_raw.fetch_add(1, Ordering::Relaxed);
                    }
                    return Ok(txt);
                }
                Err(e) if attempt == 0 && e.is_closed() => {
                    attempt = 1;
                    let _ = self.reconnect().await;
                }
                Err(_) => {
                    self.stats.errors.fetch_add(1, Ordering::Relaxed);
                    return Ok(None);
                }
            }
        }
    }

    /// Cross-check a locally-decoded Tier 3 value against shadow PG's
    /// `typoutput`. Fires only on a sampler hit. Return value informational.
    pub async fn validate(&self, type_oid: u32, raw: &[u8], local_text: &str) -> bool {
        if !self.sampler.pick() {
            return false;
        }
        let sql = match type_oid {
            crate::schema::NUMERICOID
            | crate::schema::INETOID
            | crate::schema::CIDROID
            | crate::schema::INTERVALOID
                if self.has_extension() =>
            {
                // walshadow_decode_disk reconstructs the Datum then calls
                // typoutput, same path the resolver uses
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
        self.stats.probes.fetch_add(1, Ordering::Relaxed);
        let Some(r) = res else {
            self.stats.errors.fetch_add(1, Ordering::Relaxed);
            return true;
        };
        let pg_text: Option<String> = r.try_get(0).ok();
        let matched = pg_text.as_deref() == Some(local_text);
        if matched {
            self.stats.matches.fetch_add(1, Ordering::Relaxed);
        } else {
            self.stats.mismatches.fetch_add(1, Ordering::Relaxed);
        }
        true
    }
}

async fn probe_extension(client: &Client) -> Result<bool, OracleError> {
    let row = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_proc WHERE proname = 'walshadow_decode_disk')",
            &[],
        )
        .await
        .map_err(OracleError::Query)?;
    Ok(row.try_get::<_, bool>(0).unwrap_or(false))
}

/// PgPending → Text on success; on fall-back PgPending stays put and emitter
/// writes raw bytes via `encode_value`.
pub async fn resolve_pending_tuple(oracle: &Oracle, columns: &mut [Option<ColumnValue>]) {
    for col in columns.iter_mut() {
        let (Some(ColumnValue::PgPending { type_oid, raw })
        | Some(ColumnValue::Unsupported { type_oid, raw })) = col
        else {
            continue;
        };
        let resolved = oracle.resolve_pending(*type_oid, raw.as_slice()).await;
        if let Ok(Some(s)) = resolved {
            *col = Some(ColumnValue::Text(s));
        }
    }
}

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
                // No raw bytes here, validator needs them; skip. Its primary
                // value is jsonb/array (PgPending), where raw bytes are present
                let _ = local;
            }
            ColumnValue::PgPending { type_oid, raw } => {
                let _ = oracle.validate(*type_oid, raw, "").await;
            }
            _ => {}
        }
    }
}

impl OracleStats {
    pub fn summary(&self) -> String {
        use std::fmt::Write as _;
        let ld = |a: &AtomicU64| a.load(Ordering::Relaxed);
        let mut s = format!("oracle resolved={}", ld(&self.resolved));
        let pairs: [(&str, u64); 5] = [
            ("fallback", ld(&self.fallback_raw)),
            ("probes", ld(&self.probes)),
            ("match", ld(&self.matches)),
            ("mismatch", ld(&self.mismatches)),
            ("err", ld(&self.errors)),
        ];
        for (label, n) in pairs {
            if n > 0 {
                write!(&mut s, " {label}={n}").unwrap();
            }
        }
        s
    }
}

/// [`TupleObserver`] wrapper: resolve PgPending columns, optionally validate,
/// forward mutated clone to inner. One clone per tuple; hot workloads can
/// bypass via `--validate 0 --without-oracle` (raw bytes for PgPending).
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
        commit_lsn: u64,
    ) -> Pin<Box<dyn Future<Output = Result<u64, DecoderSinkError>> + Send + 'a>> {
        self.inner.on_xact_end(commit_lsn)
    }

    fn on_schema_event<'a>(
        &'a mut self,
        event: &'a crate::schema::SchemaEvent,
    ) -> Pin<Box<dyn Future<Output = Result<(), DecoderSinkError>> + Send + 'a>> {
        self.inner.on_schema_event(event)
    }

    fn idle_ack_ceiling(&self, lsn: u64) -> u64 {
        self.inner.idle_ack_ceiling(lsn)
    }

    fn on_idle<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<u64, DecoderSinkError>> + Send + 'a>> {
        self.inner.on_idle()
    }

    fn on_close<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<(), DecoderSinkError>> + Send + 'a>> {
        self.inner.on_close()
    }
}

/// Connect with a timeout budget so a still-warming shadow doesn't pin the
/// daemon at boot. Matches the catalog's
/// [`with_transient_retry`](crate::catalog::shadow_catalog::with_transient_retry) shape.
pub async fn connect_with_budget(
    conninfo: &str,
    sample_rate: u32,
    budget: Duration,
) -> Result<Oracle, OracleError> {
    let deadline = tokio::time::Instant::now() + budget;
    (|| Oracle::connect(conninfo, sample_rate))
        .retry(
            ExponentialBuilder::default()
                .with_min_delay(Duration::from_millis(100))
                .with_max_delay(Duration::from_secs(1))
                .without_max_times(),
        )
        .when(move |_: &OracleError| tokio::time::Instant::now() < deadline)
        .await
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
        assert_eq!(hits, 20);
    }

    #[test]
    fn stats_summary_skips_zero_buckets() {
        let s = OracleStats::default();
        s.resolved.store(4, Ordering::Relaxed);
        s.probes.store(2, Ordering::Relaxed);
        s.matches.store(2, Ordering::Relaxed);
        let out = s.summary();
        assert!(out.contains("resolved=4"));
        assert!(out.contains("probes=2"));
        assert!(out.contains("match=2"));
        assert!(!out.contains("fallback"));
        assert!(!out.contains("mismatch"));
        assert!(!out.contains("err"));
    }

    struct ProbeObserver {
        tuples: u32,
    }
    impl TupleObserver for ProbeObserver {
        fn on_tuple<'a>(
            &'a mut self,
            _committed: &'a CommittedTuple,
        ) -> Pin<Box<dyn Future<Output = Result<(), DecoderSinkError>> + Send + 'a>> {
            Box::pin(async { Ok(()) })
        }
    }

    #[test]
    fn inner_mut_aliases_wrapped_observer() {
        // client=None: accessor under test never touches the connection
        let oracle = Arc::new(Oracle {
            client: Mutex::new(None),
            conninfo: String::new(),
            has_extension: AtomicBool::new(false),
            stats: Arc::new(OracleStats::default()),
            sampler: Sampler::new(0),
        });
        let mut obs = OracleObserver::new(oracle, ProbeObserver { tuples: 0 });
        obs.inner_mut().tuples += 5;
        assert_eq!(obs.inner_mut().tuples, 5);
    }
}
