//! PgPending resolver backed by shadow PG.
//!
//! For varlena types outside walshadow's local matrix (`jsonb`, arrays,
//! `tsvector`, ranges, custom domains, ...), [`Oracle::resolve_pending`] runs
//! `walshadow_decode_disk(oid, bytea) -> text` on shadow PG, replacing
//! PgPending with [`ColumnValue::Text`]. Extension is optional: when absent
//! resolver returns `Ok(None)` and emitter ships raw on-disk bytes.
//!
//! Separate `Client` from [`ShadowCatalog`](crate::catalog::shadow_catalog::ShadowCatalog):
//! oracle queries don't observe replay-LSN gating and mustn't pessimise the
//! catalog's query-one path.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use backon::{ExponentialBuilder, Retryable};
use thiserror::Error;
use tokio::sync::Mutex;
use tokio_postgres::{Client, NoTls};

use crate::decode::heap_decoder::ColumnValue;

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
        /// SQL / connection errors, single bucket
        pub errors,
    }
}

pub struct Oracle {
    client: Mutex<Option<Client>>,
    conninfo: String,
    has_extension: AtomicBool,
    pub stats: Arc<OracleStats>,
}

impl Oracle {
    /// Connect to shadow PG, probe for `walshadow` extension. Absence not a
    /// failure: resolver returns `Ok(None)` thereafter (fall back to raw bytes).
    pub async fn connect(conninfo: &str) -> Result<Self, OracleError> {
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

impl OracleStats {
    pub fn summary(&self) -> String {
        use std::fmt::Write as _;
        let ld = |a: &AtomicU64| a.load(Ordering::Relaxed);
        let mut s = format!("oracle resolved={}", ld(&self.resolved));
        let pairs: [(&str, u64); 2] = [
            ("fallback", ld(&self.fallback_raw)),
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

/// Connect with a timeout budget so a still-warming shadow doesn't pin the
/// daemon at boot. Matches the catalog's
/// [`with_transient_retry`](crate::catalog::shadow_catalog::with_transient_retry) shape.
pub async fn connect_with_budget(conninfo: &str, budget: Duration) -> Result<Oracle, OracleError> {
    let deadline = tokio::time::Instant::now() + budget;
    (|| Oracle::connect(conninfo))
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
    fn stats_summary_skips_zero_buckets() {
        let s = OracleStats::default();
        s.resolved.store(4, Ordering::Relaxed);
        s.errors.store(2, Ordering::Relaxed);
        let out = s.summary();
        assert!(out.contains("resolved=4"));
        assert!(out.contains("err=2"));
        assert!(!out.contains("fallback"));
    }
}
