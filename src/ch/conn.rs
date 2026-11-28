//! Cached ClickHouse connection per configured destination

use std::future::Future;
use std::sync::Arc;
use std::time::Instant;

use backon::BackoffBuilder;
use clickhouse_c::BoxedAsyncClient;

use crate::ch::{
    CompressionChoice, ConnectionConfig, EmitterError, UNKNOWN_DATABASE, connect_client,
    exec_drain, is_retryable, quote_ident,
};
use crate::config::DestEmitter;
use crate::emitter_config::EmitterConfig;

/// Connect to `cfg.database`, first creating it over a session on `default`
/// when absent, since CH refuses a handshake naming a missing database.
/// `true` when it was created
pub async fn connect_creating_database(
    cfg: &EmitterConfig,
) -> Result<(BoxedAsyncClient, bool), EmitterError> {
    match connect_client(cfg).await {
        Err(EmitterError::Client(e)) if e.server_code == UNKNOWN_DATABASE => {}
        result => return result.map(|client| (client, false)),
    }
    let mut on_default = cfg.clone();
    on_default.conn.database = "default".into();
    let mut client = connect_client(&on_default).await?;
    exec_drain(
        &mut client,
        &format!(
            "CREATE DATABASE IF NOT EXISTS {}",
            quote_ident(&cfg.conn.database)
        ),
    )
    .await?;
    tracing::info!(database = %cfg.conn.database, "created destination database");
    Ok((connect_client(cfg).await?, true))
}

struct Connection {
    client: Option<BoxedAsyncClient>,
    dialed: Option<Arc<EmitterConfig>>,
    compression: CompressionChoice,
    last_used: Instant,
}

impl Default for Connection {
    fn default() -> Self {
        Self {
            client: None,
            dialed: None,
            compression: CompressionChoice::None,
            last_used: Instant::now(),
        }
    }
}

/// Cache one connection per destination; retry and idle expiry apply independently.
pub struct ChConn {
    instance: Option<String>,
    selected: Option<Arc<EmitterConfig>>,
    /// Dial with this instead of configured compression
    compression: Option<CompressionChoice>,
    active: Connection,
    parked: foldhash::HashMap<Option<String>, Connection>,
    dest: Arc<DestEmitter>,
    dials: u64,
}

impl ChConn {
    /// Deferred dial: first use connects
    pub fn deferred(dest: Arc<DestEmitter>) -> Self {
        Self {
            dest,
            instance: None,
            selected: None,
            compression: None,
            active: Connection::default(),
            parked: Default::default(),
            dials: 0,
        }
    }

    /// Eager dial, so an unreachable endpoint fails at construction
    pub async fn connect(dest: Arc<DestEmitter>) -> Result<Self, EmitterError> {
        let mut conn = Self::deferred(dest);
        conn.dial().await?;
        conn.dials = 0;
        Ok(conn)
    }

    /// Live config this connection dials and reads its knobs from
    pub fn config(&self) -> Arc<EmitterConfig> {
        self.selected.clone().unwrap_or_else(|| self.dest.current())
    }

    pub fn select(&mut self, instance: Option<&str>) -> Result<(), EmitterError> {
        let config = instance.map(|name| self.dest.instance(name)).transpose()?;
        if self.instance.as_deref() != instance {
            let next = self
                .parked
                .remove(&instance.map(str::to_owned))
                .unwrap_or_default();
            let previous = std::mem::replace(&mut self.active, next);
            self.parked.insert(self.instance.take(), previous);
            self.instance = instance.map(str::to_owned);
        }
        self.selected = config;
        Ok(())
    }

    /// Dial every destination with `compression`, so prepared blocks sealed
    /// in it send as is
    pub fn force_compression(&mut self, compression: CompressionChoice) {
        self.compression = Some(compression);
    }

    /// Redial now, for settings fixed at connect (codec, host, credentials)
    pub async fn dial(&mut self) -> Result<(), EmitterError> {
        self.active.client = None;
        self.ready().await?;
        Ok(())
    }

    pub async fn ready(&mut self) -> Result<&mut BoxedAsyncClient, EmitterError> {
        let client = self.take_ready().await?;
        Ok(self.active.client.insert(client))
    }

    /// Dials since the last call, excluding the one at construction
    pub fn take_dials(&mut self) -> u64 {
        std::mem::take(&mut self.dials)
    }

    /// [`Self::retry_when`] over [`is_retryable`]
    pub async fn retry<T, Fut>(
        &mut self,
        op: impl FnMut(BoxedAsyncClient) -> Fut,
        on_retry: impl FnMut(&EmitterError, u32),
    ) -> Result<T, EmitterError>
    where
        Fut: Future<Output = (BoxedAsyncClient, Result<T, EmitterError>)>,
    {
        self.retry_when(is_retryable, op, on_retry).await
    }

    /// Resend `op` until it succeeds, `when` rejects the error, or `backoff`
    /// runs out. Each resend gets a fresh connection, so a failed dial is
    /// itself a retryable attempt.
    ///
    /// `op` owns the client for one attempt and hands it back: a closure
    /// lending `&mut` client can't promise a `Send` future on stable, and the
    /// inserter pool spawns.
    pub async fn retry_when<T, Fut>(
        &mut self,
        when: impl Fn(&EmitterError) -> bool,
        mut op: impl FnMut(BoxedAsyncClient) -> Fut,
        mut on_retry: impl FnMut(&EmitterError, u32),
    ) -> Result<T, EmitterError>
    where
        Fut: Future<Output = (BoxedAsyncClient, Result<T, EmitterError>)>,
    {
        let mut backoff = self.config().retry.backoff().build();
        let mut attempt = 0u32;
        loop {
            let error = match self.take_ready().await {
                Ok(client) => {
                    let (client, result) = op(client).await;
                    self.active.client = Some(client);
                    match result {
                        Ok(value) => {
                            self.active.last_used = Instant::now();
                            return Ok(value);
                        }
                        Err(e) => e,
                    }
                }
                Err(e) => e,
            };
            if !when(&error) {
                return Err(error);
            }
            let Some(delay) = backoff.next() else {
                return Err(error);
            };
            on_retry(&error, attempt);
            attempt += 1;
            self.active.client = None;
            tokio::time::sleep(delay).await;
        }
    }

    async fn take_ready(&mut self) -> Result<BoxedAsyncClient, EmitterError> {
        if let Some(name) = &self.instance {
            self.selected = Some(self.dest.instance(name)?);
        }
        let config = self.config();
        let compression = self.compression.unwrap_or(config.compression);
        let moved = self
            .active
            .dialed
            .as_ref()
            .is_none_or(|dialed| !Arc::ptr_eq(dialed, &config));
        if moved
            || compression != self.active.compression
            || self.active.last_used.elapsed() >= config.idle_reconnect()
        {
            self.active.client = None;
        }
        if let Some(client) = self.active.client.take() {
            return Ok(client);
        }
        let mut dial = config.clone();
        if compression != config.compression {
            Arc::make_mut(&mut dial).compression = compression;
        }
        let client = if self.instance.is_some() {
            connect_creating_database(&dial).await?.0
        } else {
            connect_client(&*dial).await?
        };
        self.active.dialed = Some(config);
        self.active.compression = compression;
        self.active.last_used = Instant::now();
        self.dials += 1;
        Ok(client)
    }
}
