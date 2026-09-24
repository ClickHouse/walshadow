//! ClickHouse connection and query lifecycle

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use backon::BackoffBuilder;
use clickhouse_c::{AsyncClient, BoxedAsyncClient, ClientOpts, Codec, Compression, Event};
use thiserror::Error;

use crate::config::DestEmitter;
use crate::emit::ch_emitter::EmitterConfig;

#[cfg(test)]
pub(crate) mod test_support;
pub mod types;

#[derive(Debug, Error)]
pub enum EmitterError {
    #[error("clickhouse-c: {0}")]
    Client(#[from] clickhouse_c::Error),
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("config: {0}")]
    Config(String),
    #[error("type: {0}")]
    Type(String),
    #[error("catalog: {0}")]
    Catalog(String),
    #[error("compression `{0}` requested but feature disabled at compile time")]
    CompressionUnsupported(&'static str),
    #[error("no table mapping for source relation `{0}`")]
    NoTableMapping(String),
    #[error("unsupported column value for {target_column}: {kind}")]
    UnsupportedValue {
        target_column: String,
        kind: &'static str,
    },
    #[error("CH server exception {code}: {message}")]
    ServerException { code: i32, message: String },
    #[error("CH operation timed out after {secs}s")]
    Timeout { secs: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompressionChoice {
    None,
    #[default]
    Lz4,
    Zstd,
}

impl std::str::FromStr for CompressionChoice {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        match s.to_ascii_lowercase().as_str() {
            "none" | "off" | "" => Ok(Self::None),
            "lz4" => Ok(Self::Lz4),
            "zstd" => Ok(Self::Zstd),
            other => Err(format!(
                "unknown compression `{other}` (expected none / lz4 / zstd)"
            )),
        }
    }
}

impl CompressionChoice {
    fn to_wire(self) -> Compression {
        match self {
            Self::None => Compression::None,
            Self::Lz4 => Compression::Lz4,
            Self::Zstd => Compression::Zstd,
        }
    }

    /// Every codec this build links, whatever `self` asks to *send*. The
    /// choice only sets the wire flag; a server answers in whatever method it
    /// is configured for, so a client that installed one codec — or none —
    /// fails on the first frame it cannot decode
    pub fn build_codec(self) -> Result<Option<Pin<Box<Codec>>>, EmitterError> {
        match self {
            Self::Lz4 if !cfg!(feature = "lz4") => {
                return Err(EmitterError::CompressionUnsupported("lz4"));
            }
            Self::Zstd if !cfg!(feature = "zstd") => {
                return Err(EmitterError::CompressionUnsupported("zstd"));
            }
            _ => {}
        }
        if !cfg!(any(feature = "lz4", feature = "zstd")) {
            return Ok(None);
        }
        let mut codec = Codec::empty();
        // Slots are per method and each init fills only its own, so one codec
        // decodes both
        unsafe {
            let raw = codec.as_mut().raw_mut();
            #[cfg(feature = "lz4")]
            clickhouse_c::sys::chc_lz4_codec_init(raw);
            #[cfg(feature = "zstd")]
            clickhouse_c::sys::chc_zstd_codec_init(raw);
        }
        Ok(Some(codec))
    }
}

pub trait ConnectionConfig {
    fn host(&self) -> &str;
    fn port(&self) -> u16;
    fn database(&self) -> &str;
    fn user(&self) -> &str;
    fn password(&self) -> &str;
    fn secure(&self) -> bool;
    fn tls_config(&self) -> Option<Arc<clickhouse_c::tls::rustls::ClientConfig>>;
    fn compression(&self) -> CompressionChoice;
    fn idle_reconnect(&self) -> Duration;
}

pub async fn connect_client(
    config: &impl ConnectionConfig,
) -> Result<BoxedAsyncClient, EmitterError> {
    let compression = config.compression();
    let codec = compression.build_codec()?;
    let mut opts = ClientOpts::new()
        .database(config.database())
        .user(config.user())
        .password(config.password());
    opts.compression = compression.to_wire();
    let addr = (config.host(), config.port());
    if config.secure() {
        let tls = config
            .tls_config()
            .unwrap_or_else(clickhouse_c::tls::default_config);
        let client = AsyncClient::connect_tls(addr, config.host(), opts, codec, tls).await?;
        Ok(client.boxed())
    } else {
        let client = AsyncClient::connect(addr, opts, codec).await?;
        Ok(client.boxed())
    }
}

pub async fn drain_to_end_of_stream(client: &mut BoxedAsyncClient) -> Result<(), EmitterError> {
    loop {
        match client.recv_event().await? {
            Event::EndOfStream => return Ok(()),
            Event::Exception(exc) => {
                return Err(EmitterError::ServerException {
                    code: exc.code(),
                    message: String::from_utf8_lossy(exc.display_text()).into_owned(),
                });
            }
            _ => {}
        }
    }
}

pub async fn with_timeout<T>(
    duration: Duration,
    future: impl Future<Output = Result<T, EmitterError>>,
) -> Result<T, EmitterError> {
    tokio::time::timeout(duration, future)
        .await
        .unwrap_or_else(|_| {
            Err(EmitterError::Timeout {
                secs: duration.as_secs(),
            })
        })
}

pub async fn exec_drain(
    client: &mut BoxedAsyncClient,
    sql: &str,
    timeout: Duration,
) -> Result<(), EmitterError> {
    with_timeout(timeout, async {
        client.send_query(sql, None).await?;
        drain_to_end_of_stream(client).await
    })
    .await
}

/// Connection owned by its retry loop. A cleared client redials on next use,
/// so a failed reconnect spends retry budget instead of aborting the caller.
pub struct ChConn {
    dest: Arc<DestEmitter>,
    client: Option<BoxedAsyncClient>,
    dialed: Option<Arc<EmitterConfig>>,
    last_used: Instant,
    dials: u64,
}

impl ChConn {
    /// Deferred dial: first use connects
    pub fn deferred(dest: Arc<DestEmitter>) -> Self {
        Self {
            dest,
            client: None,
            dialed: None,
            last_used: Instant::now(),
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
        self.dest.current()
    }

    /// Redial now, for settings fixed at connect (codec, host, credentials)
    pub async fn dial(&mut self) -> Result<(), EmitterError> {
        self.client = None;
        self.ready().await?;
        Ok(())
    }

    pub async fn ready(&mut self) -> Result<&mut BoxedAsyncClient, EmitterError> {
        let config = self.dest.current();
        let moved = self
            .dialed
            .as_ref()
            .is_none_or(|dialed| !Arc::ptr_eq(dialed, &config));
        if moved || self.last_used.elapsed() >= config.idle_reconnect() {
            self.client = None;
        }
        if self.client.is_none() {
            self.client = Some(connect_client(&*config).await?);
            self.dialed = Some(config);
            self.last_used = Instant::now();
            self.dials += 1;
        }
        Ok(self.client.as_mut().expect("just connected"))
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
        let mut backoff = self.dest.current().retry.backoff().build();
        let mut attempt = 0u32;
        loop {
            let error = match self.take_ready().await {
                Ok(client) => {
                    let (client, result) = op(client).await;
                    self.client = Some(client);
                    match result {
                        Ok(value) => {
                            self.last_used = Instant::now();
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
            self.client = None;
            tokio::time::sleep(delay).await;
        }
    }

    async fn take_ready(&mut self) -> Result<BoxedAsyncClient, EmitterError> {
        self.ready().await?;
        Ok(self.client.take().expect("just connected"))
    }
}

pub fn is_retryable(error: &EmitterError) -> bool {
    matches!(
        error,
        EmitterError::Io(_)
            | EmitterError::Client(_)
            | EmitterError::ServerException { .. }
            | EmitterError::Timeout { .. }
    )
}

pub fn quote_ident(name: &str) -> String {
    format!("`{}`", name.replace('`', "``"))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `compression = "none"` still has to decode a compressed response: the
    /// wire flag says what we send, not what the server answers in. A server
    /// set to ZSTD against a codec-less client fails on the first frame
    #[test]
    fn every_choice_installs_a_decoder() {
        let choices = [
            CompressionChoice::None,
            #[cfg(feature = "lz4")]
            CompressionChoice::Lz4,
            #[cfg(feature = "zstd")]
            CompressionChoice::Zstd,
        ];
        for choice in choices {
            let codec = choice.build_codec().expect("codec builds");
            assert_eq!(
                codec.is_some(),
                cfg!(any(feature = "lz4", feature = "zstd")),
                "{choice:?} left the client unable to decode any frame",
            );
        }
    }

    #[test]
    fn wire_flag_tracks_the_choice() {
        assert_eq!(CompressionChoice::None.to_wire(), Compression::None);
        assert_eq!(CompressionChoice::Lz4.to_wire(), Compression::Lz4);
        assert_eq!(CompressionChoice::Zstd.to_wire(), Compression::Zstd);
    }
}
