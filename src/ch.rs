//! ClickHouse connection and query lifecycle

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use clickhouse_c::{AsyncClient, ClientOpts, Codec, Compression, Event};
use thiserror::Error;

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

impl CompressionChoice {
    pub fn parse(value: &str) -> Result<Self, EmitterError> {
        match value.to_ascii_lowercase().as_str() {
            "none" | "off" | "" => Ok(Self::None),
            "lz4" => Ok(Self::Lz4),
            "zstd" => Ok(Self::Zstd),
            other => Err(EmitterError::Config(format!(
                "unknown compression `{other}` (expected none / lz4 / zstd)"
            ))),
        }
    }

    fn to_wire(self) -> Compression {
        match self {
            Self::None => Compression::None,
            Self::Lz4 => Compression::Lz4,
            Self::Zstd => Compression::Zstd,
        }
    }

    pub fn build_codec(self) -> Result<Option<Pin<Box<Codec>>>, EmitterError> {
        match self {
            Self::None => Ok(None),
            Self::Lz4 => {
                #[cfg(feature = "lz4")]
                {
                    Ok(Some(Codec::lz4()))
                }
                #[cfg(not(feature = "lz4"))]
                {
                    Err(EmitterError::CompressionUnsupported("lz4"))
                }
            }
            Self::Zstd => {
                #[cfg(feature = "zstd")]
                {
                    Ok(Some(Codec::zstd()))
                }
                #[cfg(not(feature = "zstd"))]
                {
                    Err(EmitterError::CompressionUnsupported("zstd"))
                }
            }
        }
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

pub async fn connect_client(config: &impl ConnectionConfig) -> Result<AsyncClient, EmitterError> {
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
        Ok(AsyncClient::connect_tls(addr, config.host(), opts, codec, tls).await?)
    } else {
        Ok(AsyncClient::connect(addr, opts, codec).await?)
    }
}

pub async fn reconnect_if_idle(
    client: &mut AsyncClient,
    config: &impl ConnectionConfig,
    last_used: Instant,
) -> Result<bool, EmitterError> {
    if last_used.elapsed() < config.idle_reconnect() {
        return Ok(false);
    }
    *client = connect_client(config).await?;
    Ok(true)
}

pub async fn drain_to_end_of_stream(client: &mut AsyncClient) -> Result<(), EmitterError> {
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
    client: &mut AsyncClient,
    sql: &str,
    timeout: Duration,
) -> Result<(), EmitterError> {
    with_timeout(timeout, async {
        client.send_query(sql, None).await?;
        drain_to_end_of_stream(client).await
    })
    .await
}

pub async fn backoff_step(backoff: &mut Duration, max: Duration) {
    tokio::time::sleep(*backoff).await;
    *backoff = backoff.saturating_mul(2).min(max);
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
