//! ClickHouse connection and query lifecycle

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};

use backon::BackoffBuilder;
use clickhouse_c::tls::rustls::pki_types::ServerName;
use clickhouse_c::{
    AsyncClient, Block, BoxedAsyncClient, ClientOpts, Codec, Compression, ErrorKind, Event,
};
use thiserror::Error;
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;

use crate::config::DestEmitter;
use crate::emit::ch_emitter::EmitterConfig;

#[cfg(test)]
pub(crate) mod test_support;
pub mod types;

#[derive(Debug, Error)]
pub enum EmitterError {
    #[error(transparent)]
    Client(#[from] clickhouse_c::Error),
    #[error(
        "connection closed before ClickHouse hello reply, check service is running and its IP access list admits this host"
    )]
    HelloDropped(#[source] clickhouse_c::Error),
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

/// ClickHouse UNKNOWN_DATABASE
pub const UNKNOWN_DATABASE: i32 = 81;

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
    let sock = TcpStream::connect((config.host(), config.port())).await?;
    sock.set_nodelay(true)?;
    let client = if config.secure() {
        let tls = config
            .tls_config()
            .unwrap_or_else(clickhouse_c::tls::default_config);
        let server_name = ServerName::try_from(config.host().to_owned())
            .map_err(|e| EmitterError::Config(format!("TLS server name: {e}")))?;
        let stream = TlsConnector::from(tls)
            .connect(server_name, sock)
            .await
            .map_err(|e| std::io::Error::new(e.kind(), format!("TLS handshake: {e}")))?;
        AsyncClient::handshake_on(stream, opts, codec)
            .await
            .map(AsyncClient::boxed)
    } else {
        AsyncClient::handshake_on(sock, opts, codec)
            .await
            .map(AsyncClient::boxed)
    };
    client.map_err(|e| match e.kind {
        ErrorKind::Io | ErrorKind::Eof => EmitterError::HelloDropped(e),
        _ => e.into(),
    })
}

/// Server finished the query; an INSERT's rows are durable
pub struct EndOfStream(());

pub async fn drain_to_end_of_stream(
    client: &mut BoxedAsyncClient,
) -> Result<EndOfStream, EmitterError> {
    loop {
        match client.recv_event().await? {
            Event::EndOfStream => return Ok(EndOfStream(())),
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
        drain_to_end_of_stream(client).await?;
        Ok(())
    })
    .await
}

/// Single-column String SELECT, one attempt under `timeout`
pub async fn query_strings(
    client: &mut BoxedAsyncClient,
    sql: &str,
    timeout: Duration,
) -> Result<Vec<String>, EmitterError> {
    with_timeout(timeout, async {
        client.send_query(sql, None).await?;
        let mut out = Vec::new();
        loop {
            match client.recv_event().await? {
                Event::Data(block) => read_string_column(&block, &mut out)?,
                Event::EndOfStream => return Ok(out),
                Event::Exception(exc) => {
                    return Err(EmitterError::ServerException {
                        code: exc.code(),
                        message: String::from_utf8_lossy(exc.display_text()).into_owned(),
                    });
                }
                _ => {}
            }
        }
    })
    .await
}

/// Append one Data block's single String column into `out`; the 0-row
/// header block contributes nothing.
fn read_string_column(block: &Block, out: &mut Vec<String>) -> Result<(), EmitterError> {
    let n = block.n_rows();
    if n == 0 {
        return Ok(());
    }
    let col = block
        .column(0)
        .ok_or_else(|| EmitterError::Type("missing result column".into()))?;
    let (offsets, data) = col
        .string()
        .ok_or_else(|| EmitterError::Type("result column not String".into()))?;
    for i in 0..n {
        let start = if i == 0 { 0 } else { offsets[i - 1] as usize };
        let end = offsets[i] as usize;
        out.push(String::from_utf8_lossy(&data[start..end]).into_owned());
    }
    Ok(())
}

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
        cfg.insert_timeout,
    )
    .await?;
    tracing::info!(database = %cfg.conn.database, "created destination database");
    Ok((connect_client(cfg).await?, true))
}

struct Connection {
    client: Option<BoxedAsyncClient>,
    dialed: Option<Arc<EmitterConfig>>,
    last_used: Instant,
}

impl Default for Connection {
    fn default() -> Self {
        Self {
            client: None,
            dialed: None,
            last_used: Instant::now(),
        }
    }
}

/// Cache one connection per destination; retry and idle expiry apply independently.
pub struct ChConn {
    instance: Option<String>,
    selected: Option<Arc<EmitterConfig>>,
    active: Connection,
    parked: std::collections::HashMap<Option<String>, Connection>,
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
        let moved = self
            .active
            .dialed
            .as_ref()
            .is_none_or(|dialed| !Arc::ptr_eq(dialed, &config));
        if moved || self.active.last_used.elapsed() >= config.idle_reconnect() {
            self.active.client = None;
        }
        if let Some(client) = self.active.client.take() {
            return Ok(client);
        }
        let client = if self.instance.is_some() {
            connect_creating_database(&config).await?.0
        } else {
            connect_client(&*config).await?
        };
        self.active.dialed = Some(config);
        self.active.last_used = Instant::now();
        self.dials += 1;
        Ok(client)
    }
}

pub fn is_retryable(error: &EmitterError) -> bool {
    matches!(
        error,
        EmitterError::Io(_)
            | EmitterError::Client(_)
            | EmitterError::HelloDropped(_)
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
    use crate::emit::ch_emitter::InstanceConfig;

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

    /// ClickHouse Cloud's ingress drops a client its IP access list refuses
    /// after accepting the connection, with no server exception to report
    #[tokio::test]
    async fn hello_drop_names_likely_causes() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let config = EmitterConfig {
            conn: InstanceConfig {
                host: "127.0.0.1".into(),
                port: listener.local_addr().unwrap().port(),
                ..Default::default()
            },
            ..EmitterConfig::default()
        };
        let server = tokio::spawn(async move { drop(listener.accept().await.unwrap()) });
        let Err(e) = connect_client(&config).await else {
            panic!("handshake succeeded against a dropped connection");
        };
        server.await.unwrap();
        assert!(matches!(e, EmitterError::HelloDropped(_)), "{e:?}");
        assert!(is_retryable(&e));
        assert!(e.to_string().contains("IP access list"), "{e}");
    }

    #[test]
    fn client_error_prefix_appears_once() {
        let e = EmitterError::from(clickhouse_c::Error::from(std::io::Error::other("reset")));
        assert_eq!(e.to_string(), "clickhouse-c: Io: reset");
        assert!(std::error::Error::source(&e).is_none());
    }
}
