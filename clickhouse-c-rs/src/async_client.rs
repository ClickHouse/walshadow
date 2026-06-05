//! Tokio TCP client over `chc_async_*`, plaintext or rustls TLS.

use core::ffi::c_char;
use core::pin::Pin;
use core::ptr::NonNull;
use core::slice;
use core::task::{Context, Poll};
use std::io;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpStream, ToSocketAddrs};

use crate::alloc::Allocator;
use crate::builder::BlockBuilder;
use crate::client::{ClientOpts, Event, ServerInfo};
use crate::codec::Codec;
use crate::error::{Error, ErrorKind, Result, check};
use crate::sys;

const DEFAULT_READ_BUF_BYTES: usize = 8 * 1024;

/// Underlying byte transport: plaintext TCP, or (feature `tls`) rustls
/// over TCP. The async client only ever reads/writes opaque byte buffers
/// to it, so a hand-rolled `AsyncRead`/`AsyncWrite` delegate keeps
/// [`AsyncClient`] a single concrete type rather than a generic.
enum Transport {
    Plain(TcpStream),
    #[cfg(feature = "tls")]
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
}

impl AsyncRead for Transport {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Transport::Plain(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(feature = "tls")]
            Transport::Tls(s) => Pin::new(s.as_mut()).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Transport {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Transport::Plain(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(feature = "tls")]
            Transport::Tls(s) => Pin::new(s.as_mut()).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Transport::Plain(s) => Pin::new(s).poll_flush(cx),
            #[cfg(feature = "tls")]
            Transport::Tls(s) => Pin::new(s.as_mut()).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Transport::Plain(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(feature = "tls")]
            Transport::Tls(s) => Pin::new(s.as_mut()).poll_shutdown(cx),
        }
    }
}

/// Worker-free async ClickHouse client.
pub struct AsyncClient {
    raw: NonNull<sys::chc_async_client>,
    stream: Transport,
    alloc: Box<Allocator>,
    _codec: Option<Pin<Box<Codec>>>,
    read_buf: Vec<u8>,
}

impl AsyncClient {
    pub async fn connect<A>(
        addr: A,
        opts: ClientOpts,
        codec: Option<Pin<Box<Codec>>>,
    ) -> Result<Self>
    where
        A: ToSocketAddrs,
    {
        let sock = TcpStream::connect(addr).await?;
        // ClickHouse Native is request/response; Nagle just adds latency
        // to the small terminator/query writes between blocks.
        sock.set_nodelay(true).ok();
        Self::handshake_on(Transport::Plain(sock), opts, codec).await
    }

    /// Connect over TLS: TCP-connect to `addr`, then rustls-handshake
    /// verifying the peer against `config` for `domain` (sent as SNI).
    /// `config` typically comes from [`tls::default_config`](crate::tls::default_config).
    #[cfg(feature = "tls")]
    pub async fn connect_tls<A>(
        addr: A,
        domain: &str,
        opts: ClientOpts,
        codec: Option<Pin<Box<Codec>>>,
        config: std::sync::Arc<rustls::ClientConfig>,
    ) -> Result<Self>
    where
        A: ToSocketAddrs,
    {
        let sock = TcpStream::connect(addr).await?;
        sock.set_nodelay(true).ok();
        let server_name =
            rustls::pki_types::ServerName::try_from(domain.to_owned()).map_err(|_| {
                Error::new(
                    ErrorKind::Usage,
                    format!("invalid TLS server name: {domain}"),
                )
            })?;
        let tls = tokio_rustls::TlsConnector::from(config)
            .connect(server_name, sock)
            .await
            .map_err(|e| Error::new(ErrorKind::Io, format!("TLS handshake: {e}")))?;
        Self::handshake_on(Transport::Tls(Box::new(tls)), opts, codec).await
    }

    async fn handshake_on(
        stream: Transport,
        opts: ClientOpts,
        codec: Option<Pin<Box<Codec>>>,
    ) -> Result<Self> {
        let alloc = Box::new(Allocator::stdlib());
        let read_buf_bytes = if opts.read_buffer_bytes == 0 {
            DEFAULT_READ_BUF_BYTES
        } else {
            opts.read_buffer_bytes
        };
        // Scope the raw FFI init locals so no raw pointer is held across
        // the handshake await below; that keeps the returned future
        // `Send`, which `tokio::spawn` / a multi-thread runtime require.
        let raw = {
            let codec_ptr = codec.as_ref().map(|c| c.as_ref().as_ptr());
            let raw_opts = opts.to_raw(codec_ptr);
            let mut out: *mut sys::chc_async_client = core::ptr::null_mut();
            let mut err = sys::chc_err::zeroed();
            let rc = unsafe {
                sys::chc_async_client_init(&mut out, &raw_opts, alloc.as_ptr(), &mut err)
            };
            check(rc, &err)?;
            NonNull::new(out).expect("chc_async_client_init returned OK with NULL")
        };
        let mut client = Self {
            raw,
            stream,
            alloc,
            _codec: codec,
            read_buf: vec![0; read_buf_bytes],
        };
        client.handshake().await?;
        Ok(client)
    }

    pub async fn send_query(&mut self, sql: &str, query_id: Option<&str>) -> Result<()> {
        self.drain_out().await?;
        // Scope raw FFI args so no raw pointer is held across the drain
        // await below (keeps the future `Send`).
        {
            let (qid, qid_len) = query_id
                .map(|q| (q.as_ptr().cast::<c_char>(), q.len()))
                .unwrap_or((core::ptr::null(), 0));
            let mut err = sys::chc_err::zeroed();
            let rc = unsafe {
                sys::chc_async_send_query(
                    self.raw.as_ptr(),
                    sql.as_ptr().cast::<c_char>(),
                    sql.len(),
                    qid,
                    qid_len,
                    &mut err,
                )
            };
            check(rc, &err)?;
        }
        self.drain_out().await
    }

    pub async fn send_data(&mut self, builder: Option<&BlockBuilder<'_>>) -> Result<()> {
        self.drain_out().await?;
        {
            let bb_ptr = builder.map(|b| b.as_ptr()).unwrap_or(core::ptr::null());
            let mut err = sys::chc_err::zeroed();
            let rc = unsafe { sys::chc_async_send_data(self.raw.as_ptr(), bb_ptr, &mut err) };
            check(rc, &err)?;
        }
        self.drain_out().await
    }

    pub async fn send_data_end(&mut self) -> Result<()> {
        self.drain_out().await?;
        {
            let mut err = sys::chc_err::zeroed();
            let rc = unsafe { sys::chc_async_send_data_end(self.raw.as_ptr(), &mut err) };
            check(rc, &err)?;
        }
        self.drain_out().await
    }

    pub async fn recv_event(&mut self) -> Result<Event> {
        self.pump_until_ok(|this| this.recv_event_step()).await
    }

    pub fn server_info(&self) -> Option<ServerInfo> {
        let p = unsafe { sys::chc_async_server_info(self.raw.as_ptr().cast_const()) };
        if p.is_null() {
            None
        } else {
            Some(ServerInfo::from_raw(unsafe { &*p }))
        }
    }

    async fn handshake(&mut self) -> Result<()> {
        self.pump_until_ok(|this| {
            let mut err = sys::chc_err::zeroed();
            let rc = unsafe { sys::chc_async_handshake(this.raw.as_ptr(), &mut err) };
            step_from_rc(rc, &err)
        })
        .await
    }

    async fn drain_out(&mut self) -> Result<()> {
        let mut wrote = false;
        loop {
            // Resolve the pending slice in a tight scope so the raw
            // pointer never lives across the write await; only the
            // `&[u8]` (Send) crosses, keeping the future `Send`.
            let buf: &[u8] = {
                let mut ptr: *const u8 = core::ptr::null();
                let mut len = 0usize;
                unsafe { sys::chc_async_pending_out(self.raw.as_ptr(), &mut ptr, &mut len) };
                if len == 0 {
                    break;
                }
                if ptr.is_null() {
                    return Err(Error::new(
                        ErrorKind::Protocol,
                        "async client reported NULL output buffer",
                    ));
                }
                unsafe { slice::from_raw_parts(ptr, len) }
            };
            let n = self.stream.write(buf).await?;
            if n == 0 {
                return Err(Error::new(ErrorKind::Io, "socket write returned zero"));
            }
            unsafe { sys::chc_async_consume_out(self.raw.as_ptr(), n) };
            wrote = true;
        }
        // A TLS stream's poll_write may leave the tail of a record buffered
        // in rustls when the socket briefly back-pressures; flush forces it
        // out so the server isn't left waiting on a half-sent Hello/query.
        // No-op for the plaintext `TcpStream`, and skipped when nothing was
        // written so the recv path never flushes an idle stream.
        if wrote {
            self.stream.flush().await?;
        }
        Ok(())
    }

    async fn pump_until_ok<T>(
        &mut self,
        mut step: impl FnMut(&mut Self) -> Result<Step<T>>,
    ) -> Result<T> {
        loop {
            self.drain_out().await?;
            match step(self)? {
                Step::Done(v) => {
                    self.drain_out().await?;
                    return Ok(v);
                }
                Step::WouldBlock => {
                    self.drain_out().await?;
                    self.read_more().await?;
                }
            }
        }
    }

    async fn read_more(&mut self) -> Result<()> {
        let n = self.stream.read(&mut self.read_buf).await?;
        if n == 0 {
            return Err(Error::new(ErrorKind::Eof, "socket closed"));
        }
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe {
            sys::chc_async_submit(
                self.raw.as_ptr(),
                self.read_buf[..n].as_ptr().cast(),
                n,
                &mut err,
            )
        };
        check(rc, &err)
    }

    fn recv_event_step(&mut self) -> Result<Step<Event>> {
        let mut raw = sys::chc_packet::zeroed();
        let mut err = sys::chc_err::zeroed();
        let rc = unsafe { sys::chc_async_recv_packet(self.raw.as_ptr(), &mut raw, &mut err) };
        if rc == sys::CHC_WOULD_BLOCK {
            return Ok(Step::WouldBlock);
        }
        if let Err(e) = check(rc, &err) {
            unsafe { sys::chc_async_packet_clear(self.raw.as_ptr(), &mut raw) };
            return Err(e);
        }

        let event = Event::from_raw(&mut raw, *self.alloc);
        unsafe { sys::chc_async_packet_clear(self.raw.as_ptr(), &mut raw) };
        event.map(Step::Done)
    }
}

impl Drop for AsyncClient {
    fn drop(&mut self) {
        unsafe { sys::chc_async_client_free(self.raw.as_ptr()) };
    }
}

unsafe impl Send for AsyncClient {}

enum Step<T> {
    Done(T),
    WouldBlock,
}

fn step_from_rc(rc: i32, err: &sys::chc_err) -> Result<Step<()>> {
    if rc == sys::CHC_WOULD_BLOCK {
        Ok(Step::WouldBlock)
    } else {
        check(rc, err).map(Step::Done)
    }
}

#[cfg(test)]
mod tests {
    use super::{AsyncClient, Event};
    use crate::builder::BlockBuilder;
    use crate::client::ClientOpts;

    #[test]
    fn async_client_is_send() {
        fn assert_send<T: Send>() {}
        assert_send::<AsyncClient>();
        assert_send::<Event>();
    }

    // Compile-time guard: the method futures must be `Send`, not just
    // `AsyncClient` itself, or `tokio::spawn` on a multi-thread runtime
    // rejects them. A raw FFI pointer held across an await silently
    // makes a future `!Send` — invisible to the live `current_thread`
    // tests, so assert it here where it costs nothing.
    #[allow(dead_code)]
    fn method_futures_are_send(mut c: AsyncClient, bb: BlockBuilder<'static>) {
        fn require_send<T: Send>(_: T) {}
        require_send(AsyncClient::connect(("h", 1u16), ClientOpts::new(), None));
        #[cfg(feature = "tls")]
        require_send(AsyncClient::connect_tls(
            ("h", 1u16),
            "h",
            ClientOpts::new(),
            None,
            crate::tls::default_config(),
        ));
        require_send(c.send_query("", None));
        require_send(c.send_data(Some(&bb)));
        require_send(c.send_data_end());
        require_send(c.recv_event());
    }
}
