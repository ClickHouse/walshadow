//! TLS over rustls (feature `tls`).
//!
//! Two surfaces:
//!
//! * [`TlsIo`] — a [`ClientIo`](crate::ClientIo) backend for the blocking
//!   [`Client`](crate::Client). It owns a rustls [`StreamOwned`] over a
//!   `std::net::TcpStream` and exposes a `chc_io` vtable whose read/write
//!   callbacks drive `SSL`-equivalent rustls I/O. The C client never sees
//!   the socket, mirroring the plaintext [`PosixIo`](crate::PosixIo) path.
//! * [`default_config`] / [`config_with_roots`] — build an
//!   `Arc<rustls::ClientConfig>` (Mozilla webpki roots, no client auth)
//!   for both [`TlsIo`] and the async `AsyncClient::connect_tls`.
//!
//! `rustls` is re-exported so callers pin one version and can hand a
//! bespoke `ClientConfig` (custom roots, client certs) to either path.

use core::ffi::{c_int, c_void};
use core::marker::PhantomPinned;
use core::pin::Pin;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::sync::Arc;

pub use rustls;

use crate::error::{Error, ErrorKind, Result};
use crate::io::ClientIo;
use crate::sys;

/// `ClientConfig` trusting the Mozilla webpki root set, no client auth.
/// Suitable for public CAs (ClickHouse Cloud). For private CAs or mTLS,
/// build a config via [`config_with_roots`] or rustls directly.
pub fn default_config() -> Arc<rustls::ClientConfig> {
    let mut roots = rustls::RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    config_with_roots(roots)
}

/// `ClientConfig` over a caller-supplied root store, no client auth.
pub fn config_with_roots(roots: rustls::RootCertStore) -> Arc<rustls::ClientConfig> {
    // Pin the provider explicitly so config building never depends on a
    // process-default provider being installed elsewhere.
    let provider = Arc::new(rustls::crypto::aws_lc_rs::default_provider());
    let config = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .expect("aws-lc-rs supports the safe default protocol versions")
        .with_root_certificates(roots)
        .with_no_client_auth();
    Arc::new(config)
}

type RustlsStream = rustls::StreamOwned<rustls::ClientConnection, TcpStream>;

/// Blocking TLS [`ClientIo`] backend: rustls over an owned `TcpStream`.
///
/// Self-referential — the `chc_io` vtable's `ud` points back at the
/// `TlsIo` so the C client's read/write calls land on `stream`. Hence the
/// `Pin<Box<Self>>` the constructor returns: the node must not move while
/// the [`Client`](crate::Client) holds the vtable pointer.
pub struct TlsIo {
    io: sys::chc_io,
    stream: RustlsStream,
    _pin: PhantomPinned,
}

impl TlsIo {
    /// TLS-connect over an already TCP-connected `tcp`, verifying the peer
    /// against `config` for `server_name` (also sent as SNI). Drives the
    /// handshake to completion so certificate / SNI / verification errors
    /// surface here rather than on the first query.
    pub fn connect(
        tcp: TcpStream,
        server_name: &str,
        config: Arc<rustls::ClientConfig>,
    ) -> Result<Pin<Box<Self>>> {
        let name =
            rustls::pki_types::ServerName::try_from(server_name.to_owned()).map_err(|_| {
                Error::new(
                    ErrorKind::Usage,
                    format!("invalid TLS server name: {server_name}"),
                )
            })?;
        let conn = rustls::ClientConnection::new(config, name)
            .map_err(|e| Error::new(ErrorKind::Io, format!("rustls client: {e}")))?;
        let mut stream = rustls::StreamOwned::new(conn, tcp);
        stream
            .conn
            .complete_io(&mut stream.sock)
            .map_err(|e| Error::new(ErrorKind::Io, format!("TLS handshake: {e}")))?;

        let mut boxed = Box::pin(Self {
            io: sys::chc_io {
                ud: core::ptr::null_mut(),
                read: Some(tls_read),
                write: Some(tls_write),
                check_cancel: None,
            },
            stream,
            _pin: PhantomPinned,
        });
        // Wire ud at the pinned node so the callbacks recover `stream`.
        // SAFETY: only sets a field; never moves out of the pin.
        unsafe {
            let this = boxed.as_mut().get_unchecked_mut();
            this.io.ud = (this as *mut Self).cast();
        }
        Ok(boxed)
    }
}

// SAFETY: `io` is a fully wired chc_io embedded in the pinned TlsIo; its
// `ud` back-points at the same node (fixed address under Pin) and
// tls_read/tls_write honor the vtable contract. Valid until the retaining
// Client drops, which then drops this backend.
unsafe impl ClientIo for TlsIo {
    fn io_ptr(self: Pin<&mut Self>) -> *mut sys::chc_io {
        // SAFETY: hands back the address of a field; does not move `self`.
        unsafe { &mut self.get_unchecked_mut().io as *mut sys::chc_io }
    }

    fn set_read_timeout(self: Pin<&mut Self>, timeout: Option<core::time::Duration>) -> Result<()> {
        unsafe { self.get_unchecked_mut() }
            .stream
            .sock
            .set_read_timeout(timeout)?;
        Ok(())
    }
}

// The chc_io.ud raw pointer makes TlsIo !Send automatically. It points at
// the boxed TlsIo's own heap node, whose address is stable across moves of
// the owning Box, and is dereferenced only from the C client's read/write
// calls, which run single-threaded on whatever thread owns the Client.
unsafe impl Send for TlsIo {}

unsafe extern "C" fn tls_read(
    ud: *mut c_void,
    buf: *mut c_void,
    len: usize,
    out_n: *mut usize,
    err: *mut sys::chc_err,
) -> c_int {
    if len == 0 {
        unsafe { *out_n = 0 };
        return sys::CHC_OK;
    }
    let io = unsafe { &mut *(ud as *mut TlsIo) };
    let dst = unsafe { core::slice::from_raw_parts_mut(buf as *mut u8, len) };
    match io.stream.read(dst) {
        // n == 0 is clean EOF, mirroring the posix backend; the C reader
        // turns it into CHC_ERR_EOF if it still wanted bytes.
        Ok(n) => {
            unsafe { *out_n = n };
            sys::CHC_OK
        }
        Err(e) => unsafe { set_err(err, sys::CHC_ERR_IO, &format!("tls read: {e}")) },
    }
}

unsafe extern "C" fn tls_write(
    ud: *mut c_void,
    buf: *const c_void,
    len: usize,
    err: *mut sys::chc_err,
) -> c_int {
    let io = unsafe { &mut *(ud as *mut TlsIo) };
    let src = unsafe { core::slice::from_raw_parts(buf as *const u8, len) };
    // Contract matches the posix backend: write all `len` bytes or fail.
    // flush pushes the encrypted records out to the socket.
    match io.stream.write_all(src).and_then(|()| io.stream.flush()) {
        Ok(()) => sys::CHC_OK,
        Err(e) => unsafe { set_err(err, sys::CHC_ERR_IO, &format!("tls write: {e}")) },
    }
}

/// Copy a NUL-terminated diagnostic into `err.msg` and return `code`.
unsafe fn set_err(err: *mut sys::chc_err, code: c_int, msg: &str) -> c_int {
    if !err.is_null() {
        let e = unsafe { &mut *err };
        e.server_code = 0;
        let cap = e.msg.len();
        if cap > 0 {
            let n = msg.len().min(cap - 1);
            for (slot, b) in e.msg.iter_mut().zip(msg.as_bytes()[..n].iter()) {
                *slot = *b as core::ffi::c_char;
            }
            e.msg[n] = 0;
        }
    }
    code
}
