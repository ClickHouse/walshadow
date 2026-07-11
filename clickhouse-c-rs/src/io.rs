//! I/O glue. The C library reads & writes through `chc_io`, a small
//! read/write/cancel vtable it owns.
//!
//! [`PosixIo`] wraps a raw fd via clickhouse-c's posix-io backend. It holds
//! the `chc_posix_io` state and the `chc_io` vtable it feeds as inline
//! fields and lets upstream `chc_posix_io_init` populate both — pointing
//! the vtable's `ud` back at the state. That back-pointer makes the node
//! self-referential, so it must not move while the
//! [`Client`](crate::Client) holds the vtable pointer: hence
//! [`PhantomPinned`] and the `Pin<Box<Self>>` the constructors return,
//! mirroring [`TlsIo`](crate::tls::TlsIo). The rest of the crate
//! ([`Client`](crate::Client), [`BlockReader`](crate::BlockReader),
//! [`BlockBuilder::write`](crate::BlockBuilder::write)) expresses the
//! borrow as `Pin<&mut PosixIo>`.
//!
//! Covers TCP sockets (production) and pipes (the `clickhouse local` test
//! path) without further plumbing.

use core::marker::{PhantomData, PhantomPinned};
use core::pin::Pin;
use core::time::Duration;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd, RawFd};

use crate::error::{Error, ErrorKind, Result};
use crate::sys;

/// I/O backend a [`Client`](crate::Client) talks through. Hands the C
/// client the `chc_io` vtable pointer it retains for the connection's
/// lifetime; the implementor keeps that vtable at a fixed address (hence
/// the `Pin<&mut Self>` receiver) until the client drops.
///
/// Implemented by [`PosixIo`] (plaintext fd) and, under feature `tls`, by
/// `tls::TlsIo` (rustls over a `TcpStream`). A custom backend (eg OpenSSL
/// via `clickhouse-openssl.h`) may implement it too — hence `unsafe`:
/// [`Client::init`](crate::Client::init) passes `io_ptr`'s return straight
/// to C without validating it.
///
/// # Safety
///
/// `io_ptr` must return a non-null pointer to a fully initialized `chc_io`
/// whose `read` / `write` (and `check_cancel`, if set) callbacks honor the
/// clickhouse-c vtable contract. That pointer, and any state it
/// back-references, must stay valid and fixed in place for as long as the
/// pinned `self` lives — through the whole lifetime of the `Client` that
/// retains it. C dereferences it and calls through it on whatever thread
/// owns the `Client`.
pub unsafe trait ClientIo {
    /// Pointer to the `chc_io` vtable, valid while `self` is pinned alive.
    fn io_ptr(self: Pin<&mut Self>) -> *mut sys::chc_io;

    /// Set backend read timeout. Refresh before each operation when backend
    /// uses an absolute deadline.
    fn set_read_timeout(self: Pin<&mut Self>, _timeout: Option<Duration>) -> Result<()> {
        Err(Error::new(
            ErrorKind::Usage,
            "I/O backend does not support read timeouts",
        ))
    }
}

pub struct PosixIo<'fd> {
    state: sys::chc_posix_io,
    io: sys::chc_io,
    /// `Some` when [`PosixIo::new_owned`] handed us the fd; dropping it
    /// closes the fd, after the owning [`Client`](crate::Client) has closed
    /// the `chc_client` that reads through it. `None` for the borrowed
    /// path: caller keeps the fd open for the duration of the `'fd`
    /// lifetime.
    #[allow(dead_code)]
    owned: Option<OwnedFd>,
    _fd: PhantomData<BorrowedFd<'fd>>,
    // io.ud back-points at `state`; the node must not move once wired.
    _pin: PhantomPinned,
}

impl<'fd> PosixIo<'fd> {
    /// Wrap a borrowed file descriptor. The caller keeps ownership and
    /// must keep it open for the duration of `'fd`. Closing the fd while
    /// the [`PosixIo`] still references it is a use-after-free at the
    /// kernel level (subsequent reads land in whatever the fd table
    /// reassigned the number to).
    pub fn new(fd: BorrowedFd<'fd>) -> Pin<Box<Self>> {
        Self::build(fd.as_raw_fd(), None)
    }

    fn build(fd: RawFd, owned: Option<OwnedFd>) -> Pin<Box<Self>> {
        let mut boxed = Box::pin(Self {
            // Overwritten wholesale by chc_posix_io_init below; pre-filled
            // only to satisfy Rust's all-fields-init rule.
            state: sys::chc_posix_io {
                fd,
                check_cancel: None,
                cancel_ud: core::ptr::null_mut(),
                deadline_us: 0,
            },
            io: sys::chc_io {
                ud: core::ptr::null_mut(),
                read: None,
                write: None,
                check_cancel: None,
            },
            owned,
            _fd: PhantomData,
            _pin: PhantomPinned,
        });
        // Populates `state` + the `io` vtable with the posix read/write
        // callbacks and wires io.ud -> state at the pinned address.
        // SAFETY: only writes fields; never moves out of the pin.
        unsafe {
            let this = boxed.as_mut().get_unchecked_mut();
            sys::chc_posix_io_init(
                &mut this.state,
                &mut this.io,
                fd,
                None,
                core::ptr::null_mut(),
            );
        }
        boxed
    }

    #[inline]
    pub(crate) fn io_ptr(self: Pin<&mut Self>) -> *mut sys::chc_io {
        // Hand back the address of the inline vtable the client retains for
        // its lifetime. SAFETY: returns a field pointer; does not move self.
        unsafe { &mut self.get_unchecked_mut().io as *mut sys::chc_io }
    }

    /// Bound subsequent blocking reads by an absolute `now + timeout`
    /// deadline; `None` clears it so reads block indefinitely (default).
    ///
    /// The deadline is absolute and shared by every later read, not a
    /// rolling per-read budget: refresh it before each operation that
    /// needs a fresh window. Once elapsed, reads fail with
    /// [`ErrorKind::Io`](crate::ErrorKind::Io) ("read timeout"); a
    /// `Some(ZERO)` timeout makes the next read time out immediately.
    pub fn set_read_timeout(self: Pin<&mut Self>, timeout: Option<Duration>) {
        let deadline_us = match timeout {
            None => 0,
            Some(d) => {
                let now = unsafe { sys::chc_rs_monotonic_us() };
                let add = i64::try_from(d.as_micros()).unwrap_or(i64::MAX);
                // Keep nonzero so a near-zero deadline never reads as "disabled".
                now.saturating_add(add).max(1)
            }
        };
        // SAFETY: writes the deadline field; does not move self.
        unsafe { sys::chc_posix_io_set_deadline(&mut self.get_unchecked_mut().state, deadline_us) };
    }
}

impl PosixIo<'static> {
    /// Take ownership of the fd. The fd is closed when the [`PosixIo`]
    /// drops — typically through the owning [`Client`](crate::Client),
    /// which keeps the `PosixIo` alive for its own lifetime.
    pub fn new_owned<F: Into<OwnedFd>>(fd: F) -> Pin<Box<Self>> {
        let fd = fd.into();
        let raw = fd.as_fd().as_raw_fd();
        Self::build(raw, Some(fd))
    }
}

// SAFETY: `io` is a fully wired chc_io embedded in the pinned PosixIo, fed
// clickhouse-c's posix read/write callbacks by chc_posix_io_init; its `ud`
// back-points at the inline `state`, which stays at a fixed address behind
// the pinned Box for as long as the retaining Client lives.
unsafe impl<'fd> ClientIo for PosixIo<'fd> {
    fn io_ptr(self: Pin<&mut Self>) -> *mut sys::chc_io {
        // Inherent method wins path resolution over this trait method, so
        // no recursion; block.rs / builder.rs still call it directly.
        Self::io_ptr(self)
    }

    fn set_read_timeout(self: Pin<&mut Self>, timeout: Option<Duration>) -> Result<()> {
        Self::set_read_timeout(self, timeout);
        Ok(())
    }
}

// `state`/`io` are POD with no destructor; `owned` (if any) closes the fd
// when the node drops, after the owning Client has closed `chc_client`. No
// explicit Drop needed.

// chc_posix_io stores a non-thread-local fd; the kernel guarantees the
// safety of cross-thread fd use itself. The io.ud raw pointer (into this
// node) otherwise makes PosixIo !Send; it is dereferenced only from the C
// client's single-threaded read/write calls on whatever thread owns the
// Client, and stays valid behind the pinned Box.
unsafe impl<'fd> Send for PosixIo<'fd> {}
