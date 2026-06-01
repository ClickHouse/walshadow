//! I/O glue. The C library reads/writes through `chc_io`, a small vtable.
//!
//! [`PosixIo`] wraps a raw fd via clickhouse-c's `chc_posix_io_init`. It
//! covers TCP sockets (the production path) and pipes (the
//! `clickhouse local` test path) without any further plumbing.
//!
//! Both the `chc_posix_io` state and the `chc_io` vtable live inside the
//! struct itself; the C code holds back-pointers into them, so the struct
//! is pinned (does not implement `Unpin`).

use core::marker::{PhantomData, PhantomPinned};
use core::pin::Pin;
use core::time::Duration;
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, OwnedFd, RawFd};

use crate::sys;

pub struct PosixIo<'fd> {
    state: sys::chc_posix_io,
    io: sys::chc_io,
    /// `Some` when [`PosixIo::new_owned`] handed us the fd; dropped
    /// after the `state` / `io` fields, closing the fd through
    /// [`OwnedFd`]. `None` for the borrowed path: caller keeps the fd
    /// open for the duration of the `'fd` lifetime.
    #[allow(dead_code)]
    owned: Option<OwnedFd>,
    _fd: PhantomData<BorrowedFd<'fd>>,
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
        let mut b = Box::pin(Self {
            state: sys::chc_posix_io {
                fd: 0,
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
        // SAFETY: pinned in place; the C call only wires raw pointers
        // between `state` and `io` inside our own struct.
        unsafe {
            let this = b.as_mut().get_unchecked_mut();
            sys::chc_posix_io_init(
                &mut this.state,
                &mut this.io,
                fd,
                None,
                core::ptr::null_mut(),
            );
        }
        b
    }

    #[inline]
    pub(crate) fn io_ptr(self: Pin<&mut Self>) -> *mut sys::chc_io {
        // SAFETY: structural pin; hand out a raw pointer to the
        // pinned-in-place `io` field.
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
        // SAFETY: structural pin; only mutates the deadline field of the
        // pinned-in-place `state`.
        unsafe {
            let this = self.get_unchecked_mut();
            sys::chc_posix_io_set_deadline(&mut this.state, deadline_us);
        }
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

// chc_posix_io stores a non-thread-local fd; the kernel guarantees the
// safety of cross-thread fd use itself.
unsafe impl<'fd> Send for PosixIo<'fd> {}
