//! I/O glue. The C library reads/writes through `chc_io`, a small vtable.
//!
//! [`PosixIo`] wraps a raw fd via clickhouse-c's `chc_posix_io_init`. It
//! covers TCP sockets (the production path) and pipes (the
//! `clickhouse local` test path) without any further plumbing.
//!
//! Both the `chc_posix_io` state and the `chc_io` vtable live inside the
//! struct itself; the C code holds back-pointers into them, so the struct
//! is pinned (does not implement `Unpin`).

use core::ffi::c_int;
use core::marker::PhantomPinned;
use core::pin::Pin;

use crate::sys;

pub struct PosixIo {
    state: sys::chc_posix_io,
    io: sys::chc_io,
    _pin: PhantomPinned,
}

impl PosixIo {
    /// Wrap a raw file descriptor. The fd is borrowed; the caller owns and
    /// closes it.
    ///
    /// Returns a `Pin<Box<Self>>` because `chc_io` stores a pointer back
    /// into `state` and any move would invalidate it.
    pub fn new(fd: c_int) -> Pin<Box<Self>> {
        let mut b = Box::pin(Self {
            state: sys::chc_posix_io {
                fd: 0,
                check_cancel: None,
                cancel_ud: core::ptr::null_mut(),
            },
            io: sys::chc_io {
                ud: core::ptr::null_mut(),
                read: None,
                write: None,
                check_cancel: None,
            },
            _pin: PhantomPinned,
        });
        // SAFETY: we never move out of the pinned box; the C call only
        // wires raw pointers between the now-pinned `state` and `io`.
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
        // SAFETY: structural pin; we hand out a raw pointer to the
        // pinned-in-place `io` field.
        unsafe { &mut self.get_unchecked_mut().io as *mut sys::chc_io }
    }
}

// chc_posix_io stores a non-thread-local fd; the kernel guarantees the
// safety of cross-thread fd use itself.
unsafe impl Send for PosixIo {}
