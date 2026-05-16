//! Allocator wrapper. Only the stdlib `malloc`/`realloc`/`free` allocator is
//! exposed; PG-extension consumers wire `palloc` themselves in C.
//!
//! Cheap to construct (a few function pointers); not Drop-managed.

use crate::sys;

#[derive(Clone, Copy)]
pub struct Allocator {
    pub(crate) raw: sys::chc_alloc,
}

impl Allocator {
    /// `malloc`/`realloc`/`free`-backed allocator from `clickhouse-c`.
    pub fn stdlib() -> Self {
        let raw = unsafe { sys::chc_alloc_stdlib() };
        Self { raw }
    }

    #[inline]
    pub(crate) fn as_ptr(&self) -> *const sys::chc_alloc {
        &self.raw
    }
}

impl Default for Allocator {
    fn default() -> Self {
        Self::stdlib()
    }
}

unsafe impl Send for Allocator {}
unsafe impl Sync for Allocator {}
