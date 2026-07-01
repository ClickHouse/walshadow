use core::alloc::GlobalAlloc;
use core::ffi::c_void;

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

    /// Bridge any [`GlobalAlloc`] into a `chc_alloc` vtable. The reference
    /// travels through the vtable's `ud` slot, so the allocator must outlive
    /// every object parsed through it, hence `'static`. Alignment is fixed at
    /// `align_of::<u128>()`, the max_align_t that `stdlib()`'s malloc gives.
    pub fn global<A: GlobalAlloc>(a: &'static A) -> Self {
        Self {
            raw: sys::chc_alloc {
                ud: a as *const A as *mut c_void,
                alloc: Some(vtable::alloc::<A>),
                realloc: Some(vtable::realloc::<A>),
                free: Some(vtable::free::<A>),
            },
        }
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

mod vtable {
    use core::alloc::{GlobalAlloc, Layout};
    use core::ffi::c_void;

    // clickhouse-c allocates up to 16-byte scalars (Int128/UUID/Decimal128);
    // matches the max_align_t that stdlib()'s malloc guarantees.
    const ALIGN: usize = core::mem::align_of::<u128>();

    // ALIGN is a fixed power of two; sizes here never approach isize::MAX.
    #[inline]
    fn layout(bytes: usize) -> Layout {
        unsafe { Layout::from_size_align_unchecked(bytes, ALIGN) }
    }

    pub extern "C" fn alloc<A: GlobalAlloc>(ud: *mut c_void, bytes: usize) -> *mut c_void {
        let a = unsafe { &*ud.cast::<A>() };
        unsafe { a.alloc(layout(bytes)).cast() }
    }

    // GlobalAlloc::realloc reads only the alignment from the old layout; the
    // allocator tracks the block size internally.
    pub extern "C" fn realloc<A: GlobalAlloc>(
        ud: *mut c_void,
        p: *mut c_void,
        _old_bytes: usize,
        new_bytes: usize,
    ) -> *mut c_void {
        let a = unsafe { &*ud.cast::<A>() };
        unsafe { a.realloc(p.cast(), layout(0), new_bytes).cast() }
    }

    pub extern "C" fn free<A: GlobalAlloc>(ud: *mut c_void, p: *mut c_void, _bytes: usize) {
        let a = unsafe { &*ud.cast::<A>() };
        unsafe { a.dealloc(p.cast(), layout(0)) }
    }
}
