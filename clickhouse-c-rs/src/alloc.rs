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
    pub fn global<A: GlobalAlloc + Sync>(a: &'static A) -> Self {
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

    #[inline]
    fn layout(bytes: usize) -> Option<Layout> {
        Layout::from_size_align(bytes.max(1), ALIGN).ok()
    }

    pub extern "C" fn alloc<A: GlobalAlloc + Sync>(ud: *mut c_void, bytes: usize) -> *mut c_void {
        let a = unsafe { &*ud.cast::<A>() };
        let Some(layout) = layout(bytes) else {
            return core::ptr::null_mut();
        };
        unsafe { a.alloc(layout).cast() }
    }

    pub extern "C" fn realloc<A: GlobalAlloc + Sync>(
        ud: *mut c_void,
        p: *mut c_void,
        old_bytes: usize,
        new_bytes: usize,
    ) -> *mut c_void {
        if p.is_null() {
            return alloc::<A>(ud, new_bytes);
        }
        let a = unsafe { &*ud.cast::<A>() };
        let Some(old_layout) = layout(old_bytes) else {
            return core::ptr::null_mut();
        };
        if new_bytes == 0 {
            unsafe { a.dealloc(p.cast(), old_layout) };
            return core::ptr::null_mut();
        }
        unsafe { a.realloc(p.cast(), old_layout, new_bytes).cast() }
    }

    pub extern "C" fn free<A: GlobalAlloc + Sync>(ud: *mut c_void, p: *mut c_void, bytes: usize) {
        if p.is_null() {
            return;
        }
        let a = unsafe { &*ud.cast::<A>() };
        let Some(layout) = layout(bytes) else {
            return;
        };
        unsafe { a.dealloc(p.cast(), layout) }
    }
}

#[cfg(test)]
mod tests {
    use core::alloc::{GlobalAlloc, Layout};
    use core::sync::atomic::{AtomicBool, Ordering};
    use std::alloc::System;
    use std::collections::HashMap;
    use std::sync::{LazyLock, Mutex};

    use super::Allocator;
    use crate::{BlockBuilder, TypeAst};

    static CHECKED: LazyLock<CheckedAlloc> = LazyLock::new(|| CheckedAlloc {
        live: Mutex::new(HashMap::new()),
        invalid_layout: AtomicBool::new(false),
    });

    struct CheckedAlloc {
        live: Mutex<HashMap<usize, Layout>>,
        invalid_layout: AtomicBool,
    }

    unsafe impl GlobalAlloc for CheckedAlloc {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let ptr = unsafe { System.alloc(layout) };
            if !ptr.is_null() {
                self.live
                    .lock()
                    .expect("checked allocator lock")
                    .insert(ptr as usize, layout);
            }
            ptr
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            let Some(actual) = self
                .live
                .lock()
                .expect("checked allocator lock")
                .remove(&(ptr as usize))
            else {
                self.invalid_layout.store(true, Ordering::Relaxed);
                return;
            };
            if actual != layout {
                self.invalid_layout.store(true, Ordering::Relaxed);
            }
            unsafe { System.dealloc(ptr, actual) };
        }

        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            let Some(actual) = self
                .live
                .lock()
                .expect("checked allocator lock")
                .remove(&(ptr as usize))
            else {
                self.invalid_layout.store(true, Ordering::Relaxed);
                return core::ptr::null_mut();
            };
            if actual != layout {
                self.invalid_layout.store(true, Ordering::Relaxed);
            }
            let new_ptr = unsafe { System.realloc(ptr, actual, new_size) };
            let mut live = self.live.lock().expect("checked allocator lock");
            if new_ptr.is_null() {
                live.insert(ptr as usize, actual);
            } else {
                let new_layout = Layout::from_size_align(new_size, actual.align()).expect("layout");
                live.insert(new_ptr as usize, new_layout);
            }
            new_ptr
        }
    }

    #[test]
    fn global_allocator_preserves_layouts() {
        CHECKED.invalid_layout.store(false, Ordering::Relaxed);
        assert!(
            CHECKED
                .live
                .lock()
                .expect("checked allocator lock")
                .is_empty()
        );

        let alloc = Allocator::global(&*CHECKED);
        drop(BlockBuilder::new(alloc).expect("empty builder"));
        let ty = TypeAst::parse("UInt32", alloc).expect("UInt32");
        let data = 7u32.to_le_bytes();
        let mut builder = BlockBuilder::new(alloc).expect("builder");
        builder
            .append_fixed("x", ty.view(), &data, 1)
            .expect("append");
        drop(builder);
        drop(ty);

        assert!(!CHECKED.invalid_layout.load(Ordering::Relaxed));
        assert!(
            CHECKED
                .live
                .lock()
                .expect("checked allocator lock")
                .is_empty()
        );
    }
}
