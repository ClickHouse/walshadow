/* ----------------------------------------------------------------------------
Copyright (c) 2018-2026, Microsoft Research, Daan Leijen
This is free software; you can redistribute it and/or modify it under the
terms of the MIT license. See vendor/mimalloc/LICENSE.
-----------------------------------------------------------------------------*/

//! Global allocator from upstream contrib/rust. Keep GlobalAlloc and FFI
//! declarations in sync with vendor/mimalloc/contrib/rust on submodule bumps.

use std::alloc::{GlobalAlloc, Layout};
use std::ffi::c_void;

const MAX_ALIGN_SIZE: usize = 16;
const SMALL_SIZE_MAX: usize = 128 * size_of::<usize>();

unsafe extern "C" {
    fn mi_malloc(size: usize) -> *mut c_void;
    fn mi_malloc_aligned(size: usize, alignment: usize) -> *mut c_void;
    fn mi_wmalloc_small(words: usize) -> *mut c_void;
    fn mi_zalloc(size: usize) -> *mut c_void;
    fn mi_zalloc_aligned(size: usize, alignment: usize) -> *mut c_void;
    fn mi_wzalloc_small(words: usize) -> *mut c_void;
    fn mi_free(ptr: *mut c_void);
    fn mi_free_small_nonnull(ptr: *mut c_void);
    fn mi_realloc(ptr: *mut c_void, size: usize) -> *mut c_void;
    fn mi_realloc_aligned(ptr: *mut c_void, size: usize, alignment: usize) -> *mut c_void;
}

#[inline]
fn is_malloc_aligned(size: usize, align: usize) -> bool {
    align <= MAX_ALIGN_SIZE && align <= size
}

/// Vendored mimalloc global allocator.
#[derive(Clone, Copy, Debug, Default)]
pub struct MiMalloc;

unsafe impl GlobalAlloc for MiMalloc {
    #[inline]
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let size = layout.size();
        unsafe {
            if !is_malloc_aligned(size, layout.align()) {
                mi_malloc_aligned(size, layout.align())
            } else if size <= SMALL_SIZE_MAX {
                mi_wmalloc_small(size.div_ceil(size_of::<usize>()))
            } else {
                mi_malloc(size)
            }
        }
        .cast()
    }

    #[inline]
    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let size = layout.size();
        unsafe {
            if !is_malloc_aligned(size, layout.align()) {
                mi_zalloc_aligned(size, layout.align())
            } else if size <= SMALL_SIZE_MAX {
                mi_wzalloc_small(size.div_ceil(size_of::<usize>()))
            } else {
                mi_zalloc(size)
            }
        }
        .cast()
    }

    #[inline]
    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe {
            if layout.align() <= layout.size() && layout.size() <= SMALL_SIZE_MAX {
                mi_free_small_nonnull(ptr.cast())
            } else {
                mi_free(ptr.cast())
            }
        }
    }

    #[inline]
    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let align = layout.align();
        unsafe {
            if is_malloc_aligned(new_size, align) {
                mi_realloc(ptr.cast(), new_size)
            } else {
                mi_realloc_aligned(ptr.cast(), new_size, align)
            }
        }
        .cast()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocate_zero_resize_and_free() {
        for (size, align) in [(3, 16), (32, 8), (1024, 256), (4097, 8)] {
            let layout = Layout::from_size_align(size, align).unwrap();
            let larger = Layout::from_size_align(size * 2, align).unwrap();
            unsafe {
                let ptr = MiMalloc.alloc(layout);
                assert!(!ptr.is_null());
                assert!(ptr.addr().is_multiple_of(align));
                ptr.write_bytes(0xab, size);
                let ptr = MiMalloc.realloc(ptr, layout, larger.size());
                assert!(!ptr.is_null());
                assert!(ptr.addr().is_multiple_of(align));
                assert!(
                    std::slice::from_raw_parts(ptr, size)
                        .iter()
                        .all(|&b| b == 0xab)
                );
                MiMalloc.dealloc(ptr, larger);

                let ptr = MiMalloc.alloc_zeroed(layout);
                assert!(!ptr.is_null());
                assert!(ptr.addr().is_multiple_of(align));
                assert!(
                    std::slice::from_raw_parts(ptr, size)
                        .iter()
                        .all(|&b| b == 0)
                );
                MiMalloc.dealloc(ptr, layout);
            }
        }
    }
}
