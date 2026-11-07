//! Global allocator wrapper counting allocations and live bytes, so benches
//! report retained memory by stage instead of payload estimates

use std::alloc::{GlobalAlloc, Layout};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

pub struct Metered<A> {
    inner: A,
    allocs: AtomicU64,
    live: AtomicUsize,
    peak: AtomicUsize,
}

impl<A> Metered<A> {
    pub const fn new(inner: A) -> Self {
        Self {
            inner,
            allocs: AtomicU64::new(0),
            live: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
        }
    }

    fn grow(&self, n: usize) {
        let live = self.live.fetch_add(n, Ordering::Relaxed) + n;
        self.peak.fetch_max(live, Ordering::Relaxed);
    }

    fn shrink(&self, n: usize) {
        self.live.fetch_sub(n, Ordering::Relaxed);
    }

    pub fn snapshot(&self) -> Meter {
        Meter {
            allocs: self.allocs.load(Ordering::Relaxed),
            live: self.live.load(Ordering::Relaxed),
            peak: self.peak.load(Ordering::Relaxed),
        }
    }

    /// Restart peak tracking from current live bytes
    pub fn reset_peak(&self) -> Meter {
        self.peak
            .store(self.live.load(Ordering::Relaxed), Ordering::Relaxed);
        self.snapshot()
    }
}

/// Requested sizes, not allocator size classes or RSS
#[derive(Clone, Copy, Debug)]
pub struct Meter {
    pub allocs: u64,
    pub live: usize,
    pub peak: usize,
}

impl Meter {
    /// Allocations, bytes still live, and peak growth since `base`
    pub fn since(self, base: Meter) -> Delta {
        Delta {
            allocs: self.allocs - base.allocs,
            retained: self.live as i64 - base.live as i64,
            peak: self.peak.saturating_sub(base.live),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct Delta {
    pub allocs: u64,
    pub retained: i64,
    pub peak: usize,
}

impl std::fmt::Display for Delta {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "allocs={} retained_bytes={} peak_bytes={}",
            self.allocs, self.retained, self.peak
        )
    }
}

// SAFETY: forwards to `inner`; counters never touch the returned memory
unsafe impl<A: GlobalAlloc> GlobalAlloc for Metered<A> {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { self.inner.alloc(layout) };
        if !p.is_null() {
            self.allocs.fetch_add(1, Ordering::Relaxed);
            self.grow(layout.size());
        }
        p
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        let p = unsafe { self.inner.alloc_zeroed(layout) };
        if !p.is_null() {
            self.allocs.fetch_add(1, Ordering::Relaxed);
            self.grow(layout.size());
        }
        p
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { self.inner.dealloc(ptr, layout) };
        self.shrink(layout.size());
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        let p = unsafe { self.inner.realloc(ptr, layout, new_size) };
        if !p.is_null() {
            self.allocs.fetch_add(1, Ordering::Relaxed);
            self.shrink(layout.size());
            self.grow(new_size);
        }
        p
    }
}
