//! `atomic_stats!` declares a `#[derive(Debug, Default)]` struct of `AtomicU64`
//! counters. No mirror / snapshot type: live struct is the API, so the
//! memory-ordering choice stays visible at every read site. A field may name
//! an explicit type (`field: Ty,`) for non-scalar counters; it must be
//! `Debug + Default`.

use std::sync::atomic::{AtomicU64, Ordering};

#[macro_export]
macro_rules! atomic_stats {
    (
        $(#[$smeta:meta])*
        $svis:vis struct $name:ident {
            $($(#[$fmeta:meta])* $fvis:vis $field:ident $(: $fty:ty)?,)*
        }
    ) => {
        $(#[$smeta])*
        #[derive(::core::fmt::Debug, ::core::default::Default)]
        $svis struct $name {
            $($(#[$fmeta])* $fvis $field: $crate::atomic_stats!(@ty $($fty)?),)*
        }
    };
    (@ty) => { ::core::sync::atomic::AtomicU64 };
    (@ty $fty:ty) => { $fty };
}

/// Fixed-bucket histogram observed lock-free off the hot path: one relaxed
/// `fetch_add` per bucket and sum. `N` finite upper bounds starting at
/// `FIRST` and doubling, in the owner's integer unit (nanoseconds, values),
/// plus an implicit `+Inf` bucket. A scrape racing an observation can see the
/// count one ahead of the sum
#[derive(Debug)]
pub struct Histogram<const FIRST: u64, const N: usize> {
    /// Per-bucket, not cumulative; the encoder accumulates
    counts: [AtomicU64; N],
    overflow: AtomicU64,
    sum: AtomicU64,
}

impl<const FIRST: u64, const N: usize> Default for Histogram<FIRST, N> {
    fn default() -> Self {
        Self {
            counts: std::array::from_fn(|_| AtomicU64::new(0)),
            overflow: AtomicU64::new(0),
            sum: AtomicU64::new(0),
        }
    }
}

impl<const FIRST: u64, const N: usize> Histogram<FIRST, N> {
    fn bound(i: usize) -> u64 {
        FIRST << i
    }

    /// A sample lands in the first bucket with `v <= bound`
    pub fn observe(&self, v: u64) {
        let i = (0..N).find(|i| v <= Self::bound(*i));
        let bucket = i.map_or(&self.overflow, |i| &self.counts[i]);
        bucket.fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(v, Ordering::Relaxed);
    }

    /// Bounds and sum divided by `scale` into the exported unit
    pub fn snapshot(&self, scale: f64) -> HistogramSnapshot {
        let mut buckets: Vec<(f64, u64)> = self
            .counts
            .iter()
            .enumerate()
            .map(|(i, c)| (Self::bound(i) as f64 / scale, c.load(Ordering::Relaxed)))
            .collect();
        buckets.push((f64::INFINITY, self.overflow.load(Ordering::Relaxed)));
        HistogramSnapshot {
            count: buckets.iter().map(|(_, c)| c).sum(),
            sum: self.sum.load(Ordering::Relaxed) as f64 / scale,
            buckets,
        }
    }
}

/// A [`Histogram`] read at scrape time: per-bucket counts under finite upper
/// bounds, closed by an `f64::INFINITY` bucket that renders as `+Inf`. Empty until
/// something publishes one
#[derive(Debug, Default, Clone, PartialEq)]
pub struct HistogramSnapshot {
    pub buckets: Vec<(f64, u64)>,
    pub sum: f64,
    pub count: u64,
}

impl HistogramSnapshot {
    /// Buckets as rendered: a snapshot nobody published still closes with
    /// `+Inf`, which every histogram sample needs
    pub(crate) fn encoded_buckets(&self) -> &[(f64, u64)] {
        if self.buckets.is_empty() {
            &[(f64::INFINITY, 0)]
        } else {
            &self.buckets
        }
    }

    /// Fold another histogram over the same bounds in, as one database's
    /// bootstrap and live bridges share a series
    pub fn merge(&mut self, other: HistogramSnapshot) {
        if self.buckets.is_empty() {
            *self = other;
            return;
        }
        debug_assert_eq!(self.buckets.len(), other.buckets.len());
        for (mine, theirs) in self.buckets.iter_mut().zip(other.buckets) {
            mine.1 += theirs.1;
        }
        self.sum += other.sum;
        self.count += other.count;
    }
}
