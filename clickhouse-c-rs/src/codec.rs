//! Compression codec handles.
//!
//! Built-in helpers for LZ4 (feature `lz4`, default) and ZSTD (feature
//! `zstd`) populate a [`Codec`] that the client passes to the server.
//! Manual codecs can be assembled by filling [`Codec::raw_mut`] directly.

use core::pin::Pin;

use crate::sys;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(i32)]
pub enum Compression {
    None = sys::CHC_COMP_NONE,
    Lz4 = sys::CHC_COMP_LZ4,
    Zstd = sys::CHC_COMP_ZSTD,
}

/// Owns a `chc_codec`. Constructed via the codec-specific factory
/// (`Codec::lz4()`, `Codec::zstd()`) or by hand-filling [`raw_mut`].
///
/// The struct is pinned because compression code calls back into the
/// function-pointer table by address.
pub struct Codec {
    raw: sys::chc_codec,
    _pin: core::marker::PhantomPinned,
}

impl Codec {
    fn zeroed() -> Self {
        Self {
            raw: sys::chc_codec {
                ud: core::ptr::null_mut(),
                lz4_compress: None,
                lz4_decompress: None,
                zstd_compress: None,
                zstd_decompress: None,
                lz4_bound: None,
                zstd_bound: None,
            },
            _pin: core::marker::PhantomPinned,
        }
    }

    #[cfg(feature = "lz4")]
    pub fn lz4() -> Pin<Box<Self>> {
        let mut b = Box::pin(Self::zeroed());
        unsafe {
            let this = b.as_mut().get_unchecked_mut();
            sys::chc_lz4_codec_init(&mut this.raw);
        }
        b
    }

    #[cfg(feature = "zstd")]
    pub fn zstd() -> Pin<Box<Self>> {
        let mut b = Box::pin(Self::zeroed());
        unsafe {
            let this = b.as_mut().get_unchecked_mut();
            sys::chc_zstd_codec_init(&mut this.raw);
        }
        b
    }

    /// Borrow the underlying `chc_codec` for manual fills (e.g. wiring a
    /// custom allocator-bound compression implementation).
    pub fn raw_mut(self: Pin<&mut Self>) -> &mut sys::chc_codec {
        unsafe { &mut self.get_unchecked_mut().raw }
    }

    #[inline]
    pub(crate) fn as_ptr(self: Pin<&Self>) -> *const sys::chc_codec {
        &self.raw
    }
}

unsafe impl Send for Codec {}

/// City Hash 128 helper. Returns `(lo, hi)` matching the on-wire
/// frame-checksum layout.
pub fn cityhash128(data: &[u8]) -> (u64, u64) {
    let mut lo = 0u64;
    let mut hi = 0u64;
    unsafe {
        sys::chc_cityhash128(data.as_ptr().cast(), data.len(), &mut lo, &mut hi);
    }
    (lo, hi)
}
