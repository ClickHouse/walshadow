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
    #[cfg(any(feature = "lz4", feature = "zstd"))]
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
    ///
    /// # Safety
    ///
    /// Caller installs raw function pointers the C library will invoke
    /// without further checks. To stay sound:
    ///
    /// * Each installed function pointer must match the exact signature
    ///   of the corresponding `chc_codec` field.
    /// * For any [`Compression`] the codec will be paired with at the
    ///   [`Client`](crate::Client), the relevant fields must be set —
    ///   e.g. `Compression::Lz4` needs `lz4_compress`, `lz4_decompress`,
    ///   `lz4_bound`. Leaving a required slot `None` reaches a null
    ///   call.
    /// * Any `ud` pointer stored on the codec must outlive the
    ///   [`Codec`] and remain dereferenceable from every thread the
    ///   codec is used from.
    pub unsafe fn raw_mut(self: Pin<&mut Self>) -> &mut sys::chc_codec {
        unsafe { &mut self.get_unchecked_mut().raw }
    }

    #[inline]
    pub(crate) fn as_ptr(self: Pin<&Self>) -> *const sys::chc_codec {
        &self.raw
    }

    pub(crate) fn supports(self: Pin<&Self>, compression: Compression) -> bool {
        match compression {
            Compression::None => true,
            Compression::Lz4 => {
                self.raw.lz4_compress.is_some() && self.raw.lz4_decompress.is_some()
            }
            Compression::Zstd => {
                self.raw.zstd_compress.is_some() && self.raw.zstd_decompress.is_some()
            }
        }
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
