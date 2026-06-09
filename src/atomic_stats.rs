//! `atomic_stats!` declares a `#[derive(Debug, Default)]` struct of `AtomicU64`
//! counters. No mirror / snapshot type: live struct is the API, so the
//! memory-ordering choice stays visible at every read site.

#[macro_export]
macro_rules! atomic_stats {
    (
        $(#[$smeta:meta])*
        $svis:vis struct $name:ident {
            $($(#[$fmeta:meta])* $fvis:vis $field:ident,)*
        }
    ) => {
        $(#[$smeta])*
        #[derive(::core::fmt::Debug, ::core::default::Default)]
        $svis struct $name {
            $($(#[$fmeta])* $fvis $field: ::core::sync::atomic::AtomicU64,)*
        }
    };
}
