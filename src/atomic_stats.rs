//! Single source of truth for atomic-fielded stats structs. `atomic_stats!`
//! declares a `#[derive(Debug, Default)]` struct of `AtomicU64` counters;
//! call sites bump via `.fetch_add(_, Relaxed)` and read via `.load(Relaxed)`.
//! No mirror / snapshot type — the live struct is the API, so the
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
