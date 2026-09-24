//! ClickHouse type names inspected through clickhouse-c's parser

use clickhouse_c::{Allocator, Kind, TypeAst, TypeRef};

/// Outer kind, `None` when `ty` does not parse
pub fn kind(ty: &str) -> Option<Kind> {
    TypeAst::parse(ty, Allocator::global(&mimalloc::MiMalloc))
        .ok()?
        .view()
        .kind()
}

pub fn is_nullable(ty: &str) -> bool {
    kind(ty) == Some(Kind::Nullable)
}

/// Kind under an optional `Nullable`, `None` when `ty` does not parse
pub fn inner_kind(ty: &str) -> Option<Kind> {
    strip_nullable(
        TypeAst::parse(ty, Allocator::global(&mimalloc::MiMalloc))
            .ok()?
            .view(),
    )
    .kind()
}

/// `T` for `Nullable(T)`, else `view`
pub fn strip_nullable(view: TypeRef<'_>) -> TypeRef<'_> {
    if view.kind() == Some(Kind::Nullable)
        && let Some(inner) = view.child(0)
    {
        inner
    } else {
        view
    }
}
