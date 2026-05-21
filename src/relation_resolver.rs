//! Phase 12 — catalog adapter trait fronting `relation_at`.
//!
//! Emitter only needs one catalog operation: resolve
//! `(RelFileNode, source_lsn) → Arc<RelDescriptor>`. Decoupling Emitter
//! from [`ShadowCatalog`] lets bootstrap (which has a pre-seeded
//! [`CatalogMap`] but no live shadow PG yet) drive an Emitter through
//! the same path WAL streaming uses.
//!
//! Trait-object dispatch: Emitter holds `Arc<dyn RelationResolver + Send + Sync>`.
//! One vtable indirection per row, no generic propagation through the
//! daemon's `Box<dyn TupleObserver>` chain.

use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::Mutex;
use wal_rs::pg::walparser::RelFileNode;

use crate::backup_page_walk::CatalogMap;
use crate::shadow_catalog::{CatalogError, RelDescriptor, ShadowCatalog};

/// Resolve a relation descriptor from a WAL-observed filenode. `at_lsn`
/// is the source LSN of the record carrying the filenode; impls may use
/// it to gate on shadow replay (live catalog) or ignore (immutable
/// snapshot).
pub trait RelationResolver: Send + Sync {
    fn relation_at<'a>(
        &'a self,
        rfn: RelFileNode,
        at_lsn: u64,
    ) -> Pin<Box<dyn Future<Output = Result<Arc<RelDescriptor>, CatalogError>> + Send + 'a>>;
}

/// Live shadow-PG catalog. Delegates to [`ShadowCatalog::relation_at`]
/// under the existing mutex; `at_lsn` flows through unchanged so the
/// `pg_last_wal_replay_lsn()` gate fires as before.
impl RelationResolver for Mutex<ShadowCatalog> {
    fn relation_at<'a>(
        &'a self,
        rfn: RelFileNode,
        at_lsn: u64,
    ) -> Pin<Box<dyn Future<Output = Result<Arc<RelDescriptor>, CatalogError>> + Send + 'a>> {
        Box::pin(async move {
            let mut cat = self.lock().await;
            cat.relation_at(rfn, at_lsn).await
        })
    }
}

/// Snapshot resolver backed by the [`CatalogMap`] seeded from source PG
/// before BASE_BACKUP. `at_lsn` is ignored — the map is a single
/// `REPEATABLE READ` snapshot, no replay gate applies. Unknown
/// filenodes surface as [`CatalogError::NotFoundByFilenode`] so the
/// Emitter's "no mapping" path (counted as `unsupported_relations`)
/// fires for tuples whose relations were not seeded.
pub struct CatalogMapResolver {
    map: CatalogMap,
}

impl CatalogMapResolver {
    pub fn new(map: CatalogMap) -> Self {
        Self { map }
    }

    pub fn into_inner(self) -> CatalogMap {
        self.map
    }

    pub fn map(&self) -> &CatalogMap {
        &self.map
    }
}

impl RelationResolver for CatalogMapResolver {
    fn relation_at<'a>(
        &'a self,
        rfn: RelFileNode,
        _at_lsn: u64,
    ) -> Pin<Box<dyn Future<Output = Result<Arc<RelDescriptor>, CatalogError>> + Send + 'a>> {
        Box::pin(async move {
            self.map
                .get(rfn.db_node, rfn.rel_node)
                .ok_or(CatalogError::NotFoundByFilenode(rfn))
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::shadow_catalog::{RelAttr, ReplIdent};

    fn mk_rel(rel_node: u32) -> Arc<RelDescriptor> {
        let name = format!("t{rel_node}");
        let qualified_name = RelDescriptor::build_qualified_name("public", &name);
        Arc::new(RelDescriptor {
            rfn: RelFileNode {
                spc_node: 1663,
                db_node: 5,
                rel_node,
            },
            oid: rel_node,
            namespace_oid: 2200,
            namespace_name: "public".into(),
            name,
            qualified_name,
            kind: 'r',
            persistence: 'p',
            replident: ReplIdent::Default { pk_attnums: None },
            attributes: vec![RelAttr {
                attnum: 1,
                name: "id".into(),
                type_oid: 23,
                typmod: -1,
                not_null: true,
                dropped: false,
                type_name: "int4".into(),
                type_byval: true,
                type_len: 4,
                type_align: 'i',
                type_storage: 'p',
                missing_text: None,
            }],
        })
    }

    #[tokio::test(flavor = "current_thread")]
    async fn catalog_map_resolver_returns_seeded_descriptor() {
        let mut map = CatalogMap::new();
        map.insert(mk_rel(16400));
        let resolver = CatalogMapResolver::new(map);
        let r = resolver
            .relation_at(
                RelFileNode {
                    spc_node: 1663,
                    db_node: 5,
                    rel_node: 16400,
                },
                0xDEAD_BEEF,
            )
            .await
            .expect("hit");
        assert_eq!(r.oid, 16400);
        assert_eq!(r.name, "t16400");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn catalog_map_resolver_misses_on_unknown_filenode() {
        let mut map = CatalogMap::new();
        map.insert(mk_rel(16400));
        let resolver = CatalogMapResolver::new(map);
        let err = resolver
            .relation_at(
                RelFileNode {
                    spc_node: 1663,
                    db_node: 5,
                    rel_node: 99999,
                },
                0,
            )
            .await
            .expect_err("must miss");
        assert!(matches!(err, CatalogError::NotFoundByFilenode(_)));
    }

    #[test]
    fn catalog_map_resolver_exposes_map_and_into_inner() {
        let mut map = CatalogMap::new();
        map.insert(mk_rel(16402));
        let resolver = CatalogMapResolver::new(map);
        assert!(
            resolver.map().get(5, 16402).is_some(),
            "map() yields immutable view",
        );
        let recovered = resolver.into_inner();
        assert!(recovered.get(5, 16402).is_some());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn catalog_map_resolver_ignores_at_lsn() {
        // Same filenode resolves identically regardless of at_lsn —
        // snapshot has no replay gate.
        let mut map = CatalogMap::new();
        map.insert(mk_rel(16401));
        let resolver = CatalogMapResolver::new(map);
        let rfn = RelFileNode {
            spc_node: 1663,
            db_node: 5,
            rel_node: 16401,
        };
        let a = resolver.relation_at(rfn, 0).await.unwrap();
        let b = resolver.relation_at(rfn, u64::MAX).await.unwrap();
        assert!(Arc::ptr_eq(&a, &b));
    }
}
