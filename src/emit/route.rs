//! Route snapshots freeze routing and encoding inputs for each WAL interval

use std::sync::Arc;

use crate::column_rules::ColumnRules;
use crate::decode::heap_decoder::DescribedHeap;
use crate::mapping::{TableMapping, TableTarget};

#[derive(Debug)]
pub struct RowEncodingSnapshot {
    pub destination: TableTarget,
    /// CH-side delete retention policy (`_is_deleted` stays queryable);
    /// boot-only knob, snapshotted so execution never reads live config
    pub soft_delete: bool,
}

/// Frozen route for one relation over one WAL interval
#[derive(Debug)]
pub struct RouteSnapshot {
    pub mapping: Arc<TableMapping>,
    pub column_rules: Arc<ColumnRules>,
    pub encoding: Arc<RowEncodingSnapshot>,
}

impl RouteSnapshot {
    /// Freeze encoder-plan inputs; destination derives from mapping target
    pub fn freeze(
        mapping: Arc<TableMapping>,
        column_rules: Arc<ColumnRules>,
        soft_delete: bool,
    ) -> Arc<Self> {
        let encoding = Arc::new(RowEncodingSnapshot {
            destination: mapping.target.clone(),
            soft_delete,
        });
        Arc::new(Self {
            mapping,
            column_rules,
            encoding,
        })
    }
}

/// Described heap plus its resolved route. `route = None` means the relation
/// is deterministically unmapped at that interval — a normal counted discard,
/// distinct from a missing descriptor
#[derive(Debug)]
pub struct RoutedHeap {
    pub described: DescribedHeap,
    pub route: Option<Arc<RouteSnapshot>>,
}
