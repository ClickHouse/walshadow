//! Route envelopes — frozen routing/encoding state attached to rows.
//!
//! [`RouteSnapshot`] freezes the encoder-plan inputs a relation resolved to
//! over one WAL interval: destination mapping, `config_column` overrides,
//! encoding policy. Rows carry the snapshot to the batcher so a mapping or
//! config change never reinterprets rows already routed.

use std::collections::HashMap;
use std::sync::Arc;

use crate::decode::heap_decoder::DescribedHeap;
use crate::mapping::{TableMapping, TableTarget};

/// `config_column` overlay slice for one relation: source attname → CH type
pub type ColumnOverrides = HashMap<String, String>;

/// Encoder-plan inputs beyond mapping + overrides. Alloc-free (no parsed
/// type ASTs) so snapshots can serialize and dedup by content
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
    /// Overlay slice frozen with the route, consumed at batcher plan build.
    /// Empty when the overlay is off or names no columns for this relation
    pub column_overrides: Arc<ColumnOverrides>,
    pub encoding: Arc<RowEncodingSnapshot>,
}

impl RouteSnapshot {
    /// Freeze encoder-plan inputs; destination derives from mapping target
    pub fn freeze(
        mapping: Arc<TableMapping>,
        column_overrides: Arc<ColumnOverrides>,
        soft_delete: bool,
    ) -> Arc<Self> {
        let encoding = Arc::new(RowEncodingSnapshot {
            destination: mapping.target.clone(),
            soft_delete,
        });
        Arc::new(Self {
            mapping,
            column_overrides,
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
