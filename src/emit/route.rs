//! Route snapshots freeze routing and encoding inputs for each WAL interval

use std::sync::Arc;

use crate::column_rules::ColumnRules;
use crate::decode::heap_decoder::DescribedHeap;
use crate::mapping::{SystemColumns, TableMapping, TableTarget};
use crate::schema::RelName;

/// Row-shape knobs, cloned into every frozen route so execution never reads
/// live config. `soft_delete` is boot-only; the system columns are the
/// cluster-wide `[system_columns]` set until [`Self::for_rel`] layers a
/// per-relation rename over it
#[derive(Debug, Clone, Default)]
pub struct RowPolicy {
    /// CH-side delete retention policy (the delete marker stays queryable)
    pub soft_delete: bool,
    /// Names + presence of the columns walshadow appends per row
    pub system: Arc<SystemColumns>,
}

impl RowPolicy {
    /// Policy for one relation: a `[table.*]` block or `config_table` row can
    /// rename its system columns or drop its delete marker. Without a config
    /// snapshot, or with no rename for this relation, the cluster-wide set
    /// stands
    pub fn for_rel(&self, config: Option<&crate::config::ResolvedConfig>, rel: &RelName) -> Self {
        let Some(rc) = config else {
            return self.clone();
        };
        Self {
            system: rc.rules.settings(rel).system_columns(&self.system),
            ..self.clone()
        }
    }
}

#[derive(Debug)]
pub struct RowEncodingSnapshot {
    pub destination: TableTarget,
    pub policy: RowPolicy,
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
        policy: RowPolicy,
    ) -> Arc<Self> {
        let encoding = Arc::new(RowEncodingSnapshot {
            destination: mapping.target.clone(),
            policy,
        });
        Arc::new(Self {
            mapping,
            column_rules,
            encoding,
        })
    }

    pub fn system_columns(&self) -> &SystemColumns {
        &self.encoding.policy.system
    }

    /// DELETE carries no marker column to set, so those rows are dropped
    pub fn drops_deletes(&self) -> bool {
        self.encoding.policy.system.is_deleted.is_none()
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
