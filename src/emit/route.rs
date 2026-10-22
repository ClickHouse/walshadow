//! Route snapshots freeze routing and encoding inputs for each WAL interval

use std::sync::Arc;

use tokio_postgres::types::Oid;

use crate::column_rules::ColumnRules;
use crate::mapping::{ColumnMapping, SystemColumns, TableMapping, TableTarget};
use crate::schema::{RelDescriptor, RelName};
use crate::table_rules::{PartitionRoute, TableRules};

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
            system: rc.rule(rel).system_columns(&self.system),
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
#[derive(Debug, Clone)]
pub struct RouteSnapshot {
    pub mapping: Arc<TableMapping>,
    pub column_rules: Arc<ColumnRules>,
    pub encoding: Arc<RowEncodingSnapshot>,
    /// Set when a partition's rows land in an ancestor's destination
    pub partition: Option<PartitionOf>,
}

/// Partition encoded under `root`'s column rules
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionOf {
    pub root: RelName,
    pub leaf: Oid,
    /// Rows land in `root`'s destination and batch with its whole tree.
    /// Otherwise rows stage in a table of their own and batch alone
    pub shared: bool,
}

/// Where a relation's rows land
#[derive(Debug, Clone, Copy)]
pub enum Routing<'a> {
    /// Its own mapping
    Own,
    /// A partition routed to this ancestor's mapping
    Ancestor(&'a RelDescriptor),
    /// Partition excluded by an exact `replicate = false` rule on its path
    Excluded,
}

impl<'a> Routing<'a> {
    /// Relation owning destination of `desc`'s rows
    pub fn owner(self, desc: &'a RelDescriptor) -> Option<&'a RelDescriptor> {
        match self {
            Routing::Own => Some(desc),
            Routing::Ancestor(root) => Some(root),
            Routing::Excluded => None,
        }
    }
}

/// Find first relation from `desc` upward with `route = "self"`, or use
/// topmost ancestor. Only exact rules exclude partitions. Pattern rules
/// apply to partitions only through `route = "self"`
pub fn routing<'a>(desc: &'a RelDescriptor, rules: Option<&TableRules>) -> Routing<'a> {
    let ancestors = &desc.ancestors;
    let Some(top) = ancestors.last() else {
        return Routing::Own;
    };
    let Some(rules) = rules else {
        return Routing::Ancestor(top);
    };
    let below = std::iter::once((&desc.rel_name, None)).chain(
        ancestors[..ancestors.len() - 1]
            .iter()
            .map(|a| (&a.rel_name, Some(a.as_ref()))),
    );
    for (name, node) in below {
        if rules.exact_settings(name).replicate == Some(false) {
            return Routing::Excluded;
        }
        if rules.partition_settings(name).route == Some(PartitionRoute::Self_) {
            return node.map_or(Routing::Own, Routing::Ancestor);
        }
    }
    Routing::Ancestor(top)
}

/// Match `root`'s mapped columns by name to `leaf`'s attribute numbers
pub fn partition_mapping(
    mapping: &TableMapping,
    root: &RelDescriptor,
    leaf: &RelDescriptor,
) -> TableMapping {
    let columns = mapping
        .columns
        .iter()
        .map(|c| ColumnMapping {
            src_attnum: root
                .live_attr(c.src_attnum)
                .and_then(|a| leaf.live_attnum(&a.name))
                .unwrap_or(0),
            ..c.clone()
        })
        .collect();
    TableMapping {
        columns,
        ..mapping.clone()
    }
}

/// Route `desc` rows by [`routing`], resolving each owning relation's route
/// through `lookup`
pub fn route_via(
    desc: &RelDescriptor,
    rules: Option<&TableRules>,
    mut lookup: impl FnMut(&RelName) -> Option<Arc<RouteSnapshot>>,
) -> Option<Arc<RouteSnapshot>> {
    match routing(desc, rules) {
        Routing::Own => lookup(&desc.rel_name),
        Routing::Ancestor(root) => lookup(&root.rel_name).map(|r| r.for_partition(root, desc)),
        Routing::Excluded => None,
    }
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
            partition: None,
        })
    }

    /// Match root columns by name to `leaf`'s attribute numbers. Preserve
    /// root column order so partitions share one encoder. Treat missing
    /// column names as absent values
    pub fn for_partition(&self, root: &RelDescriptor, leaf: &RelDescriptor) -> Arc<Self> {
        Arc::new(Self {
            mapping: Arc::new(partition_mapping(&self.mapping, root, leaf)),
            partition: Some(PartitionOf {
                root: root.rel_name.clone(),
                leaf: leaf.oid,
                shared: true,
            }),
            ..self.clone()
        })
    }

    /// Copy route with rows identified as belonging to `partition`
    pub fn with_partition(&self, partition: PartitionOf) -> Arc<Self> {
        Arc::new(Self {
            partition: Some(partition),
            ..self.clone()
        })
    }

    /// Relation whose column rules shape this route
    pub fn owner<'a>(&'a self, rel: &'a RelName) -> &'a RelName {
        self.partition.as_ref().map_or(rel, |p| &p.root)
    }

    /// Relation whose name keys this route's batches: one destination per key
    pub fn batch_owner<'a>(&'a self, rel: &'a RelName) -> &'a RelName {
        match &self.partition {
            Some(p) if p.shared => &p.root,
            _ => rel,
        }
    }

    pub fn system_columns(&self) -> &SystemColumns {
        &self.encoding.policy.system
    }

    /// DELETE carries no marker column to set, so those rows are dropped
    pub fn drops_deletes(&self) -> bool {
        self.encoding.policy.system.is_deleted.is_none()
    }
}

/// One frozen route per mapped relation, so a bounded pass never re-reads
/// config or re-clones column rules per row
pub fn freeze_routes(
    mapping: &crate::mapping::MappingSnapshot,
    config: Option<&crate::config::ResolvedConfig>,
    row_policy: &RowPolicy,
) -> FrozenRoutes {
    let rules = config.map_or_else(Arc::default, |rc| rc.column_rules.clone());
    let owned = mapping
        .iter()
        .map(|(name, m)| {
            let policy = row_policy.for_rel(config, name);
            (
                name.clone(),
                RouteSnapshot::freeze(m.clone(), rules.clone(), policy),
            )
        })
        .collect();
    FrozenRoutes {
        owned,
        rules: config.map(|rc| rc.rules.clone()),
        pinned: Default::default(),
        partitions: Default::default(),
    }
}

/// Freeze routes for one bounded pass. Derive each partition's route from
/// root's route once
#[derive(Debug)]
pub struct FrozenRoutes {
    owned: ahash::HashMap<RelName, Arc<RouteSnapshot>>,
    rules: Option<Arc<TableRules>>,
    /// Routes by row relation, bypassing partition routing
    pinned: ahash::HashMap<RelName, Arc<RouteSnapshot>>,
    partitions: std::sync::Mutex<ahash::HashMap<Oid, Option<Arc<RouteSnapshot>>>>,
}

impl FrozenRoutes {
    /// Routes keyed by the relation rows come from, partitions included
    pub fn pinned(pinned: ahash::HashMap<RelName, Arc<RouteSnapshot>>) -> Self {
        Self {
            owned: Default::default(),
            rules: None,
            pinned,
            partitions: Default::default(),
        }
    }

    pub fn route(&self, desc: &RelDescriptor) -> Option<Arc<RouteSnapshot>> {
        if let Some(route) = self.pinned.get(&desc.rel_name) {
            return Some(route.clone());
        }
        if !desc.is_partition() {
            return self.owned.get(&desc.rel_name).cloned();
        }
        let mut memo = self.partitions.lock().expect("route memo poisoned");
        memo.entry(desc.oid)
            .or_insert_with(|| {
                route_via(desc, self.rules.as_deref(), |owner| {
                    self.owned.get(owner).cloned()
                })
            })
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::int4_rel;
    use crate::table_rules::{MatchKind, TableRule, TableRulesBuilder};

    /// `m` > `a` > `a1`, `a1` storing columns in another order than `m`,
    /// whose slot 2 is a dropped column
    fn tree() -> (RelDescriptor, RelDescriptor, RelDescriptor) {
        let m = int4_rel(16480, "m", 'p', &[(1, "id"), (2, ""), (3, "v")]);
        let mut a = int4_rel(16490, "a", 'p', &[(1, "id"), (2, "v")]);
        a.ancestors = vec![m.as_ancestor()];
        let mut a1 = int4_rel(16500, "a1", 'r', &[(1, "v"), (2, "id")]);
        a1.ancestors = vec![a.as_ancestor(), m.as_ancestor()];
        (m, a, a1)
    }

    fn rules(entries: &[(&str, MatchKind, TableRule)]) -> TableRules {
        let mut b = TableRulesBuilder::new();
        for (name, kind, rule) in entries {
            b.add(&RelName::new("public", name), *kind, rule.clone());
        }
        b.finish().0
    }

    fn owner<'a>(desc: &'a RelDescriptor, rules: Option<&TableRules>) -> Option<&'a str> {
        routing(desc, rules).owner(desc).map(|d| &*d.rel_name.name)
    }

    #[test]
    fn partitions_route_to_topmost_ancestor_unless_cut() {
        let (m, _, a1) = tree();
        let route_self = TableRule {
            route: Some(PartitionRoute::Self_),
            ..TableRule::default()
        };
        assert_eq!(owner(&m, None), Some("m"));
        assert_eq!(owner(&a1, None), Some("m"));
        let cut = rules(&[("a", MatchKind::Exact, route_self.clone())]);
        assert_eq!(owner(&a1, Some(&cut)), Some("a"));
        let own = rules(&[("a1", MatchKind::Exact, route_self.clone())]);
        assert_eq!(owner(&a1, Some(&own)), Some("a1"));
        let excluded = rules(&[(
            "a1",
            MatchKind::Exact,
            TableRule {
                replicate: Some(false),
                ..TableRule::default()
            },
        )]);
        assert_eq!(owner(&a1, Some(&excluded)), None);
        assert!(route_via(&a1, Some(&excluded), |_| unreachable!()).is_none());
    }

    #[test]
    fn patterns_reach_partitions_only_through_route_self() {
        let (_, _, a1) = tree();
        let barred = rules(&[(
            "*",
            MatchKind::Glob,
            TableRule {
                replicate: Some(false),
                ..TableRule::default()
            },
        )]);
        assert_eq!(owner(&a1, Some(&barred)), Some("m"));
        let split = rules(&[(
            "a*",
            MatchKind::Glob,
            TableRule {
                route: Some(PartitionRoute::Self_),
                ..TableRule::default()
            },
        )]);
        assert_eq!(owner(&a1, Some(&split)), Some("a1"), "leaf matches first");
    }

    #[test]
    fn partition_route_renumbers_root_columns_by_name() {
        let (m, _, a1) = tree();
        let mapping = TableMapping {
            target: TableTarget::new("db", "m"),
            columns: [(1, "id"), (3, "v"), (4, "later")]
                .map(|(src_attnum, name)| ColumnMapping {
                    src_attnum,
                    target_name: name.into(),
                    target_type: "Int32".into(),
                    type_pinned: false,
                })
                .to_vec(),
            tees: Vec::new(),
        };
        let root = RouteSnapshot::freeze(Arc::new(mapping), Arc::default(), RowPolicy::default());
        let leaf = root.for_partition(&m, &a1);
        let attnums: Vec<i16> = leaf.mapping.columns.iter().map(|c| c.src_attnum).collect();
        assert_eq!(attnums, [2, 1, 0], "absent name reads as no column");
        assert_eq!(
            leaf.partition,
            Some(PartitionOf {
                root: m.rel_name.clone(),
                leaf: a1.oid,
                shared: true,
            })
        );
        assert_eq!(leaf.owner(&a1.rel_name), &m.rel_name);
        assert_eq!(leaf.batch_owner(&a1.rel_name), &m.rel_name);
        assert_eq!(root.owner(&m.rel_name), &m.rel_name);
        let staged = root.with_partition(PartitionOf {
            root: m.rel_name.clone(),
            leaf: a1.oid,
            shared: false,
        });
        assert_eq!(
            staged.batch_owner(&a1.rel_name),
            &a1.rel_name,
            "a staged partition batches alone"
        );
    }

    #[test]
    fn frozen_routes_derive_partition_routes_from_their_root() {
        let (m, _, a1) = tree();
        let mapping: crate::mapping::MappingSnapshot = Arc::new(
            [(
                m.rel_name.clone(),
                Arc::new(TableMapping {
                    target: TableTarget::new("db", "m"),
                    columns: Vec::new(),
                    tees: Vec::new(),
                }),
            )]
            .into_iter()
            .collect(),
        );
        let routes = freeze_routes(&mapping, None, &RowPolicy::default());
        let first = routes.route(&a1).expect("partition routes through root");
        assert!(Arc::ptr_eq(&first, &routes.route(&a1).unwrap()), "memoized");
        let pinned = FrozenRoutes::pinned(
            [(a1.rel_name.clone(), routes.route(&m).expect("root route"))]
                .into_iter()
                .collect(),
        );
        assert!(
            pinned.route(&a1).unwrap().partition.is_none(),
            "pinned bypasses routing"
        );
    }
}
