//! Resolves table settings from TOML and runtime config

use globset::{Glob, GlobMatcher};
use regex_automata::meta::Regex;

use crate::runtime_config::TableRow;
use crate::schema::RelName;

/// Overwrite only where the narrower layer states a value
pub(crate) fn set_if<T: Clone>(dst: &mut Option<T>, src: &Option<T>) {
    if src.is_some() {
        dst.clone_from(src);
    }
}

/// Pattern syntax used by config entry
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum MatchKind {
    #[default]
    Exact,
    Glob,
    Regex,
}

impl MatchKind {
    pub fn parse(s: &str) -> Result<Self, String> {
        match s.trim().to_ascii_lowercase().as_str() {
            "" | "exact" => Ok(Self::Exact),
            "glob" => Ok(Self::Glob),
            "regex" => Ok(Self::Regex),
            other => Err(format!(
                "unknown match kind `{other}` (expected exact / glob / regex)"
            )),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Exact => "exact",
            Self::Glob => "glob",
            Self::Regex => "regex",
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) enum NamePattern {
    Literal(String),
    Glob(GlobMatcher),
    Regex(Regex),
}

impl NamePattern {
    pub(crate) fn compile(kind: MatchKind, pattern: &str) -> Result<Self, String> {
        match kind {
            MatchKind::Exact => Ok(Self::Literal(pattern.to_owned())),
            MatchKind::Glob => Glob::new(pattern)
                .map(|g| Self::Glob(g.compile_matcher()))
                .map_err(|e| format!("invalid glob `{pattern}`: {e}")),
            MatchKind::Regex => Regex::new(&format!("^(?:{pattern})$"))
                .map(Self::Regex)
                .map_err(|e| format!("invalid regex `{pattern}`: {e}")),
        }
    }

    pub(crate) fn is_match(&self, name: &str) -> bool {
        match self {
            Self::Literal(s) => s == name,
            Self::Glob(g) => g.is_match(name),
            Self::Regex(r) => r.is_match(name),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct TableRule {
    pub target_database: Option<String>,
    pub target_table: Option<String>,
    pub replicate: Option<bool>,
    pub initial_load: Option<String>,
}

impl TableRule {
    pub fn from_row(row: &TableRow) -> Self {
        Self {
            target_database: row.target_database.clone(),
            target_table: row.target_table.clone(),
            replicate: row.replicate,
            initial_load: row.initial_load.clone(),
        }
    }

    pub fn overlay(&mut self, other: &Self) {
        set_if(&mut self.target_database, &other.target_database);
        set_if(&mut self.target_table, &other.target_table);
        set_if(&mut self.replicate, &other.replicate);
        set_if(&mut self.initial_load, &other.initial_load);
    }
}

#[derive(Debug, Clone)]
pub struct RelMatcher {
    src: RelName,
    kind: MatchKind,
    namespace: NamePattern,
    name: NamePattern,
}

impl RelMatcher {
    pub fn compile(src: &RelName, kind: MatchKind) -> Result<Self, String> {
        Ok(Self {
            src: src.clone(),
            kind,
            namespace: NamePattern::compile(kind, &src.namespace)?,
            name: NamePattern::compile(kind, &src.name)?,
        })
    }

    pub fn matches(&self, rel: &RelName) -> bool {
        self.namespace.is_match(&rel.namespace) && self.name.is_match(&rel.name)
    }

    pub(crate) fn is_pattern(&self) -> bool {
        self.kind != MatchKind::Exact
    }

    pub(crate) fn width(&self) -> usize {
        self.src.namespace.len() + self.src.name.len()
    }

    /// Broadest first, literals last
    pub(crate) fn rank(&self) -> (bool, usize, &str, &str) {
        (
            !self.is_pattern(),
            self.width(),
            &self.src.namespace,
            &self.src.name,
        )
    }
}

#[derive(Debug, Clone, Default)]
pub struct TableRules {
    /// Ranked broadest to narrowest
    rules: Vec<(RelMatcher, TableRule)>,
    has_patterns: bool,
}

impl TableRules {
    /// Merged rule plus whether a pattern entry contributed
    fn merge(&self, rel: &RelName) -> (TableRule, bool) {
        let mut merged = TableRule::default();
        let mut from_pattern = false;
        let mut barred = false;
        let mut literal_replicate = None;
        for (matcher, rule) in &self.rules {
            if !matcher.matches(rel) {
                continue;
            }
            merged.overlay(rule);
            if matcher.is_pattern() {
                from_pattern = true;
                barred |= rule.replicate == Some(false);
            } else {
                set_if(&mut literal_replicate, &rule.replicate);
            }
        }
        // Any matching exclusion wins over pattern opt-ins, literal entry aside
        if barred {
            merged.replicate = literal_replicate.or(Some(false));
        }
        (merged, from_pattern)
    }

    pub fn settings(&self, rel: &RelName) -> TableRule {
        self.merge(rel).0
    }

    fn pattern_scope(&self, rel: &RelName) -> Option<TableRow> {
        let (rule, from_pattern) = self.merge(rel);
        (from_pattern && rule.replicate.is_some()).then(|| TableRow {
            target_database: rule.target_database,
            target_table: rule.target_table,
            replicate: rule.replicate,
            initial_load: rule.initial_load,
            ..TableRow::default()
        })
    }

    /// Scope intent a pattern entry states over relations the catalog holds.
    /// `present` runs only when a pattern is in force, since listing the
    /// catalog costs more than the lookup
    pub fn pattern_scoped(
        &self,
        present: impl FnOnce() -> Vec<RelName>,
        mapped: impl Fn(&RelName) -> bool,
    ) -> Vec<(RelName, TableRow)> {
        if !self.has_patterns {
            return Vec::new();
        }
        present()
            .into_iter()
            .filter_map(|rel| self.pattern_scope(&rel).map(|row| (rel, row)))
            .filter(|(rel, row)| row.replicate != Some(true) || !mapped(rel))
            .collect()
    }

    pub fn has_patterns(&self) -> bool {
        self.has_patterns
    }
}

#[derive(Debug, Default)]
pub struct TableRulesBuilder {
    rules: Vec<(usize, RelMatcher, TableRule)>,
    layer: usize,
    rejections: u64,
}

impl TableRulesBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn next_layer(&mut self) {
        self.layer += 1;
    }

    pub fn add(&mut self, key: &RelName, kind: MatchKind, rule: TableRule) {
        match RelMatcher::compile(key, kind) {
            Ok(matcher) => self.rules.push((self.layer, matcher, rule)),
            Err(e) => {
                tracing::warn!(target: "walshadow::config", qname = %key, error = %e, "table entry rejected");
                self.rejections += 1;
            }
        }
    }

    pub fn add_row(&mut self, key: &RelName, row: &TableRow) {
        let kind = match MatchKind::parse(row.match_kind.as_deref().unwrap_or_default()) {
            Ok(k) => k,
            Err(e) => {
                tracing::warn!(target: "walshadow::config", qname = %key, error = %e, "config_table.match rejected");
                self.rejections += 1;
                return;
            }
        };
        self.add(key, kind, TableRule::from_row(row));
    }

    pub fn finish(mut self) -> (TableRules, u64) {
        self.rules
            .sort_by(|(la, ma, _), (lb, mb, _)| ma.rank().cmp(&mb.rank()).then(la.cmp(lb)));
        let rules: Vec<_> = self.rules.into_iter().map(|(_, m, r)| (m, r)).collect();
        (
            TableRules {
                has_patterns: rules.iter().any(|(m, _)| m.is_pattern()),
                rules,
            },
            self.rejections,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rel(ns: &str, name: &str) -> RelName {
        RelName::new(ns, name)
    }

    fn target(table: &str) -> TableRule {
        TableRule {
            target_table: Some(table.into()),
            ..TableRule::default()
        }
    }

    #[test]
    fn exact_entry_retargets_only_its_relation() {
        let mut b = TableRulesBuilder::new();
        b.add(&rel("public", "events"), MatchKind::Exact, target("ev"));
        let (rules, rejected) = b.finish();
        assert_eq!(rejected, 0);
        assert_eq!(
            rules.settings(&rel("public", "events")).target_table,
            Some("ev".into())
        );
        assert!(
            rules
                .settings(&rel("public", "other"))
                .target_table
                .is_none()
        );
    }

    #[test]
    fn regex_entry_applies_to_unknown_relations() {
        let mut b = TableRulesBuilder::new();
        b.add(
            &rel("public", "events_.*"),
            MatchKind::Regex,
            TableRule {
                replicate: Some(true),
                initial_load: Some("copy".into()),
                ..TableRule::default()
            },
        );
        let (rules, _) = b.finish();
        let hit = rel("public", "events_2026");
        let s = rules.settings(&hit);
        assert_eq!(s.replicate, Some(true));
        assert_eq!(s.initial_load.as_deref(), Some("copy"));
        assert!(rules.pattern_scope(&hit).is_some());
        let miss = rel("public", "my_events_2026");
        assert_eq!(
            rules.settings(&miss).replicate,
            None,
            "anchored: substring must not match"
        );
        assert!(rules.pattern_scope(&miss).is_none());
    }

    #[test]
    fn narrower_pattern_and_exact_entry_win() {
        let mut b = TableRulesBuilder::new();
        b.add(
            &rel("*", "*"),
            MatchKind::Glob,
            TableRule {
                target_database: Some("broad".into()),
                target_table: Some("broad".into()),
                ..TableRule::default()
            },
        );
        b.add(
            &rel("public", "events_*"),
            MatchKind::Glob,
            target("narrow"),
        );
        let (rules, _) = b.finish();
        let s = rules.settings(&rel("public", "events_1"));
        assert_eq!(
            s.target_table,
            Some("narrow".into()),
            "wider pattern applies first"
        );
        assert_eq!(
            s.target_database,
            Some("broad".into()),
            "broad pattern still contributes fields the narrow one omits"
        );

        let mut b = TableRulesBuilder::new();
        b.add(&rel("*", "*"), MatchKind::Glob, target("broad"));
        b.next_layer();
        b.add(
            &rel("public", "events_1"),
            MatchKind::Exact,
            target("exact"),
        );
        let (rules, _) = b.finish();
        assert_eq!(
            rules.settings(&rel("public", "events_1")).target_table,
            Some("exact".into())
        );
    }

    #[test]
    fn unparseable_pattern_rejected() {
        let mut b = TableRulesBuilder::new();
        b.add(
            &rel("public", "ev(nt"),
            MatchKind::Regex,
            TableRule::default(),
        );
        b.add(
            &rel("public", "ev[nt"),
            MatchKind::Glob,
            TableRule::default(),
        );
        let (rules, rejected) = b.finish();
        assert_eq!(rejected, 2);
        assert!(!rules.has_patterns());
    }

    #[test]
    fn pattern_scope_lists_matching_present_relations() {
        let mut b = TableRulesBuilder::new();
        b.add(
            &rel("public", "events_*"),
            MatchKind::Glob,
            TableRule {
                replicate: Some(true),
                initial_load: Some("copy".into()),
                ..TableRule::default()
            },
        );
        let (rules, _) = b.finish();
        let present = [
            rel("public", "events_1"),
            rel("public", "orders"),
            rel("other", "events_2"),
        ];
        let scoped = rules.pattern_scoped(|| present.to_vec(), |_| false);
        assert_eq!(scoped.len(), 1);
        assert_eq!(scoped[0].0, rel("public", "events_1"));
        assert_eq!(scoped[0].1.replicate, Some(true));
        assert_eq!(scoped[0].1.initial_load.as_deref(), Some("copy"));
        assert!(
            rules
                .pattern_scoped(|| present.to_vec(), |_| true)
                .is_empty(),
            "an already-mapped relation keeps its pinned projection"
        );
        assert!(
            rules.pattern_scope(&rel("public", "orders")).is_none(),
            "no scope intent without a match"
        );
    }

    #[test]
    fn excluding_pattern_beats_matching_opt_in() {
        let mut b = TableRulesBuilder::new();
        b.add(
            &rel("app", "events_*"),
            MatchKind::Glob,
            TableRule {
                replicate: Some(true),
                ..TableRule::default()
            },
        );
        b.add(
            &rel("app", "*_audit"),
            MatchKind::Glob,
            TableRule {
                replicate: Some(false),
                ..TableRule::default()
            },
        );
        let (rules, _) = b.finish();
        assert_eq!(
            rules.settings(&rel("app", "events_1")).replicate,
            Some(true)
        );
        assert_eq!(
            rules.settings(&rel("app", "events_audit")).replicate,
            Some(false),
            "guardrail regardless of which pattern is wider"
        );
        let mut b = TableRulesBuilder::new();
        b.add(
            &rel("app", "*_audit"),
            MatchKind::Glob,
            TableRule {
                replicate: Some(false),
                ..TableRule::default()
            },
        );
        b.next_layer();
        b.add(
            &rel("app", "events_audit"),
            MatchKind::Exact,
            TableRule {
                replicate: Some(true),
                ..TableRule::default()
            },
        );
        let (rules, _) = b.finish();
        assert_eq!(
            rules.settings(&rel("app", "events_audit")).replicate,
            Some(true)
        );
    }

    #[test]
    fn glob_entry_reads_wildcards_not_regex() {
        let mut b = TableRulesBuilder::new();
        b.add(
            &rel("app", "events_*"),
            MatchKind::Glob,
            TableRule {
                replicate: Some(true),
                ..TableRule::default()
            },
        );
        let (rules, rejected) = b.finish();
        assert_eq!(rejected, 0);
        for name in ["events_2026", "events_"] {
            assert_eq!(
                rules.settings(&rel("app", name)).replicate,
                Some(true),
                "glob `events_*` must match {name}"
            );
        }
        for name in ["events", "other_events_1"] {
            assert_eq!(
                rules.settings(&rel("app", name)).replicate,
                None,
                "glob `events_*` must not match {name}"
            );
        }
    }

    #[test]
    fn glob_takes_dots_and_regex_metacharacters_literally() {
        let mut b = TableRulesBuilder::new();
        b.add(
            &rel("app", "v1.*"),
            MatchKind::Glob,
            TableRule {
                replicate: Some(true),
                ..TableRule::default()
            },
        );
        let (rules, _) = b.finish();
        assert_eq!(
            rules.settings(&rel("app", "v1.events")).replicate,
            Some(true)
        );
        assert_eq!(
            rules.settings(&rel("app", "v1x")).replicate,
            None,
            "the dot is a dot, not any-character"
        );
    }

    #[test]
    fn match_kind_parse() {
        assert_eq!(MatchKind::parse("").unwrap(), MatchKind::Exact);
        assert_eq!(MatchKind::parse(" Regex ").unwrap(), MatchKind::Regex);
        assert_eq!(MatchKind::parse("glob").unwrap(), MatchKind::Glob);
        assert!(MatchKind::parse("like").is_err());
    }
}
