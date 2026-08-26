//! Resolves column settings from TOML and runtime config

use ahash::HashMap;

use crate::schema::RelName;
use crate::table_rules::{MatchKind, NamePattern, RelMatcher, set_if};

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ColumnRule {
    pub target_name: Option<String>,
    pub target_type: Option<String>,
}

impl ColumnRule {
    pub fn overlay(&mut self, other: &Self) {
        set_if(&mut self.target_name, &other.target_name);
        set_if(&mut self.target_type, &other.target_type);
    }

    pub fn is_empty(&self) -> bool {
        self.target_name.is_none() && self.target_type.is_none()
    }
}

#[derive(Debug, Clone)]
pub struct ColumnEntry {
    pub rel: RelName,
    pub rel_kind: MatchKind,
    pub attname: String,
    pub att_kind: MatchKind,
    pub rule: ColumnRule,
}

#[derive(Debug, Clone)]
struct ColumnMatcher {
    rel: RelMatcher,
    att_kind: MatchKind,
    attname: NamePattern,
    attname_src: String,
}

impl ColumnMatcher {
    fn compile(
        rel: &RelName,
        rel_kind: MatchKind,
        attname: &str,
        att_kind: MatchKind,
    ) -> Result<Self, String> {
        Ok(Self {
            rel: RelMatcher::compile(rel, rel_kind)?,
            att_kind,
            attname: NamePattern::compile(att_kind, attname)?,
            attname_src: attname.to_owned(),
        })
    }

    fn matches(&self, rel: &RelName, attname: &str) -> bool {
        self.rel.matches(rel) && self.attname.is_match(attname)
    }

    /// Broadest first, literals last
    fn rank(&self) -> (bool, usize, &str, &str, &str) {
        let (_, rel_width, namespace, name) = self.rel.rank();
        (
            !self.rel.is_pattern() && self.att_kind == MatchKind::Exact,
            rel_width + self.attname_src.len(),
            namespace,
            name,
            &self.attname_src,
        )
    }
}

#[derive(Debug, Clone, Default)]
pub struct ColumnRules {
    /// Ranked broadest to narrowest
    rules: Vec<(ColumnMatcher, ColumnRule)>,
    /// Last valid type for each runtime config row
    accepted: HashMap<RelName, HashMap<String, String>>,
}

impl ColumnRules {
    pub fn settings(&self, rel: &RelName, attname: &str) -> ColumnRule {
        let mut merged = ColumnRule::default();
        for (matcher, rule) in &self.rules {
            if matcher.matches(rel, attname) {
                merged.overlay(rule);
            }
        }
        merged
    }

    pub fn accepted_type(&self, rel: &RelName, attname: &str) -> Option<&str> {
        self.accepted
            .get(rel)
            .and_then(|m| m.get(attname))
            .map(String::as_str)
    }

    pub fn is_empty(&self) -> bool {
        self.rules.is_empty()
    }
}

#[derive(Debug, Default)]
pub struct ColumnRulesBuilder {
    rules: Vec<(usize, ColumnMatcher, ColumnRule)>,
    accepted: HashMap<RelName, HashMap<String, String>>,
    layer: usize,
    rejections: u64,
}

impl ColumnRulesBuilder {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn next_layer(&mut self) {
        self.layer += 1;
    }

    pub fn add(
        &mut self,
        rel: &RelName,
        rel_kind: MatchKind,
        attname: &str,
        att_kind: MatchKind,
        rule: ColumnRule,
    ) {
        match ColumnMatcher::compile(rel, rel_kind, attname, att_kind) {
            Ok(matcher) => self.rules.push((self.layer, matcher, rule)),
            Err(e) => {
                tracing::warn!(target: "walshadow::config", qname = %rel, attname = %attname, error = %e, "column entry rejected");
                self.rejections += 1;
            }
        }
    }

    pub fn record_accepted(&mut self, rel: &RelName, attname: &str, target_type: &str) {
        self.accepted
            .entry(rel.clone())
            .or_default()
            .insert(attname.to_owned(), target_type.to_owned());
    }

    pub fn bump_rejections(&mut self) {
        self.rejections += 1;
    }

    pub fn finish(mut self) -> (ColumnRules, u64) {
        self.rules
            .sort_by(|(la, ma, _), (lb, mb, _)| ma.rank().cmp(&mb.rank()).then(la.cmp(lb)));
        (
            ColumnRules {
                rules: self.rules.into_iter().map(|(_, m, r)| (m, r)).collect(),
                accepted: self.accepted,
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

    fn ty(t: &str) -> ColumnRule {
        ColumnRule {
            target_type: Some(t.into()),
            ..ColumnRule::default()
        }
    }

    #[test]
    fn exact_entry_types_only_its_column() {
        let mut b = ColumnRulesBuilder::new();
        b.add(
            &rel("public", "events"),
            MatchKind::Exact,
            "amount",
            MatchKind::Exact,
            ty("Decimal(38, 9)"),
        );
        let (rules, rejected) = b.finish();
        assert_eq!(rejected, 0);
        assert_eq!(
            rules
                .settings(&rel("public", "events"), "amount")
                .target_type,
            Some("Decimal(38, 9)".into())
        );
        assert!(
            rules
                .settings(&rel("public", "events"), "net_amount")
                .target_type
                .is_none(),
            "anchored: substring must not match"
        );
        assert!(
            rules
                .settings(&rel("public", "other"), "amount")
                .target_type
                .is_none()
        );
    }

    #[test]
    fn glob_attname_spans_relations_a_pattern_block_names() {
        let mut b = ColumnRulesBuilder::new();
        b.add(
            &rel("app", "*"),
            MatchKind::Glob,
            "*_at",
            MatchKind::Glob,
            ty("DateTime64(6, 'UTC')"),
        );
        let (rules, _) = b.finish();
        for (r, c) in [("events", "created_at"), ("orders", "shipped_at")] {
            assert_eq!(
                rules.settings(&rel("app", r), c).target_type,
                Some("DateTime64(6, 'UTC')".into()),
                "{r}.{c}"
            );
        }
        assert!(
            rules
                .settings(&rel("app", "events"), "at_rest")
                .target_type
                .is_none()
        );
        assert!(
            rules
                .settings(&rel("other", "events"), "created_at")
                .target_type
                .is_none()
        );
    }

    #[test]
    fn narrower_entry_and_exact_entry_win() {
        let mut b = ColumnRulesBuilder::new();
        b.add(
            &rel("*", "*"),
            MatchKind::Glob,
            "*",
            MatchKind::Glob,
            ColumnRule {
                target_name: Some("broad".into()),
                target_type: Some("String".into()),
            },
        );
        b.add(
            &rel("app", "events"),
            MatchKind::Exact,
            "amt_*",
            MatchKind::Glob,
            ty("Decimal(38, 9)"),
        );
        let (rules, _) = b.finish();
        let s = rules.settings(&rel("app", "events"), "amt_net");
        assert_eq!(s.target_type, Some("Decimal(38, 9)".into()));
        assert_eq!(
            s.target_name,
            Some("broad".into()),
            "broad entry still contributes fields the narrow one omits"
        );

        let mut b = ColumnRulesBuilder::new();
        b.add(
            &rel("app", "events"),
            MatchKind::Exact,
            "amt_*",
            MatchKind::Glob,
            ty("Decimal(38, 9)"),
        );
        b.next_layer();
        b.add(
            &rel("app", "events"),
            MatchKind::Exact,
            "amt_net",
            MatchKind::Exact,
            ty("Int128"),
        );
        let (rules, _) = b.finish();
        assert_eq!(
            rules.settings(&rel("app", "events"), "amt_net").target_type,
            Some("Int128".into())
        );
    }

    #[test]
    fn unparseable_pattern_rejected() {
        let mut b = ColumnRulesBuilder::new();
        b.add(
            &rel("app", "events"),
            MatchKind::Exact,
            "am(t",
            MatchKind::Regex,
            ty("String"),
        );
        let (rules, rejected) = b.finish();
        assert_eq!(rejected, 1);
        assert!(rules.is_empty());
    }

    #[test]
    fn accepted_type_survives_for_retention() {
        let mut b = ColumnRulesBuilder::new();
        b.record_accepted(&rel("app", "events"), "amount", "Int128");
        let (rules, _) = b.finish();
        assert_eq!(
            rules.accepted_type(&rel("app", "events"), "amount"),
            Some("Int128")
        );
        assert_eq!(rules.accepted_type(&rel("app", "events"), "other"), None);
    }
}
