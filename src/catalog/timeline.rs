//! Build descriptor history and ambiguity intervals for one relation at commit.
//!
//! Each filenode the relation uses is a generation. Keep older generations
//! in the log and retire each when its replacement starts. Only the filenode
//! still in use at commit can leave rows behind, so check it for gaps.
//!
//! A gap starts at the first catalog change or a missed read and ends at
//! the next known descriptor, including the one read at commit. Use that
//! descriptor from the start of the gap if it can decode rows written under
//! the previous descriptor ([`compatible_reader`]). Otherwise, mark the gap
//! as ambiguous and stop if decoding reaches it. For a new filenode, use
//! the first known descriptor from its start LSN. PostgreSQL reads every row
//! in a filenode using its current descriptor until a rewrite replaces it.

use std::sync::Arc;

use tokio_postgres::types::Oid;
use walrus::pg::walparser::RelFileNode;

use crate::catalog::compat::{Incompat, compatible_reader};
use crate::catalog::desc_log::{LogEntry, LogValue};
use crate::catalog::pending::PendingSlot;
use crate::schema::RelDescriptor;

/// Descriptors and missed reads for one relation that survives commit.
pub struct Shapes<'a> {
    /// Committed descriptor before this transaction, if the relation existed.
    pub pred: Option<&'a Arc<RelDescriptor>>,
    /// LSN of the relation's first catalog change in this transaction.
    pub first_touch: u64,
    /// Descriptors read at command boundaries, in LSN order.
    pub slots: &'a [PendingSlot],
    /// Missed reads for this relation, in LSN order.
    pub misses: &'a [u64],
    pub fin: &'a Arc<RelDescriptor>,
    pub next_lsn: u64,
}

/// LSN range `[from, through)` where decoding could be unsafe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fenced {
    pub from: u64,
    pub through: u64,
    pub why: &'static str,
}

#[derive(Debug, Default)]
pub struct Timeline {
    /// Sorted by `valid_from`.
    pub entries: Vec<LogEntry>,
    pub fenced: Vec<Fenced>,
    /// Descriptor changes that leave row decoding unchanged.
    pub benign: Vec<&'static str>,
    /// Number of entries from command boundary reads.
    pub promoted: usize,
}

struct Generation<'a> {
    rfn: RelFileNode,
    /// Start LSN, or `None` for the filenode in use before this transaction.
    start: Option<u64>,
    slots: Vec<&'a PendingSlot>,
}

/// Use `marker` to find the `XLOG_SMGR_CREATE` record that precedes rows
/// written to a new filenode.
pub fn build(s: &Shapes<'_>, marker: impl Fn(RelFileNode) -> Option<u64>) -> Timeline {
    let gens = generations(s, marker);
    let mut out = Timeline::default();
    let (last, older) = gens.split_last().expect("commit has a filenode");
    for (at, g) in older.iter().enumerate() {
        let end = gens[at + 1].start.expect("missing filenode start LSN");
        retire(s, g, end, &mut out);
    }
    survive(s, last, &mut out);
    debug_assert!(
        out.entries.is_sorted_by_key(|e| e.valid_from),
        "timeline out of order"
    );
    out
}

/// List the relation's filenodes in order of use. If a creation marker is
/// missing, use the first catalog read. For the final filenode, also consider
/// the first catalog change. Keep start LSNs in order.
fn generations<'a>(
    s: &Shapes<'a>,
    marker: impl Fn(RelFileNode) -> Option<u64>,
) -> Vec<Generation<'a>> {
    let mut gens: Vec<Generation<'a>> = Vec::new();
    if let Some(pred) = s.pred {
        gens.push(Generation {
            rfn: pred.rfn,
            start: None,
            slots: Vec::new(),
        });
    }
    for slot in s.slots {
        if gens.last().is_none_or(|g| g.rfn != slot.desc.rfn) {
            gens.push(Generation {
                rfn: slot.desc.rfn,
                start: Some(slot.boundary),
                slots: Vec::new(),
            });
        }
        gens.last_mut().expect("just pushed").slots.push(slot);
    }
    if gens.last().is_none_or(|g| g.rfn != s.fin.rfn) {
        gens.push(Generation {
            rfn: s.fin.rfn,
            start: Some(s.first_touch),
            slots: Vec::new(),
        });
    } else if let Some(g) = gens.last_mut().filter(|g| g.start.is_some()) {
        g.start = Some(g.start.expect("filtered").min(s.first_touch));
    }
    let mut floor = 0;
    for g in &mut gens {
        if let Some(start) = &mut g.start {
            *start = marker(g.rfn).unwrap_or(*start).max(floor);
            floor = *start;
        }
    }
    gens
}

/// Keep historical descriptors up to the LSN where the replacement starts.
fn retire(s: &Shapes<'_>, g: &Generation<'_>, end: u64, out: &mut Timeline) {
    let oid = s.fin.oid;
    let mut prior = s.pred.filter(|p| p.rfn == g.rfn);
    let mut present = prior.is_some();
    for slot in g.slots.iter().filter(|sl| sl.boundary < end) {
        if prior == Some(&slot.desc) {
            continue;
        }
        prior = Some(&slot.desc);
        present = true;
        out.entries.push(present_at(slot.boundary, oid, &slot.desc));
        out.promoted += 1;
    }
    if present {
        out.entries.push(LogEntry {
            valid_from: end,
            oid,
            rfn: g.rfn,
            value: LogValue::Retired,
        });
    }
}

/// Extend each descriptor back to the start of the preceding gap when safe.
fn survive(s: &Shapes<'_>, g: &Generation<'_>, out: &mut Timeline) {
    let oid = s.fin.oid;
    let mut prior = s.pred.filter(|p| p.rfn == g.rfn);
    let start = g.start.unwrap_or(s.first_touch);
    let mut open = Some(start);
    let mut misses = s.misses.iter().copied().filter(|m| *m >= start).peekable();
    let known = g
        .slots
        .iter()
        .map(|sl| (sl.boundary, &sl.desc, true))
        .chain(std::iter::once((s.next_lsn, s.fin, false)));
    for (pos, shape, from_slot) in known {
        while let Some(m) = misses.next_if(|m| *m < pos) {
            open = Some(open.map_or(m, |o| o.min(m)));
        }
        let run = open.take().filter(|u| *u < pos);
        if prior == Some(shape) {
            continue;
        }
        let verdict = run.zip(prior).map(|(_, p)| compatible_reader(p, shape));
        let valid_from = match (run, verdict) {
            (Some(from), Some(Err(Incompat::Physical(why)))) => {
                out.fenced.push(Fenced {
                    from,
                    through: pos,
                    why,
                });
                pos
            }
            (Some(from), Some(Err(Incompat::Benign(why)))) => {
                out.benign.push(why);
                from
            }
            (Some(from), _) => from,
            (None, _) => pos,
        };
        out.entries.push(present_at(valid_from, oid, shape));
        out.promoted += usize::from(from_slot);
        prior = Some(shape);
    }
}

fn present_at(valid_from: u64, oid: Oid, desc: &Arc<RelDescriptor>) -> LogEntry {
    LogEntry {
        valid_from,
        oid,
        rfn: desc.rfn,
        value: LogValue::Present(desc.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::{RelAttr, RelName, ReplIdent};

    const OID: Oid = 42;

    fn rfn(rel_node: u32) -> RelFileNode {
        RelFileNode {
            spc_node: 1663,
            db_node: 5,
            rel_node,
        }
    }

    fn attr(attnum: i16, type_oid: u32, type_len: i16, typmod: i32) -> RelAttr {
        RelAttr {
            attnum,
            name: format!("c{attnum}"),
            type_oid,
            typmod,
            not_null: false,
            dropped: false,
            type_name: "t".into(),
            type_byval: type_len > 0,
            type_len,
            type_align: b'i',
            type_storage: b'p',
            missing_default: None,
        }
    }

    /// Pass `(type_oid, type_len, typmod)` for each column in order.
    fn shape(rel_node: u32, cols: &[(u32, i16, i32)]) -> Arc<RelDescriptor> {
        Arc::new(RelDescriptor {
            rfn: rfn(rel_node),
            oid: OID,
            toast_oid: 0,
            namespace_oid: 2200,
            rel_name: RelName::new("public", "t"),
            kind: b'r',
            persistence: b'p',
            ancestors: Vec::new(),
            replident: ReplIdent::Default { pk_attnums: None },
            attributes: cols
                .iter()
                .zip(1..)
                .map(|(&(ty, len, typmod), n)| attr(n, ty, len, typmod))
                .collect(),
        })
    }

    const INT4: (u32, i16, i32) = (23, 4, -1);
    const INT8: (u32, i16, i32) = (20, 8, -1);
    const VARCHAR: (u32, i16, i32) = (1043, -1, 14);
    const WIDE_VARCHAR: (u32, i16, i32) = (1043, -1, 24);
    const TEXT: (u32, i16, i32) = (25, -1, -1);

    fn slot(boundary: u64, desc: &Arc<RelDescriptor>) -> PendingSlot {
        PendingSlot {
            boundary,
            writer_xid: 1,
            desc: desc.clone(),
        }
    }

    fn no_marker(_: RelFileNode) -> Option<u64> {
        None
    }

    /// List each entry's LSN, filenode, and column count (`None` if retired).
    fn summary(t: &Timeline) -> Vec<(u64, u32, Option<usize>)> {
        t.entries
            .iter()
            .map(|e| {
                let atts = e.value.present().map(|d| d.attributes.len());
                (e.valid_from, e.rfn.rel_node, atts)
            })
            .collect()
    }

    fn fence(from: u64, through: u64, why: &'static str) -> Fenced {
        Fenced { from, through, why }
    }

    #[test]
    fn unchanged_relation_lands_nothing() {
        let pred = shape(7000, &[INT4]);
        let fin = pred.clone();
        let t = build(
            &Shapes {
                pred: Some(&pred),
                first_touch: 100,
                slots: &[],
                misses: &[300],
                fin: &fin,
                next_lsn: 500,
            },
            no_marker,
        );
        assert!(t.entries.is_empty());
        assert!(t.fenced.is_empty());
    }

    #[test]
    fn readable_in_place_change_answers_from_first_touch() {
        let pred = shape(7000, &[INT4]);
        let fin = shape(7000, &[INT4, INT4]);
        let t = build(
            &Shapes {
                pred: Some(&pred),
                first_touch: 100,
                slots: &[],
                misses: &[],
                fin: &fin,
                next_lsn: 500,
            },
            no_marker,
        );
        assert_eq!(summary(&t), [(100, 7000, Some(2))]);
        assert!(t.fenced.is_empty());
        assert_eq!(t.promoted, 0);
    }

    #[test]
    fn benign_in_place_drift_stays_unfenced() {
        let pred = shape(7000, &[VARCHAR]);
        let fin = shape(7000, &[WIDE_VARCHAR]);
        let t = build(
            &Shapes {
                pred: Some(&pred),
                first_touch: 100,
                slots: &[],
                misses: &[],
                fin: &fin,
                next_lsn: 500,
            },
            no_marker,
        );
        assert_eq!(summary(&t), [(100, 7000, Some(1))]);
        assert_eq!(t.benign, ["typmod change"]);
        assert!(t.fenced.is_empty());
    }

    #[test]
    fn unreadable_in_place_change_fences_to_commit() {
        let pred = shape(7000, &[INT4]);
        let fin = shape(7000, &[INT8]);
        let t = build(
            &Shapes {
                pred: Some(&pred),
                first_touch: 100,
                slots: &[],
                misses: &[],
                fin: &fin,
                next_lsn: 500,
            },
            no_marker,
        );
        assert_eq!(summary(&t), [(500, 7000, Some(1))]);
        assert_eq!(t.fenced, [fence(100, 500, "physical walk fields change")]);
    }

    /// Use the first catalog read to check the gap before it. An incompatible
    /// change later leaves that earlier interval readable.
    #[test]
    fn head_run_checks_against_the_first_read_shape() {
        let pred = shape(7000, &[VARCHAR]);
        let added = shape(7000, &[VARCHAR, INT4]);
        let fin = shape(7000, &[TEXT, INT4]);
        let slots = [slot(200, &added), slot(300, &fin)];
        let t = build(
            &Shapes {
                pred: Some(&pred),
                first_touch: 100,
                slots: &slots,
                misses: &[],
                fin: &fin,
                next_lsn: 500,
            },
            no_marker,
        );
        assert_eq!(summary(&t), [(100, 7000, Some(2)), (300, 7000, Some(2))]);
        assert!(t.fenced.is_empty());
        assert_eq!(t.promoted, 2, "commit repeats the last catalog read");
    }

    #[test]
    fn unreadable_head_run_fences_up_to_the_first_slot() {
        let pred = shape(7000, &[VARCHAR]);
        let fin = shape(7000, &[TEXT]);
        let slots = [slot(200, &fin)];
        let t = build(
            &Shapes {
                pred: Some(&pred),
                first_touch: 100,
                slots: &slots,
                misses: &[],
                fin: &fin,
                next_lsn: 500,
            },
            no_marker,
        );
        assert_eq!(summary(&t), [(200, 7000, Some(1))]);
        assert_eq!(t.fenced, [fence(100, 200, "type change")]);
    }

    /// With no missed reads, the commit descriptor takes effect at commit.
    #[test]
    fn commit_shape_never_sorts_before_a_slot() {
        let pred = shape(7000, &[INT4]);
        let added = shape(7000, &[INT4, INT4]);
        let fin = shape(7000, &[INT4, INT4, INT4]);
        let slots = [slot(200, &added)];
        let t = build(
            &Shapes {
                pred: Some(&pred),
                first_touch: 100,
                slots: &slots,
                misses: &[],
                fin: &fin,
                next_lsn: 500,
            },
            no_marker,
        );
        assert_eq!(summary(&t), [(100, 7000, Some(2)), (500, 7000, Some(3))]);
    }

    /// Apply the commit descriptor from the missed read if it can decode
    /// rows written under the last known descriptor.
    #[test]
    fn readable_tail_after_a_miss_answers_from_the_miss() {
        let pred = shape(7000, &[INT4]);
        let added = shape(7000, &[INT4, INT4]);
        let fin = shape(7000, &[INT4, INT4, INT4]);
        let slots = [slot(200, &added)];
        let t = build(
            &Shapes {
                pred: Some(&pred),
                first_touch: 100,
                slots: &slots,
                misses: &[300, 400],
                fin: &fin,
                next_lsn: 500,
            },
            no_marker,
        );
        assert_eq!(summary(&t), [(100, 7000, Some(2)), (300, 7000, Some(3))]);
        assert!(t.fenced.is_empty());
    }

    #[test]
    fn unreadable_tail_after_a_miss_fences_from_the_miss() {
        let pred = shape(7000, &[INT4]);
        let added = shape(7000, &[INT4, INT4]);
        let fin = shape(7000, &[INT4, INT8]);
        let slots = [slot(200, &added)];
        let t = build(
            &Shapes {
                pred: Some(&pred),
                first_touch: 100,
                slots: &slots,
                misses: &[300],
                fin: &fin,
                next_lsn: 500,
            },
            no_marker,
        );
        assert_eq!(summary(&t), [(100, 7000, Some(2)), (500, 7000, Some(2))]);
        assert_eq!(t.fenced, [fence(300, 500, "physical walk fields change")]);
    }

    /// End the gap at the next successful read.
    #[test]
    fn mid_run_miss_is_answered_by_the_next_slot() {
        let pred = shape(7000, &[INT4]);
        let a = shape(7000, &[INT4, INT4]);
        let b = shape(7000, &[INT4, INT8]);
        let slots = [slot(200, &a), slot(400, &b)];
        let t = build(
            &Shapes {
                pred: Some(&pred),
                first_touch: 100,
                slots: &slots,
                misses: &[300],
                fin: &b,
                next_lsn: 500,
            },
            no_marker,
        );
        assert_eq!(summary(&t), [(100, 7000, Some(2)), (400, 7000, Some(2))]);
        assert_eq!(t.fenced, [fence(300, 400, "physical walk fields change")]);
    }

    /// After a rewrite and a successful read, check later misses on the new
    /// filenode for compatibility.
    #[test]
    fn rotated_generation_gets_a_tail_check() {
        let pred = shape(7000, &[INT4]);
        let rewritten = shape(7001, &[INT4, INT4]);
        let fin = shape(7001, &[INT4, INT8]);
        let slots = [slot(250, &rewritten)];
        let t = build(
            &Shapes {
                pred: Some(&pred),
                first_touch: 100,
                slots: &slots,
                misses: &[300],
                fin: &fin,
                next_lsn: 500,
            },
            |r| (r.rel_node == 7001).then_some(150),
        );
        assert_eq!(
            summary(&t),
            [
                (150, 7000, None),
                (150, 7001, Some(2)),
                (500, 7001, Some(2))
            ]
        );
        assert_eq!(t.fenced, [fence(300, 500, "physical walk fields change")]);
    }

    /// With no earlier reads on a new filenode, use the commit descriptor
    /// from the creation marker.
    #[test]
    fn fresh_generation_without_a_read_trusts_the_commit_shape() {
        let fin = shape(7001, &[INT8]);
        let t = build(
            &Shapes {
                pred: None,
                first_touch: 100,
                slots: &[],
                misses: &[300],
                fin: &fin,
                next_lsn: 500,
            },
            |_| Some(120),
        );
        assert_eq!(summary(&t), [(120, 7001, Some(1))]);
        assert!(t.fenced.is_empty());
    }

    /// With no reads after a rewrite, use the commit descriptor from the
    /// creation marker. A miss before the rewrite belongs to the old filenode.
    #[test]
    fn rotated_generation_without_a_read_trusts_the_commit_shape() {
        let pred = shape(7000, &[INT4]);
        let fin = shape(7001, &[INT8]);
        let t = build(
            &Shapes {
                pred: Some(&pred),
                first_touch: 100,
                slots: &[],
                misses: &[120, 300],
                fin: &fin,
                next_lsn: 500,
            },
            |r| (r.rel_node == 7001).then_some(150),
        );
        assert_eq!(summary(&t), [(150, 7000, None), (150, 7001, Some(1))]);
        assert!(t.fenced.is_empty());
        assert_eq!(t.promoted, 0);
    }

    #[test]
    fn fresh_generation_without_a_marker_starts_at_first_touch() {
        let first = shape(7001, &[INT4]);
        let fin = shape(7001, &[INT4, INT4]);
        let slots = [slot(200, &first)];
        let t = build(
            &Shapes {
                pred: None,
                first_touch: 100,
                slots: &slots,
                misses: &[],
                fin: &fin,
                next_lsn: 500,
            },
            no_marker,
        );
        assert_eq!(summary(&t), [(100, 7001, Some(1)), (500, 7001, Some(2))]);
    }

    /// After two rewrites, keep the middle filenode's history up to the
    /// creation of the final filenode.
    #[test]
    fn middle_generation_retires_where_the_next_starts() {
        let pred = shape(7000, &[INT4]);
        let pred_alter = shape(7000, &[INT4, INT4]);
        let middle = shape(7001, &[INT4, INT4]);
        let fin = shape(7002, &[INT4, INT4]);
        let slots = [
            slot(150, &pred),
            slot(160, &pred_alter),
            slot(250, &middle),
            // Equal descriptors can occupy different allocations.
            slot(330, &Arc::new((*middle).clone())),
            slot(350, &middle),
            slot(400, &fin),
        ];
        let t = build(
            &Shapes {
                pred: Some(&pred),
                first_touch: 100,
                slots: &slots,
                misses: &[],
                fin: &fin,
                next_lsn: 500,
            },
            |r| Some(if r.rel_node == 7001 { 200 } else { 340 }),
        );
        assert_eq!(
            summary(&t),
            [
                (160, 7000, Some(2)),
                (200, 7000, None),
                (250, 7001, Some(2)),
                (340, 7001, None),
                (340, 7002, Some(2)),
            ]
        );
        assert_eq!(t.promoted, 3);
    }

    /// Without markers, keep filenode start LSNs in order.
    #[test]
    fn markerless_generations_start_in_order() {
        let pred = shape(7000, &[INT4]);
        let middle = shape(7001, &[INT4]);
        let fin = shape(7002, &[INT4]);
        let slots = [slot(250, &middle)];
        let t = build(
            &Shapes {
                pred: Some(&pred),
                first_touch: 100,
                slots: &slots,
                misses: &[],
                fin: &fin,
                next_lsn: 500,
            },
            no_marker,
        );
        assert_eq!(
            summary(&t),
            [(250, 7000, None), (250, 7002, Some(1))],
            "the replacement starts at the middle filenode's only read",
        );
    }
}
