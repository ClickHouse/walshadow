//! PostgreSQL timeline history, parsed from the bytes `TIMELINE_HISTORY`
//! returns.
//!
//! A history file is one line per ancestor, `<tli>\t<hi>/<lo>\t<reason>`, with
//! `#` comments and strictly increasing timeline IDs. Each line's switchpoint
//! is where that timeline ended, which is also the next line's start, and the
//! target timeline has no line of its own — it gets an open-ended entry
//! (`src/backend/access/transam/timeline.c`, `readTimeLineHistory`). Timeline 1
//! has no history file.
//!
//! Selection is per LSN, never once per run: a resume point decides which
//! branch serves it, mirroring `tliOfPointInHistory`.

use std::fmt;

use thiserror::Error;
use walrus::pg::backup::parse_pg_lsn;

/// `TLHistoryFileName`: zero-padded hexadecimal timeline plus suffix.
pub fn history_filename(tli: u32) -> String {
    format!("{tli:08X}.history")
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum HistoryError {
    #[error("history line {line}: {reason}")]
    Syntax { line: usize, reason: String },
    #[error("history line {line}: timeline {tli} not above {previous}")]
    NonIncreasing {
        line: usize,
        tli: u32,
        previous: u32,
    },
    #[error("history for timeline {target} lists {highest}, which is not below it")]
    TargetNotAbove { target: u32, highest: u32 },
}

/// One branch of the chain. `end` is the switchpoint where it stopped; `None`
/// for the target timeline, which is still open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HistoryEntry {
    pub tli: u32,
    pub begin: u64,
    pub end: Option<u64>,
}

impl HistoryEntry {
    fn covers(&self, lsn: u64) -> bool {
        self.begin <= lsn && self.end.is_none_or(|end| lsn < end)
    }
}

/// Ancestor chain of one timeline, oldest first, with the raw bytes kept for
/// persistence — a synthesized history would lose the `reason` column PG
/// writes and every byte a shadow's `restore_command` has to serve back.
#[derive(Clone, PartialEq, Eq)]
pub struct TimelineHistory {
    target: u32,
    entries: Vec<HistoryEntry>,
    raw: Vec<u8>,
}

impl fmt::Debug for TimelineHistory {
    /// Raw bytes are noise in a log line; the parsed chain is the fact.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TimelineHistory")
            .field("target", &self.target)
            .field("entries", &self.entries)
            .finish()
    }
}

impl TimelineHistory {
    /// Timeline 1, or any timeline whose history file the source lacks: one
    /// open entry from LSN 0.
    pub fn root(target: u32) -> Self {
        Self {
            target,
            entries: vec![HistoryEntry {
                tli: target,
                begin: 0,
                end: None,
            }],
            raw: Vec::new(),
        }
    }

    pub fn parse(target: u32, raw: &[u8]) -> Result<Self, HistoryError> {
        let text = String::from_utf8_lossy(raw);
        let mut entries: Vec<HistoryEntry> = Vec::new();
        let mut prev_end = 0u64;
        for (idx, line) in text.lines().enumerate() {
            let line_no = idx + 1;
            let trimmed = line.trim_start();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            let mut fields = trimmed.split_whitespace();
            let tli_field = fields.next().unwrap_or_default();
            let tli: u32 = tli_field.parse().map_err(|_| HistoryError::Syntax {
                line: line_no,
                reason: format!("expected a numeric timeline ID, got {tli_field:?}"),
            })?;
            let switchpoint = fields.next().ok_or_else(|| HistoryError::Syntax {
                line: line_no,
                reason: "expected a write-ahead log switchpoint location".into(),
            })?;
            let end = parse_pg_lsn(switchpoint).map_err(|e| HistoryError::Syntax {
                line: line_no,
                reason: format!("switchpoint {switchpoint:?}: {e}"),
            })?;
            if let Some(previous) = entries.last()
                && tli <= previous.tli
            {
                return Err(HistoryError::NonIncreasing {
                    line: line_no,
                    tli,
                    previous: previous.tli,
                });
            }
            entries.push(HistoryEntry {
                tli,
                begin: prev_end,
                end: Some(end),
            });
            prev_end = end;
        }
        if let Some(highest) = entries.last()
            && target <= highest.tli
        {
            return Err(HistoryError::TargetNotAbove {
                target,
                highest: highest.tli,
            });
        }
        entries.push(HistoryEntry {
            tli: target,
            begin: prev_end,
            end: None,
        });
        Ok(Self {
            target,
            entries,
            raw: raw.to_vec(),
        })
    }

    pub fn target(&self) -> u32 {
        self.target
    }

    /// Exactly what the source served, for `<tli>.history` beside the filtered
    /// archive.
    pub fn raw(&self) -> &[u8] {
        &self.raw
    }

    pub fn entries(&self) -> &[HistoryEntry] {
        &self.entries
    }

    /// Branch serving `lsn`, mirroring `tliOfPointInHistory`. A switchpoint
    /// belongs to the descendant, so a resume exactly at a fork asks the
    /// timeline that continues past it rather than the one that ended there.
    pub fn tli_of_point(&self, lsn: u64) -> Option<u32> {
        self.entries.iter().find(|e| e.covers(lsn)).map(|e| e.tli)
    }

    /// Branch whose *file* serves `lsn`, mirroring `XLogFileReadAnyTLI`: a
    /// segment belongs to the newest branch that began at or before the
    /// segment's own start, because a fork copies the ancestor prefix into a
    /// descendant-named file (`XLogInitNewTimeline`).
    ///
    /// Coarser than [`tli_of_point`](Self::tli_of_point), and the resolution a
    /// resume position needs: a crossing commits the fork segment's start as
    /// the floor, which sits below the fork itself yet on the descendant
    /// (plans/failover.md §Crossing order).
    pub fn tli_of_segment(&self, lsn: u64, seg_size: u64) -> Option<u32> {
        let seg = lsn / seg_size;
        self.entries
            .iter()
            .rev()
            .find(|e| e.begin / seg_size <= seg)
            .map(|e| e.tli)
    }

    /// Where `tli` started, which is its ancestor's switchpoint. `0` for the
    /// oldest branch in the chain.
    pub fn begin_of(&self, tli: u32) -> Option<u64> {
        self.entries.iter().find(|e| e.tli == tli).map(|e| e.begin)
    }

    pub fn contains(&self, tli: u32) -> bool {
        self.entries.iter().any(|e| e.tli == tli)
    }

    /// Where `tli` ended. `None` for the target (still open) or an absent
    /// timeline.
    pub fn switchpoint_of(&self, tli: u32) -> Option<u64> {
        self.entries.iter().find(|e| e.tli == tli)?.end
    }

    /// Timeline that took over from `tli`.
    pub fn successor_of(&self, tli: u32) -> Option<u32> {
        let at = self.entries.iter().position(|e| e.tli == tli)?;
        self.entries.get(at + 1).map(|e| e.tli)
    }

    /// `tli` owns `lsn`: both that the branch is an ancestor and that the
    /// position is on it rather than past its fork.
    pub fn proves_ancestor(&self, tli: u32, lsn: u64) -> bool {
        self.tli_of_point(lsn) == Some(tli)
    }

    /// Select restart branch, keeping stored ancestor until crossing commits
    pub fn resume_branch(&self, stored_timeline: u32, aligned: u64, seg_size: u64) -> Option<u32> {
        let serves = self.tli_of_segment(aligned, seg_size)?;
        if self.proves_ancestor(stored_timeline, aligned) {
            return Some(stored_timeline);
        }
        (self.contains(stored_timeline) && stored_timeline <= serves).then_some(serves)
    }

    /// Select floor branch, clamped to current stream branch
    pub fn floor_branch(
        &self,
        floor: u64,
        live_timeline: u32,
        stream_timeline: u32,
        seg_size: u64,
    ) -> u32 {
        self.tli_of_segment(floor, seg_size)
            .unwrap_or(live_timeline)
            .min(stream_timeline)
    }

    /// Select oldest branch shadow may still replay at boot
    pub fn shadow_boot_branch(
        &self,
        stored_timeline: u32,
        aligned: u64,
        start_timeline: u32,
    ) -> u32 {
        let behind = stored_timeline.min(self.tli_of_point(aligned).unwrap_or(start_timeline));
        if behind < start_timeline && self.contains(behind) {
            behind
        } else {
            start_timeline
        }
    }

    /// Check whether historic stream reached its switchpoint
    pub fn branch_exhausted(&self, tli: u32, next_lsn: u64) -> bool {
        self.switchpoint_of(tli) == Some(next_lsn)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Two forks, a comment, and the reason column PG writes.
    const SAMPLE: &str = "# comment line\n\
                          1\t0/3000000\tno recovery target specified\n\
                          \n\
                          2\t0/5000000\tno recovery target specified\n";

    #[test]
    fn parses_chain_with_comments_and_blank_lines() {
        let h = TimelineHistory::parse(4, SAMPLE.as_bytes()).unwrap();
        assert_eq!(
            h.entries(),
            [
                HistoryEntry {
                    tli: 1,
                    begin: 0,
                    end: Some(0x300_0000)
                },
                HistoryEntry {
                    tli: 2,
                    begin: 0x300_0000,
                    end: Some(0x500_0000)
                },
                HistoryEntry {
                    tli: 4,
                    begin: 0x500_0000,
                    end: None
                },
            ],
        );
        assert_eq!(h.raw(), SAMPLE.as_bytes(), "bytes kept verbatim");
    }

    #[test]
    fn switchpoint_belongs_to_the_descendant() {
        let h = TimelineHistory::parse(4, SAMPLE.as_bytes()).unwrap();
        assert_eq!(h.tli_of_point(0x2FF_FFFF), Some(1));
        assert_eq!(
            h.tli_of_point(0x300_0000),
            Some(2),
            "fork LSN is the child's"
        );
        assert_eq!(h.tli_of_point(0x4FF_FFFF), Some(2));
        assert_eq!(h.tli_of_point(0x500_0000), Some(4));
        assert_eq!(h.tli_of_point(u64::MAX), Some(4), "target end is open");
    }

    /// Fork segments are shared: the descendant's file holds the ancestor
    /// prefix, so the whole segment resolves to the descendant even below the
    /// switchpoint. This is what lets a crossing commit the fork segment's
    /// start as a descendant-branch resume position
    #[test]
    fn segment_resolution_gives_the_fork_segment_to_the_descendant() {
        const SEG: u64 = 0x100_0000;
        // Forks mid-segment, at offset 0x800000 of segment 1
        let h = TimelineHistory::parse(2, b"1\t0/1800000\tno recovery target\n").unwrap();
        assert_eq!(h.tli_of_segment(0, SEG), Some(1), "segment below the fork");
        assert_eq!(h.tli_of_segment(0xFF_FFFF, SEG), Some(1));
        assert_eq!(
            h.tli_of_segment(0x100_0000, SEG),
            Some(2),
            "fork segment's start is the descendant's, prefix and all",
        );
        assert_eq!(
            h.tli_of_point(0x100_0000),
            Some(1),
            "per-LSN resolution still names the branch that wrote it",
        );
        assert_eq!(h.tli_of_segment(0x200_0000, SEG), Some(2));
    }

    #[test]
    fn begin_of_reads_the_ancestors_switchpoint() {
        let h = TimelineHistory::parse(4, SAMPLE.as_bytes()).unwrap();
        assert_eq!(h.begin_of(1), Some(0));
        assert_eq!(h.begin_of(2), Some(0x300_0000));
        assert_eq!(h.begin_of(4), Some(0x500_0000));
        assert_eq!(h.begin_of(3), None);
    }

    #[test]
    fn ancestry_proof_rejects_a_position_past_the_fork() {
        let h = TimelineHistory::parse(4, SAMPLE.as_bytes()).unwrap();
        assert!(h.proves_ancestor(1, 0x2FF_FFFF));
        assert!(
            !h.proves_ancestor(1, 0x300_0000),
            "at its own fork timeline 1 no longer serves"
        );
        assert!(!h.proves_ancestor(3, 0x100));
    }

    #[test]
    fn successor_and_switchpoint_walk_the_chain() {
        let h = TimelineHistory::parse(4, SAMPLE.as_bytes()).unwrap();
        assert_eq!(h.successor_of(1), Some(2));
        assert_eq!(h.successor_of(2), Some(4), "non-consecutive IDs are fine");
        assert_eq!(h.successor_of(4), None);
        assert_eq!(h.switchpoint_of(2), Some(0x500_0000));
        assert_eq!(h.switchpoint_of(4), None);
        assert_eq!(h.switchpoint_of(9), None);
    }

    #[test]
    fn root_history_has_one_open_entry() {
        let h = TimelineHistory::root(1);
        assert!(h.proves_ancestor(1, 0));
        assert_eq!(h.tli_of_point(u64::MAX), Some(1));
        assert!(h.raw().is_empty());
    }

    #[test]
    fn rejects_malformed_and_out_of_order_history() {
        let missing_lsn = TimelineHistory::parse(2, b"1\n");
        assert!(matches!(
            missing_lsn,
            Err(HistoryError::Syntax { line: 1, .. })
        ));
        let bad_lsn = TimelineHistory::parse(2, b"1\tnotanlsn\treason\n");
        assert!(matches!(bad_lsn, Err(HistoryError::Syntax { line: 1, .. })));
        let non_numeric = TimelineHistory::parse(2, b"one\t0/1000\n");
        assert!(matches!(
            non_numeric,
            Err(HistoryError::Syntax { line: 1, .. })
        ));
        let backwards = TimelineHistory::parse(9, b"3\t0/1000\n2\t0/2000\n");
        assert_eq!(
            backwards,
            Err(HistoryError::NonIncreasing {
                line: 2,
                tli: 2,
                previous: 3
            }),
        );
        let sibling = TimelineHistory::parse(2, b"1\t0/1000\n2\t0/2000\n");
        assert_eq!(
            sibling,
            Err(HistoryError::TargetNotAbove {
                target: 2,
                highest: 2
            }),
        );
    }

    #[test]
    fn history_filename_is_zero_padded_hex() {
        assert_eq!(history_filename(2), "00000002.history");
        assert_eq!(history_filename(0x1F), "0000001F.history");
    }

    #[test]
    fn resume_branch_keeps_the_stored_branch_until_the_crossing_commits() {
        const SEG: u64 = 0x100_0000;
        let h = TimelineHistory::parse(2, b"1\t0/3001000\tno recovery target specified\n").unwrap();
        let fork_segment = 3 * SEG;
        assert_eq!(
            h.tli_of_segment(fork_segment, SEG),
            Some(2),
            "the descendant's file holds the ancestor prefix",
        );
        assert_eq!(h.resume_branch(1, fork_segment, SEG), Some(1));
        assert_eq!(
            h.resume_branch(2, fork_segment, SEG),
            Some(2),
            "a committed fork resume position adopts the descendant",
        );
        assert_eq!(h.resume_branch(1, 2 * SEG, SEG), Some(1));
        assert_eq!(h.resume_branch(7, fork_segment, SEG), None);
    }

    #[test]
    fn shadow_boot_branch_names_the_branch_the_shadow_can_still_be_on() {
        const SEG: u64 = 0x100_0000;
        let h = TimelineHistory::parse(2, b"1\t0/3001000\tno recovery target specified\n").unwrap();
        let fork_segment = 3 * SEG;
        assert_eq!(h.shadow_boot_branch(2, fork_segment, 2), 1);
        assert_eq!(
            h.shadow_boot_branch(1, 2 * SEG, 1),
            1,
            "no fork behind the floor leaves nothing to re-advertise",
        );
        let boundary =
            TimelineHistory::parse(2, b"1\t0/3000000\tno recovery target specified\n").unwrap();
        assert_eq!(boundary.resume_branch(1, fork_segment, SEG), Some(2));
        assert_eq!(boundary.shadow_boot_branch(1, fork_segment, 2), 1);
    }

    #[test]
    fn branch_exhausted_only_at_the_branchs_own_switchpoint() {
        let h = TimelineHistory::parse(2, b"1\t0/3000000\tno recovery target\n").unwrap();
        assert!(!h.branch_exhausted(1, 0x200_0000));
        assert!(h.branch_exhausted(1, 0x300_0000));
        assert!(
            !h.branch_exhausted(2, 0x300_0000),
            "the live branch has no switchpoint to sit on",
        );
    }
}
