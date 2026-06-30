//! Boot-time resume decision. Replaces the old `should_run_bootstrap`
//! boolean with a three-way plan derived from the durable cursor, whether
//! the shadow data dir is initialized, and **live source state** (write
//! head + physical-slot `restart_lsn`).
//!
//! The three outcomes:
//!
//! * [`ResumePlan::Fresh`] — no usable prior state, seed from a base
//!   backup / object store / copy.
//! * [`ResumePlan::Resume`] — the source still holds `[resume_lsn, head]`
//!   (a physical slot pins `restart_lsn <= resume_lsn`); plain
//!   `START_REPLICATION` at `resume_lsn`.
//! * [`ResumePlan::Refill`] — the source recycled `[resume_lsn, head]`;
//!   replay it from the `[backup]` archive through the pump, then rejoin
//!   live.
//!
//! A recycled resume point with no archive to refill from falls back to
//! `Fresh`. There is no automatic partial re-seed — an operator resets
//! explicitly with `--ignore-cursor`.
//!
//! `bootstrap_off` (shadow externally managed, e.g. `pg_basebackup`) can
//! never reach `Fresh` — it runs walshadow's own bootstrap — so it degrades
//! to `Resume`/`Refill` only.

use std::num::NonZeroU64;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResumePlan {
    Fresh,
    Resume { start_lsn: u64 },
    Refill { from: u64, to: u64 },
}

/// Pure inputs to [`resolve`]; keeps the decision testable in isolation
/// from the daemon's IO.
#[derive(Debug, Clone, Copy)]
pub struct Inputs {
    /// `--bootstrap-mode off`: walshadow must not run its own bootstrap.
    pub bootstrap_off: bool,
    /// `--ignore-cursor`: force a greenfield reseed.
    pub ignore_cursor: bool,
    /// Durable CH resume point (`cursor.emitter_ack_lsn`); `None` = no
    /// cursor (LSN `0/0` is invalid, so it collapses to absent).
    pub resume_lsn: Option<NonZeroU64>,
    /// Shadow data dir holds `PG_VERSION` (bootstrapped or external).
    pub shadow_initialized: bool,
    /// Source write head (`IDENTIFY_SYSTEM.xlogpos`).
    pub head: u64,
    /// Physical slot `restart_lsn`; `None` = no slot / slot absent.
    pub slot_restart_lsn: Option<u64>,
    /// A `[backup]` archive is configured; required for Refill.
    pub archive_configured: bool,
}

/// Decide the boot path. See module docs for the full matrix.
pub fn resolve(inp: Inputs) -> ResumePlan {
    let resume_lsn = inp.resume_lsn.map_or(0, NonZeroU64::get);
    let have_state = inp.resume_lsn.is_some() && inp.shadow_initialized;

    // No usable prior state (or an operator-forced reseed): start over.
    if inp.ignore_cursor || !have_state {
        // Off can't bootstrap; resume from whatever cursor/greenfield
        // resolves to (resume_lsn may be 0 → greenfield head downstream).
        return if inp.bootstrap_off {
            ResumePlan::Resume {
                start_lsn: resume_lsn,
            }
        } else {
            ResumePlan::Fresh
        };
    }

    // Slot still pins the WAL we need: plain live resume.
    if inp
        .slot_restart_lsn
        .is_some_and(|restart| restart <= resume_lsn)
    {
        return ResumePlan::Resume {
            start_lsn: resume_lsn,
        };
    }

    // Source recycled it. Refill from the archive whenever one is configured;
    // the fetch surfaces a hard error if the archive lacks the range.
    if inp.archive_configured {
        return ResumePlan::Refill {
            from: resume_lsn,
            to: inp.head,
        };
    }

    // No archive to refill from — full greenfield reseed. Off can't bootstrap,
    // so it best-effort resumes and lets the source surface "segment removed"
    // if the WAL is truly gone.
    if inp.bootstrap_off {
        ResumePlan::Resume {
            start_lsn: resume_lsn,
        }
    } else {
        ResumePlan::Fresh
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn base() -> Inputs {
        Inputs {
            bootstrap_off: false,
            ignore_cursor: false,
            resume_lsn: NonZeroU64::new(0x2000),
            shadow_initialized: true,
            head: 0x3000,
            slot_restart_lsn: Some(0x1000),
            archive_configured: true,
        }
    }

    #[test]
    fn slot_covers_gap_resumes() {
        // restart_lsn <= resume_lsn → source retains [resume_lsn, head].
        assert_eq!(resolve(base()), ResumePlan::Resume { start_lsn: 0x2000 });
    }

    #[test]
    fn no_cursor_bootstraps_fresh() {
        let inp = Inputs {
            resume_lsn: None,
            ..base()
        };
        assert_eq!(resolve(inp), ResumePlan::Fresh);
    }

    #[test]
    fn uninitialized_shadow_bootstraps_fresh() {
        let inp = Inputs {
            shadow_initialized: false,
            ..base()
        };
        assert_eq!(resolve(inp), ResumePlan::Fresh);
    }

    #[test]
    fn ignore_cursor_forces_fresh() {
        let inp = Inputs {
            ignore_cursor: true,
            ..base()
        };
        assert_eq!(resolve(inp), ResumePlan::Fresh);
    }

    #[test]
    fn recycled_refills_from_archive() {
        // No slot (or slot advanced past resume_lsn) → source recycled it;
        // an archive is configured → refill.
        let inp = Inputs {
            slot_restart_lsn: None,
            ..base()
        };
        assert_eq!(
            resolve(inp),
            ResumePlan::Refill {
                from: 0x2000,
                to: 0x3000
            }
        );
    }

    #[test]
    fn slot_advanced_past_resume_refills() {
        // restart_lsn > resume_lsn: the slot no longer pins what we need.
        let inp = Inputs {
            slot_restart_lsn: Some(0x2500),
            ..base()
        };
        assert!(matches!(resolve(inp), ResumePlan::Refill { .. }));
    }

    #[test]
    fn no_archive_config_reseeds_fresh() {
        // Recycled + no [backup] configured → Refill unavailable → Fresh.
        let inp = Inputs {
            slot_restart_lsn: None,
            archive_configured: false,
            ..base()
        };
        assert_eq!(resolve(inp), ResumePlan::Fresh);
    }

    #[test]
    fn off_no_archive_config_resumes() {
        let inp = Inputs {
            bootstrap_off: true,
            slot_restart_lsn: None,
            archive_configured: false,
            ..base()
        };
        assert_eq!(resolve(inp), ResumePlan::Resume { start_lsn: 0x2000 });
    }

    #[test]
    fn off_never_bootstraps() {
        // Off + no state → Resume greenfield, not Fresh.
        let inp = Inputs {
            bootstrap_off: true,
            resume_lsn: None,
            ..base()
        };
        assert_eq!(resolve(inp), ResumePlan::Resume { start_lsn: 0 });
        // Off + recycled + no archive → best-effort Resume, not Fresh.
        let inp = Inputs {
            bootstrap_off: true,
            slot_restart_lsn: None,
            archive_configured: false,
            ..base()
        };
        assert_eq!(resolve(inp), ResumePlan::Resume { start_lsn: 0x2000 });
    }

    #[test]
    fn off_still_refills_when_archive_has_it() {
        let inp = Inputs {
            bootstrap_off: true,
            slot_restart_lsn: None,
            ..base()
        };
        assert!(matches!(resolve(inp), ResumePlan::Refill { .. }));
    }
}
