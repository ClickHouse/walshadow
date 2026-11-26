//! Terminal stop for deterministic fail-closed errors.
//!
//! Some drain and plan verdicts depend only on the WAL: a stash record
//! inside an ambiguity interval, an unmodelled WAL operation, a partial
//! update. A restart resumes from the same floor, re-reads the same
//! records and fails the same way. Left to a process supervisor that loops
//! forever and pages as whatever symptom the restart churn shows, rather
//! than as the verdict.
//!
//! The pipeline trips a [`FailClosedLatch`] at the failure site with the
//! verdict's reason label and LSN. On session exit the daemon persists that
//! verdict as [`MARKER_FILE`] in the spill directory and exits with
//! [`EXIT_FAIL_CLOSED`], which the shipped systemd drop-in lists in
//! `RestartPreventExitStatus=`. A later start finds the marker and refuses
//! before reading WAL. `walshadow-stream fail-closed clear --spill-dir DIR`
//! removes it once the operator has rebuilt or otherwise resolved the
//! cause.

use std::io;
use std::path::Path;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

/// Process exit status for a deterministic fail-closed stop. `EX_DATAERR`
/// from sysexits(3): the input, here source WAL, is what fails
pub const EXIT_FAIL_CLOSED: i32 = 65;

/// Marker file name inside the spill directory
pub const MARKER_FILE: &str = "fail_closed.json";

/// One deterministic verdict, as persisted in [`MARKER_FILE`]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FailClosed {
    /// `xact_plan_failures_total` reason label, e.g. `stash_ambiguous`
    pub reason: String,
    /// Commit LSN of the transaction whose drain or plan failed
    pub lsn: u64,
    /// Full error text
    pub error: String,
    /// Unix seconds the verdict was reached
    pub at_unix: u64,
}

impl FailClosed {
    pub fn new(reason: &str, lsn: u64, error: impl Into<String>) -> Self {
        let at_unix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
        Self {
            reason: reason.into(),
            lsn,
            error: error.into(),
            at_unix,
        }
    }

    /// `pg_lsn` text form, `X/Y`
    pub fn lsn_text(&self) -> String {
        format!("{:X}/{:X}", self.lsn >> 32, self.lsn & 0xFFFF_FFFF)
    }
}

impl std::fmt::Display for FailClosed {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "fail-closed {} at {}: {}",
            self.reason,
            self.lsn_text(),
            self.error
        )
    }
}

/// First deterministic verdict a session reached. Shared between the
/// pipeline, which trips it, and the daemon's exit path, which reads it;
/// later trips keep the first so the root cause survives follow-on errors
#[derive(Debug, Clone, Default)]
pub struct FailClosedLatch(Arc<Mutex<Option<FailClosed>>>);

impl FailClosedLatch {
    /// Record a verdict unless one is already held. Returns whether this
    /// call set it
    pub fn trip(&self, verdict: FailClosed) -> bool {
        let mut slot = self.0.lock().expect("fail-closed latch poisoned");
        if slot.is_some() {
            return false;
        }
        tracing::error!(
            target: "walshadow",
            reason = %verdict.reason,
            lsn = %verdict.lsn_text(),
            error = %verdict.error,
            "deterministic fail-closed verdict; stopping without restart",
        );
        *slot = Some(verdict);
        true
    }

    /// Trip with `reason` when `Some`, i.e. when the caller's classifier
    /// judged the error deterministic
    pub fn trip_if(&self, reason: Option<&'static str>, lsn: u64, error: &dyn std::fmt::Display) {
        if let Some(reason) = reason {
            self.trip(FailClosed::new(reason, lsn, error.to_string()));
        }
    }

    pub fn get(&self) -> Option<FailClosed> {
        self.0.lock().expect("fail-closed latch poisoned").clone()
    }
}

/// Persist `verdict` into `spill_dir`, crash-safe
pub async fn write_marker(spill_dir: &Path, verdict: &FailClosed) -> io::Result<()> {
    let body = serde_json::to_vec_pretty(verdict).map_err(io::Error::other)?;
    crate::fs::write_atomic(spill_dir, MARKER_FILE, &body).await
}

/// Marker in `spill_dir`, if any. An unparseable marker still reads as a
/// verdict: refusing on it is safer than resuming past it
pub fn read_marker(spill_dir: &Path) -> io::Result<Option<FailClosed>> {
    let path = spill_dir.join(MARKER_FILE);
    let raw = match std::fs::read(&path) {
        Ok(raw) => raw,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    Ok(Some(serde_json::from_slice(&raw).unwrap_or_else(|e| {
        FailClosed {
            reason: "unreadable_marker".into(),
            lsn: 0,
            error: format!("{} does not parse: {e}", path.display()),
            at_unix: 0,
        }
    })))
}

/// Remove the marker, returning what it held. `Ok(None)` when absent
pub fn clear_marker(spill_dir: &Path) -> io::Result<Option<FailClosed>> {
    let held = read_marker(spill_dir)?;
    if held.is_some() {
        std::fs::remove_file(spill_dir.join(MARKER_FILE))?;
    }
    Ok(held)
}

/// Daemon error carrying a fail-closed verdict, so `main` can map it onto
/// [`EXIT_FAIL_CLOSED`] instead of the generic failure status
#[derive(Debug, thiserror::Error)]
#[error(
    "{verdict}\nwalshadow stopped fail-closed and will not resume from this spill directory. \
     Resolve the cause (usually a fresh snapshot), then run \
     `walshadow-stream fail-closed clear --spill-dir {spill_dir}`"
)]
pub struct FailClosedExit {
    pub verdict: FailClosed,
    pub spill_dir: String,
}

impl FailClosedExit {
    pub fn new(verdict: FailClosed, spill_dir: &Path) -> Self {
        Self {
            verdict,
            spill_dir: spill_dir.display().to_string(),
        }
    }
}

/// Exit status for a daemon error: [`EXIT_FAIL_CLOSED`] when it carries a
/// fail-closed verdict anywhere in its chain, 1 otherwise
pub fn exit_status(e: &anyhow::Error) -> i32 {
    if e.chain().any(|c| c.is::<FailClosedExit>()) {
        EXIT_FAIL_CLOSED
    } else {
        1
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::emit::pipeline::planner::{PlanError, deterministic_reason};
    use crate::xact::xact_buffer::{FailClosedReason, XactBufferError};

    #[test]
    fn classification_separates_deterministic_from_transient() {
        let fail_closed = |reason| XactBufferError::OrdinaryFailClosed {
            relid: 1,
            lsn: 2,
            rm: 10,
            info: 0,
            reason,
        };
        for (e, label) in [
            (
                XactBufferError::StashAmbiguous {
                    rel_node: 1,
                    lsn: 2,
                    from_lsn: 1,
                    through_lsn: 3,
                },
                "stash_ambiguous",
            ),
            (
                fail_closed(FailClosedReason::ImageOnly),
                "fail_closed_image_only",
            ),
            (
                fail_closed(FailClosedReason::Malformed("x".into())),
                "fail_closed_malformed",
            ),
            (
                fail_closed(FailClosedReason::UnsupportedOperation),
                "fail_closed_unsupported_op",
            ),
            (
                fail_closed(FailClosedReason::PartialUpdate),
                "partial_update",
            ),
            (
                XactBufferError::MissingStashResolution { top_xid: 7 },
                "missing_stash_resolution",
            ),
            (
                XactBufferError::IncompleteToastGeneration { relid: 1 },
                "incomplete_toast",
            ),
            (
                XactBufferError::ForeignXid { xid: 1, top: 2 },
                "foreign_xid",
            ),
        ] {
            assert_eq!(deterministic_reason(&e), Some(label), "{e}");
            assert_eq!(PlanError::Detoast(e).deterministic_reason(), Some(label));
        }
        assert_eq!(
            PlanError::PartialUpdate { lsn: 1 }.deterministic_reason(),
            Some("partial_update")
        );
        // I/O, store and route failures can clear on retry
        for e in [
            XactBufferError::Observer("ch down".into()),
            XactBufferError::Detoast("x".into()),
            XactBufferError::ValueTooLarge { rawsize: 2, max: 1 },
            XactBufferError::Spill(crate::xact::spill::SpillError::Io(io::Error::other("disk"))),
        ] {
            assert_eq!(deterministic_reason(&e), None, "{e}");
        }
        assert_eq!(PlanError::View("x".into()).deterministic_reason(), None);
    }

    #[test]
    fn latch_keeps_the_first_verdict() {
        let latch = FailClosedLatch::default();
        assert!(latch.get().is_none());
        latch.trip_if(None, 1, &"transient");
        assert!(latch.get().is_none());
        latch.trip_if(Some("stash_ambiguous"), 0x1_0000_0010, &"first");
        latch.trip_if(Some("partial_update"), 2, &"second");
        let held = latch.clone().get().unwrap();
        assert_eq!(held.reason, "stash_ambiguous");
        assert_eq!(held.error, "first");
        assert_eq!(held.lsn_text(), "1/10");
    }

    #[tokio::test(flavor = "current_thread")]
    async fn marker_round_trips_and_clears() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(read_marker(dir.path()).unwrap(), None);
        assert_eq!(clear_marker(dir.path()).unwrap(), None);
        let v = FailClosed::new("stash_ambiguous", 42, "boom");
        write_marker(dir.path(), &v).await.unwrap();
        assert_eq!(read_marker(dir.path()).unwrap(), Some(v.clone()));
        assert_eq!(clear_marker(dir.path()).unwrap(), Some(v));
        assert_eq!(read_marker(dir.path()).unwrap(), None);
    }

    #[test]
    fn unreadable_marker_still_refuses() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join(MARKER_FILE), b"{not json").unwrap();
        let v = read_marker(dir.path()).unwrap().unwrap();
        assert_eq!(v.reason, "unreadable_marker");
    }

    #[test]
    fn exit_status_finds_the_verdict_in_the_chain() {
        let v = FailClosed::new("stash_ambiguous", 1, "boom");
        let e = anyhow::Error::new(FailClosedExit::new(v, Path::new("/spill")))
            .context("session failed");
        assert_eq!(exit_status(&e), EXIT_FAIL_CLOSED);
        assert!(format!("{e:#}").contains("fail-closed clear --spill-dir /spill"));
        assert_eq!(exit_status(&anyhow::anyhow!("source unreachable")), 1);
    }
}
