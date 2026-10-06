//! Process crash injection for daemon integration tests.

use std::path::PathBuf;
use std::sync::OnceLock;
use std::sync::atomic::{AtomicU64, Ordering};

static ARM: OnceLock<Option<PathBuf>> = OnceLock::new();
static COMMIT: AtomicU64 = AtomicU64::new(0);

#[derive(Debug, PartialEq, Eq)]
pub enum Point {
    BeforeDescriptor,
    AfterDescriptor,
    BeforeDdl,
    AfterAlter,
    AfterDdl,
    AfterAck,
    AfterManifest,
}

#[cfg(coverage)]
unsafe extern "C" {
    fn __llvm_profile_write_file() -> i32;
}

fn arm() -> Option<&'static PathBuf> {
    ARM.get_or_init(|| std::env::var_os("WALSHADOW_TEST_CRASH").map(PathBuf::from))
        .as_ref()
}

pub fn capture(commit_lsn: u64) {
    if arm().is_some_and(|path| path.exists()) {
        let _ = COMMIT.compare_exchange(0, commit_lsn, Ordering::SeqCst, Ordering::SeqCst);
    }
}

pub fn hit(point: Point, lsn: u64) {
    let commit = COMMIT.load(Ordering::SeqCst);
    if commit == 0 || lsn < commit {
        return;
    }
    let path = arm().expect("capture stored commit without arm");
    if std::fs::read_to_string(path).is_ok_and(|armed| armed == format!("{point:?}")) {
        std::fs::write(path.with_extension("hit"), commit.to_string())
            .expect("record crash boundary");
        // SIGKILL skips atexit, where llvm-cov would otherwise dump counters
        #[cfg(coverage)]
        unsafe {
            __llvm_profile_write_file()
        };
        // SIGKILL prevents unwinding, shutdown drains, and final manifest writes.
        unsafe { libc::raise(libc::SIGKILL) };
        unreachable!("SIGKILL returned");
    }
}
