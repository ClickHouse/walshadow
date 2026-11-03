//! Shared objects tests load, built by `make -C pgext test-deps`. Neither is
//! optional, so an unbuilt tree fails rather than skips

#![allow(dead_code)]

use std::path::{Path, PathBuf};

/// Build tree holding the module and the fault shim, fed to shadow as
/// `dynamic_library_path`
pub fn pgext_dir() -> PathBuf {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("pgext");
    for lib in ["walshadow.so", "faultshim.so"] {
        assert!(
            dir.join(lib).is_file(),
            "pgext/{lib} missing, run `make -C pgext test-deps`"
        );
    }
    dir
}

/// Test-only module tests load into their source cluster
pub fn wstest_module() -> PathBuf {
    let so = Path::new(env!("CARGO_MANIFEST_DIR")).join("pgext/test/wstest.so");
    assert!(
        so.is_file(),
        "pgext/test/wstest.so missing, run `make -C pgext test-deps`"
    );
    so
}
