//! Build required test modules with `make -C pgext test-deps` before running tests

#![allow(dead_code)]

use std::path::{Path, PathBuf};

/// Build directory used as shadow's `dynamic_library_path`
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

/// Test module loaded into source clusters
pub fn wstest_module() -> PathBuf {
    let so = Path::new(env!("CARGO_MANIFEST_DIR")).join("pgext/test/wstest.so");
    assert!(
        so.is_file(),
        "pgext/test/wstest.so missing, run `make -C pgext test-deps`"
    );
    so
}
