//! Shared test helper: read a captured WAL segment fixture into bytes.
//!
//! Fixtures are gzipped under `fixtures/wal/**/segments/`;
//! `open_segment_file` transparently decompresses. Several round-trip
//! and classifier drills need the raw segment bytes, so the loader
//! lives here rather than being copied per test file.
//!
//! Included via `#[path = "common/segment.rs"]` rather than
//! `tests/common/mod.rs` so cargo doesn't build it as a free-standing
//! test binary.

#![allow(dead_code)]

use std::path::Path;

use pgwalrs::pg::wal::segment_file::open_segment_file;
use tokio::io::AsyncReadExt;

pub async fn load_segment(path: &Path) -> anyhow::Result<Vec<u8>> {
    let (_seg, mut r) = open_segment_file(path).await?;
    let mut out = Vec::new();
    r.read_to_end(&mut out).await?;
    Ok(out)
}
