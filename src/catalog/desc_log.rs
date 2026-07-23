//! Durable descriptor log: append-only relation-shape history, captured at
//! catalog-commit boundaries, read interval-scoped by decode.
//!
//! Decode asks `descriptor(rfn, L)` / `descriptor(oid, L)` — a point-in-time
//! question. The log stores per-key version chains (`valid_from` ascending);
//! lookup binary-searches the last entry at or below `L`. Capture appends one
//! batch per catalog boundary keyed `captured_at` = the commit's EndRecPtr,
//! fsyncs, then publishes to the in-memory index — records reaching decode
//! always have coverage.
//!
//! ## On-disk layout
//!
//! Two files under the spill dir: `desc_log.ckpt` (compacted snapshot,
//! replaced atomically at GC) + `desc_log.tail` (framed appends since).
//! Both open with the same header:
//!
//! ```text
//! [2 "WL"] [u16 LE version] [u32 pg_major] [str system_id] [u32 timeline]
//! [u32 db_oid] [u32 wal_seg_size]
//! ```
//!
//! then frames `[u32 LE len] [u32 LE crc32c(body)] [body]`; `body[0]` tags
//! `0` meta (ckpt only: `covered_through`, `floor_at_write`) or `1` batch.
//! Identity mismatch is fatal (foreign spill dir), version mismatch is fatal
//! (no cross-version compatibility by design).
//!
//! Tail repair: a frame extending past EOF or failing CRC as the final frame
//! is a torn append — truncate + `sync_data`. CRC failure with valid data
//! after is interior corruption — fail closed. GC writes the ckpt first,
//! then truncates the tail; a crash between leaves duplicate batches, which
//! load dedupes by `captured_at` (byte-equal skip, divergent fail-closed).
//!
//! ## Interval semantics
//!
//! `Present` opens an interval at `valid_from` (bias-early: the descriptor is
//! a backward-compatible reader of older tuples, never the reverse).
//! `Dropped` tombstones relation + rfn. `Retired` closes an rfn's interval on
//! filenode rotation without relation-level effects: rewrite/TRUNCATE hold
//! AccessExclusiveLock so no decode query lands past it — the entry exists so
//! GC can drop rotated-away chains and buggy callers fail closed, not open.
//!
//! ## Ambiguity intervals
//!
//! A boundary whose shape change cannot be proven safe for one descriptor
//! records an [`Ambiguity`]: a `[from_lsn, through_lsn)` interval scoped to
//! an rfn, oid, or whole database. Lookup consults ambiguity intervals
//! before the descriptor chain — a covered LSN answers
//! [`LookupResult::Ambiguous`] even when a chain entry exists, so callers
//! fail closed instead of decoding under an unproven layout. The final
//! post-commit `Present` entry stays usable past `through_lsn`
//!
//! Identity keys the full physical `RelFileNode`: PG guarantees
//! relfilenumber uniqueness only per database of one tablespace
//! (`GetNewRelFileNumber`, PG `src/backend/catalog/catalog.c`), so
//! `(db_node, rel_node)` alone can alias two live relations after OID
//! wraparound. Capture resolves the `pg_class.reltablespace` 0 sentinel to
//! the database's `dattablespace`, so stored rfns compare directly against
//! WAL locators' physical spcOid. See `plans/future/TABLESPACES.md` §0.
//!
//! Known scope debt (closed by the ordinary-heap stash fence lift, not
//! here): same-xact DDL+DML on an already-visible rfn stays timing-dependent
//! exactly as with the live-oracle path.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use thiserror::Error;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt};
use tokio_postgres::types::Oid;
use walrus::pg::walparser::RelFileNode;

use crate::schema::{RelAttr, RelDescriptor, RelName, ReplIdent};

pub const CKPT_FILE: &str = "desc_log.ckpt";
pub const TAIL_FILE: &str = "desc_log.tail";

const MAGIC: &[u8; 2] = b"WL";
const VERSION: u16 = 2;
/// Reject frames past this before allocating: no realistic batch (thousands
/// of descriptors) approaches it, garbage lengths do
const MAX_FRAME: u32 = 256 * 1024 * 1024;
const TAG_META: u8 = 0;
const TAG_BATCH: u8 = 1;

/// GC when this many entries are droppable below the floor
const GC_DEAD_ENTRIES: usize = 512;
/// or when the tail outgrows this
const GC_TAIL_BYTES: u64 = 8 * 1024 * 1024;

pub type Result<T> = std::result::Result<T, DescLogError>;

#[derive(Debug, Error)]
pub enum DescLogError {
    #[error("descriptor log io: {0}")]
    Io(#[from] std::io::Error),
    #[error("unsupported descriptor log version {0} (this build expects {VERSION})")]
    Version(u16),
    #[error("foreign descriptor log: {field} mismatch (log has {log}, this source is {ours})")]
    ForeignLog {
        field: &'static str,
        log: String,
        ours: String,
    },
    #[error("descriptor log corrupt at {file}:{offset}: {detail}")]
    Corrupt {
        file: &'static str,
        offset: u64,
        detail: String,
    },
}

/// Binds log files to one source + shadow pairing; any mismatch at open is
/// fatal, mirroring `ManifestError::ForeignSource`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DescLogIdentity {
    pub pg_major: u32,
    pub system_id: String,
    pub timeline: u32,
    pub db_oid: Oid,
    pub wal_seg_size: u32,
}

#[derive(Debug, Clone, PartialEq)]
pub enum LogValue {
    Present(Arc<RelDescriptor>),
    /// Relation dropped: tombstones oid + rfn chains
    Dropped,
    /// Filenode rotated away (rewrite/TRUNCATE/SET TABLESPACE): closes the
    /// rfn chain only, relation lives on under its new rfn
    Retired,
}

#[derive(Debug, Clone, PartialEq)]
pub struct LogEntry {
    pub valid_from: u64,
    pub oid: Oid,
    /// Full physical rfn (tablespace sentinel resolved at capture). For
    /// `Present` this is the descriptor's rfn; for tombstones the chain
    /// being closed
    pub rfn: RelFileNode,
    pub value: LogValue,
}

/// Interval `[from_lsn, through_lsn)` where no single descriptor provably
/// decodes rows in `scope`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ambiguity {
    pub scope: AmbiguityScope,
    pub from_lsn: u64,
    pub through_lsn: u64,
    pub reason: AmbiguityReason,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AmbiguityScope {
    Rfn(RelFileNode),
    Oid(Oid),
    /// Conservative fallback when affected relations cannot be enumerated
    Database(Oid),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AmbiguityReason {
    UnknownAffectedRelation,
    UnknownMutationPosition,
    MultipleIncompatibleLayouts,
    NeverVisibleGeneration,
    IncompleteInvalidation,
}

#[derive(Debug, Clone, PartialEq)]
pub struct BatchRecord {
    /// Boundary commit EndRecPtr (or seed LSN); the replay-from-log key
    pub captured_at: u64,
    /// Commit record start LSN; 0 for seed batches
    pub commit_lsn: u64,
    /// Evidence the boundary verdict derives from, kept so replay
    /// reproduces the verdict instead of reinferring it from current
    /// catalog. Deterministic order (capture sorts)
    pub observations: Vec<RelationObservation>,
    /// Intervals this boundary could not prove decodable under one
    /// descriptor
    pub ambiguities: Vec<Arc<Ambiguity>>,
    /// Empty = stub: boundary produced no shape change, recorded so boot
    /// replay distinguishes "captured, nothing changed" from "never captured"
    pub entries: Vec<Arc<LogEntry>>,
}

impl BatchRecord {
    /// Digest over the deterministic encoding; divergence diagnostics
    pub fn digest(&self) -> u32 {
        crc32c::crc32c(&encode_batch(self))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelationObservation {
    pub oid: Option<Oid>,
    pub rfn: Option<RelFileNode>,
    pub first_touch_lsn: u64,
    /// Main-fork smgr create marker: new generation lower bound
    pub smgr_create_lsn: Option<u64>,
    pub kind: ObservationKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ObservationKind {
    /// Dirty-tracker pg_class decode or commit relcache inval named the oid
    AffectedOid,
    /// Smgr create marker registered for the filenode
    SmgrCreate,
    /// Enumeration incomplete, capture fell back to full catalog scan
    FullScan,
}

#[derive(Debug, Clone, PartialEq)]
pub enum LookupResult {
    Present(Arc<RelDescriptor>),
    Dropped,
    Retired,
    /// LSN falls inside a recorded ambiguity interval: no descriptor is
    /// proven safe, callers fail closed
    Ambiguous(Arc<Ambiguity>),
    NotCovered,
    /// Foreign `db_node`: preserves the `ForeignDatabase` row-skip control
    /// flow — never a stash or a fatal
    ForeignDb,
}

/// One key's version chain slot; `captured_at` denormalized for
/// [`DescriptorLog::predecessor_before`]
#[derive(Debug, Clone)]
struct Slot {
    valid_from: u64,
    captured_at: u64,
    entry: Arc<LogEntry>,
}

#[derive(Debug, Default)]
struct Index {
    by_rfn: HashMap<RelFileNode, Vec<Slot>>,
    by_oid: HashMap<Oid, Vec<Slot>>,
    amb_rfn: HashMap<RelFileNode, Vec<Arc<Ambiguity>>>,
    amb_oid: HashMap<Oid, Vec<Arc<Ambiguity>>>,
    amb_db: HashMap<Oid, Vec<Arc<Ambiguity>>>,
    batches: BTreeMap<u64, Arc<BatchRecord>>,
    covered_through: u64,
    floor_at_write: u64,
    entries_total: usize,
}

impl Index {
    fn insert_batch(&mut self, batch: Arc<BatchRecord>) {
        for amb in &batch.ambiguities {
            let list = match amb.scope {
                AmbiguityScope::Rfn(rfn) => self.amb_rfn.entry(rfn).or_default(),
                AmbiguityScope::Oid(oid) => self.amb_oid.entry(oid).or_default(),
                AmbiguityScope::Database(db) => self.amb_db.entry(db).or_default(),
            };
            let key = (amb.from_lsn, amb.through_lsn);
            let pos = list.partition_point(|a| (a.from_lsn, a.through_lsn) <= key);
            list.insert(pos, amb.clone());
        }
        for entry in &batch.entries {
            let slot = Slot {
                valid_from: entry.valid_from,
                captured_at: batch.captured_at,
                entry: entry.clone(),
            };
            let rfn_chain = self.by_rfn.entry(entry.rfn).or_default();
            // Retired closes an rfn chain only; the relation lives on under
            // its new rfn, whose Present entry shares this oid + batch — a
            // Retired slot in by_oid would shadow it
            if !matches!(entry.value, LogValue::Retired) {
                let oid_chain = self.by_oid.entry(entry.oid).or_default();
                insert_sorted(oid_chain, slot.clone());
            }
            insert_sorted(rfn_chain, slot);
            self.entries_total += 1;
        }
        self.batches.insert(batch.captured_at, batch);
    }
}

/// Chains stay `(valid_from, captured_at)`-ascending. Appends hit the tail
/// (DDL on one rel serializes under AccessExclusiveLock, so per-key order is
/// total); boot replay re-inserts nothing (batch dedupe upstream).
fn insert_sorted(chain: &mut Vec<Slot>, slot: Slot) {
    let key = (slot.valid_from, slot.captured_at);
    let pos = chain.partition_point(|s| (s.valid_from, s.captured_at) <= key);
    chain.insert(pos, slot);
}

#[derive(Debug)]
struct Writer {
    tail: File,
    tail_len: u64,
    header_len: u64,
    dir: PathBuf,
}

crate::atomic_stats! {
    pub struct DescLogStats {
        pub lookups_present,
        pub lookups_dropped,
        pub lookups_retired,
        pub lookups_ambiguous,
        pub lookups_not_covered,
        pub lookups_foreign_db,
        pub batches_appended,
        pub gc_runs,
        pub gc_dropped_entries,
        /// `descriptor_ambiguous_total{reason}` split of `lookups_ambiguous`
        pub ambiguous_unknown_relation,
        pub ambiguous_unknown_position,
        pub ambiguous_incompatible_layouts,
        pub ambiguous_never_visible,
        pub ambiguous_incomplete_invalidation,
    }
}

#[derive(Debug)]
pub struct DescriptorLog {
    identity: DescLogIdentity,
    index: RwLock<Index>,
    writer: tokio::sync::Mutex<Writer>,
    stats: Arc<DescLogStats>,
}

impl DescriptorLog {
    /// Load `desc_log.ckpt` + `desc_log.tail` under `dir`, repairing a torn
    /// tail. Creates an empty tail (header only) when absent. Never creates
    /// the ckpt — [`Self::seed`] and GC own it.
    pub async fn open(dir: &Path, identity: DescLogIdentity) -> Result<Self> {
        let mut index = Index::default();
        let ckpt_path = dir.join(CKPT_FILE);
        if let Ok(bytes) = tokio::fs::read(&ckpt_path).await {
            // write_atomic guarantees complete-or-absent: any parse failure
            // here is corruption, never a torn write
            load_frames(&bytes, CKPT_FILE, &identity, &mut index, false)?;
        }

        let tail_path = dir.join(TAIL_FILE);
        let header = encode_header(&identity);
        let (tail, tail_len) = match tokio::fs::read(&tail_path).await {
            Ok(bytes) => {
                let good_len = load_frames(&bytes, TAIL_FILE, &identity, &mut index, true)?;
                let mut f = OpenOptions::new().write(true).open(&tail_path).await?;
                if good_len < bytes.len() as u64 {
                    f.set_len(good_len).await?;
                    f.sync_data().await?;
                }
                f.seek(SeekFrom::Start(good_len)).await?;
                (f, good_len)
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let mut f = OpenOptions::new()
                    .write(true)
                    .create_new(true)
                    .open(&tail_path)
                    .await?;
                f.write_all(&header).await?;
                f.sync_data().await?;
                crate::fs::fsync_dir(dir).await?;
                (f, header.len() as u64)
            }
            Err(e) => return Err(e.into()),
        };

        Ok(Self {
            identity,
            index: RwLock::new(index),
            writer: tokio::sync::Mutex::new(Writer {
                tail,
                tail_len,
                header_len: header.len() as u64,
                dir: dir.to_path_buf(),
            }),
            stats: Arc::new(DescLogStats::default()),
        })
    }

    pub fn stats_handle(&self) -> Arc<DescLogStats> {
        self.stats.clone()
    }

    pub fn covered_through(&self) -> u64 {
        self.index.read().unwrap().covered_through
    }

    /// Floor recorded by the last GC ckpt write; `--start-lsn` below it has
    /// no descriptor history
    pub fn floor_at_write(&self) -> u64 {
        self.index.read().unwrap().floor_at_write
    }

    /// Highest `captured_at`, 0 when empty
    pub fn head(&self) -> u64 {
        self.index
            .read()
            .unwrap()
            .batches
            .keys()
            .next_back()
            .copied()
            .unwrap_or(0)
    }

    pub fn is_empty(&self) -> bool {
        let idx = self.index.read().unwrap();
        idx.batches.is_empty() && idx.covered_through == 0
    }

    pub fn descriptor_at(&self, rfn: RelFileNode, lsn: u64) -> LookupResult {
        match self.descriptor_at_spanned(rfn, lsn) {
            Ok((rel, _)) => LookupResult::Present(rel),
            Err(other) => other,
        }
    }

    /// `Present` descriptor plus its entry's `valid_from` under one index
    /// read (two reads could interleave with a bias-early capture append
    /// and pair a stale descriptor with a fresh span). Non-Present outcomes
    /// return the plain lookup for the caller's arms
    pub fn descriptor_at_spanned(
        &self,
        rfn: RelFileNode,
        lsn: u64,
    ) -> std::result::Result<(Arc<RelDescriptor>, u64), LookupResult> {
        use std::sync::atomic::Ordering::Relaxed;
        if rfn.db_node != 0 && rfn.db_node != self.identity.db_oid {
            self.stats.lookups_foreign_db.fetch_add(1, Relaxed);
            return Err(LookupResult::ForeignDb);
        }
        let idx = self.index.read().unwrap();
        // Ambiguity precedes the chain: a chain entry inside an ambiguous
        // interval is not proven safe for rows there
        let result = ambiguity_covering(idx.amb_rfn.get(&rfn), lsn)
            .or_else(|| ambiguity_covering(idx.amb_db.get(&rfn.db_node), lsn))
            .map_or_else(
                || lookup_spanned(idx.by_rfn.get(&rfn), lsn),
                |a| Err(LookupResult::Ambiguous(a)),
            );
        self.count_spanned(&result);
        result
    }

    pub fn descriptor_by_oid_at(&self, oid: Oid, lsn: u64) -> LookupResult {
        match self.descriptor_by_oid_at_spanned(oid, lsn) {
            Ok((rel, _)) => LookupResult::Present(rel),
            Err(other) => other,
        }
    }

    /// Oid-keyed twin of [`Self::descriptor_at_spanned`]
    pub fn descriptor_by_oid_at_spanned(
        &self,
        oid: Oid,
        lsn: u64,
    ) -> std::result::Result<(Arc<RelDescriptor>, u64), LookupResult> {
        let idx = self.index.read().unwrap();
        let result = ambiguity_covering(idx.amb_oid.get(&oid), lsn)
            .or_else(|| ambiguity_covering(idx.amb_db.get(&self.identity.db_oid), lsn))
            .map_or_else(
                || lookup_spanned(idx.by_oid.get(&oid), lsn),
                |a| Err(LookupResult::Ambiguous(a)),
            );
        self.count_spanned(&result);
        result
    }

    fn count_spanned(&self, result: &std::result::Result<(Arc<RelDescriptor>, u64), LookupResult>) {
        use std::sync::atomic::Ordering::Relaxed;
        match result {
            Ok(_) => {
                self.stats.lookups_present.fetch_add(1, Relaxed);
            }
            Err(other) => self.count(other),
        }
    }

    fn count(&self, result: &LookupResult) {
        use std::sync::atomic::Ordering::Relaxed;
        match result {
            LookupResult::Present(_) => self.stats.lookups_present.fetch_add(1, Relaxed),
            LookupResult::Dropped => self.stats.lookups_dropped.fetch_add(1, Relaxed),
            LookupResult::Retired => self.stats.lookups_retired.fetch_add(1, Relaxed),
            LookupResult::Ambiguous(a) => {
                self.stats.lookups_ambiguous.fetch_add(1, Relaxed);
                let by_reason = match a.reason {
                    AmbiguityReason::UnknownAffectedRelation => {
                        &self.stats.ambiguous_unknown_relation
                    }
                    AmbiguityReason::UnknownMutationPosition => {
                        &self.stats.ambiguous_unknown_position
                    }
                    AmbiguityReason::MultipleIncompatibleLayouts => {
                        &self.stats.ambiguous_incompatible_layouts
                    }
                    AmbiguityReason::NeverVisibleGeneration => &self.stats.ambiguous_never_visible,
                    AmbiguityReason::IncompleteInvalidation => {
                        &self.stats.ambiguous_incomplete_invalidation
                    }
                };
                by_reason.fetch_add(1, Relaxed)
            }
            LookupResult::NotCovered => self.stats.lookups_not_covered.fetch_add(1, Relaxed),
            LookupResult::ForeignDb => self.stats.lookups_foreign_db.fetch_add(1, Relaxed),
        };
    }

    /// Newest `Present` at or below `lsn` on the rfn chain, skipping
    /// tombstones. Truncate apply resolves the relation an rfn named even
    /// after its own commit retired it: rotation records `Retired` at the
    /// new generation's bias-early valid_from, which precedes the truncate
    /// record itself.
    pub fn present_before(&self, rfn: RelFileNode, lsn: u64) -> Option<Arc<RelDescriptor>> {
        let idx = self.index.read().unwrap();
        let chain = idx.by_rfn.get(&rfn)?;
        let upto = chain.partition_point(|s| s.valid_from <= lsn);
        chain[..upto]
            .iter()
            .rev()
            .find_map(|s| match &s.entry.value {
                LogValue::Present(d) => Some(d.clone()),
                _ => None,
            })
    }

    /// Replay-from-log hit test for a boundary at `captured_at`
    pub fn batch_at(&self, captured_at: u64) -> Option<Arc<BatchRecord>> {
        self.index
            .read()
            .unwrap()
            .batches
            .get(&captured_at)
            .cloned()
    }

    /// The oid's entry preceding the batch at `captured_at` in history
    /// order — replay derives events against this, never the loaded head
    /// (boot loads the full log before the WAL re-read)
    pub fn predecessor_before(&self, oid: Oid, captured_at: u64) -> Option<Arc<LogEntry>> {
        let idx = self.index.read().unwrap();
        let chain = idx.by_oid.get(&oid)?;
        let pos = chain.partition_point(|s| s.captured_at < captured_at);
        pos.checked_sub(1).map(|i| chain[i].entry.clone())
    }

    /// Ambiguity intervals recorded for an rfn, `(from_lsn, through_lsn)`
    /// ascending — introspection for diagnostics and tests
    pub fn rfn_ambiguities(&self, rfn: RelFileNode) -> Vec<Arc<Ambiguity>> {
        let idx = self.index.read().unwrap();
        idx.amb_rfn.get(&rfn).cloned().unwrap_or_default()
    }

    /// Oids whose newest entry is `Present` — capture-all diffs its SQL
    /// enumeration against this to tombstone vanished relations
    pub fn present_oids(&self) -> Vec<Oid> {
        let idx = self.index.read().unwrap();
        idx.by_oid
            .iter()
            .filter(|(_, chain)| {
                matches!(
                    chain.last().map(|s| &s.entry.value),
                    Some(LogValue::Present(_))
                )
            })
            .map(|(oid, _)| *oid)
            .collect()
    }

    /// Descriptors `Present` at `lsn` — boot Added enumeration
    pub fn active_present_at(&self, lsn: u64) -> Vec<Arc<RelDescriptor>> {
        let idx = self.index.read().unwrap();
        idx.by_oid
            .values()
            .filter_map(|chain| Some(lookup_spanned(Some(chain), lsn).ok()?.0))
            .collect()
    }

    /// One-time baseline on an empty log: writes the ckpt (meta +
    /// seed batch) so `covered_through` is durable before any tail append.
    /// Boundaries at or below `covered_through` are baked into the seed —
    /// no capture, no event replay.
    pub async fn seed(&self, batch: BatchRecord, covered_through: u64) -> Result<()> {
        let w = self.writer.lock().await;
        {
            let idx = self.index.read().unwrap();
            if !idx.batches.is_empty() || idx.covered_through != 0 {
                return Err(DescLogError::Corrupt {
                    file: CKPT_FILE,
                    offset: 0,
                    detail: "seed on a non-empty log".into(),
                });
            }
        }
        let batch = Arc::new(batch);
        let mut bytes = encode_header(&self.identity);
        push_frame(&mut bytes, &encode_meta(covered_through, 0));
        push_frame(&mut bytes, &encode_batch(&batch));
        crate::fs::write_atomic(&w.dir, CKPT_FILE, &bytes).await?;
        let mut idx = self.index.write().unwrap();
        idx.covered_through = covered_through;
        idx.insert_batch(batch);
        Ok(())
    }

    /// Append one boundary batch: frame → `sync_data` → index publish.
    /// Idempotent per `captured_at` (byte-identical replays no-op); a
    /// divergent batch at an existing key fails closed.
    pub async fn append_batch(&self, batch: BatchRecord) -> Result<()> {
        let mut w = self.writer.lock().await;
        {
            let idx = self.index.read().unwrap();
            if let Some(existing) = idx.batches.get(&batch.captured_at) {
                if **existing == batch {
                    return Ok(());
                }
                return Err(DescLogError::Corrupt {
                    file: TAIL_FILE,
                    offset: w.tail_len,
                    detail: format!(
                        "append at captured_at {:#x} diverges from stored batch \
                         (digest {:#010x} vs stored {:#010x})",
                        batch.captured_at,
                        batch.digest(),
                        existing.digest(),
                    ),
                });
            }
        }
        let batch = Arc::new(batch);
        let mut frame = Vec::new();
        push_frame(&mut frame, &encode_batch(&batch));
        w.tail.write_all(&frame).await?;
        w.tail.sync_data().await?;
        w.tail_len += frame.len() as u64;
        self.stats
            .batches_appended
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.index.write().unwrap().insert_batch(batch);
        Ok(())
    }

    /// Compact when the tail outgrew `GC_TAIL_BYTES` or at least
    /// `GC_DEAD_ENTRIES` entries are droppable below `floor`. Retention
    /// keeps, per key, the state active at the floor: the last entry at or
    /// below it when `Present`; a `Dropped`/`Retired` there drops the whole
    /// at-or-below history (nothing above can reference it — records
    /// predate the drop, and the floor never exceeds the re-read start).
    /// Batches above the floor survive whole (stubs included) for boot
    /// replay; below it they exist only as carriers of retained entries.
    pub async fn maybe_gc(&self, floor: u64) -> Result<bool> {
        let w = self.writer.lock().await;
        let (retained, dropped_entries) = {
            let idx = self.index.read().unwrap();
            let retained = compute_retained(&idx, floor);
            let dropped = idx.entries_total - retained.entries_total;
            (retained, dropped)
        };
        let tail_bytes = w.tail_len - w.header_len;
        if dropped_entries < GC_DEAD_ENTRIES && tail_bytes < GC_TAIL_BYTES {
            return Ok(false);
        }
        self.gc_locked(w, retained, dropped_entries as u64, floor)
            .await?;
        Ok(true)
    }

    #[cfg(test)]
    pub async fn force_gc(&self, floor: u64) -> Result<()> {
        let w = self.writer.lock().await;
        let (retained, dropped) = {
            let idx = self.index.read().unwrap();
            let retained = compute_retained(&idx, floor);
            let dropped = idx.entries_total - retained.entries_total;
            (retained, dropped)
        };
        self.gc_locked(w, retained, dropped as u64, floor).await
    }

    async fn gc_locked(
        &self,
        mut w: tokio::sync::MutexGuard<'_, Writer>,
        mut retained: Index,
        dropped_entries: u64,
        floor: u64,
    ) -> Result<()> {
        use std::sync::atomic::Ordering::Relaxed;
        retained.floor_at_write = floor;
        let mut bytes = encode_header(&self.identity);
        push_frame(&mut bytes, &encode_meta(retained.covered_through, floor));
        for batch in retained.batches.values() {
            push_frame(&mut bytes, &encode_batch(batch));
        }
        crate::fs::write_atomic(&w.dir, CKPT_FILE, &bytes).await?;
        // Crash before this truncate leaves ckpt+tail overlapping; load
        // dedupes by captured_at
        let header_len = w.header_len;
        w.tail.set_len(header_len).await?;
        w.tail.seek(SeekFrom::Start(header_len)).await?;
        w.tail.sync_data().await?;
        w.tail_len = w.header_len;
        *self.index.write().unwrap() = retained;
        self.stats.gc_runs.fetch_add(1, Relaxed);
        self.stats
            .gc_dropped_entries
            .fetch_add(dropped_entries, Relaxed);
        Ok(())
    }

    /// (entries, bytes-in-tail, batches) gauges
    pub fn gauges(&self) -> (u64, u64, u64) {
        let idx = self.index.read().unwrap();
        let entries = idx.entries_total as u64;
        let batches = idx.batches.len() as u64;
        drop(idx);
        let tail = self
            .writer
            .try_lock()
            .map(|w| w.tail_len - w.header_len)
            .unwrap_or(0);
        (entries, tail, batches)
    }
}

/// First interval covering `lsn` under `[from_lsn, through_lsn)`; lists stay
/// `(from_lsn, through_lsn)`-sorted so overlap resolution is deterministic
fn ambiguity_covering(list: Option<&Vec<Arc<Ambiguity>>>, lsn: u64) -> Option<Arc<Ambiguity>> {
    list?
        .iter()
        .find(|a| a.from_lsn <= lsn && lsn < a.through_lsn)
        .cloned()
}

fn lookup_spanned(
    chain: Option<&Vec<Slot>>,
    lsn: u64,
) -> std::result::Result<(Arc<RelDescriptor>, u64), LookupResult> {
    let Some(chain) = chain else {
        return Err(LookupResult::NotCovered);
    };
    let pos = chain.partition_point(|s| s.valid_from <= lsn);
    let Some(slot) = pos.checked_sub(1).map(|i| &chain[i]) else {
        return Err(LookupResult::NotCovered);
    };
    match &slot.entry.value {
        LogValue::Present(d) => Ok((d.clone(), slot.valid_from)),
        LogValue::Dropped => Err(LookupResult::Dropped),
        LogValue::Retired => Err(LookupResult::Retired),
    }
}

fn compute_retained(idx: &Index, floor: u64) -> Index {
    // Entry identity by allocation: each Arc<LogEntry> is inserted once and
    // shared between its batch and both chains
    let mut keep: HashSet<*const LogEntry> = HashSet::new();
    for chain in idx.by_rfn.values().chain(idx.by_oid.values()) {
        let below = &chain[..chain.partition_point(|s| s.valid_from <= floor)];
        if let Some(last) = below.last()
            && matches!(last.entry.value, LogValue::Present(_))
        {
            keep.insert(Arc::as_ptr(&last.entry));
        }
    }
    let mut out = Index {
        covered_through: idx.covered_through,
        floor_at_write: idx.floor_at_write,
        ..Default::default()
    };
    for (&captured_at, batch) in &idx.batches {
        if captured_at > floor {
            out.insert_batch(batch.clone());
            continue;
        }
        let entries: Vec<Arc<LogEntry>> = batch
            .entries
            .iter()
            .filter(|e| e.valid_from > floor || keep.contains(&Arc::as_ptr(e)))
            .cloned()
            .collect();
        // Half-open interval: through_lsn == floor covers nothing at or
        // above the floor
        let ambiguities: Vec<Arc<Ambiguity>> = batch
            .ambiguities
            .iter()
            .filter(|a| a.through_lsn > floor)
            .cloned()
            .collect();
        if !entries.is_empty() || !ambiguities.is_empty() {
            out.insert_batch(Arc::new(BatchRecord {
                captured_at,
                commit_lsn: batch.commit_lsn,
                observations: batch.observations.clone(),
                ambiguities,
                entries,
            }));
        }
    }
    out
}

// ── encoding ────────────────────────────────────────────────────────

fn push_u8(out: &mut Vec<u8>, v: u8) {
    out.push(v);
}
fn push_u16(out: &mut Vec<u8>, v: u16) {
    out.extend_from_slice(&v.to_le_bytes());
}
fn push_i16(out: &mut Vec<u8>, v: i16) {
    out.extend_from_slice(&v.to_le_bytes());
}
fn push_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}
fn push_i32(out: &mut Vec<u8>, v: i32) {
    out.extend_from_slice(&v.to_le_bytes());
}
fn push_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}
fn push_str(out: &mut Vec<u8>, s: &str) {
    push_u32(out, s.len() as u32);
    out.extend_from_slice(s.as_bytes());
}
fn push_opt_str(out: &mut Vec<u8>, s: Option<&str>) {
    match s {
        None => push_u8(out, 0),
        Some(s) => {
            push_u8(out, 1);
            push_str(out, s);
        }
    }
}

fn encode_header(id: &DescLogIdentity) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(MAGIC);
    push_u16(&mut out, VERSION);
    push_u32(&mut out, id.pg_major);
    push_str(&mut out, &id.system_id);
    push_u32(&mut out, id.timeline);
    push_u32(&mut out, id.db_oid);
    push_u32(&mut out, id.wal_seg_size);
    out
}

fn encode_meta(covered_through: u64, floor_at_write: u64) -> Vec<u8> {
    let mut out = vec![TAG_META];
    push_u64(&mut out, covered_through);
    push_u64(&mut out, floor_at_write);
    out
}

fn encode_batch(batch: &BatchRecord) -> Vec<u8> {
    let mut out = vec![TAG_BATCH];
    push_u64(&mut out, batch.captured_at);
    push_u64(&mut out, batch.commit_lsn);
    push_u32(&mut out, batch.entries.len() as u32);
    for entry in &batch.entries {
        push_u64(&mut out, entry.valid_from);
        push_u32(&mut out, entry.oid);
        push_u32(&mut out, entry.rfn.spc_node);
        push_u32(&mut out, entry.rfn.db_node);
        push_u32(&mut out, entry.rfn.rel_node);
        match &entry.value {
            LogValue::Present(d) => {
                push_u8(&mut out, 0);
                encode_descriptor(&mut out, d);
            }
            LogValue::Dropped => push_u8(&mut out, 1),
            LogValue::Retired => push_u8(&mut out, 2),
        }
    }
    push_u32(&mut out, batch.ambiguities.len() as u32);
    for amb in &batch.ambiguities {
        match amb.scope {
            AmbiguityScope::Rfn(rfn) => {
                push_u8(&mut out, 0);
                push_u32(&mut out, rfn.spc_node);
                push_u32(&mut out, rfn.db_node);
                push_u32(&mut out, rfn.rel_node);
            }
            AmbiguityScope::Oid(oid) => {
                push_u8(&mut out, 1);
                push_u32(&mut out, oid);
            }
            AmbiguityScope::Database(db) => {
                push_u8(&mut out, 2);
                push_u32(&mut out, db);
            }
        }
        push_u64(&mut out, amb.from_lsn);
        push_u64(&mut out, amb.through_lsn);
        push_u8(&mut out, amb.reason as u8);
    }
    push_u32(&mut out, batch.observations.len() as u32);
    for obs in &batch.observations {
        match obs.oid {
            None => push_u8(&mut out, 0),
            Some(oid) => {
                push_u8(&mut out, 1);
                push_u32(&mut out, oid);
            }
        }
        match obs.rfn {
            None => push_u8(&mut out, 0),
            Some(rfn) => {
                push_u8(&mut out, 1);
                push_u32(&mut out, rfn.spc_node);
                push_u32(&mut out, rfn.db_node);
                push_u32(&mut out, rfn.rel_node);
            }
        }
        push_u64(&mut out, obs.first_touch_lsn);
        match obs.smgr_create_lsn {
            None => push_u8(&mut out, 0),
            Some(lsn) => {
                push_u8(&mut out, 1);
                push_u64(&mut out, lsn);
            }
        }
        push_u8(&mut out, obs.kind as u8);
    }
    out
}

fn encode_descriptor(out: &mut Vec<u8>, d: &RelDescriptor) {
    push_u32(out, d.rfn.spc_node);
    push_u32(out, d.rfn.db_node);
    push_u32(out, d.rfn.rel_node);
    push_u32(out, d.oid);
    push_u32(out, d.toast_oid);
    push_u32(out, d.namespace_oid);
    push_str(out, &d.rel_name.namespace);
    push_str(out, &d.rel_name.name);
    push_u8(out, d.kind as u8);
    push_u8(out, d.persistence as u8);
    match &d.replident {
        ReplIdent::Default { pk_attnums } => {
            push_u8(out, 0);
            encode_opt_attnums(out, pk_attnums.as_deref());
        }
        ReplIdent::Nothing => push_u8(out, 1),
        ReplIdent::Full { pk_attnums } => {
            push_u8(out, 2);
            encode_opt_attnums(out, pk_attnums.as_deref());
        }
        ReplIdent::UsingIndex {
            index_oid,
            key_attnums,
        } => {
            push_u8(out, 3);
            push_u32(out, *index_oid);
            encode_attnums(out, key_attnums);
        }
    }
    push_u32(out, d.attributes.len() as u32);
    for a in &d.attributes {
        push_i16(out, a.attnum);
        push_str(out, &a.name);
        push_u32(out, a.type_oid);
        push_i32(out, a.typmod);
        push_u8(out, a.not_null as u8);
        push_u8(out, a.dropped as u8);
        push_str(out, &a.type_name);
        push_u8(out, a.type_byval as u8);
        push_i16(out, a.type_len);
        push_u8(out, a.type_align as u8);
        push_u8(out, a.type_storage as u8);
        push_opt_str(out, a.missing_text.as_deref());
    }
}

/// Spill descriptor-dictionary reuse: same codec, byte-slice framing
pub(crate) fn encode_descriptor_bytes(out: &mut Vec<u8>, d: &RelDescriptor) {
    encode_descriptor(out, d);
}

/// Returns decoded descriptor + consumed byte count
pub(crate) fn decode_descriptor_bytes(
    buf: &[u8],
) -> std::result::Result<(RelDescriptor, usize), String> {
    let mut cur = Cur::new(buf, "spill", 0);
    let d = decode_descriptor(&mut cur).map_err(|e| e.to_string())?;
    Ok((d, cur.pos))
}

fn encode_opt_attnums(out: &mut Vec<u8>, nums: Option<&[i16]>) {
    match nums {
        None => push_u8(out, 0),
        Some(nums) => {
            push_u8(out, 1);
            encode_attnums(out, nums);
        }
    }
}

fn encode_attnums(out: &mut Vec<u8>, nums: &[i16]) {
    push_u32(out, nums.len() as u32);
    for n in nums {
        push_i16(out, *n);
    }
}

fn push_frame(out: &mut Vec<u8>, body: &[u8]) {
    push_u32(out, body.len() as u32);
    push_u32(out, crc32c::crc32c(body));
    out.extend_from_slice(body);
}

// ── decoding ────────────────────────────────────────────────────────

struct Cur<'a> {
    buf: &'a [u8],
    pos: usize,
    file: &'static str,
    base: u64,
}

impl<'a> Cur<'a> {
    fn new(buf: &'a [u8], file: &'static str, base: u64) -> Self {
        Self {
            buf,
            pos: 0,
            file,
            base,
        }
    }

    fn corrupt(&self, detail: impl Into<String>) -> DescLogError {
        DescLogError::Corrupt {
            file: self.file,
            offset: self.base + self.pos as u64,
            detail: detail.into(),
        }
    }

    fn need(&mut self, n: usize) -> Result<&'a [u8]> {
        if self.pos + n > self.buf.len() {
            return Err(self.corrupt(format!(
                "short read: need {n}, have {}",
                self.buf.len() - self.pos
            )));
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.need(1)?[0])
    }
    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_le_bytes(self.need(2)?.try_into().unwrap()))
    }
    fn i16(&mut self) -> Result<i16> {
        Ok(i16::from_le_bytes(self.need(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_le_bytes(self.need(4)?.try_into().unwrap()))
    }
    fn i32(&mut self) -> Result<i32> {
        Ok(i32::from_le_bytes(self.need(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_le_bytes(self.need(8)?.try_into().unwrap()))
    }
    fn string(&mut self) -> Result<String> {
        let n = self.u32()? as usize;
        let bs = self.need(n)?;
        String::from_utf8(bs.to_vec()).map_err(|e| self.corrupt(format!("utf8: {e}")))
    }
    fn opt_string(&mut self) -> Result<Option<String>> {
        Ok(match self.u8()? {
            0 => None,
            _ => Some(self.string()?),
        })
    }
    fn charlike(&mut self) -> Result<char> {
        Ok(self.u8()? as char)
    }
    fn boolish(&mut self) -> Result<bool> {
        Ok(self.u8()? != 0)
    }
}

/// Parse header + frames into `index`. Returns the byte offset after the
/// last good frame. `repairable = true` (tail): a frame torn at EOF stops
/// the load there for the caller to truncate; interior corruption still
/// fails. `repairable = false` (ckpt): any malformation fails.
fn load_frames(
    bytes: &[u8],
    file: &'static str,
    identity: &DescLogIdentity,
    index: &mut Index,
    repairable: bool,
) -> Result<u64> {
    let mut cur = Cur::new(bytes, file, 0);
    let magic = cur.need(2)?;
    if magic != MAGIC {
        return Err(DescLogError::Corrupt {
            file,
            offset: 0,
            detail: format!("bad magic {magic:02x?}"),
        });
    }
    let version = cur.u16()?;
    if version != VERSION {
        return Err(DescLogError::Version(version));
    }
    check_identity(&mut cur, identity)?;
    let mut good = cur.pos as u64;
    loop {
        let frame_start = cur.pos;
        if frame_start == bytes.len() {
            break;
        }
        let torn = |detail: String| -> Result<u64> {
            if repairable {
                Ok(good)
            } else {
                Err(DescLogError::Corrupt {
                    file,
                    offset: frame_start as u64,
                    detail,
                })
            }
        };
        if bytes.len() - frame_start < 8 {
            return torn("torn frame header".into());
        }
        let len = u32::from_le_bytes(bytes[frame_start..frame_start + 4].try_into().unwrap());
        let crc = u32::from_le_bytes(bytes[frame_start + 4..frame_start + 8].try_into().unwrap());
        let body_start = frame_start + 8;
        let body_end = body_start as u64 + len as u64;
        if body_end > bytes.len() as u64 {
            return torn(format!("frame len {len} past EOF"));
        }
        if len > MAX_FRAME {
            // Fits in the file yet exceeds any real batch: garbage with
            // data after it, not a torn append
            return Err(DescLogError::Corrupt {
                file,
                offset: frame_start as u64,
                detail: format!("frame len {len} exceeds bound"),
            });
        }
        let body = &bytes[body_start..body_end as usize];
        if crc32c::crc32c(body) != crc {
            if body_end == bytes.len() as u64 {
                return torn("crc mismatch on final frame".into());
            }
            return Err(DescLogError::Corrupt {
                file,
                offset: frame_start as u64,
                detail: "crc mismatch on interior frame".into(),
            });
        }
        let mut body_cur = Cur::new(body, file, body_start as u64);
        match body_cur.u8()? {
            TAG_META => {
                index.covered_through = body_cur.u64()?;
                index.floor_at_write = body_cur.u64()?;
            }
            TAG_BATCH => {
                let batch = decode_batch(&mut body_cur)?;
                match index.batches.get(&batch.captured_at) {
                    // ckpt/tail overlap from a crash between GC's ckpt
                    // write and tail truncate
                    Some(existing) if **existing == batch => {}
                    Some(existing) => {
                        return Err(DescLogError::Corrupt {
                            file,
                            offset: frame_start as u64,
                            detail: format!(
                                "batch at captured_at {:#x} diverges from earlier copy \
                                 (digest {:#010x} vs {:#010x})",
                                batch.captured_at,
                                batch.digest(),
                                existing.digest(),
                            ),
                        });
                    }
                    None => index.insert_batch(Arc::new(batch)),
                }
            }
            other => {
                return Err(DescLogError::Corrupt {
                    file,
                    offset: body_start as u64,
                    detail: format!("unknown frame tag {other}"),
                });
            }
        }
        cur.pos = body_end as usize;
        good = body_end;
    }
    Ok(good)
}

fn check_identity(cur: &mut Cur<'_>, ours: &DescLogIdentity) -> Result<()> {
    let log = DescLogIdentity {
        pg_major: cur.u32()?,
        system_id: cur.string()?,
        timeline: cur.u32()?,
        db_oid: cur.u32()?,
        wal_seg_size: cur.u32()?,
    };
    let mismatch = |field: &'static str, log: String, ours: String| {
        Err(DescLogError::ForeignLog { field, log, ours })
    };
    if log.pg_major != ours.pg_major {
        return mismatch(
            "pg_major",
            log.pg_major.to_string(),
            ours.pg_major.to_string(),
        );
    }
    if log.system_id != ours.system_id {
        return mismatch("system_id", log.system_id, ours.system_id.clone());
    }
    if log.timeline != ours.timeline {
        return mismatch(
            "timeline",
            log.timeline.to_string(),
            ours.timeline.to_string(),
        );
    }
    if log.db_oid != ours.db_oid {
        return mismatch("db_oid", log.db_oid.to_string(), ours.db_oid.to_string());
    }
    if log.wal_seg_size != ours.wal_seg_size {
        return mismatch(
            "wal_seg_size",
            log.wal_seg_size.to_string(),
            ours.wal_seg_size.to_string(),
        );
    }
    Ok(())
}

fn decode_batch(cur: &mut Cur<'_>) -> Result<BatchRecord> {
    let captured_at = cur.u64()?;
    let commit_lsn = cur.u64()?;
    let n = cur.u32()? as usize;
    let mut entries = Vec::with_capacity(n);
    for _ in 0..n {
        let valid_from = cur.u64()?;
        let oid = cur.u32()?;
        let rfn = RelFileNode {
            spc_node: cur.u32()?,
            db_node: cur.u32()?,
            rel_node: cur.u32()?,
        };
        let value = match cur.u8()? {
            0 => LogValue::Present(Arc::new(decode_descriptor(cur)?)),
            1 => LogValue::Dropped,
            2 => LogValue::Retired,
            other => return Err(cur.corrupt(format!("unknown entry kind {other}"))),
        };
        entries.push(Arc::new(LogEntry {
            valid_from,
            oid,
            rfn,
            value,
        }));
    }
    let n = cur.u32()? as usize;
    let mut ambiguities = Vec::with_capacity(n);
    for _ in 0..n {
        let scope = match cur.u8()? {
            0 => AmbiguityScope::Rfn(RelFileNode {
                spc_node: cur.u32()?,
                db_node: cur.u32()?,
                rel_node: cur.u32()?,
            }),
            1 => AmbiguityScope::Oid(cur.u32()?),
            2 => AmbiguityScope::Database(cur.u32()?),
            other => return Err(cur.corrupt(format!("unknown ambiguity scope {other}"))),
        };
        let from_lsn = cur.u64()?;
        let through_lsn = cur.u64()?;
        let reason = match cur.u8()? {
            0 => AmbiguityReason::UnknownAffectedRelation,
            1 => AmbiguityReason::UnknownMutationPosition,
            2 => AmbiguityReason::MultipleIncompatibleLayouts,
            3 => AmbiguityReason::NeverVisibleGeneration,
            4 => AmbiguityReason::IncompleteInvalidation,
            other => return Err(cur.corrupt(format!("unknown ambiguity reason {other}"))),
        };
        ambiguities.push(Arc::new(Ambiguity {
            scope,
            from_lsn,
            through_lsn,
            reason,
        }));
    }
    let n = cur.u32()? as usize;
    let mut observations = Vec::with_capacity(n);
    for _ in 0..n {
        let oid = match cur.u8()? {
            0 => None,
            _ => Some(cur.u32()?),
        };
        let rfn = match cur.u8()? {
            0 => None,
            _ => Some(RelFileNode {
                spc_node: cur.u32()?,
                db_node: cur.u32()?,
                rel_node: cur.u32()?,
            }),
        };
        let first_touch_lsn = cur.u64()?;
        let smgr_create_lsn = match cur.u8()? {
            0 => None,
            _ => Some(cur.u64()?),
        };
        let kind = match cur.u8()? {
            0 => ObservationKind::AffectedOid,
            1 => ObservationKind::SmgrCreate,
            2 => ObservationKind::FullScan,
            other => return Err(cur.corrupt(format!("unknown observation kind {other}"))),
        };
        observations.push(RelationObservation {
            oid,
            rfn,
            first_touch_lsn,
            smgr_create_lsn,
            kind,
        });
    }
    Ok(BatchRecord {
        captured_at,
        commit_lsn,
        observations,
        ambiguities,
        entries,
    })
}

fn decode_descriptor(cur: &mut Cur<'_>) -> Result<RelDescriptor> {
    let rfn = RelFileNode {
        spc_node: cur.u32()?,
        db_node: cur.u32()?,
        rel_node: cur.u32()?,
    };
    let oid = cur.u32()?;
    let toast_oid = cur.u32()?;
    let namespace_oid = cur.u32()?;
    let namespace = cur.string()?;
    let name = cur.string()?;
    let kind = cur.charlike()?;
    let persistence = cur.charlike()?;
    let replident = match cur.u8()? {
        0 => ReplIdent::Default {
            pk_attnums: decode_opt_attnums(cur)?,
        },
        1 => ReplIdent::Nothing,
        2 => ReplIdent::Full {
            pk_attnums: decode_opt_attnums(cur)?,
        },
        3 => ReplIdent::UsingIndex {
            index_oid: cur.u32()?,
            key_attnums: decode_attnums(cur)?,
        },
        other => return Err(cur.corrupt(format!("unknown replident tag {other}"))),
    };
    let n = cur.u32()? as usize;
    let mut attributes = Vec::with_capacity(n);
    for _ in 0..n {
        attributes.push(RelAttr {
            attnum: cur.i16()?,
            name: cur.string()?,
            type_oid: cur.u32()?,
            typmod: cur.i32()?,
            not_null: cur.boolish()?,
            dropped: cur.boolish()?,
            type_name: cur.string()?,
            type_byval: cur.boolish()?,
            type_len: cur.i16()?,
            type_align: cur.charlike()?,
            type_storage: cur.charlike()?,
            missing_text: cur.opt_string()?,
        });
    }
    Ok(RelDescriptor {
        rfn,
        oid,
        toast_oid,
        namespace_oid,
        rel_name: RelName::new(&namespace, &name),
        kind,
        persistence,
        replident,
        attributes,
    })
}

fn decode_opt_attnums(cur: &mut Cur<'_>) -> Result<Option<Vec<i16>>> {
    Ok(match cur.u8()? {
        0 => None,
        _ => Some(decode_attnums(cur)?),
    })
}

fn decode_attnums(cur: &mut Cur<'_>) -> Result<Vec<i16>> {
    let n = cur.u32()? as usize;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        out.push(cur.i16()?);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ident() -> DescLogIdentity {
        DescLogIdentity {
            pg_major: 17,
            system_id: "7300000000000000001".into(),
            timeline: 1,
            db_oid: 5,
            wal_seg_size: 16 * 1024 * 1024,
        }
    }

    fn rfn(rel_node: u32) -> RelFileNode {
        RelFileNode {
            spc_node: 1663,
            db_node: 5,
            rel_node,
        }
    }

    fn desc(oid: Oid, rel_node: u32, extra_col: bool) -> Arc<RelDescriptor> {
        let mut attributes = vec![
            RelAttr {
                attnum: 1,
                name: "id".into(),
                type_oid: 20,
                typmod: -1,
                not_null: true,
                dropped: false,
                type_name: "int8".into(),
                type_byval: true,
                type_len: 8,
                type_align: 'd',
                type_storage: 'p',
                missing_text: None,
            },
            // Dropped slot: physical layout retained, type link severed
            RelAttr {
                attnum: 2,
                name: "........pg.dropped.2........".into(),
                type_oid: 0,
                typmod: -1,
                not_null: false,
                dropped: true,
                type_name: String::new(),
                type_byval: false,
                type_len: -1,
                type_align: 'i',
                type_storage: 'x',
                missing_text: None,
            },
        ];
        if extra_col {
            attributes.push(RelAttr {
                attnum: 3,
                name: "extra".into(),
                type_oid: 23,
                typmod: -1,
                not_null: false,
                dropped: false,
                type_name: "int4".into(),
                type_byval: true,
                type_len: 4,
                type_align: 'i',
                type_storage: 'p',
                missing_text: Some("42".into()),
            });
        }
        Arc::new(RelDescriptor {
            rfn: rfn(rel_node),
            oid,
            toast_oid: 0,
            namespace_oid: 2200,
            rel_name: RelName::new("public", &format!("t{oid}")),
            kind: 'r',
            persistence: 'p',
            replident: if extra_col {
                ReplIdent::UsingIndex {
                    index_oid: 900,
                    key_attnums: vec![1, 3],
                }
            } else {
                ReplIdent::Default {
                    pk_attnums: Some(vec![1]),
                }
            },
            attributes,
        })
    }

    fn present(valid_from: u64, d: &Arc<RelDescriptor>) -> Arc<LogEntry> {
        Arc::new(LogEntry {
            valid_from,
            oid: d.oid,
            rfn: d.rfn,
            value: LogValue::Present(d.clone()),
        })
    }

    fn tombstone(valid_from: u64, oid: Oid, rel_node: u32, value: LogValue) -> Arc<LogEntry> {
        Arc::new(LogEntry {
            valid_from,
            oid,
            rfn: rfn(rel_node),
            value,
        })
    }

    fn batch(captured_at: u64, entries: Vec<Arc<LogEntry>>) -> BatchRecord {
        BatchRecord {
            captured_at,
            commit_lsn: 0,
            observations: Vec::new(),
            ambiguities: Vec::new(),
            entries,
        }
    }

    fn amb(scope: AmbiguityScope, from_lsn: u64, through_lsn: u64) -> Arc<Ambiguity> {
        Arc::new(Ambiguity {
            scope,
            from_lsn,
            through_lsn,
            reason: AmbiguityReason::UnknownMutationPosition,
        })
    }

    async fn open(dir: &Path) -> DescriptorLog {
        DescriptorLog::open(dir, ident()).await.unwrap()
    }

    #[tokio::test(flavor = "current_thread")]
    async fn round_trip_reopen() {
        let tmp = tempfile::tempdir().unwrap();
        let d1 = desc(101, 6001, false);
        let d2 = desc(101, 6001, true);
        {
            let log = open(tmp.path()).await;
            assert!(log.is_empty());
            log.seed(batch(100, vec![present(90, &d1)]), 100)
                .await
                .unwrap();
            log.append_batch(batch(200, vec![present(180, &d2)]))
                .await
                .unwrap();
            // Stub: boundary with no shape change
            log.append_batch(batch(300, vec![])).await.unwrap();
        }
        let log = open(tmp.path()).await;
        assert!(!log.is_empty());
        assert_eq!(log.covered_through(), 100);
        assert_eq!(log.head(), 300);
        match log.descriptor_at(rfn(6001), 179) {
            LookupResult::Present(d) => assert_eq!(d, d1),
            other => panic!("expected d1, got {other:?}"),
        }
        match log.descriptor_at(rfn(6001), 180) {
            LookupResult::Present(d) => assert_eq!(d, d2),
            other => panic!("expected d2, got {other:?}"),
        }
        match log.descriptor_by_oid_at(101, u64::MAX) {
            LookupResult::Present(d) => assert_eq!(d, d2),
            other => panic!("expected d2 by oid, got {other:?}"),
        }
        assert_eq!(log.descriptor_at(rfn(6001), 89), LookupResult::NotCovered);
        assert!(log.batch_at(300).unwrap().entries.is_empty());
        assert!(log.batch_at(150).is_none());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn rotation_retires_old_rfn_interval() {
        let tmp = tempfile::tempdir().unwrap();
        let old = desc(42, 7000, false);
        let mut new_inner = (*desc(42, 7001, false)).clone();
        new_inner.rfn = rfn(7001);
        let new = Arc::new(new_inner);
        let log = open(tmp.path()).await;
        log.seed(batch(100, vec![present(90, &old)]), 100)
            .await
            .unwrap();
        log.append_batch(batch(
            210,
            vec![
                tombstone(200, 42, 7000, LogValue::Retired),
                present(200, &new),
            ],
        ))
        .await
        .unwrap();
        assert!(matches!(
            log.descriptor_at(rfn(7000), 199),
            LookupResult::Present(_)
        ));
        assert_eq!(log.descriptor_at(rfn(7000), 200), LookupResult::Retired);
        assert!(matches!(
            log.descriptor_at(rfn(7001), 200),
            LookupResult::Present(_)
        ));
        assert_eq!(log.descriptor_at(rfn(7001), 199), LookupResult::NotCovered);
        // by_oid stays Present across the rotation
        assert!(matches!(
            log.descriptor_by_oid_at(42, 300),
            LookupResult::Present(_)
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn dropped_tombstone_and_foreign_db() {
        let tmp = tempfile::tempdir().unwrap();
        let d = desc(55, 8000, false);
        let log = open(tmp.path()).await;
        log.seed(batch(100, vec![present(90, &d)]), 100)
            .await
            .unwrap();
        log.append_batch(batch(
            200,
            vec![tombstone(200, 55, 8000, LogValue::Dropped)],
        ))
        .await
        .unwrap();
        assert!(matches!(
            log.descriptor_at(rfn(8000), 150),
            LookupResult::Present(_)
        ));
        assert_eq!(log.descriptor_at(rfn(8000), 200), LookupResult::Dropped);
        assert_eq!(log.descriptor_by_oid_at(55, 200), LookupResult::Dropped);
        let foreign = RelFileNode {
            spc_node: 1663,
            db_node: 999,
            rel_node: 8000,
        };
        assert_eq!(log.descriptor_at(foreign, 150), LookupResult::ForeignDb);
        // Shared-catalog db_node 0 is local, not foreign
        let shared = RelFileNode {
            spc_node: 1664,
            db_node: 0,
            rel_node: 8000,
        };
        assert_eq!(log.descriptor_at(shared, 150), LookupResult::NotCovered);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn same_db_rel_across_tablespaces_stay_distinct() {
        let tmp = tempfile::tempdir().unwrap();
        // Post-wraparound relfilenumber reuse: two live rels share
        // (db, rel), differ only in tablespace
        let a = desc(42, 7000, false);
        let mut b_inner = (*desc(84, 7000, true)).clone();
        b_inner.rfn.spc_node = 9999;
        let b = Arc::new(b_inner);
        let log = open(tmp.path()).await;
        // One batch, shared valid_from + captured_at: insertion order must
        // not decide which relation a locator resolves to
        log.append_batch(batch(100, vec![present(90, &a), present(90, &b)]))
            .await
            .unwrap();
        match log.descriptor_at(a.rfn, 150) {
            LookupResult::Present(d) => assert_eq!(d, a),
            other => panic!("expected a, got {other:?}"),
        }
        match log.descriptor_at(b.rfn, 150) {
            LookupResult::Present(d) => assert_eq!(d, b),
            other => panic!("expected b, got {other:?}"),
        }
        // Tombstoning one chain leaves the sibling untouched
        log.append_batch(batch(
            200,
            vec![Arc::new(LogEntry {
                valid_from: 200,
                oid: 84,
                rfn: b.rfn,
                value: LogValue::Dropped,
            })],
        ))
        .await
        .unwrap();
        assert_eq!(log.descriptor_at(b.rfn, 200), LookupResult::Dropped);
        match log.descriptor_at(a.rfn, 200) {
            LookupResult::Present(d) => assert_eq!(d, a),
            other => panic!("expected a to survive b's drop, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn torn_tail_truncates_and_appends_resume() {
        let tmp = tempfile::tempdir().unwrap();
        let d = desc(60, 8100, false);
        {
            let log = open(tmp.path()).await;
            log.append_batch(batch(100, vec![present(90, &d)]))
                .await
                .unwrap();
            log.append_batch(batch(200, vec![])).await.unwrap();
        }
        let path = tmp.path().join(TAIL_FILE);
        let len = std::fs::metadata(&path).unwrap().len();
        let f = std::fs::OpenOptions::new().write(true).open(&path).unwrap();
        f.set_len(len - 3).unwrap();
        drop(f);
        {
            let log = open(tmp.path()).await;
            assert!(log.batch_at(100).is_some());
            assert!(log.batch_at(200).is_none(), "torn frame dropped");
            log.append_batch(batch(300, vec![])).await.unwrap();
        }
        let log = open(tmp.path()).await;
        assert!(log.batch_at(100).is_some());
        assert!(log.batch_at(300).is_some());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn interior_corruption_fails_closed() {
        let tmp = tempfile::tempdir().unwrap();
        let d = desc(61, 8200, false);
        {
            let log = open(tmp.path()).await;
            log.append_batch(batch(100, vec![present(90, &d)]))
                .await
                .unwrap();
            log.append_batch(batch(200, vec![])).await.unwrap();
        }
        let path = tmp.path().join(TAIL_FILE);
        let mut bytes = std::fs::read(&path).unwrap();
        // Flip one byte inside the first frame's body (past header + frame
        // header) while the second frame follows intact
        let hdr = encode_header(&ident()).len();
        bytes[hdr + 8 + 4] ^= 0xff;
        std::fs::write(&path, &bytes).unwrap();
        let err = DescriptorLog::open(tmp.path(), ident()).await.unwrap_err();
        assert!(
            matches!(err, DescLogError::Corrupt { .. }),
            "expected Corrupt, got {err:?}"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn identity_and_version_mismatch_fatal() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let log = open(tmp.path()).await;
            log.append_batch(batch(100, vec![])).await.unwrap();
        }
        let mut other = ident();
        other.db_oid = 6;
        let err = DescriptorLog::open(tmp.path(), other).await.unwrap_err();
        assert!(matches!(
            err,
            DescLogError::ForeignLog {
                field: "db_oid",
                ..
            }
        ));

        let path = tmp.path().join(TAIL_FILE);
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[2] = 9; // version u16 LE low byte
        std::fs::write(&path, &bytes).unwrap();
        let err = DescriptorLog::open(tmp.path(), ident()).await.unwrap_err();
        assert!(matches!(err, DescLogError::Version(9)));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn append_idempotent_divergent_fails() {
        let tmp = tempfile::tempdir().unwrap();
        let d = desc(70, 8300, false);
        let log = open(tmp.path()).await;
        log.append_batch(batch(100, vec![present(90, &d)]))
            .await
            .unwrap();
        // Byte-identical replay no-ops
        log.append_batch(batch(100, vec![present(90, &d)]))
            .await
            .unwrap();
        assert_eq!(log.batch_at(100).unwrap().entries.len(), 1);
        let err = log.append_batch(batch(100, vec![])).await.unwrap_err();
        assert!(matches!(err, DescLogError::Corrupt { .. }));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn gc_supersede_keeps_active_at_floor() {
        let tmp = tempfile::tempdir().unwrap();
        let d1 = desc(80, 8400, false);
        let d2 = desc(80, 8400, true);
        let log = open(tmp.path()).await;
        log.append_batch(batch(20, vec![present(10, &d1)]))
            .await
            .unwrap();
        log.append_batch(batch(60, vec![present(50, &d2)]))
            .await
            .unwrap();
        log.force_gc(100).await.unwrap();
        assert_eq!(log.floor_at_write(), 100);
        // Active-at-floor survives, superseded predecessor dropped
        match log.descriptor_at(rfn(8400), 150) {
            LookupResult::Present(d) => assert_eq!(d, d2),
            other => panic!("expected d2, got {other:?}"),
        }
        assert_eq!(log.descriptor_at(rfn(8400), 20), LookupResult::NotCovered);
        // Batches at/below floor exist only as entry carriers
        assert!(log.batch_at(20).is_none());
        assert!(log.batch_at(60).is_some());
        // Survives reopen from ckpt
        drop(log);
        let log = open(tmp.path()).await;
        match log.descriptor_at(rfn(8400), 150) {
            LookupResult::Present(d) => assert_eq!(d, d2),
            other => panic!("expected d2 post-reopen, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn gc_drop_before_floor_no_resurrection() {
        let tmp = tempfile::tempdir().unwrap();
        let d = desc(81, 8500, false);
        let log = open(tmp.path()).await;
        log.append_batch(batch(20, vec![present(10, &d)]))
            .await
            .unwrap();
        log.append_batch(batch(60, vec![tombstone(60, 81, 8500, LogValue::Dropped)]))
            .await
            .unwrap();
        log.force_gc(100).await.unwrap();
        // Dropped at floor: whole chain gone, absence is inactive — never
        // the earlier Present
        assert_eq!(log.descriptor_at(rfn(8500), 300), LookupResult::NotCovered);
        assert_eq!(log.descriptor_by_oid_at(81, 300), LookupResult::NotCovered);
        drop(log);
        let log = open(tmp.path()).await;
        assert_eq!(log.descriptor_at(rfn(8500), 300), LookupResult::NotCovered);
        // Filenode reuse starts a fresh chain
        let mut reuse_inner = (*desc(82, 8500, false)).clone();
        reuse_inner.rfn = rfn(8500);
        let reuse = Arc::new(reuse_inner);
        log.append_batch(batch(400, vec![present(390, &reuse)]))
            .await
            .unwrap();
        assert!(matches!(
            log.descriptor_at(rfn(8500), 400),
            LookupResult::Present(_)
        ));
        assert_eq!(log.descriptor_at(rfn(8500), 380), LookupResult::NotCovered);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn gc_retired_rotation_drops_old_chain() {
        let tmp = tempfile::tempdir().unwrap();
        let old = desc(83, 8600, false);
        let mut new_inner = (*desc(83, 8601, false)).clone();
        new_inner.rfn = rfn(8601);
        let new = Arc::new(new_inner);
        let log = open(tmp.path()).await;
        log.append_batch(batch(20, vec![present(10, &old)]))
            .await
            .unwrap();
        log.append_batch(batch(
            60,
            vec![
                tombstone(50, 83, 8600, LogValue::Retired),
                present(50, &new),
            ],
        ))
        .await
        .unwrap();
        log.force_gc(100).await.unwrap();
        assert_eq!(log.descriptor_at(rfn(8600), 300), LookupResult::NotCovered);
        assert!(matches!(
            log.descriptor_at(rfn(8601), 300),
            LookupResult::Present(_)
        ));
        assert!(matches!(
            log.descriptor_by_oid_at(83, 300),
            LookupResult::Present(_)
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn gc_keeps_batches_above_floor_whole() {
        let tmp = tempfile::tempdir().unwrap();
        let d1 = desc(84, 8700, false);
        let d2 = desc(84, 8700, true);
        let log = open(tmp.path()).await;
        log.append_batch(batch(20, vec![present(10, &d1)]))
            .await
            .unwrap();
        log.append_batch(batch(150, vec![present(140, &d2)]))
            .await
            .unwrap();
        log.append_batch(batch(200, vec![])).await.unwrap();
        log.force_gc(100).await.unwrap();
        // Above floor: batch + stub retained for boot replay
        assert_eq!(log.batch_at(150).unwrap().entries.len(), 1);
        assert!(log.batch_at(200).unwrap().entries.is_empty());
        // At-floor active entry retained below
        match log.descriptor_at(rfn(8700), 100) {
            LookupResult::Present(d) => assert_eq!(d, d1),
            other => panic!("expected d1 at floor, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn predecessor_before_uses_history_not_head() {
        let tmp = tempfile::tempdir().unwrap();
        let v1 = desc(85, 8800, false);
        let v2 = desc(85, 8800, true);
        let mut v3_inner = (*desc(85, 8800, true)).clone();
        v3_inner.rel_name = RelName::new("public", "renamed");
        let v3 = Arc::new(v3_inner);
        let log = open(tmp.path()).await;
        log.append_batch(batch(100, vec![present(90, &v1)]))
            .await
            .unwrap();
        log.append_batch(batch(200, vec![present(190, &v2)]))
            .await
            .unwrap();
        log.append_batch(batch(300, vec![present(290, &v3)]))
            .await
            .unwrap();
        // Replaying the middle boundary diffs against v1 even though the
        // loaded head is v3
        let pred = log.predecessor_before(85, 200).unwrap();
        assert_eq!(pred.value, LogValue::Present(v1.clone()));
        assert!(log.predecessor_before(85, 100).is_none());
        let pred = log.predecessor_before(85, 300).unwrap();
        assert_eq!(pred.value, LogValue::Present(v2.clone()));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn seed_rules() {
        let tmp = tempfile::tempdir().unwrap();
        let d = desc(86, 8900, false);
        let log = open(tmp.path()).await;
        log.seed(batch(500, vec![present(400, &d)]), 500)
            .await
            .unwrap();
        assert_eq!(log.covered_through(), 500);
        // Seed entry answers the aligned-prefix re-read
        assert!(matches!(
            log.descriptor_at(rfn(8900), 400),
            LookupResult::Present(_)
        ));
        let err = log.seed(batch(600, vec![]), 600).await.unwrap_err();
        assert!(matches!(err, DescLogError::Corrupt { .. }));
        drop(log);
        let log = open(tmp.path()).await;
        assert_eq!(log.covered_through(), 500);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn oversize_frame_len_fails_when_interior() {
        let tmp = tempfile::tempdir().unwrap();
        {
            let log = open(tmp.path()).await;
            log.append_batch(batch(100, vec![])).await.unwrap();
        }
        let path = tmp.path().join(TAIL_FILE);
        let mut bytes = std::fs::read(&path).unwrap();
        let hdr = encode_header(&ident()).len();
        // Claimed len fits nothing sane but extends past EOF → torn, repaired
        bytes[hdr..hdr + 4].copy_from_slice(&u32::MAX.to_le_bytes());
        std::fs::write(&path, &bytes).unwrap();
        let log = open(tmp.path()).await;
        assert!(log.batch_at(100).is_none());
        drop(log);
        // Oversize len with enough trailing bytes to "fit" → interior garbage
        let mut bytes = std::fs::read(&path).unwrap();
        bytes.truncate(hdr);
        let huge = (MAX_FRAME + 1) as usize;
        bytes.extend_from_slice(&(MAX_FRAME + 1).to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes());
        bytes.resize(hdr + 8 + huge, 0);
        std::fs::write(&path, &bytes).unwrap();
        let err = DescriptorLog::open(tmp.path(), ident()).await.unwrap_err();
        assert!(matches!(err, DescLogError::Corrupt { .. }));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ckpt_tail_overlap_dedupes_divergent_fails() {
        let tmp = tempfile::tempdir().unwrap();
        let d = desc(87, 9000, false);
        let b = batch(100, vec![present(90, &d)]);
        {
            let log = open(tmp.path()).await;
            log.append_batch(b.clone()).await.unwrap();
            log.force_gc(0).await.unwrap();
        }
        // Simulate crash between GC's ckpt write and tail truncate: the
        // batch reappears in the tail
        let path = tmp.path().join(TAIL_FILE);
        let mut bytes = std::fs::read(&path).unwrap();
        push_frame(&mut bytes, &encode_batch(&Arc::new(b)));
        std::fs::write(&path, &bytes).unwrap();
        {
            let log = open(tmp.path()).await;
            assert_eq!(log.batch_at(100).unwrap().entries.len(), 1);
        }
        // Divergent duplicate fails closed
        let mut bytes = std::fs::read(&path).unwrap();
        push_frame(&mut bytes, &encode_batch(&Arc::new(batch(100, vec![]))));
        std::fs::write(&path, &bytes).unwrap();
        let err = DescriptorLog::open(tmp.path(), ident()).await.unwrap_err();
        assert!(matches!(err, DescLogError::Corrupt { .. }));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn present_oids_and_active_set() {
        let tmp = tempfile::tempdir().unwrap();
        let d1 = desc(88, 9100, false);
        let d2 = desc(89, 9101, false);
        let log = open(tmp.path()).await;
        log.append_batch(batch(100, vec![present(90, &d1), present(90, &d2)]))
            .await
            .unwrap();
        log.append_batch(batch(
            200,
            vec![tombstone(200, 89, 9101, LogValue::Dropped)],
        ))
        .await
        .unwrap();
        let mut oids = log.present_oids();
        oids.sort_unstable();
        assert_eq!(oids, vec![88]);
        assert_eq!(log.active_present_at(150).len(), 2);
        assert_eq!(log.active_present_at(250).len(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ambiguity_precedes_chain_half_open() {
        let tmp = tempfile::tempdir().unwrap();
        let d1 = desc(90, 9200, false);
        let d2 = desc(90, 9200, true);
        let log = open(tmp.path()).await;
        log.append_batch(batch(100, vec![present(90, &d1)]))
            .await
            .unwrap();
        // Uncertain in-place change over [200, 300): final version Present
        // at commit next_lsn for future rows
        let mut b = batch(300, vec![present(300, &d2)]);
        b.ambiguities
            .push(amb(AmbiguityScope::Rfn(rfn(9200)), 200, 300));
        log.append_batch(b).await.unwrap();
        match log.descriptor_at(rfn(9200), 199) {
            LookupResult::Present(d) => assert_eq!(d, d1),
            other => panic!("expected d1 before interval, got {other:?}"),
        }
        // [from, through): from covered, through not
        assert!(matches!(
            log.descriptor_at(rfn(9200), 200),
            LookupResult::Ambiguous(_)
        ));
        assert!(matches!(
            log.descriptor_at(rfn(9200), 299),
            LookupResult::Ambiguous(_)
        ));
        match log.descriptor_at(rfn(9200), 300) {
            LookupResult::Present(d) => assert_eq!(d, d2),
            other => panic!("expected d2 at through_lsn, got {other:?}"),
        }
        // Chain entry inside the interval stays shadowed even though it
        // exists: ambiguity wins over Present
        assert!(matches!(
            log.descriptor_at(rfn(9200), 250),
            LookupResult::Ambiguous(_)
        ));
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ambiguity_scopes_oid_and_database() {
        let tmp = tempfile::tempdir().unwrap();
        let d = desc(91, 9300, false);
        let log = open(tmp.path()).await;
        let mut b = batch(100, vec![present(90, &d)]);
        b.ambiguities.push(amb(AmbiguityScope::Oid(91), 200, 250));
        b.ambiguities
            .push(amb(AmbiguityScope::Database(5), 400, 450));
        log.append_batch(b).await.unwrap();
        // Oid scope hits by-oid lookup only
        assert!(matches!(
            log.descriptor_by_oid_at(91, 220),
            LookupResult::Ambiguous(_)
        ));
        assert!(matches!(
            log.descriptor_at(rfn(9300), 220),
            LookupResult::Present(_)
        ));
        // Database scope hits both
        assert!(matches!(
            log.descriptor_at(rfn(9300), 420),
            LookupResult::Ambiguous(_)
        ));
        assert!(matches!(
            log.descriptor_by_oid_at(91, 420),
            LookupResult::Ambiguous(_)
        ));
        // Shared-catalog db_node 0 skips database-scoped ambiguity
        let shared = RelFileNode {
            spc_node: 1664,
            db_node: 0,
            rel_node: 9300,
        };
        assert_eq!(log.descriptor_at(shared, 420), LookupResult::NotCovered);
        // Foreign db answers before ambiguity
        let foreign = RelFileNode {
            spc_node: 1663,
            db_node: 999,
            rel_node: 9300,
        };
        assert_eq!(log.descriptor_at(foreign, 420), LookupResult::ForeignDb);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn ambiguity_round_trip_and_idempotent_append() {
        let tmp = tempfile::tempdir().unwrap();
        let d = desc(92, 9400, false);
        let mut b = batch(100, vec![present(90, &d)]);
        b.ambiguities
            .push(amb(AmbiguityScope::Rfn(rfn(9400)), 50, 100));
        {
            let log = open(tmp.path()).await;
            log.append_batch(b.clone()).await.unwrap();
            // Byte-identical replay no-ops
            log.append_batch(b.clone()).await.unwrap();
            // Same entries, divergent ambiguities fail closed
            let mut divergent = b.clone();
            divergent.ambiguities = vec![amb(AmbiguityScope::Rfn(rfn(9400)), 50, 120)];
            let err = log.append_batch(divergent).await.unwrap_err();
            assert!(matches!(err, DescLogError::Corrupt { .. }));
        }
        let log = open(tmp.path()).await;
        assert!(matches!(
            log.descriptor_at(rfn(9400), 60),
            LookupResult::Ambiguous(a) if a.through_lsn == 100
        ));
        assert_eq!(
            log.stats_handle()
                .ambiguous_unknown_position
                .load(std::sync::atomic::Ordering::Relaxed),
            1,
            "reason-labelled counter",
        );
        assert!(matches!(
            log.descriptor_at(rfn(9400), 100),
            LookupResult::Present(_)
        ));
        assert_eq!(log.batch_at(100).unwrap().ambiguities.len(), 1);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn gc_keeps_ambiguity_spanning_floor() {
        let tmp = tempfile::tempdir().unwrap();
        let d = desc(93, 9500, false);
        let log = open(tmp.path()).await;
        let mut b = batch(60, vec![present(10, &d)]);
        // Wholly below floor: dead, no lookup lands under it
        b.ambiguities
            .push(amb(AmbiguityScope::Rfn(rfn(9500)), 20, 40));
        // Spans floor: still answers lookups at/above it
        b.ambiguities
            .push(amb(AmbiguityScope::Rfn(rfn(9500)), 80, 150));
        log.append_batch(b).await.unwrap();
        log.force_gc(100).await.unwrap();
        assert!(matches!(
            log.descriptor_at(rfn(9500), 120),
            LookupResult::Ambiguous(a) if a.from_lsn == 80
        ));
        assert_eq!(log.batch_at(60).unwrap().ambiguities.len(), 1);
        drop(log);
        let log = open(tmp.path()).await;
        assert!(matches!(
            log.descriptor_at(rfn(9500), 120),
            LookupResult::Ambiguous(_)
        ));
        match log.descriptor_at(rfn(9500), 160) {
            LookupResult::Present(d2) => assert_eq!(d2, d),
            other => panic!("expected retained Present past interval, got {other:?}"),
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn old_format_rejected_both_files() {
        let tmp = tempfile::tempdir().unwrap();
        let d = desc(95, 9700, false);
        {
            let log = open(tmp.path()).await;
            log.append_batch(batch(100, vec![present(90, &d)]))
                .await
                .unwrap();
            log.force_gc(0).await.unwrap();
        }
        // Pre-ambiguity v1 dir rejects at open, ckpt or tail alike; the
        // explicit epoch reset (--ignore-cursor / re-bootstrap) is the only
        // way forward, never silent translation
        for file in [TAIL_FILE, CKPT_FILE] {
            let path = tmp.path().join(file);
            let orig = std::fs::read(&path).unwrap();
            let mut bytes = orig.clone();
            bytes[2] = 1; // version u16 LE low byte
            std::fs::write(&path, &bytes).unwrap();
            let err = DescriptorLog::open(tmp.path(), ident()).await.unwrap_err();
            assert!(matches!(err, DescLogError::Version(1)), "{file}");
            std::fs::write(&path, &orig).unwrap();
        }
        let log = open(tmp.path()).await;
        assert!(!log.is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn evidence_round_trip_divergence_fails() {
        let tmp = tempfile::tempdir().unwrap();
        let d = desc(94, 9600, false);
        let mut b = batch(200, vec![present(190, &d)]);
        b.commit_lsn = 180;
        b.observations = vec![
            RelationObservation {
                oid: Some(94),
                rfn: None,
                first_touch_lsn: 185,
                smgr_create_lsn: None,
                kind: ObservationKind::AffectedOid,
            },
            RelationObservation {
                oid: None,
                rfn: Some(rfn(9600)),
                first_touch_lsn: 185,
                smgr_create_lsn: Some(186),
                kind: ObservationKind::SmgrCreate,
            },
        ];
        {
            let log = open(tmp.path()).await;
            log.append_batch(b.clone()).await.unwrap();
            // Identical evidence no-ops
            log.append_batch(b.clone()).await.unwrap();
        }
        let log = open(tmp.path()).await;
        let stored = log.batch_at(200).unwrap();
        assert_eq!(*stored, b);
        assert_eq!(stored.digest(), b.digest());
        // Same final entries, divergent evidence fails closed
        let mut divergent = b.clone();
        divergent.observations[0].first_touch_lsn = 184;
        assert_ne!(divergent.digest(), b.digest());
        let err = log.append_batch(divergent).await.unwrap_err();
        assert!(matches!(err, DescLogError::Corrupt { .. }));
        let mut divergent = b.clone();
        divergent.commit_lsn = 181;
        let err = log.append_batch(divergent).await.unwrap_err();
        assert!(matches!(err, DescLogError::Corrupt { .. }));
    }
}
