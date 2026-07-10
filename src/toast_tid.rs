//! TID death tracking for TOAST chunk GC (`plans/TOAST.md`).
//!
//! PG deletes superseded toast chunks by TID (`heap_toast_delete`), while
//! the chunk store is keyed `(chunk_id, chunk_seq)`. This module keeps the
//! bridge: a persistent map from every chunk tuple's TID to its `chunk_id`,
//! built from observed chunk INSERTs, so a later TID-keyed DELETE resolves
//! to a dead `(chunk_id, death commit LSN)` with no source PG query.
//! `toast_delete_datum` deletes every chunk of a value in one xact, so each
//! sibling delete resolves against its own entry and the value's death is
//! deduplicated on `(relid, chunk_id, commit LSN)`; per-chunk (not
//! per-value) mapping is what keeps `toast_deaths_unresolved` an exact
//! leak signal rather than sibling noise. Map cost is one entry per stored
//! chunk (~2KB of value bytes each).
//!
//! Death LSN is an exact generation boundary. `va_valueid` reuse
//! (`GetNewOidWithIndex` checks only the live toast index) re-puts a dead
//! id's chunks under a higher commit LSN; deleting `lsn <= death_lsn`
//! collects the dead generation while any rebirth survives, even mid-GC.
//!
//! ## Journal
//!
//! Append-only file of fixed-size records (births, resolved deaths, GC
//! completions), rebuilt into memory at startup, torn tail truncated.
//! Appends fsync per applied commit *before* the commit's rows dispatch to
//! the emitter, so `emitter_ack` never covers a commit whose events aren't
//! durable: replay from ack re-observes anything lost. Re-applied events
//! are no-ops (birth already mapped, death already pending), so replay
//! never double-journals. Compaction (live map + pending deaths rewritten,
//! atomic rename) runs after GC when the file outgrows its live state.
//!
//! ## Coverage
//!
//! Deaths are collected only for tracked births. Untracked classes leak
//! (storage-only, never a correctness fault) and tick
//! `toast_deaths_unresolved`: values whose chunks predate the store /
//! journal, toast rels rewritten by `VACUUM FULL`/`CLUSTER` (new heap
//! arrives as FPIs, no tuple-level inserts), TRUNCATE / DROP of the owning
//! table (no per-tuple deletes at all). A birth landing on an occupied TID
//! replaces the stale mapping but never implies death: relation rewrites
//! can reuse numeric TIDs while the prior value remains live.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::Ordering;

use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

use crate::ch_emitter::EmitterStats;
use crate::toast::ChunkStoreError;

/// One toast-rel tuple event, in WAL order within a commit. Ordering
/// matters: an xact can insert a value then delete it (INSERT + UPDATE of
/// the same row toasts twice), so a death may target a same-commit birth.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TidEvent {
    /// Chunk INSERT observed at this TID.
    Birth {
        toast_relid: u32,
        blkno: u32,
        offnum: u16,
        value_id: u32,
    },
    /// Toast tuple DELETE observed at this TID.
    Death {
        toast_relid: u32,
        blkno: u32,
        offnum: u16,
    },
}

/// A resolved chunk death: `value_id`'s rows with `lsn <= death_lsn` are
/// the dead generation. One entry per dead chunk TID (all sharing the
/// value + commit LSN); keeping every TID is what lets a replayed sibling
/// delete recognise itself instead of counting as an untracked leak. GC
/// applies once `emitter_ack >= death_lsn`: replay re-decode starts at ack
/// and every record referencing the value precedes its death, so no fetch
/// can want the collected generation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ValueDeath {
    pub toast_relid: u32,
    pub value_id: u32,
    pub death_lsn: u64,
    /// The dead chunk's TID, for journal rebuild + replay dedup.
    pub blkno: u32,
    pub offnum: u16,
}

const JOURNAL_MAGIC: [u8; 4] = *b"WTID";
const JOURNAL_VERSION: u16 = 1;
/// Magic + version u16 + reserved u16.
const JOURNAL_HEADER: usize = 8;

const TAG_BIRTH: u8 = 1;
const TAG_DEATH: u8 = 2;
const TAG_GC_DONE: u8 = 3;
/// Every record: tag u8 + relid u32 + blkno u32 + offnum u16 + value_id
/// u32 + lsn u64. Fixed size keeps torn-tail truncation trivial.
const REC_LEN: usize = 23;

/// Compaction floor; below this the rewrite isn't worth the IO.
const COMPACT_MIN_BYTES: u64 = 1 << 20;

fn push_rec(out: &mut Vec<u8>, tag: u8, relid: u32, blkno: u32, offnum: u16, value: u32, lsn: u64) {
    out.push(tag);
    out.extend_from_slice(&relid.to_le_bytes());
    out.extend_from_slice(&blkno.to_le_bytes());
    out.extend_from_slice(&offnum.to_le_bytes());
    out.extend_from_slice(&value.to_le_bytes());
    out.extend_from_slice(&lsn.to_le_bytes());
}

/// Resolved deaths awaiting `emitter_ack >= death_lsn` + store delete,
/// indexed for the hot path: `apply` dedups per event by TID and by
/// `(value, LSN)`, and one xact can carry millions of toast deletes —
/// linear scans would go quadratic inside a single commit.
#[derive(Default)]
struct PendingDeaths {
    /// Keyed by chunk TID. A TID can hold several entries: a reused line
    /// pointer can die again before the first death collects.
    by_tid: HashMap<(u32, u32, u16), Vec<ValueDeath>>,
    /// `(relid, value_id, death_lsn)` → entry count, for value-level
    /// resolved counting across sibling chunk deaths.
    by_value: HashMap<(u32, u32, u64), u32>,
    len: usize,
}

impl PendingDeaths {
    fn len(&self) -> usize {
        self.len
    }

    fn iter(&self) -> impl Iterator<Item = &ValueDeath> {
        self.by_tid.values().flatten()
    }

    fn tid_has_death(
        &self,
        relid: u32,
        blkno: u32,
        offnum: u16,
        value_id: u32,
        birth_lsn: u64,
    ) -> bool {
        self.by_tid.get(&(relid, blkno, offnum)).is_some_and(|v| {
            v.iter()
                .any(|d| d.value_id == value_id && d.death_lsn >= birth_lsn)
        })
    }

    fn tid_has_death_lsn(&self, relid: u32, blkno: u32, offnum: u16, death_lsn: u64) -> bool {
        self.by_tid
            .get(&(relid, blkno, offnum))
            .is_some_and(|v| v.iter().any(|d| d.death_lsn == death_lsn))
    }

    /// Insert; returns whether this is the value's first entry at this LSN.
    fn insert(&mut self, d: ValueDeath) -> bool {
        let count = self
            .by_value
            .entry((d.toast_relid, d.value_id, d.death_lsn))
            .or_insert(0);
        *count += 1;
        let first = *count == 1;
        self.by_tid
            .entry((d.toast_relid, d.blkno, d.offnum))
            .or_default()
            .push(d);
        self.len += 1;
        first
    }

    /// Remove one exact entry; absent is a no-op (replayed GC completion).
    fn remove(&mut self, d: &ValueDeath) {
        let key = (d.toast_relid, d.blkno, d.offnum);
        let Some(v) = self.by_tid.get_mut(&key) else {
            return;
        };
        let Some(i) = v.iter().position(|p| p == d) else {
            return;
        };
        v.swap_remove(i);
        if v.is_empty() {
            self.by_tid.remove(&key);
        }
        self.len -= 1;
        let vkey = (d.toast_relid, d.value_id, d.death_lsn);
        if let Some(count) = self.by_value.get_mut(&vkey) {
            *count -= 1;
            if *count == 0 {
                self.by_value.remove(&vkey);
            }
        }
    }

    fn ready(&self, ack_lsn: u64) -> Vec<ValueDeath> {
        self.iter()
            .filter(|d| d.death_lsn <= ack_lsn)
            .copied()
            .collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TidBirth {
    value_id: u32,
    birth_lsn: u64,
}

struct TrackerInner {
    /// relid → chunk TID → birth. One entry per live stored chunk.
    map: HashMap<u32, HashMap<(u32, u16), TidBirth>>,
    pending: PendingDeaths,
    journal: tokio::fs::File,
    journal_bytes: u64,
}

impl TrackerInner {
    fn live_entries(&self) -> u64 {
        self.map.values().map(|m| m.len() as u64).sum()
    }

    async fn append(&mut self, recs: &[u8]) -> Result<(), ChunkStoreError> {
        self.journal.write_all(recs).await?;
        self.journal.sync_data().await?;
        self.journal_bytes += recs.len() as u64;
        Ok(())
    }

    /// Remove the mapped entry and queue its chunk death. Returns whether
    /// this is the value's first pending entry at this commit — sibling
    /// chunk deletes share the value + LSN, and counting is value-level.
    fn resolve_death(
        &mut self,
        relid: u32,
        blkno: u32,
        offnum: u16,
        lsn: u64,
        recs: &mut Vec<u8>,
    ) -> bool {
        let Some(birth) = self
            .map
            .get_mut(&relid)
            .and_then(|m| m.remove(&(blkno, offnum)))
        else {
            return false;
        };
        let value_id = birth.value_id;
        push_rec(recs, TAG_DEATH, relid, blkno, offnum, value_id, lsn);
        self.pending.insert(ValueDeath {
            toast_relid: relid,
            value_id,
            death_lsn: lsn,
            blkno,
            offnum,
        })
    }
}

/// Persistent TID→value map + pending-death queue, shared between the
/// pipeline's commit apply ([`crate::toast::ToastResolver`]) and the GC
/// task ([`crate::toast_gc`]).
pub struct TidTracker {
    path: PathBuf,
    inner: Mutex<TrackerInner>,
    stats: Arc<EmitterStats>,
}

impl TidTracker {
    /// Open (or create) the journal and rebuild in-memory state.
    /// Synchronous: called once at daemon start.
    pub fn open(path: PathBuf, stats: Arc<EmitterStats>) -> Result<Self, ChunkStoreError> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let bytes = match std::fs::read(&path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(e.into()),
        };
        let mut map: HashMap<u32, HashMap<(u32, u16), TidBirth>> = HashMap::new();
        let mut pending = PendingDeaths::default();
        let mut clean = 0usize;
        if bytes.len() >= JOURNAL_HEADER {
            if bytes[..4] != JOURNAL_MAGIC {
                return Err(ChunkStoreError::Format(format!(
                    "toast tid journal {}: bad magic",
                    path.display()
                )));
            }
            let version = u16::from_le_bytes([bytes[4], bytes[5]]);
            if version != JOURNAL_VERSION {
                return Err(ChunkStoreError::Format(format!(
                    "toast tid journal {}: version {version}, expected {JOURNAL_VERSION}",
                    path.display()
                )));
            }
            clean = JOURNAL_HEADER;
            while clean + REC_LEN <= bytes.len() {
                let r = &bytes[clean..clean + REC_LEN];
                let relid = u32::from_le_bytes(r[1..5].try_into().unwrap());
                let blkno = u32::from_le_bytes(r[5..9].try_into().unwrap());
                let offnum = u16::from_le_bytes(r[9..11].try_into().unwrap());
                let value_id = u32::from_le_bytes(r[11..15].try_into().unwrap());
                let lsn = u64::from_le_bytes(r[15..23].try_into().unwrap());
                match r[0] {
                    TAG_BIRTH => {
                        map.entry(relid).or_default().insert(
                            (blkno, offnum),
                            TidBirth {
                                value_id,
                                birth_lsn: lsn,
                            },
                        );
                    }
                    TAG_DEATH => {
                        if let Some(m) = map.get_mut(&relid)
                            && m.get(&(blkno, offnum)).map(|b| b.value_id) == Some(value_id)
                        {
                            m.remove(&(blkno, offnum));
                        }
                        pending.insert(ValueDeath {
                            toast_relid: relid,
                            value_id,
                            death_lsn: lsn,
                            blkno,
                            offnum,
                        });
                    }
                    TAG_GC_DONE => pending.remove(&ValueDeath {
                        toast_relid: relid,
                        value_id,
                        death_lsn: lsn,
                        blkno,
                        offnum,
                    }),
                    other => {
                        return Err(ChunkStoreError::Format(format!(
                            "toast tid journal {}: unknown tag {other} at offset {clean}",
                            path.display()
                        )));
                    }
                }
                clean += REC_LEN;
            }
        }
        if clean == 0 {
            // Fresh (or shorter-than-header) file: write the header.
            let mut header = Vec::with_capacity(JOURNAL_HEADER);
            header.extend_from_slice(&JOURNAL_MAGIC);
            header.extend_from_slice(&JOURNAL_VERSION.to_le_bytes());
            header.extend_from_slice(&[0u8; 2]);
            std::fs::write(&path, &header)?;
            clean = JOURNAL_HEADER;
        }
        // Torn trailing record from a crash mid-append: drop it. Its commit
        // never acked (fsync precedes dispatch), so replay re-applies it.
        let file = std::fs::OpenOptions::new().append(true).open(&path)?;
        file.set_len(clean as u64)?;
        file.sync_data()?;
        Ok(Self {
            path,
            inner: Mutex::new(TrackerInner {
                map,
                pending,
                journal: tokio::fs::File::from_std(file),
                journal_bytes: clean as u64,
            }),
            stats,
        })
    }

    /// Apply one commit's events in WAL order, then journal + fsync them.
    /// Call before the commit's rows dispatch (its chunks already `put`),
    /// so ack implies durable tracking.
    pub async fn apply(&self, events: &[TidEvent], commit_lsn: u64) -> Result<(), ChunkStoreError> {
        if events.is_empty() {
            return Ok(());
        }
        let mut inner = self.inner.lock().await;
        let mut recs = Vec::new();
        let mut resolved = 0u64;
        let mut unresolved = 0u64;
        for ev in events {
            match *ev {
                TidEvent::Birth {
                    toast_relid,
                    blkno,
                    offnum,
                    value_id,
                } => {
                    let existing = inner
                        .map
                        .get(&toast_relid)
                        .and_then(|m| m.get(&(blkno, offnum)).copied());
                    match existing {
                        // Exact or older replay cannot replace newer mapping.
                        Some(b)
                            if b.birth_lsn > commit_lsn
                                || (b.birth_lsn == commit_lsn && b.value_id == value_id) =>
                        {
                            continue;
                        }
                        // Numeric TID collision may follow relation rewrite;
                        // replace mapping, leak prior value rather than delete it.
                        Some(_) => unresolved += 1,
                        // Replay re-observation of a birth whose death is
                        // already pending: re-arming the map would double the
                        // death when the delete replays right after
                        None if inner.pending.tid_has_death(
                            toast_relid,
                            blkno,
                            offnum,
                            value_id,
                            commit_lsn,
                        ) =>
                        {
                            continue;
                        }
                        None => {}
                    }
                    inner.map.entry(toast_relid).or_default().insert(
                        (blkno, offnum),
                        TidBirth {
                            value_id,
                            birth_lsn: commit_lsn,
                        },
                    );
                    push_rec(
                        &mut recs,
                        TAG_BIRTH,
                        toast_relid,
                        blkno,
                        offnum,
                        value_id,
                        commit_lsn,
                    );
                }
                TidEvent::Death {
                    toast_relid,
                    blkno,
                    offnum,
                } => {
                    let birth = inner
                        .map
                        .get(&toast_relid)
                        .and_then(|m| m.get(&(blkno, offnum)).copied());
                    if birth.is_some_and(|b| b.birth_lsn <= commit_lsn) {
                        if inner.resolve_death(toast_relid, blkno, offnum, commit_lsn, &mut recs) {
                            resolved += 1;
                        }
                    } else if birth.is_some() {
                        // Stale DELETE replay must not remove newer occupant.
                    } else if inner.pending.tid_has_death_lsn(
                        toast_relid,
                        blkno,
                        offnum,
                        commit_lsn,
                    ) {
                        // Replay re-observation of a still-pending chunk death
                    } else {
                        unresolved += 1;
                    }
                }
            }
        }
        if !recs.is_empty() {
            inner.append(&recs).await?;
        }
        drop(inner);
        if resolved > 0 {
            self.stats
                .toast_deaths_resolved
                .fetch_add(resolved, Ordering::Relaxed);
        }
        if unresolved > 0 {
            self.stats
                .toast_deaths_unresolved
                .fetch_add(unresolved, Ordering::Relaxed);
        }
        Ok(())
    }

    /// Pending deaths whose `death_lsn` the emitter ack has passed. Entries
    /// stay pending until [`Self::mark_collected`]; a failed sweep retries.
    pub async fn ready(&self, ack_lsn: u64) -> Vec<ValueDeath> {
        self.inner.lock().await.pending.ready(ack_lsn)
    }

    pub async fn pending_len(&self) -> usize {
        self.inner.lock().await.pending.len()
    }

    /// Journal GC completions, drop them from pending, compact if the file
    /// outgrew its live state. Store deletion already succeeded; a crash
    /// before the append just re-deletes (idempotent) next sweep.
    pub async fn mark_collected(&self, deaths: &[ValueDeath]) -> Result<(), ChunkStoreError> {
        if deaths.is_empty() {
            return Ok(());
        }
        let mut inner = self.inner.lock().await;
        let mut recs = Vec::with_capacity(deaths.len() * REC_LEN);
        for d in deaths {
            push_rec(
                &mut recs,
                TAG_GC_DONE,
                d.toast_relid,
                d.blkno,
                d.offnum,
                d.value_id,
                d.death_lsn,
            );
            inner.pending.remove(d);
        }
        inner.append(&recs).await?;
        let live = (inner.live_entries() + inner.pending.len() as u64 + 1) * REC_LEN as u64;
        if inner.journal_bytes > COMPACT_MIN_BYTES && inner.journal_bytes > 4 * live {
            self.compact(&mut inner).await?;
        }
        Ok(())
    }

    /// Rewrite the journal as births-for-map + deaths-for-pending, atomic
    /// rename. Holds the tracker lock: GC-cadence only, never the hot path.
    async fn compact(&self, inner: &mut TrackerInner) -> Result<(), ChunkStoreError> {
        let mut out = Vec::with_capacity(
            JOURNAL_HEADER + (inner.live_entries() + inner.pending.len() as u64) as usize * REC_LEN,
        );
        out.extend_from_slice(&JOURNAL_MAGIC);
        out.extend_from_slice(&JOURNAL_VERSION.to_le_bytes());
        out.extend_from_slice(&[0u8; 2]);
        for (relid, m) in &inner.map {
            for ((blkno, offnum), birth) in m {
                push_rec(
                    &mut out,
                    TAG_BIRTH,
                    *relid,
                    *blkno,
                    *offnum,
                    birth.value_id,
                    birth.birth_lsn,
                );
            }
        }
        for d in inner.pending.iter() {
            push_rec(
                &mut out,
                TAG_DEATH,
                d.toast_relid,
                d.blkno,
                d.offnum,
                d.value_id,
                d.death_lsn,
            );
        }
        let tmp = self.path.with_extension("journal.tmp");
        tokio::fs::write(&tmp, &out).await?;
        let f = tokio::fs::File::open(&tmp).await?;
        f.sync_all().await?;
        drop(f);
        tokio::fs::rename(&tmp, &self.path).await?;
        let file = std::fs::OpenOptions::new().append(true).open(&self.path)?;
        inner.journal = tokio::fs::File::from_std(file);
        inner.journal_bytes = out.len() as u64;
        Ok(())
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn birth(relid: u32, blkno: u32, offnum: u16, value_id: u32) -> TidEvent {
        TidEvent::Birth {
            toast_relid: relid,
            blkno,
            offnum,
            value_id,
        }
    }

    fn death(relid: u32, blkno: u32, offnum: u16) -> TidEvent {
        TidEvent::Death {
            toast_relid: relid,
            blkno,
            offnum,
        }
    }

    fn tracker(path: &Path) -> TidTracker {
        TidTracker::open(path.to_path_buf(), Arc::new(EmitterStats::default())).unwrap()
    }

    #[tokio::test]
    async fn birth_then_death_resolves_with_commit_lsn() {
        let tmp = tempfile::tempdir().unwrap();
        let t = tracker(&tmp.path().join("tids.journal"));
        t.apply(&[birth(16500, 3, 2, 7)], 0x1000).await.unwrap();
        assert!(t.ready(u64::MAX).await.is_empty());
        t.apply(&[death(16500, 3, 2)], 0x2000).await.unwrap();
        let ready = t.ready(0x2000).await;
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].value_id, 7);
        assert_eq!(ready[0].death_lsn, 0x2000);
        // Ack below the death: not ready
        assert!(t.ready(0x1fff).await.is_empty());
    }

    #[tokio::test]
    async fn rebuild_restores_map_and_pending() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("tids.journal");
        {
            let t = tracker(&path);
            t.apply(&[birth(1, 1, 1, 10), birth(1, 2, 1, 11)], 0x100)
                .await
                .unwrap();
            t.apply(&[death(1, 1, 1)], 0x200).await.unwrap();
        }
        let t = tracker(&path);
        // Pending death survives restart
        let ready = t.ready(0x200).await;
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].value_id, 10);
        // Mapped birth survives: killing it resolves
        t.apply(&[death(1, 2, 1)], 0x300).await.unwrap();
        assert_eq!(t.ready(0x300).await.len(), 2);
        // GC completion drops pending across restart
        let all = t.ready(u64::MAX).await;
        t.mark_collected(&all).await.unwrap();
        drop(t);
        let t = tracker(&path);
        assert!(t.ready(u64::MAX).await.is_empty());
    }

    #[tokio::test]
    async fn replayed_events_are_noops() {
        let tmp = tempfile::tempdir().unwrap();
        let t = tracker(&tmp.path().join("tids.journal"));
        let stats = t.stats.clone();
        t.apply(&[birth(1, 1, 1, 10)], 0x100).await.unwrap();
        t.apply(&[death(1, 1, 1)], 0x200).await.unwrap();
        // Replay from ack re-observes both
        t.apply(&[birth(1, 1, 1, 10)], 0x100).await.unwrap();
        t.apply(&[death(1, 1, 1)], 0x200).await.unwrap();
        assert_eq!(t.ready(u64::MAX).await.len(), 1, "no duplicate pending");
        assert_eq!(stats.toast_deaths_resolved.load(Ordering::Relaxed), 1);
        assert_eq!(stats.toast_deaths_unresolved.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn sibling_chunk_deletes_resolve_once_per_value() {
        let tmp = tempfile::tempdir().unwrap();
        let t = tracker(&tmp.path().join("tids.journal"));
        // Three chunks of one value, distinct TIDs
        t.apply(
            &[birth(1, 1, 1, 10), birth(1, 1, 2, 10), birth(1, 2, 1, 10)],
            0x100,
        )
        .await
        .unwrap();
        // toast_delete_datum kills all three in one commit
        t.apply(&[death(1, 1, 1), death(1, 1, 2), death(1, 2, 1)], 0x200)
            .await
            .unwrap();
        assert_eq!(t.stats.toast_deaths_resolved.load(Ordering::Relaxed), 1);
        assert_eq!(t.stats.toast_deaths_unresolved.load(Ordering::Relaxed), 0);
        let ready = t.ready(0x200).await;
        assert_eq!(ready.len(), 3, "one pending entry per dead chunk TID");
        assert!(
            ready
                .iter()
                .all(|d| d.value_id == 10 && d.death_lsn == 0x200)
        );
        // Replay of the whole commit: no double counts, no unresolved noise
        t.apply(&[death(1, 1, 1), death(1, 1, 2), death(1, 2, 1)], 0x200)
            .await
            .unwrap();
        assert_eq!(t.ready(0x200).await.len(), 3);
        assert_eq!(t.stats.toast_deaths_resolved.load(Ordering::Relaxed), 1);
        assert_eq!(t.stats.toast_deaths_unresolved.load(Ordering::Relaxed), 0);
        t.mark_collected(&ready).await.unwrap();
        assert!(t.ready(u64::MAX).await.is_empty());
    }

    #[tokio::test]
    async fn unmapped_death_counts_unresolved() {
        let tmp = tempfile::tempdir().unwrap();
        let t = tracker(&tmp.path().join("tids.journal"));
        t.apply(&[death(1, 9, 9)], 0x100).await.unwrap();
        assert!(t.ready(u64::MAX).await.is_empty());
        assert_eq!(t.stats.toast_deaths_unresolved.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn birth_on_occupied_tid_never_implies_death() {
        let tmp = tempfile::tempdir().unwrap();
        let t = tracker(&tmp.path().join("tids.journal"));
        t.apply(&[birth(1, 1, 1, 10)], 0x100).await.unwrap();
        // Could be relation rewrite: old value may still be live.
        t.apply(&[birth(1, 1, 1, 20)], 0x300).await.unwrap();
        assert!(t.ready(0x300).await.is_empty());
        assert_eq!(t.stats.toast_deaths_resolved.load(Ordering::Relaxed), 0);
        assert_eq!(t.stats.toast_deaths_unresolved.load(Ordering::Relaxed), 1);

        // Stale replay cannot replace newer occupant.
        t.apply(&[birth(1, 1, 1, 10)], 0x100).await.unwrap();
        assert_eq!(t.stats.toast_deaths_unresolved.load(Ordering::Relaxed), 1);

        // Later DELETE resolves only current occupant.
        t.apply(&[death(1, 1, 1)], 0x400).await.unwrap();
        let ready = t.ready(0x400).await;
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].value_id, 20);
        assert_eq!(ready[0].death_lsn, 0x400);
    }

    #[tokio::test]
    async fn stale_delete_replay_keeps_newer_occupant() {
        let tmp = tempfile::tempdir().unwrap();
        let t = tracker(&tmp.path().join("tids.journal"));
        t.apply(&[birth(1, 1, 1, 10)], 0x100).await.unwrap();
        t.apply(&[death(1, 1, 1)], 0x200).await.unwrap();
        t.apply(&[birth(1, 1, 1, 20)], 0x300).await.unwrap();

        t.apply(&[death(1, 1, 1)], 0x200).await.unwrap();
        t.apply(&[death(1, 1, 1)], 0x400).await.unwrap();

        let ready = t.ready(0x400).await;
        assert!(
            ready
                .iter()
                .any(|d| d.value_id == 10 && d.death_lsn == 0x200)
        );
        assert!(
            ready
                .iter()
                .any(|d| d.value_id == 20 && d.death_lsn == 0x400)
        );
        assert!(
            !ready
                .iter()
                .any(|d| d.value_id == 20 && d.death_lsn == 0x200)
        );
    }

    #[tokio::test]
    async fn same_commit_birth_death_orders() {
        let tmp = tempfile::tempdir().unwrap();
        let t = tracker(&tmp.path().join("tids.journal"));
        // INSERT toasts V at tid, UPDATE in the same xact deletes V and
        // toasts W elsewhere
        t.apply(
            &[birth(1, 1, 1, 10), death(1, 1, 1), birth(1, 2, 1, 11)],
            0x100,
        )
        .await
        .unwrap();
        let ready = t.ready(0x100).await;
        assert_eq!(ready.len(), 1);
        assert_eq!(ready[0].value_id, 10);
        // W remains mapped
        t.apply(&[death(1, 2, 1)], 0x200).await.unwrap();
        assert_eq!(t.ready(0x200).await.len(), 2);
    }

    #[tokio::test]
    async fn compaction_shrinks_journal_and_preserves_state() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("tids.journal");
        let t = tracker(&path);
        // Survivors: one live birth, one death GC won't reach (ack below)
        t.apply(&[birth(2, 9, 1, 900), birth(2, 9, 2, 901)], 0x50)
            .await
            .unwrap();
        t.apply(&[death(2, 9, 2)], 0x9000_0000).await.unwrap();
        // Churn past COMPACT_MIN_BYTES: birth + death + gc_done per chunk
        let n: u32 = 25_000;
        let births: Vec<TidEvent> = (0..n).map(|i| birth(1, i, 1, 10_000 + i)).collect();
        t.apply(&births, 0x100).await.unwrap();
        let deaths: Vec<TidEvent> = (0..n).map(|i| death(1, i, 1)).collect();
        t.apply(&deaths, 0x200).await.unwrap();
        assert!(
            std::fs::metadata(&path).unwrap().len() > COMPACT_MIN_BYTES,
            "churn must exceed the compaction floor"
        );
        let ready = t.ready(0x200).await;
        assert_eq!(ready.len(), n as usize);
        t.mark_collected(&ready).await.unwrap();
        // Live state is 2 births + 1 pending death; journal rewritten
        let sz = std::fs::metadata(&path).unwrap().len();
        assert!(sz < 1024, "journal compacted, still {sz} bytes");

        // Compacted journal rebuilds the same state
        drop(t);
        let t = tracker(&path);
        let pending = t.ready(u64::MAX).await;
        assert_eq!(pending.len(), 1, "uncollected death survives compaction");
        assert_eq!(pending[0].value_id, 901);
        t.apply(&[death(2, 9, 1)], 0xA000_0000).await.unwrap();
        assert_eq!(
            t.ready(u64::MAX).await.len(),
            2,
            "live birth survives compaction and resolves"
        );
    }

    #[tokio::test]
    async fn torn_tail_truncated_on_open() {
        let tmp = tempfile::tempdir().unwrap();
        let path = tmp.path().join("tids.journal");
        {
            let t = tracker(&path);
            t.apply(&[birth(1, 1, 1, 10)], 0x100).await.unwrap();
        }
        // Torn record: half a birth
        {
            use std::io::Write as _;
            let mut f = std::fs::OpenOptions::new()
                .append(true)
                .open(&path)
                .unwrap();
            f.write_all(&[TAG_BIRTH, 1, 2, 3]).unwrap();
        }
        let t = tracker(&path);
        t.apply(&[death(1, 1, 1)], 0x200).await.unwrap();
        assert_eq!(t.ready(0x200).await.len(), 1, "clean prefix intact");
        // File boundary is clean again for the next open
        drop(t);
        let t = tracker(&path);
        assert_eq!(t.ready(0x200).await.len(), 1);
    }
}
