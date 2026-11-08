//! Encode planned records off the commit path.
//!
//! Planning keeps routed records as WAL bytes, so the sink task that plans
//! every commit in order does no value encoding. Each outbox flush hands its
//! pending segments to one job on the runtime; the batcher awaits jobs in
//! channel order, so rows still merge in seq order. A job encodes one
//! table's rows from every segment into one chunk, tagging rows with their
//! seq, so small commits share column buffers instead of each sealing
//! their own.
//!
//! Planning already validated every record, so a job fails only on the
//! value codec. That failure is pipeline fatal; a control or truncate
//! behind it never applies, since both wait for earlier rows to be durable.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, PoisonError};

use tokio::task::JoinHandle;

use crate::budget::MemoryPermit;
use crate::emit::ch_emitter::{EmitterStats, EncodedChunk};
use crate::emit::pipeline::plan_spool::{ChunkBuilder, EncoderCache, PendingRows, SeqRows};

/// Chunks one job encoded, each with the seqs of its rows
pub struct EncodedRun {
    pub chunks: Vec<(EncodedChunk, SeqRows)>,
    /// Admission covering the run, settled to encoded bytes
    pub permit: Option<MemoryPermit>,
}

/// Pending segment placed under its seq
pub struct PendingSegment {
    pub seq: u64,
    /// Rows planning counted, which the ack collector expects
    pub rows: u64,
    pub records: PendingRows,
}

/// Encoder caches jobs check out whole, one per concurrent job, so a route
/// compiles its table plan once per cache rather than per job
#[derive(Clone, Default)]
pub struct Encoders(Arc<Mutex<Vec<EncoderCache>>>);

impl Encoders {
    /// Caches kept between jobs; a burst past this many concurrent jobs
    /// compiles its own and drops them after
    const KEPT: usize = 8;

    fn take(&self) -> EncoderCache {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .pop()
            .unwrap_or_default()
    }

    fn put(&self, cache: EncoderCache) {
        let mut caches = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if caches.len() < Self::KEPT {
            caches.push(cache);
        }
    }
}

/// Encode `segments` on the runtime
pub fn spawn(
    segments: Vec<PendingSegment>,
    permit: Option<MemoryPermit>,
    encoders: Encoders,
    stats: Arc<EmitterStats>,
) -> JoinHandle<Result<EncodedRun, String>> {
    tokio::spawn(async move {
        let run = encode(&segments, permit, &encoders);
        if run.is_err() {
            stats.plan_failures_encode.fetch_add(1, Ordering::Relaxed);
        }
        run
    })
}

fn encode(
    segments: &[PendingSegment],
    mut permit: Option<MemoryPermit>,
    encoders: &Encoders,
) -> Result<EncodedRun, String> {
    let mut b = ChunkBuilder::new(usize::MAX, usize::MAX).with_encoders(encoders.take());
    for s in segments {
        b.tag(s.seq);
        let rows = s.records.encode_into(&mut b).map_err(|e| e.to_string())?;
        if rows as u64 != s.rows {
            return Err(format!(
                "seq {} encoded {rows} rows, planned {}",
                s.seq, s.rows
            ));
        }
    }
    let chunks = b.take_runs();
    encoders.put(b.into_encoders());
    if let Some(p) = permit.as_mut() {
        p.settle(chunks.iter().map(|(c, _)| c.allocated_bytes() as u64).sum());
    }
    Ok(EncodedRun { chunks, permit })
}
