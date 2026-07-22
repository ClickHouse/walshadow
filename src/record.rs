//! WAL record hand-off contracts

use std::collections::BTreeMap;
use std::future::{self, Future};
use std::pin::Pin;

use serde::{Deserialize, Serialize};
use thiserror::Error;
use walrus::pg::wal::segment::SegmentName;
use walrus::pg::walparser::{RmId, XLogRecord};

use crate::filter::manifest::Manifest;

pub const WAL_SEG_SIZE: u64 = walrus::pg::wal::segment::DEFAULT_WAL_SEG_SIZE;

/// Numeric id fallback for unknown rmgrs
pub fn rmgr_label(rm: u8) -> String {
    let named = match rm {
        x if x == RmId::Xlog as u8 => "xlog",
        x if x == RmId::Xact as u8 => "xact",
        x if x == RmId::Smgr as u8 => "smgr",
        x if x == RmId::Clog as u8 => "clog",
        x if x == RmId::Dbase as u8 => "dbase",
        x if x == RmId::Tblspc as u8 => "tblspc",
        x if x == RmId::MultiXact as u8 => "multixact",
        x if x == RmId::RelMap as u8 => "relmap",
        x if x == RmId::Standby as u8 => "standby",
        x if x == RmId::Heap2 as u8 => "heap2",
        x if x == RmId::Heap as u8 => "heap",
        x if x == RmId::Btree as u8 => "btree",
        x if x == RmId::Hash as u8 => "hash",
        x if x == RmId::Gin as u8 => "gin",
        x if x == RmId::Gist as u8 => "gist",
        x if x == RmId::Seq as u8 => "seq",
        x if x == RmId::Spgist as u8 => "spgist",
        x if x == RmId::Brin as u8 => "brin",
        x if x == RmId::CommitTs as u8 => "commit_ts",
        x if x == RmId::ReplOrigin as u8 => "repl_origin",
        x if x == RmId::Generic as u8 => "generic",
        x if x == RmId::LogicalMsg as u8 => "logical_msg",
        _ => return format!("rmgr_{rm}"),
    };
    named.into()
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub enum Route {
    #[default]
    ToShadow,
    ToDecoder,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CatalogSignal {
    #[default]
    None,
    Invalidate,
    InvalidateSweep,
}

#[derive(Debug, Error)]
pub enum SinkError {
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialize manifest: {0}")]
    Manifest(#[from] serde_json::Error),
    #[error("{0}")]
    Other(String),
}

#[derive(Debug, Clone, Default)]
pub struct Record<'a> {
    pub parsed: XLogRecord<'a>,
    pub source_lsn: u64,
    /// PG `XLogReaderState::EndRecPtr`: aligned end of this record, the
    /// position `pg_last_wal_replay_lsn()` reports once shadow applies it.
    /// `XLOG_SWITCH` advances to segment end. Replay comparisons use this,
    /// never the last physical wire byte.
    pub next_lsn: u64,
    pub page_magic: u16,
    pub route: Route,
    pub catalog_signal: CatalogSignal,
    /// Commit of a catalog-mutating xact: pump must hold successor-byte
    /// publication until shadow replays through `next_lsn`
    pub catalog_boundary: bool,
}

pub trait RecordSink {
    fn on_record<'a>(
        &'a mut self,
        record: &'a Record<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>>;

    fn on_idle<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(future::ready(Ok(())))
    }

    fn on_close<'a>(
        &'a mut self,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(future::ready(Ok(())))
    }

    fn on_idle_advance<'a>(
        &'a mut self,
        _lsn: u64,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(future::ready(Ok(())))
    }
}

pub trait RecordBytesSink: Send {
    fn on_wire_chunk<'a>(
        &'a mut self,
        start_lsn: u64,
        bytes: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>>;

    fn on_segment_boundary<'a>(
        &'a mut self,
        _start_lsn: u64,
        _trailing_bytes: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(future::ready(Ok(())))
    }

    fn on_segment_retired<'a>(
        &'a mut self,
        _new_start_lsn: u64,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(future::ready(Ok(())))
    }
}

pub trait SegmentSink {
    fn on_segment<'a>(
        &'a mut self,
        seg: SegmentName,
        bytes: &'a [u8],
        manifest: &'a Manifest,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>>;

    fn on_partial_segment<'a>(
        &'a mut self,
        seg: SegmentName,
        bytes: &'a [u8],
        manifest: &'a Manifest,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        self.on_segment(seg, bytes, manifest)
    }
}

#[derive(Debug, Default)]
pub struct CollectingRecordSink {
    pub records: Vec<Record<'static>>,
}

impl RecordSink for CollectingRecordSink {
    fn on_record<'a>(
        &'a mut self,
        record: &'a Record<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            self.records.push(Record {
                parsed: record.parsed.clone().into_owned(),
                source_lsn: record.source_lsn,
                next_lsn: record.next_lsn,
                page_magic: record.page_magic,
                route: record.route,
                catalog_signal: record.catalog_signal,
                catalog_boundary: record.catalog_boundary,
            });
            Ok(())
        })
    }
}

#[derive(Debug, Default)]
pub struct CountingRecordSink {
    pub count: u64,
}

impl RecordSink for CountingRecordSink {
    fn on_record<'a>(
        &'a mut self,
        _record: &'a Record<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            self.count += 1;
            Ok(())
        })
    }
}

#[derive(Debug, Default)]
pub struct MetricsRecordSink {
    pub by_rm_route: BTreeMap<(u8, Route), u64>,
    pub total: u64,
}

impl MetricsRecordSink {
    pub fn summary(&self) -> String {
        use std::fmt::Write as _;
        let mut summary = format!("total={}", self.total);
        for ((rm, route), count) in &self.by_rm_route {
            let route = match route {
                Route::ToShadow => "to_shadow",
                Route::ToDecoder => "to_decoder",
            };
            write!(summary, " {}/{}={count}", rmgr_label(*rm), route).unwrap();
        }
        summary
    }
}

impl RecordSink for MetricsRecordSink {
    fn on_record<'a>(
        &'a mut self,
        record: &'a Record<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            *self
                .by_rm_route
                .entry((record.parsed.header.resource_manager_id, record.route))
                .or_default() += 1;
            self.total += 1;
            Ok(())
        })
    }
}

pub struct CompositeRecordSink {
    pub inner: Vec<Box<dyn RecordSink + Send>>,
}

impl CompositeRecordSink {
    pub fn new(inner: Vec<Box<dyn RecordSink + Send>>) -> Self {
        Self { inner }
    }
}

impl RecordSink for CompositeRecordSink {
    fn on_record<'a>(
        &'a mut self,
        record: &'a Record<'a>,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            for sink in &mut self.inner {
                sink.on_record(record).await?;
            }
            Ok(())
        })
    }
}

#[derive(Debug, Default)]
pub struct CollectingSegmentSink {
    pub segments: Vec<(SegmentName, Vec<u8>, Manifest)>,
}

impl SegmentSink for CollectingSegmentSink {
    fn on_segment<'a>(
        &'a mut self,
        seg: SegmentName,
        bytes: &'a [u8],
        manifest: &'a Manifest,
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            self.segments.push((seg, bytes.to_vec(), manifest.clone()));
            Ok(())
        })
    }
}

#[derive(Debug, Default)]
pub struct CollectingBytesSink {
    pub chunks: Vec<(u64, Vec<u8>)>,
    pub segment_boundaries: Vec<(u64, Vec<u8>)>,
}

impl RecordBytesSink for CollectingBytesSink {
    fn on_wire_chunk<'a>(
        &'a mut self,
        start_lsn: u64,
        bytes: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            self.chunks.push((start_lsn, bytes.to_vec()));
            Ok(())
        })
    }

    fn on_segment_boundary<'a>(
        &'a mut self,
        start_lsn: u64,
        bytes: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(async move {
            self.segment_boundaries.push((start_lsn, bytes.to_vec()));
            Ok(())
        })
    }
}

#[derive(Debug, Default)]
pub struct NoopBytesSink;

impl RecordBytesSink for NoopBytesSink {
    fn on_wire_chunk<'a>(
        &'a mut self,
        _start_lsn: u64,
        _bytes: &'a [u8],
    ) -> Pin<Box<dyn Future<Output = Result<(), SinkError>> + Send + 'a>> {
        Box::pin(future::ready(Ok(())))
    }
}
