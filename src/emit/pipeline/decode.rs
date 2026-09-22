//! Row placement: turn one planned slice into batcher row chunks
//!
//! Planning already detoasted and routed every heap, so reorder places
//! each dispatched seq inline: narrow local extensions render here,
//! deletes drop for destinations without a delete marker, and rows chunk
//! onto the shared batcher channel in dispatch order. Oracle resolution
//! happens later, in the inserter.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use tokio::sync::mpsc;

use crate::decode::heap_decoder::{CommittedTuple, HeapOp};
use crate::emit::ch_emitter::EmitterStats;
use crate::emit::pipeline::batcher::{BatcherMsg, RoutedRow, RowChunk};
use crate::emit::route::RoutedHeap;

/// Byte half of dual trigger with configured row cap. Bounds channel item for
/// fat detoasted rows that would pin many MiB before row cap fires; above
/// ~100 KiB an ordinary row-cap chunk reaches, so steady-state coalescing is
/// unchanged.
pub const DECODE_CHUNK_BYTES: usize = 4 << 20;

/// `Err` means the batcher channel closed (tail tripped fatal).
async fn route_chunk(
    msg_tx: &mpsc::Sender<BatcherMsg>,
    rows: Vec<RoutedRow>,
    permit: Option<Arc<crate::budget::MemoryPermit>>,
) -> Result<(), String> {
    msg_tx
        .send(BatcherMsg::Rows(RowChunk { rows, permit }))
        .await
        .map_err(|_| "batcher channel closed".to_string())
}

/// Chunk every routed heap of one seq onto the batcher channel. Returns
/// rows routed (the `R` the collector compares against).
///
/// Every routed row is on the channel by return, so the caller's `Placed`
/// and any later `FlushAll` order after them. `route = None`
/// (deterministically unmapped, counted at planning) discards here.
#[allow(clippy::too_many_arguments)]
pub async fn place_rows(
    msg_tx: &mpsc::Sender<BatcherMsg>,
    stats: &EmitterStats,
    chunk_rows: usize,
    seq: u64,
    commit_ts: i64,
    commit_lsn: u64,
    heaps: Vec<RoutedHeap>,
    permit: Option<Arc<crate::budget::MemoryPermit>>,
) -> Result<u64, String> {
    stats.decode_jobs_in.fetch_add(1, Ordering::Relaxed);
    let mut routed = 0u64;
    let mut heaps = heaps.into_iter();
    let mut buf: Vec<RoutedRow> = Vec::with_capacity(chunk_rows.min(heaps.len()));
    let mut buf_bytes = 0usize;
    while let Some(envelope) = heaps.next() {
        let Some(route) = envelope.route else {
            continue;
        };
        // No delete-marker column: a DELETE would land as a phantom insert of
        // the old image, so drop it (append-only destination)
        if route.drops_deletes() && matches!(envelope.described.decoded.op, HeapOp::Delete) {
            stats.deletes_discarded.fetch_add(1, Ordering::Relaxed);
            continue;
        }
        let heap = envelope.described;
        let rel = heap.descriptor;
        let mut committed = CommittedTuple {
            decoded: heap.decoded,
            commit_ts,
            commit_lsn,
        };
        // PostGIS WKT differs from typoutput HEXEWKB
        if let Some(t) = committed.decoded.new.as_mut() {
            crate::ops::oracle::render_ext_columns(&rel.attributes, &mut t.columns);
        }
        if let Some(t) = committed.decoded.old.as_mut() {
            crate::ops::oracle::render_ext_columns(&rel.attributes, &mut t.columns);
        }
        buf_bytes += committed.decoded.approx_bytes();
        buf.push(RoutedRow {
            seq,
            rel,
            route,
            committed,
            value_permit: None,
        });
        routed += 1;
        if buf.len() >= chunk_rows || buf_bytes >= DECODE_CHUNK_BYTES {
            route_chunk(msg_tx, std::mem::take(&mut buf), permit.clone()).await?;
            buf.reserve(chunk_rows.min(heaps.len()));
            buf_bytes = 0;
        }
    }
    if !buf.is_empty() {
        route_chunk(msg_tx, buf, permit).await?;
    }
    stats.decode_rows_out.fetch_add(routed, Ordering::Relaxed);
    Ok(routed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::decode::heap_decoder::{ColumnValue, DecodedHeap, DecodedTuple, DescribedHeap};
    use crate::emit::route::{RouteSnapshot, RowPolicy};
    use crate::mapping::{ColumnMapping, SystemColumns, TableMapping, TableTarget};
    use crate::schema::{RelAttr, RelDescriptor, RelName, ReplIdent};
    use walrus::pg::walparser::RelFileNode;

    const RFN: RelFileNode = RelFileNode {
        spc_node: 1663,
        db_node: 5,
        rel_node: 16385,
    };

    fn rel() -> Arc<RelDescriptor> {
        Arc::new(RelDescriptor {
            rfn: RFN,
            oid: 16385,
            toast_oid: 0,
            namespace_oid: 2200,
            rel_name: RelName::new("public", "t"),
            kind: 'r',
            persistence: 'p',
            replident: ReplIdent::Default { pk_attnums: None },
            attributes: vec![RelAttr {
                attnum: 1,
                name: "id".into(),
                type_oid: 23,
                typmod: -1,
                not_null: true,
                dropped: false,
                type_name: "int4".into(),
                type_byval: true,
                type_len: 4,
                type_align: 'i',
                type_storage: 'p',
                missing_default: None,
            }],
        })
    }

    fn heap(op: HeapOp, route: Arc<RouteSnapshot>) -> RoutedHeap {
        let tuple = Some(DecodedTuple {
            columns: vec![Some(ColumnValue::Int4(1))],
            partial: false,
        });
        let (new, old) = match op {
            HeapOp::Delete => (None, tuple),
            _ => (tuple, None),
        };
        RoutedHeap {
            described: DescribedHeap {
                decoded: DecodedHeap {
                    rfn: RFN,
                    xid: 7,
                    source_lsn: 0x1000,
                    op,
                    new,
                    old,
                },
                descriptor: rel(),
                descriptor_valid_from: 0x40,
            },
            route: Some(route),
        }
    }

    fn route(system: SystemColumns) -> Arc<RouteSnapshot> {
        RouteSnapshot::freeze(
            Arc::new(TableMapping {
                target: TableTarget::new("default", "t"),
                columns: vec![ColumnMapping {
                    src_attnum: 1,
                    target_name: "id".into(),
                    target_type: "Int32".into(),
                }],
            }),
            Arc::default(),
            RowPolicy {
                soft_delete: false,
                system: Arc::new(system),
            },
        )
    }

    /// Without a delete-marker column a DELETE has nowhere to land: it must
    /// drop here, before the placed count the ack collector reconciles
    #[tokio::test]
    async fn deletes_drop_when_marker_disabled() {
        let (msg_tx, mut msg_rx) = mpsc::channel(8);
        let stats = EmitterStats::default();
        let no_marker = route(SystemColumns {
            is_deleted: None,
            ..SystemColumns::default()
        });
        let routed = place_rows(
            &msg_tx,
            &stats,
            8,
            0,
            0,
            0x2000,
            vec![
                heap(HeapOp::Insert, no_marker.clone()),
                heap(HeapOp::Delete, no_marker),
            ],
            None,
        )
        .await
        .expect("place");
        assert_eq!(routed, 1, "insert routed, delete dropped");
        assert_eq!(stats.deletes_discarded.load(Ordering::Relaxed), 1);
        match msg_rx.recv().await {
            Some(BatcherMsg::Rows(chunk)) => assert_eq!(chunk.rows.len(), 1),
            other => panic!("expected one row chunk, got {}", other.is_some()),
        }

        // Default policy keeps the marker, so the DELETE rides through
        let marked = route(SystemColumns::default());
        let routed = place_rows(
            &msg_tx,
            &stats,
            8,
            1,
            0,
            0x3000,
            vec![heap(HeapOp::Delete, marked)],
            None,
        )
        .await
        .expect("place");
        assert_eq!(routed, 1);
        assert_eq!(stats.deletes_discarded.load(Ordering::Relaxed), 1);
    }
}
