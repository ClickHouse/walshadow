//! Bootstrap producer — feeds the shared insert [`tail`](crate::pipeline::tail)
//! from a page walk.
//!
//! Greenfield base backup is the *easy* case for the tail: every row is
//! `op=Insert` at one `_lsn = start_lsn`, with no aborts, no TRUNCATE, and
//! no DDL barriers. This drain plays the role the WAL decode pool plays —
//! turn a source row into a [`RoutedRow`] and report `Placed{seq, rows}` —
//! but cheaply: resolve + map only. No detoast and no oracle resolution,
//! matching today's bootstrap semantics exactly (Option A in
//! `plans/future/parallel_decode_and_insert.md`); page-walk decode stays
//! single-threaded in [`PageWalkSink`](crate::backup_page_walk::PageWalkSink).
//!
//! ## Synthetic seq scheme — one seq per rfn
//!
//! The ack collector keys everything on dense `seq`s registered in commit
//! order, each with a `commit_lsn` monotonic in `seq`. Bootstrap has no
//! commit boundaries and one uniform `_lsn`, so it synthesizes seqs.
//! `PageWalkSink` emits all rows for one rfn contiguously, so the natural
//! unit is one seq per rfn: `register(seq, commit_lsn = start_lsn)` at the
//! rfn's first row, `placed(seq, rows)` at the rfn flip (or channel close).
//!
//! Every seq carries the same `commit_lsn = start_lsn`, so the
//! contiguous-done *frontier* proves bootstrap durability
//! (`ack.wait_through(K)`) but the *published* `emitter_ack` saturates at
//! `start_lsn` (`advance`'s `fetch_max` over a uniform `commit_lsn`). The
//! caller advances the resume LSN to `end_lsn` after `wait_through(K)` —
//! durability and the persisted resume LSN are not the same thing here.
//!
//! Under `object_store` fan-out the drain is the single channel consumer,
//! so it assigns seqs by rfn-flips *as observed in the channel*. Concurrent
//! parts interleaving just yields more (still dense) seqs and a given rfn
//! may span more than one seq, which the per-seq refcount handles like any
//! two relations.
//!
//! ## TOAST — fail-fast, preserved
//!
//! Externally-toasted columns reach the batcher's encoder as
//! `ColumnValue::ExternalToast` and are rejected (`EmitterError::Unsupported
//! Value`) → the shared `fatal` trips, exactly as the old transitional
//! emitter rejected them. Latent only because tested fixtures keep values
//! inline; TOAST assembly is its own work item (see `plans/future/TOAST.md`).

use std::sync::Arc;
use std::sync::atomic::Ordering;

use tokio::sync::mpsc;

use crate::backup_page_walk::{BackfillTuple, CatalogMap};
use crate::ch_emitter::{EmitterStats, MappingHandle};
use crate::pipeline::ack::AckHandle;
use crate::pipeline::batcher::{BatcherMsg, RoutedRow};

/// What the drain reports back so the caller can run the completion
/// sequence (`FlushAll` → `wait_through(next_seq)` → advance resume LSN).
#[derive(Debug, Clone, Copy, Default)]
pub struct BootstrapDrainOutcome {
    /// Count of synthetic seqs registered, dense over `[0, next_seq)`. The
    /// durability proof is `ack.wait_through(next_seq)`.
    pub next_seq: u64,
    /// Rows actually routed to the batcher (mapped relations only).
    pub rows_routed: u64,
}

/// Drain page-walk tuples into the shared tail.
///
/// Consumes [`BackfillTuple`]s off `rx` (the `PageWalkSink` channel),
/// resolves each via `catalog` + `mapping`, and sends a [`RoutedRow`] to
/// the batcher on `msg_tx`. Synthesizes one seq per rfn against `ack`.
/// Unmapped / unresolved relations are skipped (bumping
/// `unsupported_relations`, matching the WAL decode pool). Returns once the
/// channel closes (the bootstrap pump dropped its sender).
///
/// Errors only when the batcher channel closes early (the tail tripped
/// `fatal`); the caller surfaces that via `fatal.message()`.
pub async fn drain(
    mut rx: mpsc::UnboundedReceiver<BackfillTuple>,
    catalog: CatalogMap,
    mapping_handle: MappingHandle,
    msg_tx: mpsc::Sender<BatcherMsg>,
    ack: AckHandle,
    stats: Arc<EmitterStats>,
) -> Result<BootstrapDrainOutcome, String> {
    let mut next_seq = 0u64;
    let mut rows_routed = 0u64;
    // The rfn currently accumulating: (rfn, its seq, rows routed for it).
    let mut open: Option<(wal_rs::pg::walparser::RelFileNode, u64, u64)> = None;

    while let Some(tuple) = rx.recv().await {
        let rfn = tuple.rfn;
        let source_lsn = tuple.source_lsn;

        // rfn flip (or first tuple): place the prior seq, register a new
        // one. `commit_lsn = source_lsn = start_lsn` for every bootstrap row.
        let same = matches!(&open, Some((r, _, _)) if *r == rfn);
        let seq = if same {
            open.as_ref().expect("same implies open").1
        } else {
            if let Some((_, prev_seq, prev_rows)) = open.take() {
                ack.placed(prev_seq, prev_rows);
            }
            let s = next_seq;
            next_seq += 1;
            ack.register(s, source_lsn);
            open = Some((rfn, s, 0));
            s
        };

        // Resolve + map. Page walk only emits known filenodes, so the
        // catalog miss is defensive; the mapping miss is an unconfigured
        // relation (not part of any destination).
        let Some(rel) = catalog.get(rfn.db_node, rfn.rel_node) else {
            stats.unsupported_relations.fetch_add(1, Ordering::Relaxed);
            continue;
        };
        let Some(mapping) =
            crate::pipeline::lookup_mapping(&mapping_handle, rel.qualified_name.as_ref(), &stats)
                .await
        else {
            continue;
        };

        let committed = tuple.into_committed_insert();
        if msg_tx
            .send(BatcherMsg::Row(RoutedRow {
                seq,
                rel,
                mapping,
                committed,
            }))
            .await
            .is_err()
        {
            // Batcher gone — the tail tripped fatal. Caller surfaces it.
            return Err("bootstrap: batcher channel closed".into());
        }
        if let Some(slot) = open.as_mut() {
            slot.2 += 1;
        }
        rows_routed += 1;
    }

    // Channel closed: place the final rfn's seq.
    if let Some((_, seq, rows)) = open.take() {
        ack.placed(seq, rows);
    }
    Ok(BootstrapDrainOutcome {
        next_seq,
        rows_routed,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ch_emitter::{ColumnMapping, TableMapping};
    use crate::heap_decoder::ColumnValue;
    use crate::pipeline::ack;
    use crate::pipeline::batcher::BatcherMsg;
    use crate::shadow_catalog::{RelAttr, RelDescriptor, ReplIdent};
    use std::collections::HashMap;
    use std::sync::atomic::AtomicU64;
    use wal_rs::pg::walparser::RelFileNode;

    fn rel(rel_node: u32) -> Arc<RelDescriptor> {
        let name = format!("t{rel_node}");
        let qualified_name = RelDescriptor::build_qualified_name("public", &name);
        Arc::new(RelDescriptor {
            rfn: RelFileNode {
                spc_node: 1663,
                db_node: 5,
                rel_node,
            },
            oid: rel_node,
            namespace_oid: 2200,
            namespace_name: "public".into(),
            name,
            qualified_name,
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
                missing_text: None,
            }],
        })
    }

    fn mapping_for(rel_node: u32) -> TableMapping {
        TableMapping {
            target: format!("default.t{rel_node}"),
            columns: vec![ColumnMapping {
                src_attnum: 1,
                target_name: "id".into(),
                target_type: "Int32".into(),
            }],
        }
    }

    fn tuple(rel_node: u32, id: i32) -> BackfillTuple {
        BackfillTuple {
            rfn: RelFileNode {
                spc_node: 1663,
                db_node: 5,
                rel_node,
            },
            xid: 99,
            source_lsn: 0x1000,
            columns: vec![Some(ColumnValue::Int4(id))],
        }
    }

    /// Two rels, contiguous rows each: one seq per rfn, every row routed,
    /// each seq placed with its exact count. Drives the drain against a
    /// real ack collector and a stub batcher receiver.
    #[tokio::test]
    async fn seq_per_rfn_places_exact_counts() {
        let mut catalog = CatalogMap::new();
        catalog.insert(rel(16400));
        catalog.insert(rel(16401));
        let mut tables = HashMap::new();
        tables.insert("public.t16400".to_string(), mapping_for(16400));
        tables.insert("public.t16401".to_string(), mapping_for(16401));
        let mapping: MappingHandle = Arc::new(tokio::sync::RwLock::new(tables));

        let emitter_ack = Arc::new(AtomicU64::new(0));
        let (ack, collector) = ack::spawn(emitter_ack);
        let (msg_tx, mut msg_rx) = mpsc::channel::<BatcherMsg>(64);
        let (tup_tx, tup_rx) = mpsc::unbounded_channel::<BackfillTuple>();

        // rel 16400 ×3, then rel 16401 ×2.
        for id in 0..3 {
            tup_tx.send(tuple(16400, id)).unwrap();
        }
        for id in 0..2 {
            tup_tx.send(tuple(16401, id)).unwrap();
        }
        drop(tup_tx);

        let stats = Arc::new(EmitterStats::default());
        let drain_task = tokio::spawn(drain(
            tup_rx,
            catalog,
            mapping,
            msg_tx,
            ack.clone(),
            stats.clone(),
        ));

        let mut by_seq: HashMap<u64, u64> = HashMap::new();
        while let Some(BatcherMsg::Row(r)) = msg_rx.recv().await {
            *by_seq.entry(r.seq).or_default() += 1;
        }
        let outcome = drain_task.await.unwrap().unwrap();
        assert_eq!(outcome.next_seq, 2, "one seq per rfn");
        assert_eq!(outcome.rows_routed, 5);
        assert_eq!(by_seq.get(&0), Some(&3), "rel 16400 → seq 0, 3 rows");
        assert_eq!(by_seq.get(&1), Some(&2), "rel 16401 → seq 1, 2 rows");
        assert_eq!(stats.unsupported_relations.load(Ordering::Relaxed), 0);

        // Once every seq is placed AND acked it would advance; here we only
        // placed (no inserter), so drop ack and let the collector exit.
        drop(ack);
        collector.await.unwrap();
    }

    /// An rfn reappearing non-contiguously (object_store interleave) gets a
    /// fresh seq each run; the unmapped rel is skipped but still consumes a
    /// dense seq placed with zero rows.
    #[tokio::test]
    async fn reappearing_and_unmapped_rfns() {
        let mut catalog = CatalogMap::new();
        catalog.insert(rel(16400));
        catalog.insert(rel(16401)); // resolvable but unmapped below
        let mut tables = HashMap::new();
        tables.insert("public.t16400".to_string(), mapping_for(16400));
        let mapping: MappingHandle = Arc::new(tokio::sync::RwLock::new(tables));

        let emitter_ack = Arc::new(AtomicU64::new(0));
        let (ack, collector) = ack::spawn(emitter_ack);
        let (msg_tx, mut msg_rx) = mpsc::channel::<BatcherMsg>(64);
        let (tup_tx, tup_rx) = mpsc::unbounded_channel::<BackfillTuple>();

        // 16400, 16401(unmapped), 16400 again → seqs 0,1,2; only 0 and 2 route.
        tup_tx.send(tuple(16400, 1)).unwrap();
        tup_tx.send(tuple(16401, 9)).unwrap();
        tup_tx.send(tuple(16400, 2)).unwrap();
        drop(tup_tx);

        let stats = Arc::new(EmitterStats::default());
        let drain_task = tokio::spawn(drain(
            tup_rx,
            catalog,
            mapping,
            msg_tx,
            ack.clone(),
            stats.clone(),
        ));

        let mut seqs: Vec<u64> = Vec::new();
        while let Some(BatcherMsg::Row(r)) = msg_rx.recv().await {
            seqs.push(r.seq);
        }
        let outcome = drain_task.await.unwrap().unwrap();
        assert_eq!(outcome.next_seq, 3, "three distinct rfn runs");
        assert_eq!(outcome.rows_routed, 2, "unmapped rel routed nothing");
        assert_eq!(seqs, vec![0, 2], "seq 1 (unmapped) routed no rows");
        assert_eq!(stats.unsupported_relations.load(Ordering::Relaxed), 1);
        drop(ack);
        collector.await.unwrap();
    }
}
