//! Bootstrap producer — feeds shared insert [`tail`](crate::pipeline::tail)
//! from a page walk.
//!
//! Greenfield base backup: every row `op=Insert` at one `_lsn = start_lsn`,
//! no aborts/TRUNCATE/DDL barriers. Resolve + map only, no detoast or oracle
//! resolution (Option A in `plans/future/pipeline_backpressure_and_scaling.md`);
//! page-walk decode stays single-threaded in
//! [`PageWalkSink`](crate::backup_page_walk::PageWalkSink).
//!
//! ## Synthetic seq scheme — one seq per rfn
//!
//! Collector keys on dense `seq`s with `commit_lsn` monotonic in `seq`.
//! Bootstrap has no commit boundaries and one uniform `_lsn`, so synthesizes
//! seqs. `PageWalkSink` emits a rfn's rows contiguously, so unit is one seq
//! per rfn: `register(seq, commit_lsn = start_lsn)` at first row,
//! `placed(seq, rows)` at rfn flip (or channel close).
//!
//! Every seq shares `commit_lsn = start_lsn`, so the contiguous-done
//! frontier proves durability (`wait_through(K)`) but published `emitter_ack`
//! saturates at `start_lsn`. Caller advances resume LSN to `end_lsn` after
//! `wait_through(K)`: durability and persisted resume LSN differ here.
//!
//! Under `object_store` fan-out the drain is sole channel consumer, assigning
//! seqs by rfn-flips as observed in the channel. Interleaving parts yield more
//! dense seqs and a rfn may span several, handled by the per-seq refcount.
//!
//! ## TOAST — explicit fail-fast at the producer
//!
//! Option-A drain does no detoast and page walk has no `pg_toast_*`
//! reassembly, so externally-toasted mapped columns can't be reproduced.
//! Detect `ColumnValue::ExternalToast` before routing and fail fast with
//! relation + column, rather than a generic `EmitterError::UnsupportedValue`
//! deep in the inserter pool whose "xact buffer should have reassembled"
//! wording is meaningless here. TOAST assembly is its own work item (see
//! `plans/future/TOAST.md`).

use std::sync::Arc;
use std::sync::atomic::Ordering;

use tokio::sync::mpsc;

use crate::backup_page_walk::{BackfillTuple, CatalogMap};
use crate::ch_emitter::{EmitterStats, MappingHandle, TableMapping};
use crate::heap_decoder::ColumnValue;
use crate::pipeline::ack::AckHandle;
use crate::pipeline::batcher::{BatcherMsg, RoutedRow};
use crate::shadow_catalog::RelDescriptor;
use crate::spill::ToastChunk;
use crate::toast::{ChunkMap, ToastResolver};
use crate::xact_buffer::{detoasted_value, try_reassemble};

/// Caller runs the completion sequence from this: `FlushAll` →
/// `wait_through(next_seq)` → advance resume LSN.
#[derive(Debug, Clone, Copy, Default)]
pub struct BootstrapDrainOutcome {
    /// Dense over `[0, next_seq)`; durability proof is `wait_through(next_seq)`.
    pub next_seq: u64,
    pub rows_routed: u64,
}

/// Chunk-put batch size: a value's chunks span pages, so accumulate before
/// hitting the store rather than one append per chunk.
const CHUNK_PUT_BATCH: usize = 256;

/// A main-table tuple whose mapped value is externally TOASTed, held until
/// the whole walk completes so its chunks (in any later `pg_toast_*` file)
/// are durable before reassembly. Bounded by the count of toasted rows.
struct Deferred {
    tuple: BackfillTuple,
    rel: Arc<RelDescriptor>,
    mapping: Arc<TableMapping>,
}

/// Drain page-walk tuples into the shared tail.
///
/// Synthesizes one seq per rfn against `ack`. Unmapped/unresolved relations
/// skipped (bumping `unsupported_relations`, matching WAL decode pool).
///
/// TOAST per `resolver`:
/// * `pg_toast_*` tuples (a chunk store is configured, so the page walk
///   surfaced them) are reinterpreted into chunks and persisted, never
///   routed to a destination table.
/// * a main-table tuple with a mapped externally-TOASTed value is, with a
///   store, *deferred* and resolved after the walk (its chunks may live in a
///   `pg_toast_*` file walked later); disabled, it is NULL/default-filled
///   inline.
///
/// Errors when the batcher channel closes early (tail tripped `fatal`) or,
/// in disk/CH mode, when a deferred value's chunks are genuinely absent.
pub async fn drain(
    mut rx: mpsc::Receiver<BackfillTuple>,
    catalog: CatalogMap,
    mapping_handle: MappingHandle,
    msg_tx: mpsc::Sender<BatcherMsg>,
    ack: AckHandle,
    stats: Arc<EmitterStats>,
    resolver: ToastResolver,
) -> Result<BootstrapDrainOutcome, String> {
    let mut next_seq = 0u64;
    let mut rows_routed = 0u64;
    // rfn currently accumulating: (rfn, its seq, rows routed for it)
    let mut open: Option<(pgwalrs::pg::walparser::RelFileNode, u64, u64)> = None;
    let mut chunk_batch: Vec<ToastChunk> = Vec::new();
    let mut deferred: Vec<Deferred> = Vec::new();
    let mut start_lsn = 0u64;

    while let Some(tuple) = rx.recv().await {
        let rfn = tuple.rfn;
        let source_lsn = tuple.source_lsn;
        start_lsn = source_lsn;

        // rfn flip (or first tuple): place prior seq, register new.
        // commit_lsn = source_lsn = start_lsn for every bootstrap row
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

        // Page walk only emits known filenodes, so catalog miss is defensive;
        // mapping miss is a relation in no destination.
        let Some(rel) = catalog.get(rfn.db_node, rfn.rel_node) else {
            stats.unsupported_relations.fetch_add(1, Ordering::Relaxed);
            continue;
        };

        // pg_toast_* tuple: reinterpret into a chunk + persist. The seq stays
        // a zero-row placeholder (no destination row). `rel.oid` matches the
        // referring pointer's `va_toastrelid`.
        if catalog.is_toast(rfn.db_node, rfn.rel_node) {
            if let Some(chunk) = chunk_from_columns(&tuple.columns, rel.oid, source_lsn) {
                chunk_batch.push(chunk);
                if chunk_batch.len() >= CHUNK_PUT_BATCH {
                    resolver
                        .put(&chunk_batch)
                        .await
                        .map_err(|e| format!("bootstrap: toast store put: {e}"))?;
                    chunk_batch.clear();
                }
            }
            continue;
        }

        let Some(mapping) =
            crate::pipeline::lookup_mapping(&mapping_handle, rel.qualified_name.as_ref(), &stats)
                .await
        else {
            continue;
        };

        if has_mapped_external_toast(&tuple, &mapping) {
            if resolver.stores_chunks() {
                // Defer: chunks may live in a pg_toast_* file not yet walked.
                deferred.push(Deferred {
                    tuple,
                    rel,
                    mapping,
                });
                continue;
            }
            // Disabled: NULL/default-fill inline (no store to consult).
            let mut tuple = tuple;
            resolve_or_fill_toast(&mut tuple, &rel, &mapping, &resolver).await?;
            route_row(&msg_tx, seq, rel, mapping, tuple).await?;
            bump(&mut open, &mut rows_routed);
            continue;
        }

        route_row(&msg_tx, seq, rel, mapping, tuple).await?;
        bump(&mut open, &mut rows_routed);
    }

    if let Some((_, seq, rows)) = open.take() {
        ack.placed(seq, rows);
    }

    // All chunks durable before resolving deferred referrers.
    if !chunk_batch.is_empty() {
        resolver
            .put(&chunk_batch)
            .await
            .map_err(|e| format!("bootstrap: toast store put: {e}"))?;
    }

    if !deferred.is_empty() {
        tracing::info!(
            target: "walshadow::bootstrap",
            deferred = deferred.len(),
            "resolving deferred TOAST tuples from chunk store",
        );
        let seq = next_seq;
        next_seq += 1;
        ack.register(seq, start_lsn);
        let mut placed = 0u64;
        for d in deferred {
            let Deferred {
                mut tuple,
                rel,
                mapping,
            } = d;
            resolve_or_fill_toast(&mut tuple, &rel, &mapping, &resolver).await?;
            route_row(&msg_tx, seq, rel, mapping, tuple).await?;
            placed += 1;
            rows_routed += 1;
        }
        ack.placed(seq, placed);
    }

    Ok(BootstrapDrainOutcome {
        next_seq,
        rows_routed,
    })
}

/// Increment the open seq's routed-row count + the running total.
fn bump(open: &mut Option<(pgwalrs::pg::walparser::RelFileNode, u64, u64)>, rows_routed: &mut u64) {
    if let Some(slot) = open.as_mut() {
        slot.2 += 1;
    }
    *rows_routed += 1;
}

async fn route_row(
    msg_tx: &mpsc::Sender<BatcherMsg>,
    seq: u64,
    rel: Arc<RelDescriptor>,
    mapping: Arc<TableMapping>,
    tuple: BackfillTuple,
) -> Result<(), String> {
    let committed = tuple.into_committed_insert();
    msg_tx
        .send(BatcherMsg::Row(RoutedRow {
            seq,
            rel,
            mapping,
            committed,
        }))
        .await
        .map_err(|_| "bootstrap: batcher channel closed".to_string())
}

/// True when a mapped (shipped) column carries an unresolved TOAST pointer.
/// Encoder ships exactly `mapping.columns`, so a toasted column outside the
/// mapping never reaches ClickHouse and needn't resolve.
fn has_mapped_external_toast(tuple: &BackfillTuple, mapping: &TableMapping) -> bool {
    mapping.columns.iter().any(|c| {
        usize::try_from(c.src_attnum as i32 - 1)
            .ok()
            .and_then(|idx| tuple.columns.get(idx))
            .is_some_and(|col| matches!(col, Some(ColumnValue::ExternalToast(_))))
    })
}

/// Resolve every mapped externally-TOASTed column in place: fetch its chunks
/// from the store (disk/CH) and reassemble, or — disabled — NULL/default-fill.
/// A genuine store gap (disk/CH) is a hard error.
async fn resolve_or_fill_toast(
    tuple: &mut BackfillTuple,
    rel: &RelDescriptor,
    mapping: &TableMapping,
    resolver: &ToastResolver,
) -> Result<(), String> {
    let mut chunks = ChunkMap::new();
    if resolver.stores_chunks() {
        for c in &mapping.columns {
            let Ok(idx) = usize::try_from(c.src_attnum as i32 - 1) else {
                continue;
            };
            let Some(Some(ColumnValue::ExternalToast(p))) = tuple.columns.get(idx) else {
                continue;
            };
            let key = (p.va_toastrelid, p.va_valueid);
            if chunks.contains_key(&key) {
                continue;
            }
            resolver
                .fetch_into(key.0, key.1, &mut chunks)
                .await
                .map_err(|e| format!("bootstrap: toast store fetch: {e}"))?;
        }
    }
    let maps: [&ChunkMap; 1] = [&chunks];
    for c in &mapping.columns {
        let Ok(idx) = usize::try_from(c.src_attnum as i32 - 1) else {
            continue;
        };
        let Some(Some(ColumnValue::ExternalToast(p))) = tuple.columns.get(idx) else {
            continue;
        };
        let p = *p;
        let type_oid = rel.attributes.get(idx).map(|a| a.type_oid).unwrap_or(0);
        match try_reassemble(&p, &maps).map_err(|e| e.to_string())? {
            Some(raw) => tuple.columns[idx] = Some(detoasted_value(raw, type_oid)),
            None if resolver.fill_on_miss() => {
                resolver.note_filled_default();
                tuple.columns[idx] = Some(ColumnValue::Null);
            }
            None => {
                resolver.note_fetch_miss();
                return Err(format!(
                    "bootstrap: relation {} column {} value_id={} on toast relid={} \
                     has no chunks in the store (see plans/future/TOAST.md)",
                    rel.qualified_name, c.target_name, p.va_valueid, p.va_toastrelid
                ));
            }
        }
    }
    Ok(())
}

/// Reinterpret a `pg_toast_*` tuple's 3 columns (`chunk_id oid`,
/// `chunk_seq int4`, `chunk_data bytea`) into a [`ToastChunk`]. `toast_relid`
/// is the toast rel's pg_class OID (`RelDescriptor::oid`), matching the
/// referring pointer's `va_toastrelid`.
fn chunk_from_columns(
    cols: &[Option<ColumnValue>],
    toast_relid: u32,
    source_lsn: u64,
) -> Option<ToastChunk> {
    if cols.len() < 3 {
        return None;
    }
    let value_id = match cols[0].as_ref()? {
        ColumnValue::Oid(v) => *v,
        _ => return None,
    };
    let chunk_seq = match cols[1].as_ref()? {
        ColumnValue::Int4(v) => *v as u32,
        _ => return None,
    };
    let chunk_data = match cols[2].as_ref()? {
        ColumnValue::Bytea(b) => b.clone(),
        ColumnValue::Text(s) => s.clone().into_bytes(),
        _ => return None,
    };
    Some(ToastChunk {
        toast_relid,
        value_id,
        chunk_seq,
        source_lsn,
        chunk_data,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ch_emitter::{ColumnMapping, EmitterConfig, TableMapping};
    use crate::heap_decoder::{ColumnValue, ToastPointer};
    use crate::pipeline::ack;
    use crate::pipeline::batcher::BatcherMsg;
    use crate::shadow_catalog::{RelAttr, RelDescriptor, ReplIdent};
    use crate::toast::DiskChunkStore;
    use pgwalrs::pg::walparser::RelFileNode;
    use std::collections::HashMap;
    use std::sync::atomic::AtomicU64;

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

    /// Main rel with one mapped `bytea` column (attnum 1), the detoast target.
    fn bytea_rel(rel_node: u32) -> Arc<RelDescriptor> {
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
                name: "b".into(),
                type_oid: 17,
                typmod: -1,
                not_null: false,
                dropped: false,
                type_name: "bytea".into(),
                type_byval: false,
                type_len: -1,
                type_align: 'i',
                type_storage: 'x',
                missing_text: None,
            }],
        })
    }

    fn bytea_mapping_for(rel_node: u32) -> TableMapping {
        TableMapping {
            target: format!("default.t{rel_node}"),
            columns: vec![ColumnMapping {
                src_attnum: 1,
                target_name: "b".into(),
                target_type: "String".into(),
            }],
        }
    }

    /// `pg_toast` rel so [`CatalogMap::is_toast`] fires; `oid` matches the
    /// referring pointer's `va_toastrelid`. Attributes unread — the drain
    /// reinterprets a toast tuple's columns positionally (`chunk_from_columns`).
    fn toast_rel(rel_node: u32) -> Arc<RelDescriptor> {
        let name = format!("pg_toast_{rel_node}");
        let qualified_name = RelDescriptor::build_qualified_name("pg_toast", &name);
        Arc::new(RelDescriptor {
            rfn: RelFileNode {
                spc_node: 1663,
                db_node: 5,
                rel_node,
            },
            oid: rel_node,
            namespace_oid: 99,
            namespace_name: "pg_toast".into(),
            name,
            qualified_name,
            kind: 't',
            persistence: 'p',
            replident: ReplIdent::Default { pk_attnums: None },
            attributes: vec![],
        })
    }

    /// Main-rel tuple whose mapped bytea column is an on-disk TOAST pointer
    /// into `toast_relid`/`value_id`, uncompressed (`va_extinfo` high bits
    /// clear so reassembly returns the concatenated chunks verbatim).
    fn bytea_toast_tuple(rel_node: u32, toast_relid: u32, value_id: u32) -> BackfillTuple {
        BackfillTuple {
            rfn: RelFileNode {
                spc_node: 1663,
                db_node: 5,
                rel_node,
            },
            xid: 99,
            source_lsn: 0x1000,
            columns: vec![Some(ColumnValue::ExternalToast(ToastPointer {
                va_rawsize: 9,
                va_extinfo: 5,
                va_valueid: value_id,
                va_toastrelid: toast_relid,
            }))],
        }
    }

    /// `pg_toast_*` page tuple: 3 columns (`chunk_id oid`, `chunk_seq int4`,
    /// `chunk_data bytea`) the drain reinterprets into a stored chunk.
    fn toast_chunk_tuple(rel_node: u32, value_id: u32, seq: i32, body: &[u8]) -> BackfillTuple {
        BackfillTuple {
            rfn: RelFileNode {
                spc_node: 1663,
                db_node: 5,
                rel_node,
            },
            xid: 99,
            source_lsn: 0x1000,
            columns: vec![
                Some(ColumnValue::Oid(value_id)),
                Some(ColumnValue::Int4(seq)),
                Some(ColumnValue::Bytea(body.to_vec())),
            ],
        }
    }

    /// Two rels, contiguous rows each: one seq per rfn, every row routed,
    /// each seq placed with its exact count.
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
        let (tup_tx, tup_rx) = mpsc::channel::<BackfillTuple>(64);

        for id in 0..3 {
            tup_tx.send(tuple(16400, id)).await.unwrap();
        }
        for id in 0..2 {
            tup_tx.send(tuple(16401, id)).await.unwrap();
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
            ToastResolver::disabled(),
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

        // No inserter, so only placed (not acked); drop ack to let collector exit.
        drop(ack);
        collector.await.unwrap();
    }

    /// rfn reappearing non-contiguously (object_store interleave) gets a fresh
    /// seq each run; unmapped rel skipped but still consumes a zero-row seq.
    #[tokio::test]
    async fn reappearing_and_unmapped_rfns() {
        let mut catalog = CatalogMap::new();
        catalog.insert(rel(16400));
        catalog.insert(rel(16401)); // resolvable but unmapped
        let mut tables = HashMap::new();
        tables.insert("public.t16400".to_string(), mapping_for(16400));
        let mapping: MappingHandle = Arc::new(tokio::sync::RwLock::new(tables));

        let emitter_ack = Arc::new(AtomicU64::new(0));
        let (ack, collector) = ack::spawn(emitter_ack);
        let (msg_tx, mut msg_rx) = mpsc::channel::<BatcherMsg>(64);
        let (tup_tx, tup_rx) = mpsc::channel::<BackfillTuple>(64);

        // 16400, 16401(unmapped), 16400 → seqs 0,1,2; only 0 and 2 route
        tup_tx.send(tuple(16400, 1)).await.unwrap();
        tup_tx.send(tuple(16401, 9)).await.unwrap();
        tup_tx.send(tuple(16400, 2)).await.unwrap();
        drop(tup_tx);

        let stats = Arc::new(EmitterStats::default());
        let drain_task = tokio::spawn(drain(
            tup_rx,
            catalog,
            mapping,
            msg_tx,
            ack.clone(),
            stats.clone(),
            ToastResolver::disabled(),
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

    /// Disabled resolver: a mapped externally-TOASTed column has no store to
    /// consult, so it NULL-fills inline, routes the row, and counts the fill.
    #[tokio::test]
    async fn disabled_resolver_fills_toast_with_null() {
        let mut catalog = CatalogMap::new();
        catalog.insert(bytea_rel(16400));
        let mut tables = HashMap::new();
        tables.insert("public.t16400".to_string(), bytea_mapping_for(16400));
        let mapping: MappingHandle = Arc::new(tokio::sync::RwLock::new(tables));

        let emitter_ack = Arc::new(AtomicU64::new(0));
        let (ack, collector) = ack::spawn(emitter_ack);
        let (msg_tx, mut msg_rx) = mpsc::channel::<BatcherMsg>(64);
        let (tup_tx, tup_rx) = mpsc::channel::<BackfillTuple>(64);

        tup_tx
            .send(bytea_toast_tuple(16400, 16500, 1))
            .await
            .unwrap();
        drop(tup_tx);

        let stats = Arc::new(EmitterStats::default());
        // from_config (not disabled()) so the resolver shares this stats handle
        let resolver =
            ToastResolver::from_config(&EmitterConfig::default(), stats.clone()).unwrap();
        let drain_task = tokio::spawn(drain(
            tup_rx,
            catalog,
            mapping,
            msg_tx,
            ack.clone(),
            stats.clone(),
            resolver,
        ));

        let mut rows = Vec::new();
        while let Some(BatcherMsg::Row(r)) = msg_rx.recv().await {
            rows.push(r);
        }
        let outcome = drain_task.await.unwrap().unwrap();
        assert_eq!(outcome.next_seq, 1);
        assert_eq!(outcome.rows_routed, 1);
        assert_eq!(rows.len(), 1);
        let cols = &rows[0].committed.decoded.new.as_ref().unwrap().columns;
        assert_eq!(
            cols[0],
            Some(ColumnValue::Null),
            "unresolved toast NULL-filled"
        );
        assert_eq!(stats.toast_values_filled_default.load(Ordering::Relaxed), 1);
        drop(ack);
        collector.await.unwrap();
    }

    /// Disk resolver: a `pg_toast_*` page tuple is persisted as a chunk, then
    /// a deferred main tuple fetches it back, reassembles the value, and routes
    /// it as a `Bytea` under the trailing deferred-resolution seq.
    #[tokio::test]
    async fn disk_resolver_reassembles_toast_from_chunk() {
        let mut catalog = CatalogMap::new();
        catalog.insert(bytea_rel(16400));
        catalog.insert(toast_rel(16500));
        let mut tables = HashMap::new();
        tables.insert("public.t16400".to_string(), bytea_mapping_for(16400));
        let mapping: MappingHandle = Arc::new(tokio::sync::RwLock::new(tables));

        let emitter_ack = Arc::new(AtomicU64::new(0));
        let (ack, collector) = ack::spawn(emitter_ack);
        let (msg_tx, mut msg_rx) = mpsc::channel::<BatcherMsg>(64);
        let (tup_tx, tup_rx) = mpsc::channel::<BackfillTuple>(64);

        // toast chunk first (its own zero-row seq), then the referring main row
        tup_tx
            .send(toast_chunk_tuple(16500, 1, 0, b"hello"))
            .await
            .unwrap();
        tup_tx
            .send(bytea_toast_tuple(16400, 16500, 1))
            .await
            .unwrap();
        drop(tup_tx);

        let tmp = tempfile::tempdir().unwrap();
        let stats = Arc::new(EmitterStats::default());
        let store = Arc::new(DiskChunkStore::new(tmp.path().to_path_buf()).unwrap());
        let drain_task = tokio::spawn(drain(
            tup_rx,
            catalog,
            mapping,
            msg_tx,
            ack.clone(),
            stats.clone(),
            ToastResolver::with_store(store, stats.clone()),
        ));

        let mut rows = Vec::new();
        while let Some(BatcherMsg::Row(r)) = msg_rx.recv().await {
            rows.push(r);
        }
        let outcome = drain_task.await.unwrap().unwrap();
        assert_eq!(outcome.next_seq, 3, "toast seq, main seq, deferred seq");
        assert_eq!(outcome.rows_routed, 1);
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].seq, 2, "deferred row routed under the trailing seq");
        let cols = &rows[0].committed.decoded.new.as_ref().unwrap().columns;
        assert_eq!(cols[0], Some(ColumnValue::Bytea(b"hello".to_vec())));
        assert_eq!(stats.toast_chunks_stored.load(Ordering::Relaxed), 1);
        assert_eq!(stats.toast_values_fetched.load(Ordering::Relaxed), 1);
        drop(ack);
        collector.await.unwrap();
    }
}
