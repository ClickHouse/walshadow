//! Operator-visible counters shared across emit, backfill, and TOAST paths

use std::sync::Arc;

/// Values-per-fetch buckets: 1 doubling to 256, where one means no batching
/// and the top is past a typical deferred-resolution batch
pub type ValuesPerFetch = crate::atomic_stats::Histogram<1, 9>;

pub type ReplicationLatency = crate::atomic_stats::Histogram<1_000, 18>;

crate::atomic_stats! {
    /// CH emitter counters. `fetch_add(_, Relaxed)`; status loop reads
    /// via `.load(Relaxed)`.
    pub struct EmitterStats {
        pub rows_emitted,
        pub backfill_copy_rows,
        pub backfill_copy_bytes,
        pub backfill_backup_pump: Arc<crate::backfill::backup_source::PumpStats>,
        pub backfill_backup_walk: Arc<PageWalkStats>,
        pub blocks_sent,
        pub inserter_idle_nanos,
        pub batcher_busy_nanos,
        pub batcher_idle_nanos,
        pub batcher_send_nanos,
        pub queue_blocked_out_nanos: Arc<std::sync::atomic::AtomicU64>,
        pub xacts_committed,
        pub unsupported_relations,
        /// DELETE rows dropped because `[system_columns] is_deleted = false`
        /// leaves them nowhere to land
        pub deletes_discarded,
        /// `retries_attempted` counts one per failing operation, not per
        /// attempt (one op needing 3 retries adds 3)
        pub reconnects,
        pub retries_attempted,
        pub truncates_emitted,
        /// Legacy serial-emitter counter; pooled pipeline seals via the
        /// batcher's own `flush_timeout` deadline and never bumps this
        pub flush_deadline_trips,
        pub toast_chunks_stored,
        /// With the seconds counter, per-part commit latency
        pub toast_chunk_puts,
        pub toast_chunk_put_nanos,
        pub toast_tombstones_stored,
        /// Toasted values reassembled from the store (not the in-xact buffer)
        pub toast_values_fetched,
        /// Store fetch round trips; a batch covers many values
        pub toast_value_fetch_batches,
        pub toast_value_fetch_nanos,
        /// Store reads started ahead of planning, off the serial path
        pub toast_values_read_ahead,
        /// Planning blocked on reads started ahead of it
        pub toast_value_wait_nanos,
        /// Values requested per store fetch
        pub toast_values_per_fetch: ValuesPerFetch,
        pub replication_latency: ReplicationLatency,
        /// Toasted values NULL/default-filled because no store could rebuild
        /// them (disabled mode). Surfaced, never silent
        pub toast_values_filled_default,
        pub toast_values_filled_superseded,
        pub toast_values_filled_mismatch,
        /// Shadow values replaced with a fill after detecting value-ID reuse
        pub toast_values_filled_generation,
        /// Oversized values replaced under `inline_value_overflow = "null"`
        pub toast_values_filled_oversize,
        pub toast_fetch_miss,
        pub bootstrap_replay_covered_rows,
        pub gate_emitted,
        pub gate_gated,
        pub gate_deferred,
        pub gate_chunks_gated,
        pub gate_multixact_emitted,
        pub gate_pending,
        pub gate_rechecked,
        pub gate_recheck_total,
        pub bootstrap_fill_bytes,
        pub bootstrap_filled_bytes,
        /// Chunk rows mirrored from a restored TOAST page image, repairing
        /// backup page copies read mid-write
        pub toast_image_rows_mirrored,
        /// Gauge: bytes resident in the in-memory prefixes of every
        /// bootstrap TOAST-deferred spool, released as each one replays
        pub bootstrap_deferred_bytes,
        /// Gauge: encoded bytes in every bootstrap TOAST-deferred spool file
        pub bootstrap_deferred_spool_bytes,
        pub bootstrap_deferred_replay_bytes,
        pub bootstrap_deferred_replayed_bytes,
        /// Undecided backup tuples written to pending tables
        /// ([`crate::backfill::visibility_pending`])
        pub pending_rows,
        pub pending_tables,
        pub pending_tables_dropped,
        /// Deciding xids whose commit or abort a settle round learned
        pub pending_xacts_settled,
        /// Gauge: xids the pending row ledger still waits on
        pub pending_outstanding_xids,
        /// Gauge: outstanding xids the shadow's `pg_xact` no longer covers,
        /// so no settle round can ever decide them
        pub pending_undecidable_xids,
        pub toast_mirror_truncates,
        pub toast_mirror_retires,
        /// Rewrite generations closed with residual `O - B` tombstones
        pub toast_rewrite_barriers,
        /// Stashed records decoded at commit against a resolved toast heap
        pub toast_stash_decoded,
        /// Stashed records discarded: filenode unresolvable post-commit
        /// (dropped or rotated away), end-state-neutral by AEL supersession
        pub toast_stash_discarded,
        /// Stashed toast filenodes that rotated nothing, so queued no barrier
        pub toast_stash_in_place,
        /// Stashed filenodes resolved to a foreign database at commit,
        /// cluster-level skip counted once per filenode
        pub stash_foreign_db_skipped,
        /// Routed heaps sealed into transaction plans (unmapped discards
        /// excluded)
        pub plan_rows,
        /// Sealed plan bytes by final backing
        pub plan_bytes_mem,
        pub plan_bytes_file,
        /// Planning-stage failures by reason; a failed plan means the whole
        /// transaction emits nothing
        pub plan_failures_spool,
        pub plan_failures_fail_closed_image_only,
        pub plan_failures_fail_closed_malformed,
        pub plan_failures_fail_closed_unsupported_op,
        pub plan_failures_stash_ambiguous,
        pub plan_failures_incomplete_toast,
        pub plan_failures_missing_stash_resolution,
        pub plan_failures_detoast,
        pub plan_failures_partial_update,
        pub plan_failures_view,
        /// Row values a mapped column cannot encode
        pub plan_failures_encode,
        pub plan_failures_drain,
        pub unmapped_rows_by_table: crate::emit::pipeline::row_ledger::TableRowCounts,
        pub rows_inserted_by_table: crate::emit::pipeline::row_ledger::TableRowCounts,
        pub cells_inserted_by_type: crate::emit::pipeline::row_ledger::CellCounts,
        /// Commit-resolve raw decode: records by verdict kind, per op
        /// (`raw_decode_records_total{kind,op}`)
        pub raw_decode_toast_ops: crate::decode::heap_decoder::OpCounters,
        pub raw_decode_ordinary_ops: crate::decode::heap_decoder::OpCounters,
        /// Rows fanned out of decoded raw records per op
        /// (`raw_decode_rows_total{op}`); MULTI_INSERT yields many per record
        pub raw_decode_rows_ops: crate::decode::heap_decoder::OpCounters,
        // Pipeline-flow counters; `_out`/`_in` pairs give channel depth. See
        // `metrics::render`.
        pub queue_jobs_out,
        pub decode_jobs_in,
        pub decode_rows_out,
        pub insertbatch_rows_in,
        pub insertbatch_batches_out,
        pub inserter_batches_in,
        /// Inserter time inside the INSERT round trip (`send_query` through
        /// `EndOfStream`), retries included. Against `inserter_pool_size ×
        /// elapsed` this is CH-side utilization
        pub inserter_ch_nanos,
        /// Resolver time inside the oracle round trip, retries included.
        /// Overlaps the inserters' ClickHouse time, so against
        /// `inserter_ch_nanos` it says which stage is the limiter
        pub oracle_resolve_nanos,
        /// Oracle-routed columns the daemon built itself: rendered cells
        /// against a `String` target, which PG would hand straight back
        pub oracle_local_columns,
        /// Serialized Native block bytes before and after resident
        /// compression; their ratio is what retained output saves
        pub resident_raw_bytes,
        pub resident_stored_bytes,
        /// Resolver time serializing and compressing sealed blocks
        pub resident_seal_nanos,
        /// Gauges: every tail's output pool (queued slabs, sealed blocks,
        /// seal workspace), open batch slabs
        pub output_resident_bytes: Arc<std::sync::atomic::AtomicU64>,
        pub batcher_open_bytes,
    }
}

crate::atomic_stats! {
    /// Per-pump counters, operator-visible. Atomic so per-entry walkers
    /// share one handle instead of funnelling through the sink lock
    pub struct PageWalkStats {
        pub files_seen,
        pub files_walked,
        /// Filenode absent from catalog map, typically a race against the seed
        pub files_skipped_unknown_filenode,
        /// Filenode outside the mapped set, declined before any page decode
        pub files_skipped_unmapped,
        /// Walked by an earlier attempt of a resumed pass, declined here
        pub files_resumed,
        pub toast_files_observed,
        pub pages_walked,
        pub slots_seen,
        pub tuples_emitted,
        pub tuples_skipped_lp_flag,
        pub tuples_skipped_truncated,
        /// Trailing partial-page bytes; PG heap files are page-aligned so
        /// nonzero is anomalous
        pub tail_bytes_dropped,
        /// `PageWalker::walk_page` CPU, decode included
        pub decode_nanos,
        /// Blocked on a free tuple-channel slot: the bootstrap
        /// backpressure point, so this is emitter drain time seen by the walk
        pub channel_block_nanos,
    }
}
