/*
 * Read stored TOAST values from shadow's physical heaps.
 *
 * Shadow stores TOAST heaps and indexes while replaying source WAL, so daemon
 * can read values locally instead of using ClickHouse chunk mirror. Return
 * stored chunks without decompression. Daemon handles pglz/lz4, va_tcinfo,
 * and raw-size validation.
 *
 * HeapTupleSatisfiesToast ignores xmax. Values remain readable after referring
 * row version dies until pruning or vacuum removes chunks.
 *
 * Ordered scan cannot distinguish reused value IDs when snapshot includes
 * multiple generations. Require sequence starting at zero with no gaps and
 * exact total size. Reject partial or interleaved runs instead of combining
 * chunks from different generations.
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/stratnum.h"
#include "access/table.h"
#include "access/toast_internals.h"
#include "access/xlogdefs.h"
#include "access/xlogrecovery.h"
#include "libpq/pqformat.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"
#include "utils/snapshot.h"

#include "walshadow.h"

/* pg_toast_* attribute numbers (PG catalog/toasting.c) */
#define WS_TOAST_ATT_ID			1
#define WS_TOAST_ATT_SEQ		2
#define WS_TOAST_ATT_DATA		3

/*
 * Assemble one value's chunk run, appending
 * `[result:u8][len:u32][stored bytes]` to `out`.
 *
 * Accept only consecutive chunk_seq values starting at zero and exact expected
 * size, matching mirror validation. Same checks reject partially pruned runs
 * and interleaved generations under reused IDs.
 */
static void
ws_fetch_one(Relation toastrel, Relation toastidx, Snapshot snap,
			 Oid value_id, uint32 expected, StringInfo out)
{
	ScanKeyData toastkey;
	SysScanDesc toastscan;
	HeapTuple	ttup;
	TupleDesc	tupdesc = RelationGetDescr(toastrel);
	StringInfoData body;
	int32		nchunks = 0;
	bool		dense = true;
	uint8		result;

	initStringInfo(&body);
	ScanKeyInit(&toastkey, (AttrNumber) WS_TOAST_ATT_ID,
				BTEqualStrategyNumber, F_OIDEQ, ObjectIdGetDatum(value_id));
	toastscan = systable_beginscan_ordered(toastrel, toastidx, snap,
										   1, &toastkey);
	while ((ttup = systable_getnext_ordered(toastscan,
										   ForwardScanDirection)) != NULL)
	{
		Datum		d;
		bool		isnull;
		int32		seq;
		Pointer		chunk;
		int32		chunksize;
		char	   *chunkdata;

		/* TOAST columns are NOT NULL, see catalog/toasting.c */
		d = heap_getattr(ttup, WS_TOAST_ATT_SEQ, tupdesc, &isnull);
		Assert(!isnull);
		seq = DatumGetInt32(d);

		d = heap_getattr(ttup, WS_TOAST_ATT_DATA, tupdesc, &isnull);
		Assert(!isnull);
		chunk = DatumGetPointer(d);
		/* TYPSTORAGE_PLAIN prevents short headers and nested TOAST values */
		Assert(!VARATT_IS_EXTENDED(chunk));
		chunksize = VARSIZE(chunk) - VARHDRSZ;
		chunkdata = VARDATA(chunk);

		if (seq != nchunks)
			dense = false;
		nchunks++;
		appendBinaryStringInfo(&body, chunkdata, chunksize);
	}
	systable_endscan_ordered(toastscan);

	if (nchunks == 0)
		result = WS_FETCH_MISSING;
	else if (!dense || (uint32) body.len != expected)
		result = WS_FETCH_MISMATCH;
	else
		result = WS_FETCH_OK;

	/* Report assembled length on failure, include bytes only on success */
	pq_sendbyte(out, result);
	pq_sendint32(out, (uint32) body.len);
	if (result == WS_FETCH_OK)
		pq_sendbytes(out, body.data, body.len);
	pfree(body.data);
}

void
ws_handle_fetch_toast(StringInfo req, StringInfo resp)
{
	uint64		min_replay_lsn = pq_getmsgint64(req);
	Oid			toast_relid = (Oid) pq_getmsgint(req, 4);
	uint8		snapmode = pq_getmsgbyte(req);
	uint32		nvalues = pq_getmsgint(req, 4);
	Oid		   *ids;
	uint32	   *sizes;
	Relation	toastrel;
	Relation   *toastidxs;
	int			num_indexes;
	int			validIndex;
	SnapshotData snap = {0};
	StringInfoData vals;
	uint64		lsn_start;
	uint64		lsn_end;
	Size		want = 0;
	uint32		i;

	if (nvalues == 0 || nvalues > WS_MAX_FETCH_VALUES)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("walshadow fetch of %u values, want 1..%d",
						nvalues, WS_MAX_FETCH_VALUES)));
	if (snapmode != WS_SNAP_TOAST && snapmode != WS_SNAP_ANY)
		ereport(ERROR,
				(errcode(ERRCODE_PROTOCOL_VIOLATION),
				 errmsg("walshadow unknown fetch snapshot mode %u", snapmode)));

	ids = palloc_array(Oid, nvalues);
	sizes = palloc_array(uint32, nvalues);
	for (i = 0; i < nvalues; i++)
	{
		ids[i] = (Oid) pq_getmsgint(req, 4);
		sizes[i] = pq_getmsgint(req, 4);
		/* Reject oversized batch before reads or StringInfo allocation */
		want += sizes[i];
		if (want > WS_MAX_RESPONSE_BYTES)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("walshadow fetch of %zu bytes over the %d cap",
							want, WS_MAX_RESPONSE_BYTES)));
	}

	/*
	 * Sample before opening relation so early rejection acquires no lock.
	 * Caller's bound is minimum replay position. Chunks precede referring
	 * record, so replay only needs to reach that record. Return positions before
	 * and after read to detect destructive WAL replay during batch.
	 */
	lsn_start = (uint64) GetXLogReplayRecPtr(NULL);
	if (min_replay_lsn != 0 && lsn_start < min_replay_lsn)
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
				 errmsg("walshadow: replay at %X/%08X is below the requested %X/%08X",
						LSN_FORMAT_ARGS((XLogRecPtr) lsn_start),
						LSN_FORMAT_ARGS((XLogRecPtr) min_replay_lsn))));

	toastrel = try_table_open(toast_relid, AccessShareLock);
	if (toastrel == NULL)
		ereport(ERROR,
				(errcode(ERRCODE_UNDEFINED_TABLE),
				 errmsg("walshadow: no toast relation %u", toast_relid)));
	validIndex = toast_open_indexes(toastrel, AccessShareLock,
									&toastidxs, &num_indexes);

	/*
	 * PostgreSQL 18 exports SnapshotToastData; 16 and 17 initialize it with
	 * InitToastSnapshot. Zero lsn and whenTaken disable PostgreSQL 16
	 * old-snapshot check
	 */
	snap.snapshot_type = snapmode == WS_SNAP_ANY ? SNAPSHOT_ANY : SNAPSHOT_TOAST;

	initStringInfo(&vals);
	for (i = 0; i < nvalues; i++)
		ws_fetch_one(toastrel, toastidxs[validIndex], &snap,
					 ids[i], sizes[i], &vals);

	toast_close_indexes(toastidxs, num_indexes, AccessShareLock);
	table_close(toastrel, AccessShareLock);
	lsn_end = (uint64) GetXLogReplayRecPtr(NULL);

	pq_sendbyte(resp, WS_STATUS_OK);
	pq_sendint64(resp, lsn_start);
	pq_sendint64(resp, lsn_end);
	pq_sendint32(resp, nvalues);
	pq_sendbytes(resp, vals.data, vals.len);
}
