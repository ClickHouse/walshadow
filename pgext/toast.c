/*
 * toast.c — read stored TOAST values out of shadow's physical heaps.
 *
 * The daemon's alternative to the ClickHouse chunk mirror: shadow already
 * replays source WAL, so if it also carries the TOAST heaps and indexes the
 * value is readable locally. Chunks are returned *stored*, not decompressed —
 * the daemon owns pglz/lz4 and the `va_tcinfo` prefix, and validating raw size
 * there keeps one decompressor in the system.
 *
 * Visibility is `HeapTupleSatisfiesToast`, which ignores xmax: a value whose
 * referring row version is dead stays readable until pruning or vacuum removes
 * the chunks. That is the property the design rests on, and this op exists to
 * measure it rather than assume it.
 *
 * Reuse of a value id across generations is the one case the ordered scan
 * cannot resolve on its own, because both generations satisfy the snapshot.
 * Requiring the run to be exactly dense from 0 covers it the same way it
 * covers a partly pruned run: either way the scan is refused, never spliced.
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heaptoast.h"
#include "access/htup_details.h"
#include "access/stratnum.h"
#include "access/table.h"
#include "access/toast_internals.h"
#include "access/xlog.h"
#include "access/xlogdefs.h"
#include "access/xlogrecovery.h"
#include "libpq/pqformat.h"
#include "utils/fmgroids.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "walshadow.h"

/* pg_toast_* attribute numbers (PG catalog/toasting.c) */
#define WS_TOAST_ATT_ID			1
#define WS_TOAST_ATT_SEQ		2
#define WS_TOAST_ATT_DATA		3

/*
 * Assemble one value's chunk run, appending
 * `[result:u8][len:u32][stored bytes]` to `out`.
 *
 * The run is accepted only when every `chunk_seq` lands exactly where the
 * previous one ended and the whole run totals `expected` — the same evidence
 * the mirror's assembler demands, so both backends fill on the same terms.
 * Density alone rejects a partly pruned run and interleaved generations
 * under a reused id, so neither needs a case of its own.
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
	int32		next_seq = 0;
	bool		dense = true;
	bool		saw_any = false;
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

		saw_any = true;
		/* A toast rel's columns are NOT NULL and its chunks are never
		 * themselves toasted (PG catalog/toasting.c), so these are
		 * structural, the same way detoast.c treats them */
		d = heap_getattr(ttup, WS_TOAST_ATT_SEQ, tupdesc, &isnull);
		Assert(!isnull);
		seq = DatumGetInt32(d);

		d = heap_getattr(ttup, WS_TOAST_ATT_DATA, tupdesc, &isnull);
		Assert(!isnull);
		chunk = DatumGetPointer(d);
		/* `chunk_data` is TYPSTORAGE_PLAIN, so it is neither packed into a
		 * short header nor toasted itself */
		Assert(!VARATT_IS_EXTENDED(chunk));
		chunksize = VARSIZE(chunk) - VARHDRSZ;
		chunkdata = VARDATA(chunk);

		if (seq != next_seq)
			dense = false;
		next_seq = seq + 1;
		appendBinaryStringInfo(&body, chunkdata, chunksize);
	}
	systable_endscan_ordered(toastscan);

	if (!saw_any)
		result = WS_FETCH_MISSING;
	else if (!dense || (uint32) body.len != expected)
		result = WS_FETCH_MISMATCH;
	else
		result = WS_FETCH_OK;

	/* Length travels either way: on a refusal it is how far the run got,
	 * which is what the daemon reports. Bytes follow only when accepted */
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
	SnapshotData snap;
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
		/*
		 * Refused on the declared sizes rather than on the assembled run: a
		 * batch the response cannot carry must cost no reads, and the
		 * StringInfo it would build errors as out-of-memory rather than as a
		 * protocol answer the daemon can act on
		 */
		want += sizes[i];
		if (want > WS_MAX_RESPONSE_BYTES)
			ereport(ERROR,
					(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
					 errmsg("walshadow fetch of %zu bytes over the %d cap",
							want, WS_MAX_RESPONSE_BYTES)));
	}

	/*
	 * Sampled before the relation is opened so a refusal costs no lock. The
	 * caller's bound is a floor, not the equality `SCAN` asserts: a value's
	 * chunks are written below the referring record, so replay only has to
	 * have reached it. Both samples go back regardless, because a destructive
	 * record replayed mid-batch is what would make an assembled run stale.
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

	snap = (snapmode == WS_SNAP_ANY) ? SnapshotAnyData : SnapshotToastData;

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
