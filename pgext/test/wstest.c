/*
 * Test-only helpers. Not part of the walshadow module: built and loaded only
 * by the daemon's integration tests, never by a deployed shadow. Each gives a
 * test a primitive SQL cannot express.
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/heapam.h"
#include "access/htup_details.h"
#include "access/table.h"
#include "access/toast_internals.h"
#include "fmgr.h"
#include "storage/lmgr.h"
#include "utils/rel.h"
#include "varatt.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(ws_test_lock_unlock_relation);
PG_FUNCTION_INFO_V1(ws_test_plant_toast_chunk);

/*
 * Reproduce VACUUM's lock lifetime (vacuumlazy.c, lazy_truncate_heap):
 * acquire AccessExclusiveLock on a relation, then release it source-side
 * while the surrounding transaction stays open. The acquire emits
 * XLOG_STANDBY_LOCK; the release emits nothing. A standby therefore keeps
 * the replayed lock until the transaction's commit record arrives, which is
 * the asymmetry tests need to drive deterministically.
 */
Datum
ws_test_lock_unlock_relation(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);

	LockRelationOid(relid, AccessExclusiveLock);
	UnlockRelationOid(relid, AccessExclusiveLock);

	PG_RETURN_VOID();
}

/*
 * Insert one chunk tuple into a toast relation with a header PostgreSQL never
 * writes there: short when `compressed` is false, 4-byte compressed otherwise.
 * Toast columns are TYPSTORAGE_PLAIN (catalog/toasting.c), so toast_save_datum
 * stores plain 4-byte chunks only, while readers still take the other shapes
 * (heap_fetch_toast_slice). heap_form_tuple copies a pre-shaped varlena as is.
 * Index maintenance mirrors toast_save_datum.
 */
Datum
ws_test_plant_toast_chunk(PG_FUNCTION_ARGS)
{
	Oid			toastrelid = PG_GETARG_OID(0);
	Oid			valueid = PG_GETARG_OID(1);
	int32		seq = PG_GETARG_INT32(2);
	bytea	   *payload = PG_GETARG_BYTEA_PP(3);
	bool		compressed = PG_GETARG_BOOL(4);
	Size		len = VARSIZE_ANY_EXHDR(payload);
	struct varlena *chunk;
	Relation	toastrel;
	Relation   *toastidxs;
	int			num_indexes;
	Datum		values[3];
	bool		isnull[3] = {false, false, false};
	HeapTuple	tup;

	if (compressed)
	{
		chunk = palloc(VARHDRSZ + len);
		SET_VARSIZE_COMPRESSED(chunk, VARHDRSZ + len);
		memcpy(VARDATA(chunk), VARDATA_ANY(payload), len);
	}
	else
	{
		if (VARHDRSZ_SHORT + len > VARATT_SHORT_MAX)
			elog(ERROR, "%zu bytes do not fit a short varlena", len);
		chunk = palloc(VARHDRSZ_SHORT + len);
		SET_VARSIZE_SHORT(chunk, VARHDRSZ_SHORT + len);
		memcpy(VARDATA_SHORT(chunk), VARDATA_ANY(payload), len);
	}

	toastrel = table_open(toastrelid, RowExclusiveLock);
	toast_open_indexes(toastrel, RowExclusiveLock, &toastidxs, &num_indexes);

	values[0] = ObjectIdGetDatum(valueid);
	values[1] = Int32GetDatum(seq);
	values[2] = PointerGetDatum(chunk);
	tup = heap_form_tuple(RelationGetDescr(toastrel), values, isnull);
	simple_heap_insert(toastrel, tup);
	for (int i = 0; i < num_indexes; i++)
		index_insert(toastidxs[i], values, isnull, &tup->t_self, toastrel,
					 UNIQUE_CHECK_YES, false, NULL);

	toast_close_indexes(toastidxs, num_indexes, RowExclusiveLock);
	table_close(toastrel, RowExclusiveLock);

	PG_RETURN_VOID();
}
