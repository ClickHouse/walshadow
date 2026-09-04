/*
 * overlay.c — read a replaying transaction's own uncommitted catalog rows.
 *
 * Shadow replays source WAL and is parked at some LSN L by the daemon
 * withholding successor bytes. At L the catalog rows of an in-flight DDL
 * transaction are on-page and uncommitted: no MVCC snapshot sees them, a
 * SnapshotAny scan does. Replay being pinned at L is what does the temporal
 * filtering, so no combocid machinery is needed: the latest uncommitted row
 * version on the page is the state as of L.
 *
 * Never opens the target relation. The replaying transaction holds
 * AccessExclusiveLock on it and standby lock replay is driven by the startup
 * process, so relation_open would block against recovery. Catalogs only.
 *
 * AccessShareLock on a catalog does not conflict with the DDL in flight, but
 * that is not the only lock replay can be holding: any unrelated source
 * transaction that took AccessExclusiveLock on the catalog itself — VACUUM
 * truncating a bloated pg_type is the one seen in practice — leaves the
 * startup process holding it until that transaction's commit record is
 * replayed, and the caller is withholding exactly that record. A pinned scan
 * therefore takes the lock only if it is free, and reads without it otherwise;
 * see `ws_overlay_scan`.
 *
 * An invalid top xid asks the same scan for the committed view, which is what
 * the daemon captures at a catalog commit. Same projections, same assembly on
 * the other side, so committed and uncommitted descriptors cannot drift apart.
 *
 * Values are emitted in each type's text output form, one projection per
 * catalog. Projections mirror daemon descriptor inputs and Rust ScanRow
 * parsers. Bump WS_PROJECTION_VERSION on any change.
 */
#include "postgres.h"

#include "access/genam.h"
#include "access/htup_details.h"
#include "access/stratnum.h"
#include "access/subtrans.h"
#include "access/table.h"
#include "access/transam.h"
#include "catalog/pg_attribute.h"
#include "catalog/pg_class.h"
#include "catalog/pg_index.h"
#include "catalog/pg_namespace.h"
#include "catalog/pg_type.h"
#include "libpq/pqformat.h"
#include "storage/lmgr.h"
#include "storage/procarray.h"
#include "utils/fmgroids.h"
#include "utils/fmgrprotos.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

#include "walshadow.h"

/* -------------------------------------------------------------------------
 * column emitters
 * ------------------------------------------------------------------------- */

static void
ws_put_null(StringInfo out)
{
	pq_sendint32(out, (uint32) -1);
}

static void
ws_put_str(StringInfo out, const char *s)
{
	int			len = (int) strlen(s);

	pq_sendint32(out, (uint32) len);
	pq_sendbytes(out, s, len);
}

static void
ws_put_oid(StringInfo out, Oid v)
{
	char		buf[16];

	snprintf(buf, sizeof(buf), "%u", v);
	ws_put_str(out, buf);
}

static void
ws_put_int(StringInfo out, int v)
{
	char		buf[16];

	snprintf(buf, sizeof(buf), "%d", v);
	ws_put_str(out, buf);
}

static void
ws_put_bool(StringInfo out, bool v)
{
	ws_put_str(out, v ? "t" : "f");
}

static void
ws_put_char(StringInfo out, char c)
{
	pq_sendint32(out, 1);
	pq_sendbytes(out, &c, 1);
}

static void
ws_put_name(StringInfo out, const NameData *n)
{
	ws_put_str(out, NameStr(*n));
}

/* -------------------------------------------------------------------------
 * visibility
 * ------------------------------------------------------------------------- */

/*
 * Does `xid` belong to the transaction tree rooted at `top`?
 *
 * An invalid `top` is the committed read: no transaction is the caller's, so
 * every in-progress writer is foreign and the predicate degenerates to what an
 * MVCC snapshot would see. Nothing to misattribute, so no mismatch to count.
 *
 * Standby pg_subtrans is only as complete as the XLOG_XACT_ASSIGNMENT records
 * that reached it (emitted only past 64 cached subxids), so ordinary savepoint
 * DDL leaves a subxact with no recorded parent, indistinguishable from a
 * foreign top-level writer. A recorded chain rooting at another top is proof
 * of foreign; no recorded parent is proof of nothing.
 *
 * Rel-scoped callers pass oids the replaying transaction holds
 * AccessExclusiveLock on, which already establishes no other writer can be
 * here; they trust that over missing parentage and only count the mismatch.
 * Whole-catalog callers have no lock argument, and guessing either way
 * returns rows that misrepresent the tree, so they fail the request.
 */
static bool
ws_xid_is_ours(TransactionId xid, TransactionId top, bool rel_scoped,
			   WsScanStats *stats)
{
	TransactionId resolved = xid;

	if (!TransactionIdIsValid(top))
		return false;
	if (TransactionIdEquals(xid, top))
		return true;

	/* SubTransGetTopmostTransaction cannot look back past TransactionXmin */
	if (TransactionIdIsValid(TransactionXmin) &&
		!TransactionIdPrecedes(xid, TransactionXmin))
		resolved = SubTransGetTopmostTransaction(xid);

	if (TransactionIdEquals(resolved, top))
		return true;

	stats->subtrans_mismatch++;
	if (!TransactionIdEquals(resolved, xid))
		return false;			/* recorded parentage roots elsewhere */

	if (rel_scoped)
		return true;
	ereport(ERROR,
			(errcode(ERRCODE_OBJECT_NOT_IN_PREREQUISITE_STATE),
			 errmsg("walshadow overlay inconclusive: in-progress xid %u has no resolvable parent",
					xid)));
}

/*
 * The transaction's own view of the row as of L: its inserts are present, its
 * deletes are applied.
 *
 * A catalog UPDATE is delete+insert, so honouring our own xmax is what keeps
 * ALTER TABLE from yielding two rows for one (attrelid, attnum). A foreign
 * uncommitted delete is not ours to apply, so the row stays.
 */
static bool
ws_tuple_visible(HeapTupleHeader th, TransactionId top, bool rel_scoped,
				 WsScanStats *stats)
{
	TransactionId xmin;
	TransactionId xmax;

	if (HeapTupleHeaderXminInvalid(th))
		return false;

	xmin = HeapTupleHeaderGetXmin(th);
	if (!HeapTupleHeaderXminFrozen(th) && !HeapTupleHeaderXminCommitted(th))
	{
		if (TransactionIdIsInProgress(xmin))
		{
			if (!ws_xid_is_ours(xmin, top, rel_scoped, stats))
				return false;
		}
		else if (!TransactionIdDidCommit(xmin))
			return false;		/* aborted or crashed inserter */
	}

	if ((th->t_infomask & HEAP_XMAX_INVALID) ||
		HEAP_XMAX_IS_LOCKED_ONLY(th->t_infomask))
		return true;

	xmax = (th->t_infomask & HEAP_XMAX_IS_MULTI)
		? HeapTupleHeaderGetUpdateXid(th)
		: HeapTupleHeaderGetRawXmax(th);
	if (!TransactionIdIsValid(xmax))
		return true;

	if (TransactionIdIsInProgress(xmax))
		return !ws_xid_is_ours(xmax, top, rel_scoped, stats);

	return !TransactionIdDidCommit(xmax);
}

/* -------------------------------------------------------------------------
 * per-catalog projections
 * ------------------------------------------------------------------------- */

static void
ws_emit_class(StringInfo out, HeapTuple tup, TupleDesc desc)
{
	Form_pg_class f = (Form_pg_class) GETSTRUCT(tup);

	ws_put_oid(out, f->oid);
	ws_put_oid(out, f->relnamespace);
	ws_put_name(out, &f->relname);
	ws_put_char(out, f->relkind);
	ws_put_char(out, f->relpersistence);
	ws_put_char(out, f->relreplident);
	ws_put_oid(out, f->reltoastrelid);
	ws_put_oid(out, f->reltablespace);
	ws_put_oid(out, f->relfilenode);
}

static void
ws_emit_attribute(StringInfo out, HeapTuple tup, TupleDesc desc)
{
	Form_pg_attribute f = (Form_pg_attribute) GETSTRUCT(tup);

	ws_put_oid(out, f->attrelid);
	ws_put_int(out, f->attnum);
	ws_put_name(out, &f->attname);
	ws_put_oid(out, f->atttypid);
	ws_put_int(out, f->atttypmod);
	ws_put_bool(out, f->attnotnull);
	ws_put_bool(out, f->attisdropped);
	ws_put_bool(out, f->attbyval);
	ws_put_int(out, f->attlen);
	ws_put_char(out, f->attalign);
	ws_put_char(out, f->attstorage);

	if (!f->atthasmissing)
		ws_put_null(out);
	else
	{
		Datum		d;
		bool		isnull;

		d = heap_getattr(tup, Anum_pg_attribute_attmissingval, desc, &isnull);
		if (isnull)
			ws_put_null(out);
		else
		{
			Oid			outfunc;
			bool		varlena;

			/*
			 * Stored as anyarray; anyarray_out reads the element type from
			 * the array header. A default whose element type was created by
			 * this same uncommitted transaction is invisible to that lookup
			 * and errors the request, degrading the capture.
			 */
			getTypeOutputInfo(ANYARRAYOID, &outfunc, &varlena);
			ws_put_str(out, OidOutputFunctionCall(outfunc, d));
		}
	}
}

static void
ws_emit_index(StringInfo out, HeapTuple tup, TupleDesc desc)
{
	Form_pg_index f = (Form_pg_index) GETSTRUCT(tup);

	ws_put_oid(out, f->indexrelid);
	ws_put_oid(out, f->indrelid);
	ws_put_bool(out, f->indisprimary);
	ws_put_bool(out, f->indisreplident);
	/* int2vectorout form: space-separated, not the int2[] braces */
	ws_put_str(out, DatumGetCString(DirectFunctionCall1(int2vectorout,
													   PointerGetDatum(&f->indkey))));
}

static void
ws_emit_namespace(StringInfo out, HeapTuple tup, TupleDesc desc)
{
	Form_pg_namespace f = (Form_pg_namespace) GETSTRUCT(tup);

	ws_put_oid(out, f->oid);
	ws_put_name(out, &f->nspname);
}

static void
ws_emit_type(StringInfo out, HeapTuple tup, TupleDesc desc)
{
	Form_pg_type f = (Form_pg_type) GETSTRUCT(tup);

	ws_put_oid(out, f->oid);
	ws_put_name(out, &f->typname);
}

typedef void (*WsEmitRow) (StringInfo out, HeapTuple tup, TupleDesc desc);

typedef struct WsCatalogPlan
{
	Oid			relid;
	Oid			indexid;		/* InvalidOid: no oid list is possible */
	AttrNumber	keyattno;		/* InvalidAttrNumber: no oid list is possible */
	/* pg_attribute only: system columns the descriptor never wants */
	int16		min_attnum;
	int			ncols;
	WsEmitRow	emit;
} WsCatalogPlan;

static bool
ws_catalog_plan(WsCatalog cat, WsCatalogPlan *plan)
{
	switch (cat)
	{
		case WS_CAT_CLASS:
			plan->relid = RelationRelationId;
			plan->indexid = ClassOidIndexId;
			plan->keyattno = Anum_pg_class_oid;
			plan->min_attnum = 0;
			plan->ncols = 9;
			plan->emit = ws_emit_class;
			return true;
		case WS_CAT_ATTRIBUTE:
			plan->relid = AttributeRelationId;
			plan->indexid = AttributeRelidNumIndexId;
			plan->keyattno = Anum_pg_attribute_attrelid;
			plan->min_attnum = 1;
			plan->ncols = 12;
			plan->emit = ws_emit_attribute;
			return true;
		case WS_CAT_INDEX:
			plan->relid = IndexRelationId;
			plan->indexid = IndexIndrelidIndexId;
			plan->keyattno = Anum_pg_index_indrelid;
			plan->min_attnum = 0;
			plan->ncols = 5;
			plan->emit = ws_emit_index;
			return true;
		case WS_CAT_NAMESPACE:
			plan->relid = NamespaceRelationId;
			plan->indexid = InvalidOid;
			plan->keyattno = InvalidAttrNumber;
			plan->min_attnum = 0;
			plan->ncols = 2;
			plan->emit = ws_emit_namespace;
			return true;
		case WS_CAT_TYPE:
			plan->relid = TypeRelationId;
			plan->indexid = InvalidOid;
			plan->keyattno = InvalidAttrNumber;
			plan->min_attnum = 0;
			plan->ncols = 2;
			plan->emit = ws_emit_type;
			return true;
	}
	return false;
}

int
ws_overlay_ncols(WsCatalog cat)
{
	WsCatalogPlan plan;

	if (!ws_catalog_plan(cat, &plan))
		return -1;
	return plan.ncols;
}

/* -------------------------------------------------------------------------
 * scan
 * ------------------------------------------------------------------------- */

static void
ws_scan_emit(Relation rel, const WsCatalogPlan *plan, Oid key,
			 TransactionId top, bool rel_scoped,
			 StringInfo out, WsScanStats *stats)
{
	SysScanDesc scan;
	ScanKeyData skey[2];
	int			nkeys = 0;
	HeapTuple	tup;
	TupleDesc	desc = RelationGetDescr(rel);

	if (rel_scoped)
		ScanKeyInit(&skey[nkeys++], plan->keyattno, BTEqualStrategyNumber,
					F_OIDEQ, ObjectIdGetDatum(key));
	if (plan->min_attnum != 0)
	{
		/*
		 * The projection is attnum >= 1 whether or not an oid list scoped it:
		 * pg_attribute's index is (attrelid, attnum) so a rel-scoped scan
		 * rides the same index, and a whole-catalog one filters on the heap.
		 */
		ScanKeyInit(&skey[nkeys++], Anum_pg_attribute_attnum,
					BTGreaterEqualStrategyNumber, F_INT2GE,
					Int16GetDatum(plan->min_attnum));
	}

	scan = systable_beginscan(rel, rel_scoped ? plan->indexid : InvalidOid,
							  rel_scoped, SnapshotAny, nkeys, skey);
	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		stats->scanned++;
		if (!ws_tuple_visible(tup->t_data, top, rel_scoped, stats))
			continue;
		stats->emitted++;
		plan->emit(out, tup, desc);
	}
	systable_endscan(scan);
}

/* qsort/bsearch comparator over Oid */
static int
ws_oid_cmp(const void *a, const void *b)
{
	Oid			x = *(const Oid *) a;
	Oid			y = *(const Oid *) b;

	if (x < y)
		return -1;
	return (x > y) ? 1 : 0;
}

/*
 * One heap pass, emitting rows whose key attribute is in `sorted` (or every
 * row when `nsorted` is 0). Used only when the catalog's lock was unavailable:
 * `systable_beginscan` with an index would `index_open` it under
 * AccessShareLock, which is the wait this path exists to avoid, so the index
 * is skipped and the oid list is applied to the heap tuple instead.
 *
 * `scanned` counts rows that passed the oid filter, so it stays comparable to
 * the indexed path.
 */
static void
ws_scan_emit_lockfree(Relation rel, const WsCatalogPlan *plan,
					  const Oid *sorted, int nsorted, TransactionId top,
					  StringInfo out, WsScanStats *stats)
{
	SysScanDesc scan;
	ScanKeyData skey[1];
	int			nkeys = 0;
	HeapTuple	tup;
	TupleDesc	desc = RelationGetDescr(rel);
	bool		scoped = nsorted > 0;

	if (plan->min_attnum != 0)
		ScanKeyInit(&skey[nkeys++], Anum_pg_attribute_attnum,
					BTGreaterEqualStrategyNumber, F_INT2GE,
					Int16GetDatum(plan->min_attnum));

	scan = systable_beginscan(rel, InvalidOid, false, SnapshotAny, nkeys, skey);
	while (HeapTupleIsValid(tup = systable_getnext(scan)))
	{
		if (scoped)
		{
			Datum		key;
			bool		isnull;
			Oid			keyoid;

			key = heap_getattr(tup, plan->keyattno, desc, &isnull);
			if (isnull)
				continue;
			keyoid = DatumGetObjectId(key);
			if (bsearch(&keyoid, sorted, (size_t) nsorted, sizeof(Oid),
						ws_oid_cmp) == NULL)
				continue;
		}
		stats->scanned++;
		if (!ws_tuple_visible(tup->t_data, top, scoped, stats))
			continue;
		stats->emitted++;
		plan->emit(out, tup, desc);
	}
	systable_endscan(scan);
}

void
ws_overlay_scan(WsCatalog cat, TransactionId top, const Oid *oids, int noids,
				WsScanLock lock, StringInfo out, WsScanStats *stats)
{
	WsCatalogPlan plan;
	bool		rel_scoped;
	bool		locked;
	Relation	rel;

	if (!ws_catalog_plan(cat, &plan))
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unknown walshadow catalog id %d", (int) cat)));

	/* An empty oid list is the whole catalog, which is the only mode
	 * pg_namespace and pg_type have. The lock argument comes with the list, so
	 * losing the list loses the argument too */
	rel_scoped = AttributeNumberIsValid(plan.keyattno) && noids > 0;

	/*
	 * See WsScanLock. A caller that named a replay position never waits here:
	 * the release for a lock replay is holding can be in the WAL that caller
	 * is withholding, so waiting deadlocks the daemon against its own shadow.
	 * Reading without the lock is licensed only once that position is known to
	 * be where replay is, which is what holds these pages still — together
	 * with the shadow being read-only, so no local backend can write either.
	 */
	if (lock == WS_SCAN_LOCK_WAIT)
	{
		LockRelationOid(plan.relid, AccessShareLock);
		locked = true;
	}
	else
	{
		locked = ConditionalLockRelationOid(plan.relid, AccessShareLock);
		if (!locked && lock == WS_SCAN_LOCK_NOWAIT)
			ereport(ERROR,
					(errcode(ERRCODE_LOCK_NOT_AVAILABLE),
					 errmsg("walshadow: %u is locked and replay is not at the position the scan named",
							plan.relid),
					 errdetail("Reading without the lock needs that position to hold; waiting for it can deadlock against withheld WAL.")));
		if (!locked)
			elog(DEBUG1,
				 "walshadow: pinned scan of %u read without AccessShareLock",
				 plan.relid);
	}
	rel = table_open(plan.relid, NoLock);

	if (!locked)
	{
		Oid		   *sorted = NULL;

		if (rel_scoped)
		{
			sorted = palloc_array(Oid, noids);
			memcpy(sorted, oids, sizeof(Oid) * (size_t) noids);
			qsort(sorted, (size_t) noids, sizeof(Oid), ws_oid_cmp);
		}
		ws_scan_emit_lockfree(rel, &plan, sorted, rel_scoped ? noids : 0,
							  top, out, stats);
	}
	else if (rel_scoped)
	{
		int			i;

		for (i = 0; i < noids; i++)
			ws_scan_emit(rel, &plan, oids[i], top, true, out, stats);
	}
	else
		ws_scan_emit(rel, &plan, InvalidOid, top, false, out, stats);

	table_close(rel, NoLock);
	if (locked)
		UnlockRelationOid(plan.relid, AccessShareLock);
}
