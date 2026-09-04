/*
 * Test-only helper. Not part of the walshadow module: built and loaded only
 * by the daemon's integration tests, never by a deployed shadow.
 *
 * Reproduces VACUUM's lock lifetime (vacuumlazy.c, lazy_truncate_heap):
 * acquire AccessExclusiveLock on a relation, then release it source-side
 * while the surrounding transaction stays open. The acquire emits
 * XLOG_STANDBY_LOCK; the release emits nothing. A standby therefore keeps
 * the replayed lock until the transaction's commit record arrives, which is
 * the asymmetry tests need to drive deterministically.
 */
#include "postgres.h"

#include "fmgr.h"
#include "storage/lmgr.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(ws_test_lock_unlock_relation);

Datum
ws_test_lock_unlock_relation(PG_FUNCTION_ARGS)
{
	Oid			relid = PG_GETARG_OID(0);

	LockRelationOid(relid, AccessExclusiveLock);
	UnlockRelationOid(relid, AccessExclusiveLock);

	PG_RETURN_VOID();
}
