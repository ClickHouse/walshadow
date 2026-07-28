/*
 * walshadow — shared declarations for the shadow-side module.
 *
 * One entry point: a background worker reached via
 * shared_preload_libraries. Needs no catalog row, which is the whole point —
 * a shadow standby's catalog is a read-only physical copy of source's, so
 * anything requiring a pg_proc row is unreachable there.
 *
 * Wire encoding is network byte order throughout (pqformat's pq_send*),
 * matching PG's own convention rather than the daemon's native LE.
 */
#ifndef WALSHADOW_H
#define WALSHADOW_H

#include "postgres.h"

#include "lib/stringinfo.h"

/* Bumped when a request or response layout changes, or when an op's reading
 * of an unchanged layout changes: 2 gave SCAN an invalid top xid (committed
 * view) and an empty oid list (whole catalog) on every catalog */
#define WS_PROTO_VERSION		2
/* Bumped when any catalog projection changes shape */
#define WS_PROJECTION_VERSION	1

/* request opcodes */
#define WS_OP_HELLO			0x01
#define WS_OP_DECODE		0x02
#define WS_OP_SCAN			0x03
#define WS_OP_REPLAY_LSN	0x04

/* response status byte */
#define WS_STATUS_OK		0x00
#define WS_STATUS_ERROR		0x01

/* per-item kind in a DECODE response */
#define WS_ITEM_TEXT		0x00
#define WS_ITEM_ERROR		0x01

/*
 * Catalogs the overlay scan covers. Ids are wire values; never renumber.
 */
typedef enum WsCatalog
{
	WS_CAT_CLASS = 1,
	WS_CAT_ATTRIBUTE = 2,
	WS_CAT_INDEX = 3,
	WS_CAT_NAMESPACE = 4,
	WS_CAT_TYPE = 5,
} WsCatalog;

/* decode.c */
extern char *ws_decode_datum_text(Oid typoid, bytea *raw);

/* overlay.c */
typedef struct WsScanStats
{
	uint32		scanned;
	uint32		emitted;
	/* writers that did not resolve to the requested top xid: provably foreign,
	 * or (rel-scoped only) unresolvable and trusted ours by the lock argument.
	 * Whole-catalog scans error on an unresolvable writer instead. A committed
	 * read owns no transaction, so nothing lands here */
	uint32		subtrans_mismatch;
} WsScanStats;

extern int	ws_overlay_ncols(WsCatalog cat);

/*
 * `top` invalid reads the committed view; an empty `oids` reads the whole
 * catalog, which is the only mode pg_namespace and pg_type have.
 */
extern void ws_overlay_scan(WsCatalog cat, TransactionId top,
							const Oid *oids, int noids,
							StringInfo out, WsScanStats *stats);

#endif							/* WALSHADOW_H */
