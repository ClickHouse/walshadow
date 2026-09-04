/* Shared declarations, wire integers use network byte order */
#ifndef WALSHADOW_H
#define WALSHADOW_H

#include "postgres.h"

#include "lib/stringinfo.h"

/* Bumped when a request or response layout changes, or when an op's reading
 * of an unchanged layout changes */
#define WS_PROTO_VERSION		3
/* Bumped when any catalog projection changes shape */
#define WS_PROJECTION_VERSION	1

/* request opcodes */
#define WS_OP_HELLO				0x01
#define WS_OP_ENCODE_NATIVE		0x02
#define WS_OP_SCAN				0x03
#define WS_OP_REPLAY_LSN		0x04

/* response status byte */
#define WS_STATUS_OK		0x00
#define WS_STATUS_ERROR		0x01

/* per-cell tag in an ENCODE_NATIVE request */
#define WS_CELL_DEFAULT		0x00
#define WS_CELL_DISK_RAW	0x01
#define WS_CELL_TEXT		0x02
#define WS_CELL_LITERAL		0x03

/* Match daemon MAX_REQUEST_BYTES, exceed maximum inline TOAST value */
#define WS_MAX_REQUEST_BYTES	(256 * 1024 * 1024)
/* Match daemon MAX_RESPONSE_BYTES */
#define WS_MAX_RESPONSE_BYTES	(256 * 1024 * 1024)

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

extern void ws_handle_encode_native(StringInfo req, StringInfo resp);

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
 * How a scan may treat the target catalog's lock.
 *
 * A caller that named a replay position has withheld every successor byte to
 * park the shadow there, so a lock this scan waits for can have its release
 * sitting in that withheld WAL — waiting deadlocks the daemon against its own
 * shadow. Such callers therefore never wait. Reading *without* the lock needs
 * more than that: the position the caller named must be where replay actually
 * is, which is what makes the pages stable.
 */
typedef enum WsScanLock
{
	/* No position named. Ordinary locking, and it may wait */
	WS_SCAN_LOCK_WAIT = 0,
	/* Position named but replay is elsewhere, so the caller's guarantee does
	 * not hold: take the lock if free, error rather than wait or read past it */
	WS_SCAN_LOCK_NOWAIT = 1,
	/* Position named and verified: take the lock if free, read without it
	 * otherwise */
	WS_SCAN_LOCK_PINNED = 2,
} WsScanLock;

/*
 * `top` invalid reads the committed view; an empty `oids` reads the whole
 * catalog, which is the only mode pg_namespace and pg_type have.
 */
extern void ws_overlay_scan(WsCatalog cat, TransactionId top,
							const Oid *oids, int noids, WsScanLock lock,
							StringInfo out, WsScanStats *stats);

#endif							/* WALSHADOW_H */
