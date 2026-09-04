/*
 * worker.c — walshadow bridge background worker.
 *
 * Serves catalog reads and on-disk decode to the walshadow daemon over a unix
 * socket. Loaded through shared_preload_libraries, so it needs no pg_proc row
 * and no CREATE EXTENSION: on a shadow standby the catalog is a physical copy
 * of source's and cannot be written. walshadow writes shadow's
 * postgresql.conf, so the worker is something it can guarantee.
 *
 * One request at a time, whichever connection is ready first. Every request
 * runs in its own transaction; errors are caught, reported on the wire, and
 * the loop continues. walshadow.h defines protocol constants and layouts.
 */
#include "postgres.h"

#include <errno.h>
#include <limits.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/un.h>
#include <unistd.h>

#include "access/xact.h"
#include "access/xlog.h"
#include "access/xlogrecovery.h"
#include "fmgr.h"
#include "libpq/pqformat.h"
#include "miscadmin.h"
#include "pgstat.h"
#include "port/pg_bswap.h"
#include "postmaster/bgworker.h"
#include "postmaster/interrupt.h"
#include "storage/ipc.h"
#include "storage/latch.h"
#include "utils/guc.h"
#include "utils/memutils.h"
#include "utils/snapmgr.h"
#include "utils/timestamp.h"
#include "utils/wait_event.h"

#include "walshadow.h"

PG_MODULE_MAGIC;

#define WS_MAX_CONNS		8
#define WS_LISTEN_BACKLOG	16
#define WS_IDLE_POLL_MS		1000
#define WS_MAX_SCAN_OIDS	65536

/* wait-set positions, in the order ws_build_wait_set adds them */
#define WS_POS_LATCH		0
#define WS_POS_PM_DEATH		1
#define WS_POS_LISTEN		2
#define WS_POS_CONN0		3

PGDLLEXPORT void ws_worker_main(Datum main_arg);

static char *ws_socket_path = NULL;
static char *ws_database = NULL;
static int	ws_io_timeout_ms = 30000;
static int	ws_lock_timeout_ms = 1000;

static MemoryContext ws_request_ctx = NULL;
static char ws_bound_path[MAXPGPATH];

/* -------------------------------------------------------------------------
 * socket plumbing
 * ------------------------------------------------------------------------- */

static void
ws_unlink_socket(int code, Datum arg)
{
	if (ws_bound_path[0] != '\0')
		(void) unlink(ws_bound_path);
}

/*
 * A leftover socket file from a crash is walshadow's to remove, but anything
 * else on the path is a misconfiguration and unlinking it would destroy data
 * that is not ours.
 */
static void
ws_reject_non_socket(const char *path)
{
	struct stat st;

	if (lstat(path, &st) < 0)
		return;					/* absent (or unreadable, and bind will say) */
	if (!S_ISSOCK(st.st_mode))
		ereport(ERROR,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("walshadow.socket_path \"%s\" exists and is not a socket",
						path)));
}

/*
 * A live listener on the same path is another cluster's worker and taking it
 * over would silently answer that daemon's requests from the wrong catalog.
 */
static bool
ws_path_has_listener(const char *path)
{
	struct sockaddr_un addr;
	pgsocket	fd;
	bool		alive;

	fd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (fd == PGINVALID_SOCKET)
		return true;			/* cannot prove it is dead */

	memset(&addr, 0, sizeof(addr));
	addr.sun_family = AF_UNIX;
	strlcpy(addr.sun_path, path, sizeof(addr.sun_path));
	alive = connect(fd, (struct sockaddr *) &addr, sizeof(addr)) == 0;
	closesocket(fd);
	return alive;
}

static pgsocket
ws_listen(const char *path)
{
	struct sockaddr_un addr;
	pgsocket	fd;

	if (strlen(path) >= sizeof(addr.sun_path))
		ereport(ERROR,
				(errcode(ERRCODE_CONFIG_FILE_ERROR),
				 errmsg("walshadow.socket_path is longer than %zu bytes",
						sizeof(addr.sun_path) - 1)));

	ws_reject_non_socket(path);
	if (ws_path_has_listener(path))
		ereport(ERROR,
				(errcode(ERRCODE_OBJECT_IN_USE),
				 errmsg("walshadow.socket_path \"%s\" already has a listener",
						path)));
	(void) unlink(path);

	fd = socket(AF_UNIX, SOCK_STREAM, 0);
	if (fd == PGINVALID_SOCKET)
		ereport(ERROR,
				(errcode_for_socket_access(),
				 errmsg("walshadow: could not create socket: %m")));

	memset(&addr, 0, sizeof(addr));
	addr.sun_family = AF_UNIX;
	strlcpy(addr.sun_path, path, sizeof(addr.sun_path));
	if (bind(fd, (struct sockaddr *) &addr, sizeof(addr)) < 0)
	{
		closesocket(fd);
		ereport(ERROR,
				(errcode_for_socket_access(),
				 errmsg("walshadow: could not bind \"%s\": %m", path)));
	}

	strlcpy(ws_bound_path, path, sizeof(ws_bound_path));
	on_proc_exit(ws_unlink_socket, 0);

	/* Protect Datum reconstruction and unrestricted catalog scans */
	if (chmod(path, S_IRUSR | S_IWUSR) < 0)
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("walshadow: could not chmod \"%s\": %m", path)));

	if (listen(fd, WS_LISTEN_BACKLOG) < 0)
		ereport(ERROR,
				(errcode_for_socket_access(),
				 errmsg("walshadow: could not listen on \"%s\": %m", path)));

	if (!pg_set_noblock(fd))
		ereport(ERROR,
				(errcode_for_socket_access(),
				 errmsg("walshadow: could not set socket non-blocking: %m")));

	return fd;
}

/*
 * Wait for `event` on one socket. `false` means the caller should abandon the
 * connection: shutdown was requested.
 */
static bool
ws_wait_socket(pgsocket fd, int event, long timeout_ms)
{
	int			rc;

	rc = WaitLatchOrSocket(MyLatch,
						   WL_LATCH_SET | WL_EXIT_ON_PM_DEATH | WL_TIMEOUT | event,
						   fd, timeout_ms, PG_WAIT_EXTENSION);
	if (rc & WL_LATCH_SET)
	{
		ResetLatch(MyLatch);
		CHECK_FOR_INTERRUPTS();
		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}
		if (ShutdownRequestPending)
			return false;
	}
	return true;
}

static bool
ws_read_exact(pgsocket fd, char *buf, size_t len)
{
	size_t		got = 0;
	TimestampTz deadline;

	deadline = TimestampTzPlusMilliseconds(GetCurrentTimestamp(),
										   ws_io_timeout_ms);
	while (got < len)
	{
		ssize_t		r;
		long		wait_ms;

		r = recv(fd, buf + got, len - got, 0);
		if (r > 0)
		{
			got += (size_t) r;
			continue;
		}
		if (r == 0)
			return false;		/* peer closed */
		if (errno == EINTR)
			continue;
		if (errno != EAGAIN && errno != EWOULDBLOCK)
		{
			ereport(LOG,
					(errcode_for_socket_access(),
					 errmsg("walshadow: recv failed: %m")));
			return false;
		}

		wait_ms = TimestampDifferenceMilliseconds(GetCurrentTimestamp(),
												  deadline);
		if (wait_ms <= 0)
		{
			ereport(LOG,
					(errmsg("walshadow: read timed out after %d ms",
							ws_io_timeout_ms)));
			return false;
		}
		if (!ws_wait_socket(fd, WL_SOCKET_READABLE, wait_ms))
			return false;
	}
	return true;
}

static bool
ws_write_all(pgsocket fd, const char *buf, size_t len)
{
	size_t		sent = 0;
	TimestampTz deadline;

	deadline = TimestampTzPlusMilliseconds(GetCurrentTimestamp(),
										   ws_io_timeout_ms);
	while (sent < len)
	{
		ssize_t		w;
		long		wait_ms;

		w = send(fd, buf + sent, len - sent, 0);
		if (w > 0)
		{
			sent += (size_t) w;
			continue;
		}
		if (w == 0)
			return false;		/* len > 0, so no progress and errno is stale */
		if (errno == EINTR)
			continue;
		if (errno != EAGAIN && errno != EWOULDBLOCK)
		{
			ereport(LOG,
					(errcode_for_socket_access(),
					 errmsg("walshadow: send failed: %m")));
			return false;
		}

		wait_ms = TimestampDifferenceMilliseconds(GetCurrentTimestamp(),
												  deadline);
		if (wait_ms <= 0)
		{
			ereport(LOG,
					(errmsg("walshadow: write timed out after %d ms",
							ws_io_timeout_ms)));
			return false;
		}
		if (!ws_wait_socket(fd, WL_SOCKET_WRITEABLE, wait_ms))
			return false;
	}
	return true;
}

/* -------------------------------------------------------------------------
 * request handlers
 * ------------------------------------------------------------------------- */

static void
ws_put_lenstr(StringInfo out, const char *s)
{
	int			len = (int) strlen(s);

	pq_sendint32(out, (uint32) len);
	pq_sendbytes(out, s, len);
}

static void
ws_handle_scan(StringInfo req, StringInfo resp)
{
	WsCatalog	cat = (WsCatalog) pq_getmsgbyte(req);
	TransactionId top = (TransactionId) pq_getmsgint(req, 4);
	/* Replay position the caller parked at, 0 when it has not parked one */
	uint64		boundary = pq_getmsgint64(req);
	uint32		noids = pq_getmsgint(req, 4);
	int			ncols = ws_overlay_ncols(cat);
	Oid		   *oids = NULL;
	StringInfoData rows;
	WsScanStats stats = {0, 0, 0};
	uint64		lsn_start;
	uint64		lsn_end;
	WsScanLock	lock;
	uint32		i;

	if (ncols < 0)
		ereport(ERROR,
				(errcode(ERRCODE_INVALID_PARAMETER_VALUE),
				 errmsg("unknown walshadow catalog id %d", (int) cat)));
	if (noids > WS_MAX_SCAN_OIDS)
		ereport(ERROR,
				(errcode(ERRCODE_PROGRAM_LIMIT_EXCEEDED),
				 errmsg("walshadow scan oid list of %u exceeds %d",
						noids, WS_MAX_SCAN_OIDS)));

	if (noids > 0)
	{
		oids = palloc_array(Oid, noids);
		for (i = 0; i < noids; i++)
			oids[i] = (Oid) pq_getmsgint(req, 4);
	}

	initStringInfo(&rows);
	/*
	 * Both LSNs go back for the caller to check against the boundary it parked
	 * replay at. Equal but wrong is impossible: replay cannot rewind, and the
	 * daemon holds the successor bytes.
	 *
	 * The pre-scan sample also decides how the scan may treat the catalog's
	 * lock: reading without it needs replay to actually be where the caller
	 * says, and that cannot be judged from the response.
	 */
	lsn_start = (uint64) GetXLogReplayRecPtr(NULL);
	if (boundary == 0)
		lock = WS_SCAN_LOCK_WAIT;
	else if (lsn_start == boundary)
		lock = WS_SCAN_LOCK_PINNED;
	else
		lock = WS_SCAN_LOCK_NOWAIT;
	ws_overlay_scan(cat, top, oids, (int) noids, lock, &rows, &stats);
	lsn_end = (uint64) GetXLogReplayRecPtr(NULL);

	pq_sendbyte(resp, WS_STATUS_OK);
	pq_sendint64(resp, lsn_start);
	pq_sendint64(resp, lsn_end);
	pq_sendint32(resp, stats.scanned);
	pq_sendint32(resp, stats.subtrans_mismatch);
	pq_sendint32(resp, stats.emitted);
	pq_sendint16(resp, (uint16) ncols);
	pq_sendbytes(resp, rows.data, rows.len);
}

static void
ws_dispatch(StringInfo req, StringInfo resp)
{
	MemoryContext oldctx = CurrentMemoryContext;
	/* written after PG_TRY, read after siglongjmp: must be volatile */
	volatile bool in_xact = false;

	PG_TRY();
	{
		uint8		op = pq_getmsgbyte(req);

		if (op == WS_OP_ENCODE_NATIVE || op == WS_OP_SCAN)
		{
			SetCurrentStatementStartTimestamp();
			StartTransactionCommand();
			PushActiveSnapshot(GetTransactionSnapshot());
			in_xact = true;
		}

		switch (op)
		{
			case WS_OP_HELLO:
				pq_sendbyte(resp, WS_STATUS_OK);
				pq_sendint32(resp, WS_PROTO_VERSION);
				pq_sendint32(resp, WS_PROJECTION_VERSION);
				pq_sendint32(resp, PG_VERSION_NUM);
				pq_sendbyte(resp, RecoveryInProgress() ? 1 : 0);
				break;
			case WS_OP_REPLAY_LSN:
				pq_sendbyte(resp, WS_STATUS_OK);
				pq_sendint64(resp, (uint64) GetXLogReplayRecPtr(NULL));
				break;
			case WS_OP_ENCODE_NATIVE:
				ws_handle_encode_native(req, resp);
				break;
			case WS_OP_SCAN:
				ws_handle_scan(req, resp);
				break;
			default:
				ereport(ERROR,
						(errcode(ERRCODE_PROTOCOL_VIOLATION),
						 errmsg("unknown walshadow opcode %u", op)));
		}

		/* trailing bytes mean the peer framed a different request */
		pq_getmsgend(req);

		if (in_xact)
		{
			PopActiveSnapshot();
			CommitTransactionCommand();
			in_xact = false;
		}
	}
	PG_CATCH();
	{
		ErrorData  *edata;

		HOLD_INTERRUPTS();
		MemoryContextSwitchTo(oldctx);
		edata = CopyErrorData();
		FlushErrorState();
		if (in_xact)
			AbortCurrentTransaction();

		resetStringInfo(resp);
		pq_sendbyte(resp, WS_STATUS_ERROR);
		if (edata->context)
		{
			/* Preserve cell coordinates carried by error context */
			char	   *msg = psprintf("%s (%s)", edata->message, edata->context);

			ws_put_lenstr(resp, msg);
			pfree(msg);
		}
		else
			ws_put_lenstr(resp, edata->message);
		FreeErrorData(edata);
		RESUME_INTERRUPTS();
	}
	PG_END_TRY();

	MemoryContextSwitchTo(oldctx);
}

/*
 * Read one framed request, answer it, write one framed response.
 * `false` closes the connection.
 */
static bool
ws_serve_request(pgsocket fd)
{
	uint32		hdr;
	uint32		len;
	StringInfoData req;
	StringInfoData resp;
	MemoryContext oldctx;
	bool		ok = false;

	if (!ws_read_exact(fd, (char *) &hdr, sizeof(hdr)))
		return false;
	len = pg_ntoh32(hdr);

	/* enlargeStringInfo ERRORs at MaxAllocSize, and that would happen outside
	 * the request catch; reject before allocating */
	StaticAssertStmt(WS_MAX_REQUEST_BYTES < MaxAllocSize,
					 "request ceiling must leave StringInfo room");
	if (len < 1 || (Size) len > (Size) WS_MAX_REQUEST_BYTES)
	{
		ereport(LOG,
				(errmsg("walshadow: rejecting request frame of %u bytes", len)));
		return false;
	}

	oldctx = MemoryContextSwitchTo(ws_request_ctx);

	initStringInfo(&req);
	enlargeStringInfo(&req, (int) len);
	if (ws_read_exact(fd, req.data, len))
	{
		req.len = (int) len;
		req.data[len] = '\0';

		/* resp is payload only; the dispatcher may reset it wholesale on
		 * error, so the frame prefix cannot live in the same buffer */
		initStringInfo(&resp);
		ws_dispatch(&req, &resp);

		hdr = pg_hton32((uint32) resp.len);
		ok = ws_write_all(fd, (char *) &hdr, sizeof(hdr)) &&
			ws_write_all(fd, resp.data, (size_t) resp.len);
	}

	MemoryContextSwitchTo(oldctx);
	MemoryContextReset(ws_request_ctx);
	return ok;
}

/* -------------------------------------------------------------------------
 * worker main
 * ------------------------------------------------------------------------- */

/*
 * Rebuilt per iteration: the wait-set API has no portable event removal, and
 * this loop is idle-dominated.
 */
static WaitEventSet *
ws_build_wait_set(pgsocket listen_fd, const pgsocket *conns, int nconns)
{
	WaitEventSet *set;
	int			i;

#if PG_VERSION_NUM >= 170000
	set = CreateWaitEventSet(NULL, nconns + WS_POS_CONN0);
#else
	set = CreateWaitEventSet(CurrentMemoryContext, nconns + WS_POS_CONN0);
#endif
	AddWaitEventToSet(set, WL_LATCH_SET, PGINVALID_SOCKET, MyLatch, NULL);
	AddWaitEventToSet(set, WL_EXIT_ON_PM_DEATH, PGINVALID_SOCKET, NULL, NULL);
	AddWaitEventToSet(set, WL_SOCKET_READABLE, listen_fd, NULL, NULL);
	for (i = 0; i < nconns; i++)
		AddWaitEventToSet(set, WL_SOCKET_READABLE, conns[i], NULL, NULL);
	return set;
}

static void
ws_accept(pgsocket listen_fd, pgsocket *conns, int *nconns)
{
	pgsocket	fd;

	fd = accept(listen_fd, NULL, NULL);
	if (fd == PGINVALID_SOCKET)
	{
		if (errno != EAGAIN && errno != EWOULDBLOCK && errno != EINTR)
			ereport(LOG,
					(errcode_for_socket_access(),
					 errmsg("walshadow: accept failed: %m")));
		return;
	}
	if (*nconns >= WS_MAX_CONNS)
	{
		ereport(LOG,
				(errmsg("walshadow: refusing connection, %d already open",
						WS_MAX_CONNS)));
		closesocket(fd);
		return;
	}
	if (!pg_set_noblock(fd))
	{
		ereport(LOG,
				(errcode_for_socket_access(),
				 errmsg("walshadow: could not set client socket non-blocking: %m")));
		closesocket(fd);
		return;
	}
	conns[(*nconns)++] = fd;
}

static void
ws_drop_conn(pgsocket *conns, int *nconns, int idx)
{
	closesocket(conns[idx]);
	conns[idx] = conns[--*nconns];
}

static void
ws_serve_loop(pgsocket listen_fd)
{
	pgsocket	conns[WS_MAX_CONNS];
	int			nconns = 0;

	for (;;)
	{
		WaitEventSet *set;
		WaitEvent	events[WS_MAX_CONNS + WS_POS_CONN0];
		int			nready;
		int			i;
		int			serve = -1;

		CHECK_FOR_INTERRUPTS();
		if (ConfigReloadPending)
		{
			ConfigReloadPending = false;
			ProcessConfigFile(PGC_SIGHUP);
		}
		if (ShutdownRequestPending)
			break;

		set = ws_build_wait_set(listen_fd, conns, nconns);
		nready = WaitEventSetWait(set, WS_IDLE_POLL_MS, events,
								  lengthof(events), PG_WAIT_EXTENSION);
		FreeWaitEventSet(set);

		for (i = 0; i < nready; i++)
		{
			int			pos = events[i].pos;

			if (pos == WS_POS_LATCH)
				ResetLatch(MyLatch);
			else if (pos == WS_POS_LISTEN)
				ws_accept(listen_fd, conns, &nconns);
			else if (serve < 0)
				serve = pos - WS_POS_CONN0;
		}

		/*
		 * One request per iteration. Dropping a connection reorders the
		 * array, so any other ready position from this round is stale; they
		 * stay readable and are picked up next time round.
		 */
		if (serve >= 0 && serve < nconns)
		{
			pgstat_report_activity(STATE_RUNNING, "walshadow request");
			if (!ws_serve_request(conns[serve]))
				ws_drop_conn(conns, &nconns, serve);
			pgstat_report_activity(STATE_IDLE, NULL);
		}
	}

	while (nconns > 0)
		ws_drop_conn(conns, &nconns, 0);
	closesocket(listen_fd);
}

void
ws_worker_main(Datum main_arg)
{
	pgsocket	listen_fd;
	char		buf[32];

	pqsignal(SIGTERM, SignalHandlerForShutdownRequest);
	pqsignal(SIGHUP, SignalHandlerForConfigReload);
	BackgroundWorkerUnblockSignals();

	BackgroundWorkerInitializeConnection(ws_database, NULL, 0);

	/*
	 * The replaying transaction holds AccessExclusiveLock on its own
	 * relations, and standby lock replay is the startup process. A catalog
	 * lock we cannot get is a hang against recovery, so bound it.
	 */
	snprintf(buf, sizeof(buf), "%d", ws_lock_timeout_ms);
	SetConfigOption("lock_timeout", buf, PGC_SUSET, PGC_S_OVERRIDE);

	/*
	 * typoutput is not a pure function of the bytes: timestamptz follows
	 * TimeZone, dates DateStyle, interval IntervalStyle, bytea bytea_output,
	 * floats extra_float_digits. The connection inherits whatever database and
	 * role defaults replicated from source, so pin a canonical environment;
	 * PGC_S_OVERRIDE outranks any SIGHUP reload.
	 */
	SetConfigOption("TimeZone", "UTC", PGC_SUSET, PGC_S_OVERRIDE);
	SetConfigOption("DateStyle", "ISO,MDY", PGC_SUSET, PGC_S_OVERRIDE);
	SetConfigOption("IntervalStyle", "postgres", PGC_SUSET, PGC_S_OVERRIDE);
	SetConfigOption("extra_float_digits", "1", PGC_SUSET, PGC_S_OVERRIDE);
	SetConfigOption("bytea_output", "hex", PGC_SUSET, PGC_S_OVERRIDE);

	ws_request_ctx = AllocSetContextCreate(TopMemoryContext,
										   "walshadow request",
										   ALLOCSET_DEFAULT_SIZES);

	listen_fd = ws_listen(ws_socket_path);
	ereport(LOG,
			(errmsg("walshadow bridge listening on \"%s\" (proto %d)",
					ws_socket_path, WS_PROTO_VERSION)));

	ws_serve_loop(listen_fd);

	/*
	 * Postmaster reads exit 0 as "terminate, forget this worker" and exit 1 as
	 * FATAL, which bgw_restart_time then covers (CleanupBackgroundWorker).
	 * pg_terminate_backend and a recovery conflict both arrive as SIGTERM, and
	 * neither should cost the bridge until the next cluster restart. Nothing
	 * restarts during postmaster shutdown, so this only costs a LOG line there.
	 */
	proc_exit(1);
}

void
_PG_init(void)
{
	BackgroundWorker worker;

	/*
	 * Worker registration and its postmaster-scoped GUCs are only legal
	 * during preload; a bare LOAD gets nothing.
	 */
	if (!process_shared_preload_libraries_in_progress)
		return;

	DefineCustomStringVariable("walshadow.socket_path",
							   "Unix socket the walshadow bridge listens on.",
							   "Empty disables the worker.",
							   &ws_socket_path,
							   "",
							   PGC_POSTMASTER, 0,
							   NULL, NULL, NULL);
	DefineCustomStringVariable("walshadow.database",
							   "Database the walshadow bridge connects to.",
							   NULL,
							   &ws_database,
							   "postgres",
							   PGC_POSTMASTER, 0,
							   NULL, NULL, NULL);
	DefineCustomIntVariable("walshadow.io_timeout_ms",
							"Abandon a bridge connection stalled this long.",
							NULL,
							&ws_io_timeout_ms,
							30000, 100, INT_MAX,
							PGC_SIGHUP, GUC_UNIT_MS,
							NULL, NULL, NULL);
	DefineCustomIntVariable("walshadow.lock_timeout_ms",
							"lock_timeout the bridge applies to its own reads.",
							NULL,
							&ws_lock_timeout_ms,
							1000, 0, INT_MAX,
							PGC_POSTMASTER, GUC_UNIT_MS,
							NULL, NULL, NULL);

	MarkGUCPrefixReserved("walshadow");

	if (ws_socket_path == NULL || ws_socket_path[0] == '\0')
		return;

	memset(&worker, 0, sizeof(worker));
	worker.bgw_flags = BGWORKER_SHMEM_ACCESS | BGWORKER_BACKEND_DATABASE_CONNECTION;
	/* Catalog reads need a database connection, so not before consistency */
	worker.bgw_start_time = BgWorkerStart_ConsistentState;
	worker.bgw_restart_time = 5;
	strlcpy(worker.bgw_library_name, "walshadow", BGW_MAXLEN);
	strlcpy(worker.bgw_function_name, "ws_worker_main", BGW_MAXLEN);
	strlcpy(worker.bgw_name, "walshadow bridge", BGW_MAXLEN);
	strlcpy(worker.bgw_type, "walshadow bridge", BGW_MAXLEN);
	RegisterBackgroundWorker(&worker);
}
