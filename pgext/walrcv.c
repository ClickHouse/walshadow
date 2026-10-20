/*
 * walrcv.c — expand the daemon's compact WAL stream inside the walreceiver.
 *
 * The daemon elides the zero runs of filtered WAL on the wire, most of it
 * NOOP bodies. Wrapping libpqwalreceiver's function table, this module asks
 * for that form by naming WS_COMPACT_SLOT in START_REPLICATION and expands
 * every 'z' frame into the 'w' XLogData frame PostgreSQL reads, byte for
 * byte. A server that does not know the slot keeps sending 'w', which
 * passes through, so either side can be the older one.
 *
 * libpqwalreceiver is loaded here, during preload, so the walreceiver's own
 * load_file finds it in dfmgr's list and runs no second _PG_init over this
 * table (walreceiver.c, dfmgr.c internal_load_library).
 *
 * 'z' frame: type, dataStart, walEnd and sendTime as in 'w', a 32-bit
 * expanded length, then pairs of LEB128 lengths, literal then zeros, each
 * literal followed by its bytes (src/source/compact.rs).
 */
#include "postgres.h"

#include "fmgr.h"
#include "port/pg_bswap.h"
#include "replication/walreceiver.h"
#include "utils/memutils.h"

#include "walshadow.h"

/* type byte, dataStart, walEnd, sendTime */
#define WS_XLOGDATA_HEADER (1 + 3 * sizeof(int64))
#define WS_COMPACT_HEADER (WS_XLOGDATA_HEADER + sizeof(uint32))

static WalReceiverFunctionsType *ws_libpq;
static WalReceiverFunctionsType ws_compact;
static StringInfoData ws_frame;

static bool
ws_startstreaming(WalReceiverConn *conn, const WalRcvStreamOptions *options) {
    WalRcvStreamOptions compact = *options;

    if (compact.slotname == NULL) {
        compact.slotname = WS_COMPACT_SLOT;
    }
    /* Parenthesized past walreceiver.h's walrcv_* macros */
    return (ws_libpq->walrcv_startstreaming)(conn, &compact);
}

/* LEB128 value at *at, false past the end */
static bool
ws_take(const char **at, const char *end, uint64 *value) {
    int shift;

    *value = 0;
    for (shift = 0; shift < 64 && *at < end; shift += 7) {
        uint8 b = (uint8) * (*at)++;

        *value |= (uint64)(b & 0x7F) << shift;
        if (b < 0x80) {
            return true;
        }
    }
    return false;
}

/* Expand `src` into ws_frame, false when it is not exactly `want` bytes */
static bool
ws_expand(const char *src, const char *end, uint64 want) {
    uint64 literal;
    uint64 zeros;
    uint64 left = want;

    enlargeStringInfo(&ws_frame, (int)want);
    while (left > 0) {
        if (!ws_take(&src, end, &literal) || literal > left ||
            literal > (uint64)(end - src)) {
            return false;
        }
        appendBinaryStringInfoNT(&ws_frame, src, (int)literal);
        src += literal;
        left -= literal;
        if (!ws_take(&src, end, &zeros) || zeros > left) {
            return false;
        }
        memset(ws_frame.data + ws_frame.len, 0, zeros);
        ws_frame.len += (int)zeros;
        left -= zeros;
    }
    return src == end;
}

static int
ws_receive(WalReceiverConn *conn, char **buffer, pgsocket *wait_fd) {
    int len = (ws_libpq->walrcv_receive)(conn, buffer, wait_fd);
    uint32 want = 0;

    if (len <= 0 || (*buffer)[0] != 'z') {
        return len;
    }
    resetStringInfo(&ws_frame);
    if (len >= WS_COMPACT_HEADER) {
        memcpy(&want, *buffer + WS_XLOGDATA_HEADER, sizeof(want));
        want = pg_ntoh32(want);
        appendBinaryStringInfoNT(&ws_frame, *buffer, WS_XLOGDATA_HEADER);
        ws_frame.data[0] = 'w';
    }
    if (len < WS_COMPACT_HEADER || want > MaxAllocSize - WS_XLOGDATA_HEADER ||
        !ws_expand(*buffer + WS_COMPACT_HEADER, *buffer + len, want)) {
        ereport(
            ERROR,
            (errcode(ERRCODE_PROTOCOL_VIOLATION),
             errmsg("walshadow: corrupt compact WAL frame of %d bytes", len))
        );
    }
    *buffer = ws_frame.data;
    return ws_frame.len;
}

void
ws_walrcv_init(void) {
    MemoryContext old;

    load_file("libpqwalreceiver", false);
    ws_libpq = WalReceiverFunctions;
    ws_compact = *ws_libpq;
    ws_compact.walrcv_startstreaming = ws_startstreaming;
    ws_compact.walrcv_receive = ws_receive;
    WalReceiverFunctions = &ws_compact;
    old = MemoryContextSwitchTo(TopMemoryContext);
    initStringInfo(&ws_frame);
    MemoryContextSwitchTo(old);
}
