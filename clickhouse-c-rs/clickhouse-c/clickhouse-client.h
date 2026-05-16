/*
 * clickhouse-client.h -- TCP packet loop over chc_io.
 *
 * Exactly one TU must `#define CHC_IMPLEMENTATION` before including;
 * the implementation uses internal varint / io / block helpers from
 * clickhouse.h.
 *
 * Caller owns the chc_io (socket setup, TLS, timeouts, cancel polling).
 * One chc_client wraps one connection. No reconnect / endpoint failover
 * / DNS — caller-side concerns.
 *
 * Compression: Phase 3b ships uncompressed only. When clickhouse-
 * compression.h ships, set opts.compression & opts.codec to enable it.
 */

#ifndef CLICKHOUSE_CLIENT_H
#define CLICKHOUSE_CLIENT_H

#include <stdbool.h>
#include <stdint.h>

#include "clickhouse.h"
#include "clickhouse-compression.h"  /* chc_compression enum, chc_codec struct */

#ifdef __cplusplus
extern "C" {
#endif

/* Default protocol revision the client speaks. Matches clickhouse-cpp's
 * DBMS_MIN_PROTOCOL_VERSION_WITH_PARAMETERS pin. */
#define CHC_CLIENT_DEFAULT_REVISION 54459u

typedef struct chc_client_opts {
    /* Identity. Defaults applied when fields are zero/NULL. */
    const char *client_name;        /* default "clickhouse-c" */
    uint64_t client_version_major;  /* default 0 */
    uint64_t client_version_minor;  /* default 0 */
    uint64_t client_version_patch;  /* default 0 */
    uint64_t client_revision;       /* default CHC_CLIENT_DEFAULT_REVISION */

    /* Hello body. */
    const char *database;           /* default "default" */
    const char *user;               /* default "default" */
    const char *password;           /* default "" */

    /* Compression. CHC_COMP_NONE if codec is NULL. */
    chc_compression compression;
    const chc_codec *codec;

    /* Internal read buffer size. 0 = use CHC_READ_BUFFER (8 KiB). */
    size_t read_buffer_bytes;
} chc_client_opts;

typedef struct chc_server_info {
    char     name[64];
    char     timezone[64];
    char     display_name[128];
    uint64_t version_major;
    uint64_t version_minor;
    uint64_t version_patch;
    uint64_t revision;              /* min(client_revision, server_revision) */
} chc_server_info;

typedef struct chc_client chc_client;

/* Performs Hello / HelloAck handshake immediately. On failure caller may
 * call chc_client_close to free any partially-allocated state. */
int  chc_client_init(chc_client **out, const chc_client_opts *opts,
                     const chc_alloc *al, chc_io *io, chc_err *err);

void chc_client_close(chc_client *c);

const chc_server_info *chc_client_server_info(const chc_client *c);

int  chc_client_send_query(chc_client *c,
                           const char *sql, size_t sql_len,
                           const char *query_id, size_t query_id_len,
                           chc_err *err);

/* Per-query setting. name / value are NUL-terminated. Matches
 * clickhouse-cpp's QuerySettingsField.flags low two bits. */
typedef struct chc_query_setting {
    const char *name;
    const char *value;
    bool        important;          /* flag bit 0 */
    bool        custom;             /* flag bit 1 (user-defined "SET custom_*=...") */
} chc_query_setting;

/* Per-query parameter (substituted into `{name:Type}` placeholders in the
 * SQL text). name / value are NUL-terminated. The wire-level flags byte is
 * always CUSTOM. The server parses value via Field::restoreFromDump, so
 * callers must format value as a typed Field literal: e.g. `'hello'` for a
 * String, `42` for an integer, `[1,2,3]` for an array. NULL is `'\\N'`.
 * (Unlike clickhouse-cpp's higher-level Client::SetParam, which auto-quotes
 * raw strings, this library passes the value through verbatim so callers
 * keep full control of typed and non-string values.) */
typedef struct chc_query_param {
    const char *name;
    const char *value;
} chc_query_param;

typedef struct chc_query_opts {
    const char *query_id;
    size_t      query_id_len;
    const chc_query_setting *settings;
    size_t                   n_settings;
    const chc_query_param   *params;
    size_t                   n_params;
} chc_query_opts;

int  chc_client_send_query_ex(chc_client *c,
                              const char *sql, size_t sql_len,
                              const chc_query_opts *opts, chc_err *err);

typedef enum chc_packet_kind {
    CHC_PKT_DATA            = 1,
    CHC_PKT_EXCEPTION       = 2,
    CHC_PKT_PROGRESS        = 3,
    CHC_PKT_PONG            = 4,
    CHC_PKT_END_OF_STREAM   = 5,
    CHC_PKT_PROFILE_INFO    = 6,
    CHC_PKT_TOTALS          = 7,
    CHC_PKT_EXTREMES        = 8,
    CHC_PKT_LOG             = 10,
    CHC_PKT_TABLE_COLUMNS   = 11,
    CHC_PKT_PROFILE_EVENTS  = 14,
} chc_packet_kind;

/* CHC_PKT_EXCEPTION payload. `nested` is the head of a singly-linked
 * chain if the server sent has_nested = 1. Caller frees with
 * chc_exception_free if produced. */
typedef struct chc_exception chc_exception;
struct chc_exception {
    int32_t        code;
    char          *name;         /* allocated in chc_alloc */
    size_t         name_len;
    char          *display_text;
    size_t         display_text_len;
    char          *stack_trace;
    size_t         stack_trace_len;
    chc_exception *nested;
};

void chc_exception_free(chc_exception *e, const chc_alloc *al);

typedef struct chc_packet {
    chc_packet_kind kind;

    /* CHC_PKT_DATA / TOTALS / EXTREMES / LOG / PROFILE_EVENTS:
     * caller-owned chc_block, freed with chc_block_destroy. NULL for
     * other kinds. */
    chc_block *block;

    /* CHC_PKT_EXCEPTION: caller-owned, freed with chc_exception_free. */
    chc_exception *exception;

    /* CHC_PKT_PROGRESS. */
    struct {
        uint64_t rows, bytes, total_rows;
        uint64_t written_rows, written_bytes;  /* >= rev 54420 */
    } progress;

    /* CHC_PKT_PROFILE_INFO. */
    struct {
        uint64_t rows, blocks, bytes, rows_before_limit;
        uint8_t  applied_limit, calculated_rows_before_limit;
    } profile;
} chc_packet;

/* Read the next packet. On exception packets the caller MUST inspect
 * out->kind == CHC_PKT_EXCEPTION; the function returns CHC_OK with the
 * exception attached, not CHC_ERR_SERVER. */
int  chc_client_recv_packet(chc_client *c, chc_packet *out, chc_err *err);

/* Free anything chc_client_recv_packet allocated for this packet:
 * the block (if any) and the exception chain (if any). Safe to call
 * with packet->{block,exception} already NULLed by the caller (after
 * the caller takes ownership). */
void chc_packet_clear(chc_client *c, chc_packet *p);

/* Send a Data block. bb == NULL emits an empty block which the server
 * interprets as "no more INSERT rows" or as the query-text terminator
 * sent at the end of every SendQuery. */
int  chc_client_send_data(chc_client *c, const chc_block_builder *bb,
                          chc_err *err);

int  chc_client_send_cancel(chc_client *c, chc_err *err);
int  chc_client_send_ping(chc_client *c, chc_err *err);

#ifdef CHC_IMPLEMENTATION

#include <stdlib.h>
#include <string.h>

/* ----- protocol revision constants (mirror clickhouse-cpp client.cpp) ----- */
#define CHC__REV_TEMPORARY_TABLES        50264u
#define CHC__REV_TOTAL_ROWS_IN_PROGRESS  51554u
#define CHC__REV_BLOCK_INFO              51903u
#define CHC__REV_CLIENT_INFO             54032u
#define CHC__REV_SERVER_TIMEZONE         54058u
#define CHC__REV_QUOTA_KEY_IN_CLIENT     54060u
#define CHC__REV_SERVER_DISPLAY_NAME     54372u
#define CHC__REV_VERSION_PATCH           54401u
#define CHC__REV_CLIENT_WRITE_INFO       54420u
#define CHC__REV_SETTINGS_AS_STRINGS     54429u
#define CHC__REV_INTERSERVER_SECRET      54441u
#define CHC__REV_OPENTELEMETRY           54442u
#define CHC__REV_DISTRIBUTED_DEPTH       54448u
#define CHC__REV_INITIAL_QUERY_START     54449u
#define CHC__REV_PARALLEL_REPLICAS       54453u
#define CHC__REV_CUSTOM_SERIALIZATION    54454u
#define CHC__REV_ADDENDUM                54458u
#define CHC__REV_QUOTA_KEY               54458u
#define CHC__REV_PARAMETERS              54459u

/* Client → server packet kinds. */
#define CHC__CLIENT_HELLO  0u
#define CHC__CLIENT_QUERY  1u
#define CHC__CLIENT_DATA   2u
#define CHC__CLIENT_CANCEL 3u
#define CHC__CLIENT_PING   4u

/* Server → client packet kinds (only the ones we react on by tag). */
#define CHC__SERVER_HELLO            0u
#define CHC__SERVER_DATA             1u
#define CHC__SERVER_EXCEPTION        2u
#define CHC__SERVER_PROGRESS         3u
#define CHC__SERVER_PONG             4u
#define CHC__SERVER_END_OF_STREAM    5u
#define CHC__SERVER_PROFILE_INFO     6u
#define CHC__SERVER_TOTALS           7u
#define CHC__SERVER_EXTREMES         8u
#define CHC__SERVER_LOG             10u
#define CHC__SERVER_TABLE_COLUMNS   11u
#define CHC__SERVER_PROFILE_EVENTS  14u

struct chc_client {
    const chc_alloc *al;
    chc_io          *io;
    chc__in          in;            /* persistent buffered input */

    chc_server_info  server;
    uint64_t         client_revision;
    chc_compression  compression;
    const chc_codec *codec;
};

void
chc_exception_free(chc_exception *e, const chc_alloc *al)
{
    while (e) {
        chc_exception *next = e->nested;
        if (e->name)         al->free(al->ud, e->name,         e->name_len         + 1);
        if (e->display_text) al->free(al->ud, e->display_text, e->display_text_len + 1);
        if (e->stack_trace)  al->free(al->ud, e->stack_trace,  e->stack_trace_len  + 1);
        al->free(al->ud, e, sizeof *e);
        e = next;
    }
}

static int
chc__read_i32_le(chc__in *in, int32_t *out, chc_err *err)
{
    uint32_t u;
    int rc = chc__read_u32_le(in, &u, err);
    if (rc != CHC_OK) return rc;
    *out = (int32_t) u;
    return CHC_OK;
}

static int
chc__client_send_hello(chc_client *c, const chc_client_opts *opts, chc_err *err)
{
    int rc;
    const char *name = opts->client_name ? opts->client_name : "clickhouse-c";
    size_t name_len = strlen(name);
    const char *db  = opts->database ? opts->database : "default";
    const char *us  = opts->user     ? opts->user     : "default";
    const char *pw  = opts->password ? opts->password : "";

    if ((rc = chc__write_varuint(c->io, CHC__CLIENT_HELLO, err))) return rc;
    if ((rc = chc__write_string (c->io, name, name_len, err)))   return rc;
    if ((rc = chc__write_varuint(c->io, opts->client_version_major, err))) return rc;
    if ((rc = chc__write_varuint(c->io, opts->client_version_minor, err))) return rc;
    if ((rc = chc__write_varuint(c->io, c->client_revision, err))) return rc;
    if ((rc = chc__write_string (c->io, db, strlen(db), err))) return rc;
    if ((rc = chc__write_string (c->io, us, strlen(us), err))) return rc;
    if ((rc = chc__write_string (c->io, pw, strlen(pw), err))) return rc;
    return CHC_OK;
}

static int
chc__copy_short(char *dst, size_t cap, const char *src, size_t len)
{
    size_t n = len < cap - 1 ? len : cap - 1;
    if (n) memcpy(dst, src, n);
    dst[n] = '\0';
    return 0;
}

/* Reads chained exception. Caller frees via chc_exception_free. */
static int
chc__read_exception(chc_client *c, chc_exception **out, chc_err *err)
{
    chc_exception *head = NULL, *tail = NULL;
    for (;;) {
        chc_exception *e = chc__calloc(c->al, sizeof *e, err);
        if (!e) { chc_exception_free(head, c->al); return CHC_ERR_OOM; }
        int rc;
        if ((rc = chc__read_i32_le (&c->in, &e->code, err)) ||
            (rc = chc__read_string (&c->in, &e->name,         &e->name_len,         err)) ||
            (rc = chc__read_string (&c->in, &e->display_text, &e->display_text_len, err)) ||
            (rc = chc__read_string (&c->in, &e->stack_trace,  &e->stack_trace_len,  err))) {
            chc_exception_free(e, c->al);
            chc_exception_free(head, c->al);
            return rc;
        }
        uint8_t has_nested;
        if ((rc = chc__read_byte(&c->in, &has_nested, err))) {
            chc_exception_free(e, c->al);
            chc_exception_free(head, c->al);
            return rc;
        }
        if (tail) tail->nested = e; else head = e;
        tail = e;
        if (!has_nested) break;
    }
    *out = head;
    return CHC_OK;
}

static int
chc__client_recv_hello(chc_client *c, chc_err *err)
{
    uint64_t kind;
    int rc = chc__read_varuint(&c->in, &kind, err);
    if (rc != CHC_OK) return rc;
    if (kind == CHC__SERVER_EXCEPTION) {
        chc_exception *e = NULL;
        rc = chc__read_exception(c, &e, err);
        if (rc != CHC_OK) return rc;
        chc__err_set(err, CHC_ERR_SERVER, "%s",
                     e->display_text ? e->display_text : (e->name ? e->name : ""));
        err->server_code = e->code;
        chc__copy_short(err->server_name, sizeof err->server_name,
                        e->name, e->name_len);
        chc_exception_free(e, c->al);
        return CHC_ERR_SERVER;
    }
    if (kind != CHC__SERVER_HELLO)
        return chc__err_set(err, CHC_ERR_PROTOCOL,
                            "expected Hello, got %llu",
                            (unsigned long long) kind);

    char *s; size_t slen;
    if ((rc = chc__read_string(&c->in, &s, &slen, err))) return rc;
    chc__copy_short(c->server.name, sizeof c->server.name, s, slen);
    c->al->free(c->al->ud, s, slen + 1);

    if ((rc = chc__read_varuint(&c->in, &c->server.version_major, err))) return rc;
    if ((rc = chc__read_varuint(&c->in, &c->server.version_minor, err))) return rc;
    if ((rc = chc__read_varuint(&c->in, &c->server.revision,      err))) return rc;

    if (c->server.revision >= CHC__REV_SERVER_TIMEZONE) {
        if ((rc = chc__read_string(&c->in, &s, &slen, err))) return rc;
        chc__copy_short(c->server.timezone, sizeof c->server.timezone, s, slen);
        c->al->free(c->al->ud, s, slen + 1);
    }
    if (c->server.revision >= CHC__REV_SERVER_DISPLAY_NAME) {
        if ((rc = chc__read_string(&c->in, &s, &slen, err))) return rc;
        chc__copy_short(c->server.display_name, sizeof c->server.display_name, s, slen);
        c->al->free(c->al->ud, s, slen + 1);
    }
    if (c->server.revision >= CHC__REV_VERSION_PATCH) {
        if ((rc = chc__read_varuint(&c->in, &c->server.version_patch, err))) return rc;
    }
    return CHC_OK;
}

int
chc_client_init(chc_client **out, const chc_client_opts *opts,
                const chc_alloc *al, chc_io *io, chc_err *err)
{
    chc_client_opts def_opts = {0};
    if (!opts) opts = &def_opts;

    chc_client *c = chc__calloc(al, sizeof *c, err);
    if (!c) return CHC_ERR_OOM;
    c->al = al;
    c->io = io;
    c->client_revision = opts->client_revision ? opts->client_revision
                                               : CHC_CLIENT_DEFAULT_REVISION;
    c->compression = opts->codec ? opts->compression : CHC_COMP_NONE;
    c->codec       = opts->codec;

    int rc = chc__in_init(&c->in, io, al, opts->read_buffer_bytes, err);
    if (rc != CHC_OK) { al->free(al->ud, c, sizeof *c); return rc; }

    rc = chc__client_send_hello(c, opts, err);
    if (rc != CHC_OK) goto fail;
    rc = chc__client_recv_hello(c, err);
    if (rc != CHC_OK) goto fail;

    /* Server's effective revision is min(ours, server). After the
     * handshake we use this to gate optional fields on subsequent
     * packets. */
    if (c->server.revision > c->client_revision)
        c->server.revision = c->client_revision;

    /* Addendum: send empty quota_key. */
    if (c->server.revision >= CHC__REV_ADDENDUM) {
        rc = chc__write_string(c->io, "", 0, err);
        if (rc != CHC_OK) goto fail;
    }

    /* Probe Ping. Server-side late-stage rejections (eg invalid
     * default_database in 24.x) only surface after the Addendum is read,
     * not in the Hello reply. Without a probe, the rejection races the
     * caller's first query: caller's writes may hit ECONNRESET before the
     * exception packet is read. The Ping forces a round-trip here so the
     * exception is delivered at init time instead. Matches clickhouse-cpp's
     * SetPingBeforeQuery posture for the connection-establishment case. */
    rc = chc_client_send_ping(c, err);
    if (rc != CHC_OK) goto fail;
    {
        uint64_t kind;
        rc = chc__read_varuint(&c->in, &kind, err);
        if (rc != CHC_OK) goto fail;
        if (kind == CHC__SERVER_EXCEPTION) {
            chc_exception *e = NULL;
            rc = chc__read_exception(c, &e, err);
            if (rc != CHC_OK) goto fail;
            chc__err_set(err, CHC_ERR_SERVER, "%s",
                         e->display_text ? e->display_text :
                         (e->name ? e->name : ""));
            err->server_code = e->code;
            chc__copy_short(err->server_name, sizeof err->server_name,
                            e->name, e->name_len);
            chc_exception_free(e, c->al);
            rc = CHC_ERR_SERVER;
            goto fail;
        }
        if (kind != CHC__SERVER_PONG) {
            rc = chc__err_set(err, CHC_ERR_PROTOCOL,
                              "expected Pong, got %llu",
                              (unsigned long long) kind);
            goto fail;
        }
    }

    *out = c;
    return CHC_OK;

fail:
    chc__in_free(&c->in);
    al->free(al->ud, c, sizeof *c);
    *out = NULL;
    return rc;
}

void
chc_client_close(chc_client *c)
{
    if (!c) return;
    chc__in_free(&c->in);
    c->al->free(c->al->ud, c, sizeof *c);
}

const chc_server_info *
chc_client_server_info(const chc_client *c)
{
    return c ? &c->server : NULL;
}

int
chc_client_send_ping(chc_client *c, chc_err *err)
{
    return chc__write_varuint(c->io, CHC__CLIENT_PING, err);
}

int
chc_client_send_cancel(chc_client *c, chc_err *err)
{
    return chc__write_varuint(c->io, CHC__CLIENT_CANCEL, err);
}

/* Write a block body (BlockInfo + cols + rows) to a chc_io. Used for
 * both the uncompressed direct path and the compressed buffer-then-emit
 * path. */
static int
chc__client_write_block_body(chc_client *c, chc_io *sink,
                             const chc_block_builder *bb, chc_err *err)
{
    int rc;
    chc_block_opts opts = {
        .has_block_info = c->server.revision >= CHC__REV_BLOCK_INFO,
        .has_custom_serialization = c->server.revision >= CHC__REV_CUSTOM_SERIALIZATION,
    };
    if (bb) return chc_block_write(sink, bb, &opts, err);

    /* Empty block: BlockInfo + 0 cols + 0 rows. */
    if (opts.has_block_info) {
        if ((rc = chc__write_varuint(sink, 1, err))) return rc;
        uint8_t z = 0;
        if ((rc = chc__write_bytes(sink, &z, 1, err))) return rc;
        if ((rc = chc__write_varuint(sink, 2, err))) return rc;
        if ((rc = chc__write_u32_le(sink, (uint32_t) -1, err))) return rc;
        if ((rc = chc__write_varuint(sink, 0, err))) return rc;
    }
    if ((rc = chc__write_varuint(sink, 0, err))) return rc;  /* n_cols */
    if ((rc = chc__write_varuint(sink, 0, err))) return rc;  /* n_rows */
    return CHC_OK;
}

/* Write a Data packet. bb may be NULL for an empty (0 columns, 0 rows)
 * block — the terminator the server uses to detect end-of-INSERT and
 * end-of-query-text. */
static int
chc__client_write_data(chc_client *c, const chc_block_builder *bb, chc_err *err)
{
    int rc;
    if ((rc = chc__write_varuint(c->io, CHC__CLIENT_DATA, err))) return rc;
    /* Temporary table name (always empty from us). */
    if (c->server.revision >= CHC__REV_TEMPORARY_TABLES) {
        if ((rc = chc__write_string(c->io, "", 0, err))) return rc;
    }

    if (c->compression == CHC_COMP_NONE) {
        return chc__client_write_block_body(c, c->io, bb, err);
    }

    if (!c->codec)
        return chc__err_set(err, CHC_ERR_USAGE,
                            "compression enabled but codec is NULL");

    chc__mem_sink ms;
    chc_io sink_io;
    chc__mem_sink_init(&ms, &sink_io, c->al);
    rc = chc__client_write_block_body(c, &sink_io, bb, err);
    if (rc != CHC_OK) { chc__mem_sink_free(&ms); return rc; }
    rc = chc__comp_emit_chunks(c->io, c->codec, c->compression,
                               ms.buf, ms.len, c->al, err);
    chc__mem_sink_free(&ms);
    return rc;
}

int
chc_client_send_data(chc_client *c, const chc_block_builder *bb, chc_err *err)
{
    return chc__client_write_data(c, bb, err);
}

int
chc_client_send_query_ex(chc_client *c, const char *sql, size_t sql_len,
                         const chc_query_opts *opts, chc_err *err)
{
    chc_query_opts def = {0};
    if (!opts) opts = &def;

    int rc;
    if ((rc = chc__write_varuint(c->io, CHC__CLIENT_QUERY, err))) return rc;
    if ((rc = chc__write_string (c->io, opts->query_id, opts->query_id_len, err))) return rc;

    /* ClientInfo. clickhouse-cpp sends a fully-populated struct; we send
     * the minimum the server tolerates (initial fields blank, iface=TCP). */
    if (c->server.revision >= CHC__REV_CLIENT_INFO) {
        uint8_t query_kind = 1;       /* INITIAL_QUERY */
        if ((rc = chc__write_bytes (c->io, &query_kind, 1, err))) return rc;
        if ((rc = chc__write_string(c->io, "", 0, err))) return rc;  /* initial_user */
        if ((rc = chc__write_string(c->io, "", 0, err))) return rc;  /* initial_query_id */
        if ((rc = chc__write_string(c->io, "[::ffff:127.0.0.1]:0", 20, err))) return rc;
        if (c->server.revision >= CHC__REV_INITIAL_QUERY_START) {
            uint8_t z8[8] = {0};
            if ((rc = chc__write_bytes(c->io, z8, 8, err))) return rc;  /* int64 */
        }
        uint8_t iface_type = 1;       /* TCP */
        if ((rc = chc__write_bytes (c->io, &iface_type, 1, err))) return rc;
        if ((rc = chc__write_string(c->io, "", 0, err))) return rc;  /* os_user */
        if ((rc = chc__write_string(c->io, "", 0, err))) return rc;  /* client_hostname */
        if ((rc = chc__write_string(c->io, "clickhouse-c client", 19, err))) return rc;
        if ((rc = chc__write_varuint(c->io, 0, err))) return rc;     /* version_major */
        if ((rc = chc__write_varuint(c->io, 0, err))) return rc;     /* version_minor */
        if ((rc = chc__write_varuint(c->io, c->client_revision, err))) return rc;

        if (c->server.revision >= CHC__REV_QUOTA_KEY_IN_CLIENT)
            if ((rc = chc__write_string(c->io, "", 0, err))) return rc;
        if (c->server.revision >= CHC__REV_DISTRIBUTED_DEPTH)
            if ((rc = chc__write_varuint(c->io, 0, err))) return rc;
        if (c->server.revision >= CHC__REV_VERSION_PATCH)
            if ((rc = chc__write_varuint(c->io, 0, err))) return rc;  /* version_patch */
        if (c->server.revision >= CHC__REV_OPENTELEMETRY) {
            uint8_t no_otel = 0;
            if ((rc = chc__write_bytes(c->io, &no_otel, 1, err))) return rc;
        }
        if (c->server.revision >= CHC__REV_PARALLEL_REPLICAS) {
            if ((rc = chc__write_varuint(c->io, 0, err))) return rc;
            if ((rc = chc__write_varuint(c->io, 0, err))) return rc;
            if ((rc = chc__write_varuint(c->io, 0, err))) return rc;
        }
    }

    /* Per-query settings: name + varuint(flags) + value, repeated, then
     * empty-string terminator. Pre-54429 binary serialization isn't
     * implemented; the empty-list path still works because the terminator
     * is shape-compatible. */
    if (c->server.revision >= CHC__REV_SETTINGS_AS_STRINGS) {
        for (size_t i = 0; i < opts->n_settings; i++) {
            const chc_query_setting *s = &opts->settings[i];
            size_t nlen = s->name  ? strlen(s->name)  : 0;
            size_t vlen = s->value ? strlen(s->value) : 0;
            uint64_t flags = (s->important ? 1u : 0u) | (s->custom ? 2u : 0u);
            if ((rc = chc__write_string (c->io, s->name,  nlen, err))) return rc;
            if ((rc = chc__write_varuint(c->io, flags, err))) return rc;
            if ((rc = chc__write_string (c->io, s->value, vlen, err))) return rc;
        }
        if ((rc = chc__write_string(c->io, "", 0, err))) return rc;
    } else {
        if (opts->n_settings)
            return chc__err_set(err, CHC_ERR_PROTOCOL,
                "server revision %llu < %u: query settings unsupported",
                (unsigned long long) c->server.revision,
                CHC__REV_SETTINGS_AS_STRINGS);
        if ((rc = chc__write_string(c->io, "", 0, err))) return rc;
    }

    if (c->server.revision >= CHC__REV_INTERSERVER_SECRET) {
        if ((rc = chc__write_string(c->io, "", 0, err))) return rc;
    }

    /* Stages::Complete = 2. */
    if ((rc = chc__write_varuint(c->io, 2, err))) return rc;
    /* Compression state: 1 if enabled, 0 otherwise. */
    if ((rc = chc__write_varuint(c->io,
        c->compression != CHC_COMP_NONE ? 1u : 0u, err))) return rc;
    /* Query text. */
    if ((rc = chc__write_string(c->io, sql, sql_len, err))) return rc;

    /* Parameters: same shape as settings; flags always CUSTOM (bit 1). */
    if (c->server.revision >= CHC__REV_PARAMETERS) {
        for (size_t i = 0; i < opts->n_params; i++) {
            const chc_query_param *p = &opts->params[i];
            size_t nlen = p->name  ? strlen(p->name)  : 0;
            size_t vlen = p->value ? strlen(p->value) : 0;
            if ((rc = chc__write_string (c->io, p->name,  nlen, err))) return rc;
            if ((rc = chc__write_varuint(c->io, 2u, err))) return rc;
            if ((rc = chc__write_string (c->io, p->value, vlen, err))) return rc;
        }
        if ((rc = chc__write_string(c->io, "", 0, err))) return rc;
    } else if (opts->n_params) {
        return chc__err_set(err, CHC_ERR_PROTOCOL,
            "server revision %llu < %u: query parameters unsupported",
            (unsigned long long) c->server.revision, CHC__REV_PARAMETERS);
    }

    /* Finalize: send an empty Data block as the query-text terminator. */
    return chc__client_write_data(c, NULL, err);
}

int
chc_client_send_query(chc_client *c, const char *sql, size_t sql_len,
                      const char *query_id, size_t query_id_len, chc_err *err)
{
    chc_query_opts opts = {
        .query_id = query_id,
        .query_id_len = query_id_len,
    };
    return chc_client_send_query_ex(c, sql, sql_len, &opts, err);
}

static int
chc__client_read_data_packet(chc_client *c, chc_packet *out, chc_err *err)
{
    int rc;
    if (c->server.revision >= CHC__REV_TEMPORARY_TABLES) {
        char *s; size_t slen;
        if ((rc = chc__read_string(&c->in, &s, &slen, err))) return rc;
        c->al->free(c->al->ud, s, slen + 1);
    }
    chc_block_opts opts = {
        .has_block_info = c->server.revision >= CHC__REV_BLOCK_INFO,
        .has_custom_serialization = c->server.revision >= CHC__REV_CUSTOM_SERIALIZATION,
    };
    if (c->compression == CHC_COMP_NONE) {
        return chc__block_read_in(&c->in, c->al, &opts, &out->block, err);
    }
    if (!c->codec)
        return chc__err_set(err, CHC_ERR_USAGE,
                            "compression enabled but codec is NULL");
    chc__decomp_src src;
    chc_io decomp_io;
    chc__decomp_src_init(&src, &c->in, c->codec, c->al, &decomp_io);
    chc__in dec_in;
    rc = chc__in_init(&dec_in, &decomp_io, c->al, 0, err);
    if (rc != CHC_OK) { chc__decomp_src_free(&src); return rc; }
    rc = chc__block_read_in(&dec_in, c->al, &opts, &out->block, err);
    chc__in_free(&dec_in);
    chc__decomp_src_free(&src);
    return rc;
}

void
chc_packet_clear(chc_client *c, chc_packet *p)
{
    if (!p) return;
    if (p->block) { chc_block_destroy(p->block, c->al); p->block = NULL; }
    if (p->exception) { chc_exception_free(p->exception, c->al); p->exception = NULL; }
}

int
chc_client_recv_packet(chc_client *c, chc_packet *out, chc_err *err)
{
    memset(out, 0, sizeof *out);
    uint64_t kind;
    int rc = chc__read_varuint(&c->in, &kind, err);
    if (rc != CHC_OK) return rc;

    switch (kind) {
    case CHC__SERVER_DATA:
        out->kind = CHC_PKT_DATA;
        return chc__client_read_data_packet(c, out, err);

    case CHC__SERVER_TOTALS:
        out->kind = CHC_PKT_TOTALS;
        return chc__client_read_data_packet(c, out, err);

    case CHC__SERVER_EXTREMES:
        out->kind = CHC_PKT_EXTREMES;
        return chc__client_read_data_packet(c, out, err);

    case CHC__SERVER_EXCEPTION:
        out->kind = CHC_PKT_EXCEPTION;
        return chc__read_exception(c, &out->exception, err);

    case CHC__SERVER_PROGRESS:
        out->kind = CHC_PKT_PROGRESS;
        if ((rc = chc__read_varuint(&c->in, &out->progress.rows,  err))) return rc;
        if ((rc = chc__read_varuint(&c->in, &out->progress.bytes, err))) return rc;
        if (c->server.revision >= CHC__REV_TOTAL_ROWS_IN_PROGRESS)
            if ((rc = chc__read_varuint(&c->in, &out->progress.total_rows, err))) return rc;
        if (c->server.revision >= CHC__REV_CLIENT_WRITE_INFO) {
            if ((rc = chc__read_varuint(&c->in, &out->progress.written_rows,  err))) return rc;
            if ((rc = chc__read_varuint(&c->in, &out->progress.written_bytes, err))) return rc;
        }
        return CHC_OK;

    case CHC__SERVER_PONG:
        out->kind = CHC_PKT_PONG;
        return CHC_OK;

    case CHC__SERVER_END_OF_STREAM:
        out->kind = CHC_PKT_END_OF_STREAM;
        return CHC_OK;

    case CHC__SERVER_PROFILE_INFO:
        out->kind = CHC_PKT_PROFILE_INFO;
        if ((rc = chc__read_varuint(&c->in, &out->profile.rows,   err))) return rc;
        if ((rc = chc__read_varuint(&c->in, &out->profile.blocks, err))) return rc;
        if ((rc = chc__read_varuint(&c->in, &out->profile.bytes,  err))) return rc;
        if ((rc = chc__read_byte   (&c->in, &out->profile.applied_limit, err))) return rc;
        if ((rc = chc__read_varuint(&c->in, &out->profile.rows_before_limit, err))) return rc;
        if ((rc = chc__read_byte   (&c->in, &out->profile.calculated_rows_before_limit, err))) return rc;
        return CHC_OK;

    case CHC__SERVER_LOG: {
        out->kind = CHC_PKT_LOG;
        /* Log packets prepend an external-table name (always empty
         * from CH server) before the block, never compressed. */
        char *tag; size_t taglen;
        if ((rc = chc__read_string(&c->in, &tag, &taglen, err))) return rc;
        c->al->free(c->al->ud, tag, taglen + 1);
        chc_block_opts opts = {
            .has_block_info = c->server.revision >= CHC__REV_BLOCK_INFO,
            .has_custom_serialization = c->server.revision >= CHC__REV_CUSTOM_SERIALIZATION,
        };
        return chc__block_read_in(&c->in, c->al, &opts, &out->block, err);
    }

    case CHC__SERVER_TABLE_COLUMNS: {
        out->kind = CHC_PKT_TABLE_COLUMNS;
        /* table name + columns metadata; both are varstrings, both ignored. */
        char *s; size_t slen;
        if ((rc = chc__read_string(&c->in, &s, &slen, err))) return rc;
        c->al->free(c->al->ud, s, slen + 1);
        if ((rc = chc__read_string(&c->in, &s, &slen, err))) return rc;
        c->al->free(c->al->ud, s, slen + 1);
        return CHC_OK;
    }

    case CHC__SERVER_PROFILE_EVENTS: {
        out->kind = CHC_PKT_PROFILE_EVENTS;
        char *tag; size_t taglen;
        if ((rc = chc__read_string(&c->in, &tag, &taglen, err))) return rc;
        c->al->free(c->al->ud, tag, taglen + 1);
        chc_block_opts opts = {
            .has_block_info = c->server.revision >= CHC__REV_BLOCK_INFO,
            .has_custom_serialization = c->server.revision >= CHC__REV_CUSTOM_SERIALIZATION,
        };
        return chc__block_read_in(&c->in, c->al, &opts, &out->block, err);
    }

    default:
        return chc__err_set(err, CHC_ERR_PROTOCOL,
                            "unknown server packet %llu",
                            (unsigned long long) kind);
    }
}

#endif /* CHC_IMPLEMENTATION */

#ifdef __cplusplus
}
#endif

#endif /* CLICKHOUSE_CLIENT_H */
