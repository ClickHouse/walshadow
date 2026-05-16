/*
 * clickhouse-posix-io.h -- blocking POSIX fd backend for chc_io.
 *
 * Exactly one TU must `#define CHC_IMPLEMENTATION` before including;
 * other TUs include for declarations only. Depends on clickhouse.h
 * (for chc_io / chc_err declarations).
 */

#ifndef CLICKHOUSE_POSIX_IO_H
#define CLICKHOUSE_POSIX_IO_H

#include <stdbool.h>

#include "clickhouse.h"

#ifdef __cplusplus
extern "C" {
#endif

typedef struct chc_posix_io {
    int   fd;
    bool (*check_cancel)(void *ud);
    void *cancel_ud;
} chc_posix_io;

void chc_posix_io_init(chc_posix_io *state, chc_io *out_io, int fd,
                       bool (*check_cancel)(void *), void *cancel_ud);

#ifdef CHC_IMPLEMENTATION

#include <errno.h>
#include <stdio.h>
#include <unistd.h>

static int
chc__posix_read(void *ud, void *buf, size_t len, size_t *out_n, chc_err *err)
{
    chc_posix_io *s = ud;
    for (;;) {
        ssize_t n = read(s->fd, buf, len);
        if (n >= 0) { *out_n = (size_t) n; return CHC_OK; }
        if (errno == EINTR) continue;
        snprintf(err->msg, sizeof err->msg, "read(fd=%d): %s",
                 s->fd, strerror(errno));
        err->code = CHC_ERR_IO;
        return CHC_ERR_IO;
    }
}

static int
chc__posix_write(void *ud, const void *buf, size_t len, chc_err *err)
{
    chc_posix_io *s = ud;
    const unsigned char *p = buf;
    while (len) {
        ssize_t n = write(s->fd, p, len);
        if (n > 0) { p += n; len -= (size_t) n; continue; }
        if (n < 0 && errno == EINTR) continue;
        snprintf(err->msg, sizeof err->msg, "write(fd=%d): %s",
                 s->fd, strerror(errno));
        err->code = CHC_ERR_IO;
        return CHC_ERR_IO;
    }
    return CHC_OK;
}

static int
chc__posix_cancel(void *ud)
{
    chc_posix_io *s = ud;
    return s->check_cancel ? (s->check_cancel(s->cancel_ud) ? 1 : 0) : 0;
}

void
chc_posix_io_init(chc_posix_io *state, chc_io *out_io, int fd,
                  bool (*check_cancel)(void *), void *cancel_ud)
{
    state->fd = fd;
    state->check_cancel = check_cancel;
    state->cancel_ud = cancel_ud;
    out_io->ud = state;
    out_io->read = chc__posix_read;
    out_io->write = chc__posix_write;
    out_io->check_cancel = check_cancel ? chc__posix_cancel : NULL;
}

#endif /* CHC_IMPLEMENTATION */

#ifdef __cplusplus
}
#endif

#endif /* CLICKHOUSE_POSIX_IO_H */
