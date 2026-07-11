/*
 * wrapper.c -- one-and-only TU that materializes clickhouse-c's
 * stb-style implementation. Feature flags toggle optional codecs via
 * CHC_NO_LZ4 / CHC_NO_ZSTD opt-outs in clickhouse-compression.h.
 */

/* posix-io read timeouts use clock_gettime/CLOCK_MONOTONIC + poll, hidden
 * under -std=c23 without a POSIX feature-test macro. Match upstream TUs. */
#define _POSIX_C_SOURCE 200809L
#define _DARWIN_C_SOURCE 200809L

#define CHC_PROVIDE_STDLIB_ALLOC
#define CHC_IMPLEMENTATION

#ifndef CHC_RS_LZ4
#define CHC_NO_LZ4
#endif

#ifndef CHC_RS_ZSTD
#define CHC_NO_ZSTD
#endif

#include "clickhouse.h"
#include "clickhouse-posix-io.h"
#include "clickhouse-compression.h"
#include "clickhouse-client.h"
#include "clickhouse-async.h"

#include <time.h>

/* Monotonic-us clock in clickhouse-c's CLOCK_MONOTONIC domain, letting Rust
 * compute absolute read deadlines comparable to the posix-io poll loop.
 * src/io.rs builds & drives the chc_posix_io / chc_io pair itself via the
 * chc_posix_io_init / chc_posix_io_set_deadline symbols this TU emits. */
int64_t
chc_rs_monotonic_us(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (int64_t) ts.tv_sec * 1000000 + ts.tv_nsec / 1000;
}

/* Persistent io-backed reader for streaming successive Native blocks. The
 * chc_in body is implementation-private (behind CHC_IMPLEMENTATION), so Rust
 * cannot allocate it by value; heap-box it here through the caller's
 * allocator. src/block.rs (BlockReader) holds the handle across
 * chc_block_read calls so bytes read past a block boundary stay buffered for
 * the next block; a fresh reader per block would drop that tail and lose
 * every block after the first. */
int
chc_rs_in_new(chc_io *io, const chc_alloc *al, size_t cap,
              chc_in **out, chc_err *err)
{
    chc_in *in = al->alloc(al->ud, sizeof *in);
    if (!in) return chc__err_set(err, CHC_ERR_OOM, "chc_in alloc failed");
    int rc = chc_in_init(in, io, al, cap, err);
    if (rc != CHC_OK) {
        al->free(al->ud, in, sizeof *in);
        return rc;
    }
    *out = in;
    return CHC_OK;
}

void
chc_rs_in_destroy(chc_in *in, const chc_alloc *al)
{
    if (!in) return;
    chc_in_free(in);
    al->free(al->ud, in, sizeof *in);
}
