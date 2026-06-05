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
