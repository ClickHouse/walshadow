/*
 * wrapper.c -- one-and-only TU that materializes clickhouse-c's
 * stb-style implementation. Feature flags toggle optional codecs via
 * CHC_NO_LZ4 / CHC_NO_ZSTD opt-outs in clickhouse-compression.h.
 */

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
