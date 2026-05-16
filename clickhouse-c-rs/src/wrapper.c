/*
 * wrapper.c -- one-and-only TU that materializes clickhouse-c's
 * stb-style implementation. Feature flags toggle optional codecs.
 */

#define CHC_PROVIDE_STDLIB_ALLOC
#define CHC_IMPLEMENTATION

#include "clickhouse.h"
#include "clickhouse-posix-io.h"
#include "clickhouse-compression.h"
#include "clickhouse-client.h"

#ifdef CHC_RS_LZ4
#include "clickhouse-lz4.h"
#endif

#ifdef CHC_RS_ZSTD
#include "clickhouse-zstd.h"
#endif
