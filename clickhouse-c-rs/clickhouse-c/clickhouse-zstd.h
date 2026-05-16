/*
 * clickhouse-zstd.h -- ready-made ZSTD chc_codec wrapper.
 *
 * Caller links -lzstd and, in the one TU that ships the implementation:
 *   #define CHC_IMPLEMENTATION
 *   #include "clickhouse-zstd.h"
 *
 * Then:
 *   chc_codec codec; chc_zstd_codec_init(&codec);
 *
 * Fills only the zstd_* function pointers; lz4_* stay NULL. Consumers
 * who need both codecs include clickhouse-lz4.h as well (or fill the
 * codec struct manually).
 */

#ifndef CLICKHOUSE_ZSTD_H
#define CLICKHOUSE_ZSTD_H

#include "clickhouse.h"
#include "clickhouse-compression.h"

#ifdef __cplusplus
extern "C" {
#endif

void chc_zstd_codec_init(chc_codec *out);

#ifdef CHC_IMPLEMENTATION

#include <zstd.h>

static int
chc__zstd_compress(void *ud, const void *src, size_t src_len,
                   void *dst, size_t dst_cap, size_t *dst_n, chc_err *err)
{
    (void) ud;
    size_t n = ZSTD_compress(dst, dst_cap, src, src_len, ZSTD_CLEVEL_DEFAULT);
    if (ZSTD_isError(n))
        return chc__err_set(err, CHC_ERR_OOM,
                            "ZSTD_compress failed: %s", ZSTD_getErrorName(n));
    *dst_n = n;
    return CHC_OK;
}

static int
chc__zstd_decompress(void *ud, const void *src, size_t src_len,
                     void *dst, size_t original, chc_err *err)
{
    (void) ud;
    size_t n = ZSTD_decompress(dst, original, src, src_len);
    if (ZSTD_isError(n))
        return chc__err_set(err, CHC_ERR_PROTOCOL,
                            "ZSTD_decompress failed: %s", ZSTD_getErrorName(n));
    if (n != original)
        return chc__err_set(err, CHC_ERR_PROTOCOL,
                            "ZSTD_decompress produced %zu bytes, expected %zu",
                            n, original);
    return CHC_OK;
}

static size_t chc__zstd_bound(size_t n) { return ZSTD_compressBound(n); }

void
chc_zstd_codec_init(chc_codec *out)
{
    out->ud              = NULL;
    out->lz4_compress    = NULL;
    out->lz4_decompress  = NULL;
    out->lz4_bound       = NULL;
    out->zstd_compress   = chc__zstd_compress;
    out->zstd_decompress = chc__zstd_decompress;
    out->zstd_bound      = chc__zstd_bound;
}

#endif /* CHC_IMPLEMENTATION */

#ifdef __cplusplus
}
#endif

#endif /* CLICKHOUSE_ZSTD_H */
