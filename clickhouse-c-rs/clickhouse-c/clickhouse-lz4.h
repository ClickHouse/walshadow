/*
 * clickhouse-lz4.h -- ready-made LZ4 chc_codec wrapper.
 *
 * Caller links -llz4 and, in the one TU that ships the implementation:
 *   #define CHC_IMPLEMENTATION
 *   #include "clickhouse-lz4.h"
 *
 * Then:
 *   chc_codec codec; chc_lz4_codec_init(&codec);
 *
 * Fills only the lz4_* function pointers; zstd_* stay NULL. Consumers
 * who need ZSTD include clickhouse-zstd.h on top (or fill the codec
 * struct themselves).
 */

#ifndef CLICKHOUSE_LZ4_H
#define CLICKHOUSE_LZ4_H

#include "clickhouse.h"
#include "clickhouse-compression.h"

#ifdef __cplusplus
extern "C" {
#endif

void chc_lz4_codec_init(chc_codec *out);

#ifdef CHC_IMPLEMENTATION

#include <lz4.h>

static int
chc__lz4_compress(void *ud, const void *src, size_t src_len,
                  void *dst, size_t dst_cap, size_t *dst_n, chc_err *err)
{
    (void) ud;
    if (src_len > (size_t) LZ4_MAX_INPUT_SIZE)
        return chc__err_set(err, CHC_ERR_USAGE,
                            "LZ4 input too big: %zu", src_len);
    int n = LZ4_compress_default((const char *) src, (char *) dst,
                                 (int) src_len, (int) dst_cap);
    if (n <= 0)
        return chc__err_set(err, CHC_ERR_OOM,
                            "LZ4_compress_default failed (rc=%d)", n);
    *dst_n = (size_t) n;
    return CHC_OK;
}

static int
chc__lz4_decompress(void *ud, const void *src, size_t src_len,
                    void *dst, size_t original, chc_err *err)
{
    (void) ud;
    int n = LZ4_decompress_safe((const char *) src, (char *) dst,
                                (int) src_len, (int) original);
    if (n < 0 || (size_t) n != original)
        return chc__err_set(err, CHC_ERR_PROTOCOL,
                            "LZ4_decompress_safe rc=%d, expected %zu",
                            n, original);
    return CHC_OK;
}

static size_t chc__lz4_bound(size_t n) { return (size_t) LZ4_compressBound((int) n); }

void
chc_lz4_codec_init(chc_codec *out)
{
    out->ud              = NULL;
    out->lz4_compress    = chc__lz4_compress;
    out->lz4_decompress  = chc__lz4_decompress;
    out->lz4_bound       = chc__lz4_bound;
    out->zstd_compress   = NULL;
    out->zstd_decompress = NULL;
    out->zstd_bound      = NULL;
}

#endif /* CHC_IMPLEMENTATION */

#ifdef __cplusplus
}
#endif

#endif /* CLICKHOUSE_LZ4_H */
