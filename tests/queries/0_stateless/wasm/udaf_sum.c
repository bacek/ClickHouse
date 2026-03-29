/* WASM aggregate function: sum and count of Float64 values.
 *
 * Exports (BUFFERED_V1 ABI, serialization_format = 'RowBinary'):
 *   sum_f64    - sums Array(Float64) → Float64
 *   count_f64  - counts elements in Array(Float64) → Float64
 *
 * Exports (BUFFERED_V1 ABI, serialization_format = 'MsgPack'):
 *   sum_f64_msgpack   - sums Array(Float64) → Float64
 *   count_f64_msgpack - counts elements in Array(Float64) → Float64
 *
 * When registered with SETTINGS is_aggregate = 1, ClickHouse accumulates rows
 * per GROUP BY key, then calls this function once at finalize with a one-row
 * block where the single column is Array(Float64).
 *
 * RowBinary encoding of Array(Float64):
 *   - ULeb128 array length N
 *   - N * 8 bytes little-endian IEEE-754 doubles
 * Output: 8 bytes little-endian double.
 *
 * MsgPack encoding of Array(Float64):
 *   - fixarray (0x9N, N≤15) / array16 (0xdc NN NN) / array32 (0xdd NN NN NN NN)
 *   - N * (0xcb + 8 bytes big-endian IEEE-754 double)
 * Output: 0xcb + 8 bytes big-endian double.
 */

#include <stdint.h>
#include <stddef.h>

typedef struct
{
    uint8_t * data;
    uint32_t size;
} Span;

#define HEAP_SIZE (1 << 20)
static _Alignas(16) uint8_t heap[HEAP_SIZE];
static uint32_t heap_pos = 0;

#define MAX_SPANS 64
static Span spans[MAX_SPANS];
static uint32_t span_pos = 0;

extern void clickhouse_log(uint32_t level, const char * message, uint32_t length);

Span * clickhouse_create_buffer(uint32_t size)
{
    uint32_t aligned_size = (size + 15u) & ~15u;
    if (span_pos >= MAX_SPANS || heap_pos + aligned_size > HEAP_SIZE)
        return NULL;
    Span * span = &spans[span_pos++];
    span->data = &heap[heap_pos];
    span->size = size;
    heap_pos += aligned_size;
    return span;
}

void clickhouse_destroy_buffer(Span * span)
{
    (void)span;
}

/* ---- RowBinary helpers ---- */

static uint64_t read_uleb128(const uint8_t ** p)
{
    uint64_t result = 0;
    int shift = 0;
    while (1)
    {
        uint8_t byte = *(*p)++;
        result |= (uint64_t)(byte & 0x7f) << shift;
        if ((byte & 0x80) == 0)
            break;
        shift += 7;
    }
    return result;
}

/* Sum all Float64 values from an Array(Float64) column (RowBinary, 1 row).
 * Returns a RowBinary-encoded Float64 result (8 bytes). */
Span * sum_f64(Span * span, uint32_t n_rows)
{
    (void)n_rows;
    const uint8_t * p = span->data;

    uint64_t arr_size = read_uleb128(&p);

    double sum = 0.0;
    for (uint64_t i = 0; i < arr_size; ++i)
    {
        double val;
        __builtin_memcpy(&val, p, 8);
        p += 8;
        sum += val;
    }

    Span * res = clickhouse_create_buffer(8);
    if (!res)
        return NULL;
    __builtin_memcpy(res->data, &sum, 8);
    res->size = 8;
    return res;
}

/* Count elements in an Array(Float64) column (RowBinary) — returns length as Float64. */
Span * count_f64(Span * span, uint32_t n_rows)
{
    (void)n_rows;
    const uint8_t * p = span->data;

    uint64_t arr_size = read_uleb128(&p);
    double count = (double)arr_size;

    Span * res = clickhouse_create_buffer(8);
    if (!res)
        return NULL;
    __builtin_memcpy(res->data, &count, 8);
    res->size = 8;
    return res;
}

/* ---- MsgPack helpers ---- */

/* Read MsgPack array length; advance *p past the array header. */
static uint32_t read_msgpack_array_len(const uint8_t ** p)
{
    uint8_t b = *(*p)++;
    if ((b & 0xf0) == 0x90)
        return b & 0x0f; /* fixarray */
    if (b == 0xdc)
    {
        uint32_t len = ((uint32_t)(*p)[0] << 8) | (*p)[1];
        *p += 2;
        return len;
    }
    if (b == 0xdd)
    {
        uint32_t len = ((uint32_t)(*p)[0] << 24) | ((uint32_t)(*p)[1] << 16)
                     | ((uint32_t)(*p)[2] << 8)  |  (*p)[3];
        *p += 4;
        return len;
    }
    return 0;
}

/* Read one MsgPack float64 (0xcb + 8 big-endian bytes); advance *p. */
static double read_msgpack_f64(const uint8_t ** p)
{
    (*p)++; /* skip 0xcb tag */
    uint64_t bits = ((uint64_t)(*p)[0] << 56) | ((uint64_t)(*p)[1] << 48)
                  | ((uint64_t)(*p)[2] << 40) | ((uint64_t)(*p)[3] << 32)
                  | ((uint64_t)(*p)[4] << 24) | ((uint64_t)(*p)[5] << 16)
                  | ((uint64_t)(*p)[6] <<  8) |  (*p)[7];
    *p += 8;
    double val;
    __builtin_memcpy(&val, &bits, 8);
    return val;
}

/* Write one MsgPack float64 (0xcb + 8 big-endian bytes); advance *p. */
static void write_msgpack_f64(uint8_t ** p, double v)
{
    uint64_t bits;
    __builtin_memcpy(&bits, &v, 8);
    *(*p)++ = 0xcb;
    *(*p)++ = (uint8_t)(bits >> 56);
    *(*p)++ = (uint8_t)(bits >> 48);
    *(*p)++ = (uint8_t)(bits >> 40);
    *(*p)++ = (uint8_t)(bits >> 32);
    *(*p)++ = (uint8_t)(bits >> 24);
    *(*p)++ = (uint8_t)(bits >> 16);
    *(*p)++ = (uint8_t)(bits >>  8);
    *(*p)++ = (uint8_t) bits;
}

/* Sum all Float64 values from an Array(Float64) column (MsgPack, 1 row).
 * Returns a MsgPack-encoded Float64 result (0xcb + 8 bytes). */
Span * sum_f64_msgpack(Span * span, uint32_t n_rows)
{
    (void)n_rows;
    const uint8_t * p = span->data;

    uint32_t arr_size = read_msgpack_array_len(&p);

    double sum = 0.0;
    for (uint32_t i = 0; i < arr_size; ++i)
        sum += read_msgpack_f64(&p);

    Span * res = clickhouse_create_buffer(9);
    if (!res)
        return NULL;
    uint8_t * out = res->data;
    write_msgpack_f64(&out, sum);
    res->size = 9;
    return res;
}

/* Count elements in an Array(Float64) column (MsgPack) — returns length as Float64. */
Span * count_f64_msgpack(Span * span, uint32_t n_rows)
{
    (void)n_rows;
    const uint8_t * p = span->data;

    uint32_t arr_size = read_msgpack_array_len(&p);

    Span * res = clickhouse_create_buffer(9);
    if (!res)
        return NULL;
    uint8_t * out = res->data;
    write_msgpack_f64(&out, (double)arr_size);
    res->size = 9;
    return res;
}
