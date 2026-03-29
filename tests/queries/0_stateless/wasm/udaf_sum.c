/* WASM aggregate function: sum of Float64 values.
 *
 * Designed for BUFFERED_V1 ABI with serialization_format = 'RowBinary'.
 *
 * When registered with SETTINGS is_aggregate = 1, ClickHouse accumulates rows
 * per GROUP BY key, then calls this function once at finalize with a one-row
 * block where the single column is Array(Float64).
 *
 * RowBinary encoding of Array(Float64):
 *   - ULeb128 array length N
 *   - N * 8 bytes little-endian IEEE-754 doubles
 *
 * Output: 8-byte little-endian double (RowBinary Float64).
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

/* Count elements in an Array(Float64) column — returns the array length as Float64. */
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
