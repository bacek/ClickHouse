#include <stdint.h>
#include <stddef.h>

typedef struct
{
    uint8_t * data;
    uint32_t size;
} Span;

#define HEAP_SIZE (1 << 24)
static _Alignas(16) uint8_t heap[HEAP_SIZE];
static uint32_t heap_pos = 0;

#define MAX_SPANS 4096
static Span spans[MAX_SPANS];
static uint32_t span_pos = 0;

Span * clickhouse_create_buffer(uint32_t size)
{
    if (span_pos >= MAX_SPANS) return NULL;
    if (heap_pos + size > HEAP_SIZE) return NULL;
    Span * span = &spans[span_pos++];
    span->data = &heap[heap_pos];
    span->size = size;
    heap_pos += (size + 15) & ~15u;
    return span;
}

void clickhouse_destroy_buffer(Span * data)
{
    (void)data;
}

/* Takes (Int32, String), returns String = string || toString(int32).
 * Input TSV rows:  "<int32>\t<string>\n"
 * Output TSV rows: "<string><int32>\n"
 *
 * Simple strings without special characters are assumed (no TSV escaping needed). */
Span * concat_str_int(Span * span, uint32_t n)
{
    /* Max output per row: original row bytes + 12 extra (sign + 10 digits + newline). */
    Span * res = clickhouse_create_buffer(span->size + n * 12);
    if (!res) return NULL;

    const uint8_t * p = span->data;
    const uint8_t * end = p + span->size;
    char * out = (char *)res->data;
    uint32_t pos = 0;

    for (uint32_t i = 0; i < n; i++)
    {
        /* Split row at the first tab: [p, tab) = int32, [tab+1, nl) = string */
        const uint8_t * tab = p;
        while (tab < end && *tab != '\t') tab++;
        const uint8_t * nl = tab + 1;
        while (nl < end && *nl != '\n') nl++;

        /* Parse the int32 field as a signed decimal. */
        int32_t sign = 1;
        const uint8_t * ip = p;
        if (ip < tab && *ip == '-') { sign = -1; ip++; }
        int32_t val = 0;
        while (ip < tab) { val = val * 10 + (*ip++ - '0'); }
        val *= sign;

        /* Copy the string field. */
        const uint8_t * s = tab + 1;
        uint32_t s_len = (uint32_t)(nl - s);
        for (uint32_t j = 0; j < s_len; j++) out[pos++] = (char)s[j];

        /* Append the int32 in decimal. */
        if (val == 0)
        {
            out[pos++] = '0';
        }
        else
        {
            char tmp[11];
            uint32_t tmp_len = 0;
            if (val < 0) { out[pos++] = '-'; val = -val; }
            uint32_t uval = (uint32_t)val;
            while (uval > 0) { tmp[tmp_len++] = '0' + (char)(uval % 10); uval /= 10; }
            for (uint32_t j = tmp_len; j > 0; j--) out[pos++] = tmp[j - 1];
        }

        out[pos++] = '\n';
        p = nl + 1;
    }

    res->size = pos;
    return res;
}
