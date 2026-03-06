#include <stdint.h>

/* Single identity function for all small integer types.
 * At the WASM level all small integers are widened to i32 by ClickHouse before
 * the call, and narrowed back to the declared return type after. */

int32_t identity(int32_t x) { return x; }

/* Arithmetic on two i32 values. */
int32_t add(int32_t a, int32_t b) { return a + b; }
