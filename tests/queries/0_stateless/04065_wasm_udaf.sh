#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan
# Test WASM aggregate user-defined functions (UDAFs).
# When SETTINGS is_aggregate = 1, ClickHouse accumulates rows per group and
# calls the WASM function once at finalize with Array(original_arg_type) arguments.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << 'EOF'
DROP FUNCTION IF EXISTS wasm_udaf_sum;
DROP FUNCTION IF EXISTS wasm_udaf_count;
DELETE FROM system.webassembly_modules WHERE name = 'udaf_test';
EOF

cat "${CUR_DIR}/wasm/udaf_sum.wasm" | ${CLICKHOUSE_CLIENT} \
    --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'udaf_test', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << 'EOF'

CREATE FUNCTION wasm_udaf_sum
    LANGUAGE WASM ABI BUFFERED_V1 FROM 'udaf_test' :: 'sum_f64'
    ARGUMENTS (x Float64) RETURNS Float64
    SETTINGS is_aggregate = 1, serialization_format = 'RowBinary';

CREATE FUNCTION wasm_udaf_count
    LANGUAGE WASM ABI BUFFERED_V1 FROM 'udaf_test' :: 'count_f64'
    ARGUMENTS (x Float64) RETURNS Float64
    SETTINGS is_aggregate = 1, serialization_format = 'RowBinary';

-- Basic aggregation over all rows
SELECT wasm_udaf_sum(x) FROM (SELECT arrayJoin([1.0, 2.0, 3.0]::Array(Float64)) AS x);

-- GROUP BY: each group is accumulated separately
SELECT key, wasm_udaf_sum(x)
FROM (
    SELECT 1 AS key, arrayJoin([10.0, 20.0]::Array(Float64)) AS x
    UNION ALL
    SELECT 2, arrayJoin([100.0, 200.0, 300.0]::Array(Float64))
)
GROUP BY key
ORDER BY key;

-- Count function: returns the number of accumulated rows per group
SELECT key, wasm_udaf_count(x)
FROM (
    SELECT 1 AS key, arrayJoin([1.1, 2.2]::Array(Float64)) AS x
    UNION ALL
    SELECT 2, arrayJoin([1.1, 2.2, 3.3]::Array(Float64))
)
GROUP BY key
ORDER BY key;

-- system.functions: WASM UDAFs appear with is_aggregate = 1
SELECT name, is_aggregate, origin
FROM system.functions
WHERE name IN ('wasm_udaf_sum', 'wasm_udaf_count')
ORDER BY name;

-- Each function must appear exactly once (no duplicates from SQL UDF storage)
SELECT name, count() AS cnt
FROM system.functions
WHERE name IN ('wasm_udaf_sum', 'wasm_udaf_count')
GROUP BY name
HAVING cnt > 1;

DROP FUNCTION wasm_udaf_sum;
DROP FUNCTION wasm_udaf_count;
DELETE FROM system.webassembly_modules WHERE name = 'udaf_test';

-- After DROP, functions must no longer appear
SELECT count() FROM system.functions WHERE name IN ('wasm_udaf_sum', 'wasm_udaf_count');
EOF
