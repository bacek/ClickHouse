#!/usr/bin/env bash
# Tags: no-fasttest, no-msan
# Test that WASM UDFs declared DETERMINISTIC are constant-folded when called
# with constant arguments, while non-deterministic UDFs are not.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

MODULE_NAME="identity_cf_test_${CLICKHOUSE_DATABASE}"
FN_DET="identity_det_${CLICKHOUSE_DATABASE}"
FN_NONDET="identity_nondet_${CLICKHOUSE_DATABASE}"

${CLICKHOUSE_CLIENT} --query "DROP FUNCTION IF EXISTS ${FN_DET}"
${CLICKHOUSE_CLIENT} --query "DROP FUNCTION IF EXISTS ${FN_NONDET}"
${CLICKHOUSE_CLIENT} --query "DELETE FROM system.webassembly_modules WHERE name = '${MODULE_NAME}'"

cat "${CUR_DIR}/wasm/identity_int.wasm" | ${CLICKHOUSE_CLIENT} \
    --query "INSERT INTO system.webassembly_modules (name, code) SELECT '${MODULE_NAME}', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} << EOF
SET webassembly_udf_max_fuel = 1000000;

-- DETERMINISTIC: constant arguments should be folded to a literal
CREATE OR REPLACE FUNCTION ${FN_DET}
    LANGUAGE WASM FROM '${MODULE_NAME}' :: 'identity_msgpack_i32'
    ARGUMENTS (x Int32) RETURNS Int32
    ABI BUFFERED_V1
    DETERMINISTIC;

-- Non-deterministic (default): should NOT be constant-folded
CREATE OR REPLACE FUNCTION ${FN_NONDET}
    LANGUAGE WASM FROM '${MODULE_NAME}' :: 'identity_msgpack_i32'
    ARGUMENTS (x Int32) RETURNS Int32
    ABI BUFFERED_V1;

-- Correct result regardless of folding
SELECT ${FN_DET}(42);
SELECT ${FN_NONDET}(42);

-- isConstant returns 1 only when the expression was constant-folded at planning time
SELECT isConstant(${FN_DET}(42));    -- expected: 1
SELECT isConstant(${FN_NONDET}(42)); -- expected: 0

-- Prove actual folding via fuel: budget enough for 1 WASM call, not 1000.
-- If ${FN_DET}(1) is folded at planning time it burns fuel once and sum() sees 1000 constants.
-- If not folded the 1000 WASM calls would exhaust fuel and throw.
SET webassembly_udf_max_fuel = 10000;
SELECT sum(${FN_DET}(1)) FROM numbers(1000); -- expected: 1000

-- Non-deterministic function must evaluate correctly for every row (not folded to a single value)
SET webassembly_udf_max_fuel = 1000000;
SELECT countIf(${FN_NONDET}(number::Int32) != number::Int32) AS wrong FROM numbers(1000); -- expected: 0

DROP FUNCTION IF EXISTS ${FN_DET};
DROP FUNCTION IF EXISTS ${FN_NONDET};
DELETE FROM system.webassembly_modules WHERE name = '${MODULE_NAME}';
EOF
