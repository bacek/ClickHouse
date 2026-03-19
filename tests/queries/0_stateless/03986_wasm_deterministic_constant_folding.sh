#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan
# Test that WASM UDFs declared DETERMINISTIC are constant-folded when called
# with constant arguments, while non-deterministic UDFs are not.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} << 'EOF'
DROP FUNCTION IF EXISTS identity_det;
DROP FUNCTION IF EXISTS identity_nondet;
DELETE FROM system.webassembly_modules WHERE name = 'identity_cf_test';
EOF

cat "${CUR_DIR}/wasm/identity_int.wasm" | ${CLICKHOUSE_CLIENT} \
    --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'identity_cf_test', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} << 'EOF'
SET webassembly_udf_max_fuel = 1000000;

-- DETERMINISTIC: constant arguments should be folded to a literal
CREATE OR REPLACE FUNCTION identity_det
    LANGUAGE WASM FROM 'identity_cf_test' :: 'identity_msgpack_i32'
    ARGUMENTS (x Int32) RETURNS Int32
    ABI BUFFERED_V1
    DETERMINISTIC;

-- Non-deterministic (default): should NOT be constant-folded
CREATE OR REPLACE FUNCTION identity_nondet
    LANGUAGE WASM FROM 'identity_cf_test' :: 'identity_msgpack_i32'
    ARGUMENTS (x Int32) RETURNS Int32
    ABI BUFFERED_V1;

-- Correct result regardless of folding
SELECT identity_det(42);
SELECT identity_nondet(42);

-- isConstant returns 1 only when the expression was constant-folded
SELECT isConstant(identity_det(42));    -- expected: 1
SELECT isConstant(identity_nondet(42)); -- expected: 0

-- Folded function should produce same result across many rows without being re-evaluated per row
SELECT countIf(identity_det(7) != 7) AS wrong FROM numbers(1000); -- expected: 0

DROP FUNCTION IF EXISTS identity_det;
DROP FUNCTION IF EXISTS identity_nondet;
DELETE FROM system.webassembly_modules WHERE name = 'identity_cf_test';
EOF
