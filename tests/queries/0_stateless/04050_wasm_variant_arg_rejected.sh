#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan
# Regression: WASM UDFs must not silently accept Variant arguments.
# Before the fix, passing Variant(UInt32, String) to a function expecting String
# triggered FunctionBaseVariantAdaptor which returned Nullable(Nothing) when all
# alternatives failed, causing WHERE clauses to return 0 rows with no error.
# After the fix, useDefaultImplementationForVariant() returns false for WASM
# functions, so type mismatches are caught at planning time.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF

DROP FUNCTION IF EXISTS wasm_test_variant_str;
DELETE FROM system.webassembly_modules WHERE name = 'buffered_abi_variant_test';

EOF

cat ${CUR_DIR}/wasm/buffered_abi.wasm | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'buffered_abi_variant_test', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF

CREATE FUNCTION wasm_test_variant_str
    LANGUAGE WASM ABI BUFFERED_V1 FROM 'buffered_abi_variant_test' :: 'digest_newline_rows'
    ARGUMENTS (s String, n UInt64, arr Array(Int64)) RETURNS UInt64
    SETTINGS serialization_format = 'CSV';

-- Variant(UInt32, String) passed where String is expected must error at planning time.
SELECT wasm_test_variant_str(v, 1::UInt64, []::Array(Int64))
FROM (SELECT CAST(42::UInt32 AS Variant(UInt32, String)) AS v); -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }

-- A plain String column must still work correctly.
SELECT wasm_test_variant_str('hello', 1::UInt64, []::Array(Int64)) > 0;

DROP FUNCTION wasm_test_variant_str;
DELETE FROM system.webassembly_modules WHERE name = 'buffered_abi_variant_test';

EOF
