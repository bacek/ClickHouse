#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF

DROP FUNCTION IF EXISTS wasm_concat_str_int;
DELETE FROM system.webassembly_modules WHERE name = 'mixed_types';

EOF

cat ${CUR_DIR}/wasm/mixed_types.wasm | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'mixed_types', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF

CREATE FUNCTION wasm_concat_str_int
    LANGUAGE WASM ABI BUFFERED_V1 FROM 'mixed_types' :: 'concat_str_int'
    ARGUMENTS (n Int32, s String) RETURNS String
    SETTINGS serialization_format = 'TSV';

-- Basic cases
SELECT wasm_concat_str_int(42, 'hello');
SELECT wasm_concat_str_int(-1, 'world');
SELECT wasm_concat_str_int(0, 'zero');

-- Multiple rows
SELECT wasm_concat_str_int(number, 'row') FROM numbers(4);

-- Boundary values
SELECT wasm_concat_str_int(2147483647, 'max');
SELECT wasm_concat_str_int(-2147483648, 'min');

DROP FUNCTION wasm_concat_str_int;
DELETE FROM system.webassembly_modules WHERE name = 'mixed_types';

EOF
