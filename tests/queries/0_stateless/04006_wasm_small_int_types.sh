#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF

DROP FUNCTION IF EXISTS wasm_identity;
DROP FUNCTION IF EXISTS wasm_add;
DELETE FROM system.webassembly_modules WHERE name = 'small_int';

EOF

cat ${CUR_DIR}/wasm/small_int.wasm | ${CLICKHOUSE_CLIENT} --query "INSERT INTO system.webassembly_modules (name, code) SELECT 'small_int', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << EOF

CREATE FUNCTION wasm_identity LANGUAGE WASM ABI ROW_DIRECT FROM 'small_int' :: 'identity' ARGUMENTS (Int32) RETURNS Int32;
CREATE FUNCTION wasm_add      LANGUAGE WASM ABI ROW_DIRECT FROM 'small_int' :: 'add'      ARGUMENTS (Int32, Int32) RETURNS Int32;

-- Int8: boundary values passed without explicit casting
SELECT wasm_identity(toInt8(-128)), wasm_identity(toInt8(0)), wasm_identity(toInt8(127));

-- UInt8: boundary values
SELECT wasm_identity(toUInt8(0)), wasm_identity(toUInt8(128)), wasm_identity(toUInt8(255));

-- Int16: boundary values
SELECT wasm_identity(toInt16(-32768)), wasm_identity(toInt16(0)), wasm_identity(toInt16(32767));

-- UInt16: boundary values
SELECT wasm_identity(toUInt16(0)), wasm_identity(toUInt16(1000)), wasm_identity(toUInt16(65535));

-- Arithmetic: result fits in Int32
SELECT wasm_add(10, 20), wasm_add(100, 55);

-- Overflow wraps at the narrowing step in ClickHouse when the caller uses Int8
SELECT toInt8(wasm_add(100, 100));

DROP FUNCTION wasm_identity;
DROP FUNCTION wasm_add;
DELETE FROM system.webassembly_modules WHERE name = 'small_int';

EOF
