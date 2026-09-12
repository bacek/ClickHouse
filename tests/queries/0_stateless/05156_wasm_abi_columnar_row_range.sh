#!/usr/bin/env bash
# Tags: no-fasttest, no-parallel, no-msan

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

# A split COLUMNAR_V1 call serializes each batch straight out of the original column at a row
# offset instead of cutting a copy of it first. Every batch after the first therefore starts at a
# non-zero offset, and a String column's offsets are cumulative, so a range that is not rebased to
# zero is exactly where such a writer goes wrong: it would hand the guest the bytes of row 0
# onwards while claiming the row count of the batch. Empty strings inside the range and a const
# argument (which carries its single stored row whatever the range asks for) are the two shapes
# that make the offset bookkeeping visible.

${CLICKHOUSE_CLIENT} << 'EOF'
DROP FUNCTION IF EXISTS byte_sum_offset_rr;
DROP FUNCTION IF EXISTS bytes_reverse_rr;
DELETE FROM system.webassembly_modules WHERE name = 'columnar_abi_row_range';
EOF

cat "${CUR_DIR}/wasm/columnar_abi.wasm" \
  | ${CLICKHOUSE_CLIENT} --query \
    "INSERT INTO system.webassembly_modules (name, code) SELECT 'columnar_abi_row_range', code FROM input('code String') FORMAT RawBlob"

${CLICKHOUSE_CLIENT} --allow_experimental_analyzer=1 << 'EOF'

CREATE OR REPLACE FUNCTION byte_sum_offset_rr
    LANGUAGE WASM ABI COLUMNAR_V1 FROM 'columnar_abi_row_range' :: 'add_offset_col'
    ARGUMENTS (s String, n UInt64) RETURNS UInt64
    DETERMINISTIC;

CREATE OR REPLACE FUNCTION bytes_reverse_rr
    LANGUAGE WASM ABI COLUMNAR_V1 FROM 'columnar_abi_row_range' :: 'bytes_reverse_col'
    ARGUMENTS (s String) RETURNS Nullable(String)
    DETERMINISTIC;

DROP TABLE IF EXISTS wasm_row_range;
CREATE TABLE wasm_row_range (id UInt64, s String, n UInt64) ENGINE = Memory;
INSERT INTO wasm_row_range VALUES (1, 'a', 10), (2, '', 20), (3, 'bc', 30), (4, 'defg', 40), (5, 'h', 50), (6, 'ijklm', 60), (7, '', 70), (8, 'nop', 80);

-- Unsplit: one batch at offset 0, the reference every split below must reproduce.
SELECT id, byte_sum_offset_rr(s, n) FROM wasm_row_range ORDER BY id;

-- Batches of at most 2 rows: offsets 0, 2, 4, 6.
SELECT id, byte_sum_offset_rr(s, n) FROM wasm_row_range ORDER BY id
SETTINGS webassembly_udf_max_input_block_size = 2;

-- Batches of at most 3 rows, so the last batch is shorter than the rest.
SELECT id, byte_sum_offset_rr(s, n) FROM wasm_row_range ORDER BY id
SETTINGS webassembly_udf_max_input_block_size = 3;

-- One row per batch: every batch but the first starts at a non-zero offset.
SELECT id, byte_sum_offset_rr(s, n) FROM wasm_row_range ORDER BY id
SETTINGS webassembly_udf_max_input_block_size = 1;

-- A const argument stays at its single stored row across every batch.
SELECT id, byte_sum_offset_rr(s, toUInt64(1)) FROM wasm_row_range ORDER BY id
SETTINGS webassembly_udf_max_input_block_size = 2;

-- A String result read back out of a batch that started at a non-zero offset.
SELECT id, bytes_reverse_rr(s) FROM wasm_row_range ORDER BY id
SETTINGS webassembly_udf_max_input_block_size = 3;

DROP TABLE wasm_row_range;
DROP FUNCTION IF EXISTS byte_sum_offset_rr;
DROP FUNCTION IF EXISTS bytes_reverse_rr;
DELETE FROM system.webassembly_modules WHERE name = 'columnar_abi_row_range';
EOF
