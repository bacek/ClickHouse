#!/usr/bin/env bash
# Tags: no-fasttest

# max_streams_for_file_reading caps the number of file-reading streams below the
# file count, so each stream reads multiple files sequentially via FilesIterator.

CUR_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CUR_DIR"/../shell_config.sh

${CLICKHOUSE_LOCAL} -q "insert into function file('${CLICKHOUSE_TMP}/t0.parquet') select * from numbers(10)"
for i in {1..7}; do cp "${CLICKHOUSE_TMP}/t0.parquet" "${CLICKHOUSE_TMP}/t${i}.parquet"; done

echo "=== no cap (default 0): 8 files, max_threads=8 -> 8 readers ==="
${CLICKHOUSE_LOCAL} -q "EXPLAIN PIPELINE SELECT count() FROM file('${CLICKHOUSE_TMP}/t{0..7}.parquet') SETTINGS max_threads=8" | grep -E 'File'

echo "=== cap=3: 8 files, max_threads=8 -> 3 readers ==="
${CLICKHOUSE_LOCAL} -q "EXPLAIN PIPELINE SELECT count() FROM file('${CLICKHOUSE_TMP}/t{0..7}.parquet') SETTINGS max_threads=8, max_streams_for_file_reading=3" | grep -E 'File'

echo "=== cap=5 but only 4 files -> 4 readers (capped by file count) ==="
${CLICKHOUSE_LOCAL} -q "EXPLAIN PIPELINE SELECT count() FROM file('${CLICKHOUSE_TMP}/t{0..3}.parquet') SETTINGS max_threads=8, max_streams_for_file_reading=5" | grep -E 'File'

echo "=== correctness: sum with cap=3 ==="
${CLICKHOUSE_LOCAL} -q "select sum(number) from file('${CLICKHOUSE_TMP}/t{0..7}.parquet') settings max_threads=8, max_streams_for_file_reading=3"

rm "${CLICKHOUSE_TMP}"/t{0..7}.parquet
