#!/usr/bin/env bash
# Tags: no-fasttest
#
# Test that Parquet column-index page pruning and bloom-filter row-group pruning
# are reflected in ProfileEvents:
#   ParquetPrunedPages              - pages skipped by column index
#   ParquetReadPages                - pages actually fetched
#   ParquetPrunedRowsByColumnIndex  - rows in skipped pages
#   ParquetPrunedRowGroupsByBloomFilter - row groups rejected by bloom filter

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
CLICKHOUSE_BINARY=${CLICKHOUSE_BINARY:="$(dirname "$(dirname "$(dirname "$CURDIR")")")/build/programs/clickhouse"}
. "$CURDIR"/../shell_config.sh

# Use a local temp dir so we can write files regardless of USER_FILES_PATH.
# clickhouse-local accepts absolute paths in file() without server path restrictions.
WORKING_DIR=$(mktemp -d)
trap 'rm -rf "${WORKING_DIR}"' EXIT

# ─── Column index test ────────────────────────────────────────────────────────
# One row group, 10 000 rows, small page size → many pages.
# A filter id < 500 covers ~5% of rows, so most pages should be pruned.

COL_IDX_FILE="${WORKING_DIR}/col_idx.parquet"
$CLICKHOUSE_LOCAL -q "
    SELECT number AS id, number * 3 AS val
    FROM numbers(10000)
    INTO OUTFILE '${COL_IDX_FILE}' FORMAT Parquet
    SETTINGS
        output_format_parquet_use_custom_encoder = 1,
        output_format_parquet_write_page_index   = 1,
        output_format_parquet_row_group_size     = 100000,
        output_format_parquet_data_page_size     = 512
"

# Helper: print "present" if the event name appears in --print-profile-events
# output, "absent" otherwise.
check_events()
{
    local output="$1"
    shift
    for event in "$@"; do
        if echo "$output" | grep -qF "$event"; then
            echo "${event}: present"
        else
            echo "${event}: absent"
        fi
    done
}

echo "=== column index pruning enabled ==="
output=$(
    $CLICKHOUSE_LOCAL --print-profile-events -q "
        SELECT count() FROM file('${COL_IDX_FILE}', Parquet) WHERE id < 500
        SETTINGS
            input_format_parquet_filter_push_down      = 1,
            input_format_parquet_page_filter_push_down = 1
    " 2>&1
)
check_events "$output" \
    ParquetReadPages \
    ParquetPrunedPages \
    ParquetPrunedRowsByColumnIndex

echo "=== column index pruning disabled ==="
output=$(
    $CLICKHOUSE_LOCAL --print-profile-events -q "
        SELECT count() FROM file('${COL_IDX_FILE}', Parquet) WHERE id < 500
        SETTINGS
            input_format_parquet_filter_push_down      = 0,
            input_format_parquet_page_filter_push_down = 0
    " 2>&1
)
check_events "$output" \
    ParquetPrunedPages \
    ParquetPrunedRowsByColumnIndex

# ─── Bloom filter test ────────────────────────────────────────────────────────
# 10 row groups × 1000 rows, written with bloom filter on `id`.
# Rows: group k contains ids [k*1000 .. (k+1)*1000 - 1].
# Query WHERE id = 500 (only in group 0).
# With min/max push-down disabled and bloom-filter push-down enabled,
# groups 1-9 are pruned by bloom filter, not by statistics.

BF_FILE="${WORKING_DIR}/bloom.parquet"
$CLICKHOUSE_LOCAL -q "
    SELECT number AS id
    FROM numbers(10000)
    INTO OUTFILE '${BF_FILE}' FORMAT Parquet
    SETTINGS
        output_format_parquet_use_custom_encoder    = 1,
        output_format_parquet_write_bloom_filter    = 1,
        output_format_parquet_row_group_size        = 1000
"

echo "=== bloom filter pruning enabled ==="
output=$(
    $CLICKHOUSE_LOCAL --print-profile-events -q "
        SELECT count() FROM file('${BF_FILE}', Parquet) WHERE id = 500
        SETTINGS
            input_format_parquet_filter_push_down        = 0,
            input_format_parquet_bloom_filter_push_down  = 1
    " 2>&1
)
check_events "$output" \
    ParquetPrunedRowGroupsByBloomFilter

echo "=== bloom filter pruning disabled ==="
output=$(
    $CLICKHOUSE_LOCAL --print-profile-events -q "
        SELECT count() FROM file('${BF_FILE}', Parquet) WHERE id = 500
        SETTINGS
            input_format_parquet_filter_push_down        = 0,
            input_format_parquet_bloom_filter_push_down  = 0
    " 2>&1
)
check_events "$output" \
    ParquetPrunedRowGroupsByBloomFilter
