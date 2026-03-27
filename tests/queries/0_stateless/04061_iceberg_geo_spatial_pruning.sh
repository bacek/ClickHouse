#!/usr/bin/env bash
# Tags: no-fasttest
#
# Test Iceberg manifest-level spatial pruning via covering.bbox flat Float64 columns.
#
# Schema:
#   id       Int32
#   pt       Tuple(Float64, Float64)      -- geometry column (lon, lat)
#   pt_bbox_xmin / ymin / xmax / ymax    -- per-row bounding box (flat Float64)
#
# Two inserts → two Parquet data files:
#   File 1: southern points  (lat ~31), bbox_ymin=29, bbox_ymax=33
#   File 2: northern points  (lat ~45), bbox_ymin=43, bbox_ymax=47
#
# A pointInPolygon query covering the southern polygon should prune file 2
# (query_ymax=33 < file2_ymin=43 → disjoint).

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
. "$CURDIR"/../shell_config.sh

TABLE="t_iceberg_geo_prune_${CLICKHOUSE_DATABASE}_${RANDOM}"
# Ask the server where its user_files directory is
SERVER_USER_FILES=$(${CLICKHOUSE_CLIENT} --query "SELECT value FROM system.server_settings WHERE name='user_files_path'" 2>/dev/null || echo "${USER_FILES_PATH}")
TABLE_PATH="${SERVER_USER_FILES%/}/${TABLE}/"

cleanup() {
    ${CLICKHOUSE_CLIENT} --query "DROP TABLE IF EXISTS ${TABLE}"
    rm -rf "${TABLE_PATH}"
}
trap cleanup EXIT

${CLICKHOUSE_CLIENT} --query "
    CREATE TABLE ${TABLE} (
        id           Int32,
        pt           Tuple(Float64, Float64),
        pt_bbox_xmin Float64,
        pt_bbox_ymin Float64,
        pt_bbox_xmax Float64,
        pt_bbox_ymax Float64
    ) ENGINE = IcebergLocal('${TABLE_PATH}', 'Parquet')
"

# Southern cluster: lat ~31, bbox [(-90, 29)..(-75, 33)]
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    INSERT INTO ${TABLE} VALUES
        (1, (-85.0, 31.0), -90.0, 29.0, -75.0, 33.0),
        (2, (-80.0, 31.5), -90.0, 29.0, -75.0, 33.0)
"

# Northern cluster: lat ~45, bbox [(-90, 43)..(-75, 47)]
${CLICKHOUSE_CLIENT} --allow_insert_into_iceberg=1 --query "
    INSERT INTO ${TABLE} VALUES
        (3, (-85.0, 45.0), -90.0, 43.0, -75.0, 47.0),
        (4, (-80.0, 45.5), -90.0, 43.0, -75.0, 47.0)
"

# --- Correctness: all 4 rows without filter ---
echo "=== all rows ==="
${CLICKHOUSE_CLIENT} --query "SELECT id FROM ${TABLE} ORDER BY id"

# --- Correctness: southern polygon filter returns only ids 1, 2 ---
echo "=== south filter ==="
${CLICKHOUSE_CLIENT} --query "
    SELECT id FROM ${TABLE}
    WHERE pointInPolygon(pt, [(-90., 28.), (-75., 28.), (-75., 33.), (-90., 33.), (-90., 28.)])
    ORDER BY id
"

# --- Correctness: northern polygon filter returns only ids 3, 4 ---
echo "=== north filter ==="
${CLICKHOUSE_CLIENT} --query "
    SELECT id FROM ${TABLE}
    WHERE pointInPolygon(pt, [(-90., 43.), (-75., 43.), (-75., 48.), (-90., 48.), (-90., 43.)])
    ORDER BY id
"

# --- Correctness: no-match polygon returns 0 rows ---
echo "=== no match ==="
${CLICKHOUSE_CLIENT} --query "
    SELECT count() FROM ${TABLE}
    WHERE pointInPolygon(pt, [(0., 0.), (1., 0.), (1., 1.), (0., 1.), (0., 0.)])
"

# --- Pruning: southern filter should prune 1 file (the northern one) ---
QUERY_ID="${TABLE}_prune_check"
${CLICKHOUSE_CLIENT} --query_id="${QUERY_ID}" \
    --query "
        SELECT id FROM ${TABLE}
        WHERE pointInPolygon(pt, [(-90., 28.), (-75., 28.), (-75., 33.), (-90., 33.), (-90., 28.)])
        ORDER BY id
        SETTINGS use_iceberg_partition_pruning=1
    " > /dev/null

${CLICKHOUSE_CLIENT} --query "SYSTEM FLUSH LOGS"

pruned=$(${CLICKHOUSE_CLIENT} --query "
    SELECT ProfileEvents['IcebergMinMaxIndexPrunedFiles']
    FROM system.query_log
    WHERE query_id = '${QUERY_ID}' AND type = 'QueryFinish'
")

if [ "${pruned}" -ge 1 ]; then
    echo "pruning=ok (${pruned} file(s) pruned)"
else
    echo "pruning=FAILED (expected >= 1 pruned file, got ${pruned})"
fi
