#!/usr/bin/env bash
# Tags: no-fasttest
#
# Test GeoParquet per-row bbox prefilter injection.
# When a spatial predicate is pushed down and the geometry column has GeoParquet
# covering.bbox metadata, a per-row bbox intersection test is injected as a
# prewhere step before the (expensive) spatial predicate runs.
#
# The test file has 20 rows in a single row group (so row-group pruning cannot help):
#   Rows  1-10: Points in South Texas (~-97 lon, ~31 lat) — inside query polygon bbox
#   Rows 11-20: Points in the Atlantic (~-20 lon, ~40 lat) — outside query polygon bbox
#
# With spatial_filter_push_down=1, only rows 1-10 pass the bbox prefilter, so the
# spatial function (pointInPolygon) processes only those rows.
# Without push-down, pointInPolygon evaluates all 20 rows.
# Either way, the result is the same 10 rows — this tests both correctness and effect.

CURDIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=../shell_config.sh
CLICKHOUSE_BINARY=${CLICKHOUSE_BINARY:="$(dirname "$(dirname "$(dirname "$CURDIR")")")/build/programs/clickhouse"}
. "$CURDIR"/../shell_config.sh

FILE="$CURDIR/data_parquet/04068_geoparquet_bbox_prefilter.parquet"

# Baseline: all 20 rows without filter
echo "=== all rows ==="
$CLICKHOUSE_LOCAL -q "SELECT id FROM file('$FILE', Parquet) ORDER BY id"

# With push-down enabled: rows 1-10 (South Texas); Atlantic rows skipped by bbox prefilter
echo "=== south texas (push-down enabled) ==="
$CLICKHOUSE_LOCAL -q "
SELECT id FROM file('$FILE', Parquet)
WHERE pointInPolygon(geometry, [(-99., 30.), (-95., 30.), (-95., 33.), (-99., 33.), (-99., 30.)])
ORDER BY id"

# With push-down disabled: same result, but all 20 rows evaluated by pointInPolygon
echo "=== south texas (push-down disabled) ==="
$CLICKHOUSE_LOCAL -q "
SELECT id FROM file('$FILE', Parquet)
WHERE pointInPolygon(geometry, [(-99., 30.), (-95., 30.), (-95., 33.), (-99., 33.), (-99., 30.)])
ORDER BY id
SETTINGS input_format_parquet_spatial_filter_push_down=0"

# No rows should be returned for a query bbox fully outside the data
echo "=== no match (query outside all data) ==="
$CLICKHOUSE_LOCAL -q "
SELECT count() FROM file('$FILE', Parquet)
WHERE pointInPolygon(geometry, [(0., 0.), (1., 0.), (1., 1.), (0., 1.), (0., 0.)])"

# Verify bbox prefilter is active: with push-down, the prewhere expression evaluates more
# steps total (bbox step on 20 rows + geometry step on 10 rows = 30), whereas without
# push-down there is only the geometry step on all 20 rows.
echo "=== ParquetRowsFilterExpression (push-down enabled) ==="
$CLICKHOUSE_LOCAL --print-profile-events -q "
SELECT id FROM file('$FILE', Parquet)
WHERE pointInPolygon(geometry, [(-99., 30.), (-95., 30.), (-95., 33.), (-99., 33.), (-99., 30.)])
ORDER BY id" 2>&1 | grep 'ParquetRowsFilterExpression' | sed 's/^.*] //'

echo "=== ParquetRowsFilterExpression (push-down disabled) ==="
$CLICKHOUSE_LOCAL --print-profile-events -q "
SELECT id FROM file('$FILE', Parquet)
WHERE pointInPolygon(geometry, [(-99., 30.), (-95., 30.), (-95., 33.), (-99., 33.), (-99., 30.)])
ORDER BY id
SETTINGS input_format_parquet_spatial_filter_push_down=0" 2>&1 | grep 'ParquetRowsFilterExpression' | sed 's/^.*] //'
