-- A constant Variant argument must stay constant when the Variant adaptor calls the nested
-- function. Expanding it to one row per input row copies the whole value per row, which for a
-- large geometry means gigabytes, and it also hides constness from functions that have a
-- cheaper constant path.

-- Constant Variant plus non-constant other argument.
SELECT sum(pointInPolygon(materialize((0.5, 0.5)), readWKB(wkb([[(0., 0.), (2., 0.), (2., 2.), (0., 2.), (0., 0.)]]::Polygon))))
FROM numbers(4);

-- Both arguments constant.
SELECT DISTINCT pointInPolygon((0.5, 0.5), readWKB(wkb([[(0., 0.), (2., 0.), (2., 2.), (0., 2.), (0., 0.)]]::Polygon)))
FROM numbers(4);

-- Point outside the polygon.
SELECT sum(pointInPolygon(materialize((9., 9.)), readWKB(wkb([[(0., 0.), (2., 0.), (2., 2.), (0., 2.), (0., 0.)]]::Polygon))))
FROM numbers(4);

-- NULL Variant.
SELECT DISTINCT pointInPolygon(materialize((0.5, 0.5)), CAST(NULL, 'Geometry')) FROM numbers(4);

-- Memory: a 320 KB constant polygon over 20000 rows. Per-row expansion needs several GB, so the
-- low limit below is what makes this a regression test rather than a correctness check.
WITH (SELECT wkb([arrayMap(i -> (cos((i / 20000.) * 2 * pi()) * 10., sin((i / 20000.) * 2 * pi()) * 10.), range(20001))]::Polygon)) AS boundary
SELECT sum(pointInPolygon(materialize((0.5, 0.5)), readWKB(boundary)))
FROM numbers(20000)
SETTINGS max_memory_usage = 500000000;
