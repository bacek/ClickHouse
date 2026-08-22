-- Planning decides "is this a spatial join?" in two places: JoinStepLogical, which
-- commits to an empty-key clause and drops the cross-join fallback, and
-- chooseJoinAlgorithm, which asks SpatialRTreeJoin whether it can execute the
-- predicate. If those two disagree the join is left with no algorithm at all.
-- Every case below is a predicate shape where they used to disagree.

DROP TABLE IF EXISTS rtree_join_pts;
DROP TABLE IF EXISTS rtree_join_polys;

-- Same geometries in both representations: native geo types and WKB bytes.
CREATE TABLE rtree_join_pts (id Int32, pt Point, wkb String) ENGINE = Memory;
CREATE TABLE rtree_join_polys (id Int32, poly Polygon, wkb Nullable(String)) ENGINE = Memory;

INSERT INTO rtree_join_pts VALUES
    (1, (1., 1.),     unhex('0101000000000000000000F03F000000000000F03F')),
    (2, (12., 12.),   unhex('010100000000000000000028400000000000002840')),
    (3, (100., 100.), unhex('010100000000000000000059400000000000005940'));

INSERT INTO rtree_join_polys VALUES
    (10, [[(0., 0.), (4., 0.), (4., 4.), (0., 4.)]],
         unhex('010300000001000000050000000000000000000000000000000000000000000000000010400000000000000000000000000000104000000000000010400000000000000000000000000000104000000000000000000000000000000000')),
    (20, [[(10., 10.), (14., 10.), (14., 14.), (10., 14.)]],
         unhex('01030000000100000005000000000000000000244000000000000024400000000000002C4000000000000024400000000000002C400000000000002C4000000000000024400000000000002C4000000000000024400000000000002440')),
    -- contains none of the points, so LEFT JOIN has an unmatched left row
    (30, [[(50., 50.), (54., 50.), (54., 54.), (50., 54.)]],
         unhex('01030000000100000005000000000000000000494000000000000049400000000000004B4000000000000049400000000000004B400000000000004B4000000000000049400000000000004B4000000000000049400000000000004940'));

SELECT '-- WKB args behind readWKB*/assumeNotNull: SpatialRTreeJoin --';
SELECT trim(explain) FROM (
    EXPLAIN SELECT p.id FROM rtree_join_polys p JOIN rtree_join_pts t
    ON pointInPolygon(readWKBPoint(t.wkb), readWKB(assumeNotNull(p.wkb)))
) WHERE explain LIKE '%Algorithm%';

SELECT '-- and it returns the right pairs --';
SELECT t.id, p.id FROM rtree_join_polys p JOIN rtree_join_pts t
ON pointInPolygon(readWKBPoint(t.wkb), readWKB(assumeNotNull(p.wkb)))
ORDER BY t.id, p.id;

SELECT '-- identical to the same predicate as a post-join filter --';
SELECT t.id, p.id FROM rtree_join_polys p CROSS JOIN rtree_join_pts t
WHERE pointInPolygon(readWKBPoint(t.wkb), readWKB(assumeNotNull(p.wkb)))
ORDER BY t.id, p.id;

-- The bbox scanner reads WKB bytes through IColumn::getDataAt, which native geo
-- columns do not implement. These must fall back rather than reach the scanner.
SELECT '-- native geo columns: no R-tree, no error --';
SELECT trim(explain) FROM (
    EXPLAIN SELECT p.id FROM rtree_join_polys p JOIN rtree_join_pts t
    ON pointInPolygon(t.pt, p.poly)
) WHERE explain LIKE '%Algorithm%';

SELECT '-- and the fallback still answers correctly --';
SELECT t.id, p.id FROM rtree_join_polys p JOIN rtree_join_pts t
ON pointInPolygon(t.pt, p.poly)
ORDER BY t.id, p.id;

-- A computed argument is not a column reference: the bbox taken from any input
-- underneath it would not bound what the predicate actually sees.
SELECT '-- computed argument: no R-tree, no error --';
SELECT trim(explain) FROM (
    EXPLAIN SELECT p.id FROM rtree_join_polys p JOIN rtree_join_pts t
    ON pointInPolygon((t.pt.1 + 20., t.pt.2 + 20.), p.poly)
) WHERE explain LIKE '%Algorithm%';

SELECT '-- shifting by -88 moves point 3 (100 100) into the second polygon --';
SELECT t.id, p.id FROM rtree_join_polys p JOIN rtree_join_pts t
ON pointInPolygon((t.pt.1 - 88., t.pt.2 - 88.), p.poly)
ORDER BY t.id, p.id;

SELECT '-- LEFT JOIN over WKB args keeps unmatched left rows --';
SELECT p.id, t.id FROM rtree_join_polys p LEFT JOIN rtree_join_pts t
ON pointInPolygon(readWKBPoint(t.wkb), readWKB(assumeNotNull(p.wkb)))
ORDER BY p.id, t.id;

DROP TABLE rtree_join_pts;
DROP TABLE rtree_join_polys;
