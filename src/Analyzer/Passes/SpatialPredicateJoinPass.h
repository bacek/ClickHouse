#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Rewrite cross-join + WHERE spatial predicate to INNER JOIN ON spatial predicate,
  * enabling SpatialRTreeJoin to activate.
  *
  * Input:  FROM a, b WHERE st_within(a.col, b.col)
  * Output: FROM a INNER JOIN b ON st_within(a.col, b.col)
  *
  * Only applies when:
  *   - The join tree is a 2-table CrossJoinNode
  *   - The WHERE clause contains exactly one spatial predicate (isSpatialPredicate())
  *     referencing columns from both tables
  *   - All spatial predicate args that are columns can be attributed to one of the two tables
  *
  * Non-spatial WHERE conditions are left in place.
  */
class SpatialPredicateJoinPass final : public IQueryTreePass
{
public:
    String getName() override { return "SpatialPredicateJoinPass"; }

    String getDescription() override
    {
        return "Rewrite CrossJoin + WHERE spatial predicate to INNER JOIN ON spatial predicate";
    }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
