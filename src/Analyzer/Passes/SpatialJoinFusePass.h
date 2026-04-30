#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Detect the chained-spatial-join pattern used in Q11-style queries:
  *
  *   FROM zone pickup_zone
  *   JOIN trip t           ON st_within(t.pickup, pickup_zone.boundary)
  *   JOIN zone dropoff_zone ON st_within(t.dropoff, dropoff_zone.boundary)
  *
  * which produces this JOIN tree:
  *
  *   outer_join(Inner):
  *     left = inner_join(Inner):
  *       left  = zone_pz
  *       right = trip
  *       ON    = spatial1(trip.pickup, zone_pz.boundary)
  *     right = zone_dz
  *     ON    = spatial2(trip.dropoff, zone_dz.boundary)
  *
  * When zone_pz and zone_dz scan the same physical data (same table function
  * + same arguments, or same table UUID) the two R-trees can be fused into a
  * single pass:  build one R-tree on zone, probe it twice per trip row.
  *
  * The pass annotates (does NOT structurally rewrite) the tree:
  *   - inner_join.is_fused_child = true    → planner returns trip plan directly
  *   - outer_join.fused_spatial_info = {…} → planner builds SpatialRTreeDoubleJoin
  */
class SpatialJoinFusePass final : public IQueryTreePass
{
public:
    String getName() override { return "SpatialJoinFusePass"; }

    String getDescription() override
    {
        return "Fuse two chained spatial joins on the same zone table into a single R-tree double-probe join";
    }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
