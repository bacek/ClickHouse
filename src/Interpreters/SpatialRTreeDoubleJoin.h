#pragma once

#include <Interpreters/SpatialRTreeJoin.h>

namespace DB
{

/// Fused double-probe spatial join: builds ONE R-tree on the right-side geometry
/// (zone table, ~263 entries) and probes it TWICE per left row (trip):
///   - First probe  with left_geom_col_1 (pickup) → matched pickup-zone rows
///   - Second probe with left_geom_col_2 (dropoff) → matched dropoff-zone rows
///
/// Emits the cross-product of matching (pz, dz) pairs per trip row along with
/// the trip columns, the normal right-side columns (__table3.z_zonekey etc.), and
/// the first-probe output columns (__table1.z_zonekey etc.).
///
/// This eliminates the 6M-row intermediate block between two sequential
/// SpatialRTreeJoin instances in chained-spatial-join queries.
///
/// Inherits only R-tree infrastructure from SpatialRTreeJoin (wkbBBox,
/// identifyGeomColumns, build/postbuild logic).
class SpatialRTreeDoubleJoin : public SpatialRTreeJoin
{
public:
    using Base = SpatialRTreeJoin;

    /// fused_first_probe_left_col  — left geometry column for the first probe
    /// fused_first_probe_output_cols — mapping (right_col, first_probe_col, type)
    SpatialRTreeDoubleJoin(
        std::shared_ptr<TableJoin> table_join_,
        SharedHeader right_header_);

    std::string getName() const override { return "SpatialRTreeDoubleJoin"; }

    JoinResultPtr joinBlock(Block left_block) override;

private:
    /// Left geometry column for the first probe (pickup).
    String left_geom_col_1;
    /// Left geometry column for the second probe (dropoff) — same as base class left_geom_col.
    String left_geom_col_2;

    struct FirstProbeOutputCol { String right_col; String first_probe_col; DataTypePtr type; };
    std::vector<FirstProbeOutputCol> first_probe_output_cols;
};

}
