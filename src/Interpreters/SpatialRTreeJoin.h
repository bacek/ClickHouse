#pragma once

#include <Core/Block.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/IJoin.h>
#include <Interpreters/TableJoin.h>
#include <mutex>

/// Warning in boost::geometry during template strategy substitution.
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-parameter"
#include <boost/geometry.hpp>
#pragma clang diagnostic pop

#include <boost/geometry/geometries/box.hpp>
#include <boost/geometry/geometries/point_xy.hpp>
#include <boost/geometry/index/rtree.hpp>

namespace DB
{

/// IJoin implementation that accelerates spatial predicate joins using an R-tree
/// index on the right-side geometry (WKB) column.
///
/// Activated by chooseJoinAlgorithm() when all of the following hold:
///   - INNER JOIN with a single ON clause and no equijoin keys
///   - The entire ON condition is a WASM spatial predicate (isSpatialPredicate() == true)
///   - The predicate takes exactly 2 arguments (one from each side)
///
/// Build phase  (addBlockToJoin): scans raw WKB bytes from the right geometry column,
///   extracts axis-aligned bounding boxes without constructing geometry objects, and
///   inserts them into a bgi::rtree.
///
/// Probe phase  (joinBlock): for each left row, queries the rtree for bbox candidates,
///   assembles a candidate block of left×right pairs, then evaluates the mixed join
///   expression (the original WASM predicate) for an exact spatial check. Only
///   matching rows are emitted. No WASM call is made for pairs eliminated by bbox.
///
/// Limitations:
///   - INNER JOIN only (getNonJoinedBlocks returns nullptr)
///   - Both predicate arguments must be direct column references (no function wrapping).
///     Expressions like st_expand_col(b, c) as an argument fall back to hash join.
///   - WKB geometry types 1-7 (Point/LineString/Polygon/Multi*/GeometryCollection)
class SpatialRTreeJoin : public IJoin
{
public:
    SpatialRTreeJoin(std::shared_ptr<TableJoin> table_join_, SharedHeader right_header_, double bbox_expand_ = 0.0);

    std::string getName() const override { return "SpatialRTreeJoin"; }
    const TableJoin & getTableJoin() const override { return *table_join; }

    /// Parallel join is safe for all kinds.  For RIGHT JOIN the matched bitmap uses
    /// byte-per-row (vector<char>) with relaxed-atomic writes so concurrent probes
    /// from multiple threads can safely mark the same right row as matched.
    bool supportParallelJoin() const override { return true; }

    void initialize(const Block & left_sample_block) override;
    bool addBlockToJoin(const Block & block, bool check_limits) override;
    void checkTypesOfKeys(const Block & /*block*/) const override {}

    /// Bulk-load the R-tree from pending_entries once all right-side data is ingested.
    /// Called by FillingRightJoinSideTransform (single thread) before any joinBlock().
    bool hasPostBuildPhase() const override { return true; }
    void runPostBuildPhase() override;

    JoinResultPtr joinBlock(Block block) override;

    size_t getTotalRowCount() const override { return total_right_rows; }
    size_t getTotalByteCount() const override { return total_right_bytes; }
    bool alwaysReturnsEmptySet() const override { return total_right_rows == 0; }

    /// Thread-safe overrides required because supportParallelJoin() = true causes
    /// multiple FillingRightJoinSideTransform instances to call setTotals() concurrently.
    void setTotals(const Block & block) override;
    const Block & getTotals() const override;

    IBlocksStreamPtr getNonJoinedBlocks(
        const Block & left_sample_block,
        const Block & result_sample_block,
        UInt64 max_block_size) const override;

    /// Try to identify left/right geometry column names from the spatial predicate
    /// ActionsDAG. Returns false if detection fails (e.g. either argument is wrapped
    /// in a function rather than being a direct column reference).
    /// expand_arg_index: 0-based index of the constant distance argument (-1 = none).
    /// On success, out_bbox_expand is set to the constant value at that index (0.0 if -1).
    static bool identifyGeomColumns(
        const ExpressionActionsPtr & expr,
        const Block & right_hdr,
        String & out_left_col,
        String & out_right_col,
        int expand_arg_index,
        double & out_bbox_expand);

protected:
    using BGPoint = boost::geometry::model::d2::point_xy<double>;
    using BGBox   = boost::geometry::model::box<BGPoint>;

    struct RightPos
    {
        UInt32 block_idx;
        UInt32 row_idx;
    };

    using RTree = boost::geometry::index::rtree<
        std::pair<BGBox, RightPos>,
        boost::geometry::index::quadratic<16>>;

    /// Scan raw WKB/EWKB bytes and return the bounding box without constructing
    /// any geometry object. Handles 2D and 3D (Z/M) coordinates, EWKB SRID flag,
    /// ISO WKB 3D type codes (1001-3007), and recursive multi-geometries.
    static BGBox wkbBBox(std::string_view wkb);

    const Block & getRightBlock(size_t idx) const { return right_blocks[idx]; }
    size_t rightBlocksCount() const { return right_blocks.size(); }
    SharedHeader getRightHeader() const { return right_header; }
    const std::vector<RTree> & getSubTrees() const { return sub_trees; }

private:
    std::shared_ptr<TableJoin> table_join;
    SharedHeader right_header;

    String left_geom_col;
    String right_geom_col;
    String filter_col_name;        /// output column name of the mixed join expression
    double bbox_expand = 0.0;      /// for distance predicates (e.g. st_dwithin): expand query bbox by this amount

    /// Optional cheap non-spatial pre-filter (e.g. `b1.id < b2.id` from the ON clause).
    /// Evaluated BEFORE the spatial predicate to prune candidates without geometry work.
    ExpressionActionsPtr pre_filter_expr;
    String              pre_filter_col_name;

    /// Serialises concurrent addBlockToJoin() calls (parallel build via supportParallelJoin).
    /// joinBlock() is read-only after build and needs no lock.
    std::mutex build_mutex;

    /// Protects setTotals/getTotals against concurrent calls from multiple
    /// FillingRightJoinSideTransform instances (one per parallel build thread).
    mutable std::mutex totals_mutex;

    std::vector<Block> right_blocks;

    /// Entries buffered during build phase; consumed by runPostBuildPhase().
    std::vector<std::pair<BGBox, RightPos>> pending_entries;

    /// Sub-trees built in parallel by runPostBuildPhase() — one per CPU core.
    /// Each covers a contiguous slice of pending_entries (packing/STR algorithm).
    /// joinBlock() queries all sub-trees and merges candidates; query overhead vs a
    /// single large tree is O(K × log(N/K)) instead of O(log N), acceptable because
    /// empty-result queries dominate and their cost scales with tree depth not output.
    std::vector<RTree> sub_trees;

    size_t total_right_rows = 0;
    size_t total_right_bytes = 0;

    Block left_sample; /// stored by initialize()

    /// RIGHT JOIN support: tracks which right rows have been matched during probe.
    /// Indexed as right_matched_per_block[block_idx][row_idx]. Populated lazily in
    /// runPostBuildPhase() (one inner vector per right block, all 0).
    /// Byte-per-row (char) allows relaxed-atomic writes from concurrent joinBlock() threads:
    /// multiple threads may race to write 1 to the same byte, which is safe because
    /// (a) all writers write the same value and (b) getNonJoinedBlocks() is called only
    /// after all joinBlock() invocations have completed (pipeline synchronisation barrier).
    std::vector<std::vector<char>> right_matched_per_block;
};

}
