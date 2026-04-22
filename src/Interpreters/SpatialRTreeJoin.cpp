#include <Interpreters/SpatialRTreeJoin.h>

#include <bit>
#include <cstring>
#include <limits>
#include <thread>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Common/Exception.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>

#include <boost/geometry/algorithms/intersects.hpp>

namespace DB
{

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
}

// ── WKB bounding-box scanner ─────────────────────────────────────────────────
//
// We scan raw WKB/EWKB bytes to extract the bounding box without constructing
// any geometry object.  The wire layout (per PostGIS EWKB spec):
//
//   byte 0      : endian flag  (0x00 = big-endian, 0x01 = little-endian)
//   bytes 1-4   : type word (uint32); high bits are EWKB flags:
//                   0x80000000  Z dimension present
//                   0x40000000  M dimension present
//                   0x20000000  SRID field follows type word
//                   0x0FFFFFFF  (mask) base geometry type (1-7)
//   bytes 5-8   : SRID (int32, same byte order) — present only when SRID flag set
//   bytes 9+    : geometry payload
//
// ISO WKB 3D encoding (alternative to EWKB flags):
//   type 1001-1007 : Z only      (base = type - 1000)
//   type 2001-2007 : Z + M       (base = type - 2000)
//   type 3001-3007 : M only      (base = type - 3000)

SpatialRTreeJoin::BGBox SpatialRTreeJoin::wkbBBox(std::string_view wkb)
{
    double xmin = std::numeric_limits<double>::infinity();
    double ymin = std::numeric_limits<double>::infinity();
    double xmax = -std::numeric_limits<double>::infinity();
    double ymax = -std::numeric_limits<double>::infinity();

    auto update_xy = [&](double x, double y)
    {
        xmin = std::min(xmin, x);
        xmax = std::max(xmax, x);
        ymin = std::min(ymin, y);
        ymax = std::max(ymax, y);
    };

    ReadBufferFromMemory buf(wkb.data(), wkb.size());

    /// Recursive geometry scanner; called once per WKB header (including
    /// sub-geometries inside MultiPolygon / GeometryCollection).
    std::function<void()> scan = [&]()
    {
        UInt8 byte_order_flag;
        readBinary(byte_order_flag, buf);
        const std::endian endian = (byte_order_flag == 1) ? std::endian::little : std::endian::big;

        UInt32 type_word;
        readBinaryEndian(type_word, buf, endian);

        bool has_z    = (type_word & 0x80000000u) != 0;
        bool has_m    = (type_word & 0x40000000u) != 0;
        const bool has_srid = (type_word & 0x20000000u) != 0;
        UInt32 base_type = type_word & 0x0FFFFFFFu;

        /// ISO WKB 3D type codes
        if (base_type > 3000 && base_type < 4000)      { has_m = true;              base_type -= 3000; }
        else if (base_type > 2000 && base_type < 3000) { has_z = true; has_m = true; base_type -= 2000; }
        else if (base_type > 1000 && base_type < 2000) { has_z = true;              base_type -= 1000; }

        if (has_srid)
        {
            UInt32 srid_ignored;
            readBinaryEndian(srid_ignored, buf, endian);
        }

        const size_t extra_bytes_per_coord = (has_z ? 8u : 0u) + (has_m ? 8u : 0u);

        auto read_point = [&]()
        {
            double x;
            double y;
            readBinaryEndian(x, buf, endian);
            readBinaryEndian(y, buf, endian);
            if (extra_bytes_per_coord)
                buf.ignore(extra_bytes_per_coord);
            update_xy(x, y);
        };

        auto read_coord_seq = [&](UInt32 n_points)
        {
            for (UInt32 i = 0; i < n_points; ++i)
                read_point();
        };

        switch (base_type)
        {
            case 1: /// Point
                read_point();
                break;

            case 2: /// LineString
            {
                UInt32 n;
                readBinaryEndian(n, buf, endian);
                read_coord_seq(n);
                break;
            }

            case 3: /// Polygon
            {
                UInt32 n_rings;
                readBinaryEndian(n_rings, buf, endian);
                for (UInt32 r = 0; r < n_rings; ++r)
                {
                    UInt32 n;
                    readBinaryEndian(n, buf, endian);
                    read_coord_seq(n);
                }
                break;
            }

            case 4: /// MultiPoint
            case 5: /// MultiLineString
            case 6: /// MultiPolygon
            case 7: /// GeometryCollection
            {
                UInt32 n_geoms;
                readBinaryEndian(n_geoms, buf, endian);
                for (UInt32 i = 0; i < n_geoms; ++i)
                    scan();
                break;
            }

            default:
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "SpatialRTreeJoin::wkbBBox: unsupported WKB geometry type {}",
                    base_type);
        }
    };

    scan();

    if (xmin > xmax) /// empty geometry — degenerate zero-area box
        return BGBox{BGPoint{0.0, 0.0}, BGPoint{0.0, 0.0}};

    return BGBox{BGPoint{xmin, ymin}, BGPoint{xmax, ymax}};
}

// ── Column identification ─────────────────────────────────────────────────────

bool SpatialRTreeJoin::identifyGeomColumns(
    const ExpressionActionsPtr & expr,
    const Block & right_hdr,
    String & out_left_col,
    String & out_right_col,
    int expand_arg_index,
    double & out_bbox_expand)
{
    const auto & dag = expr->getActionsDAG();
    const auto & outputs = dag.getOutputs();

    if (outputs.size() != 1)
        return false;

    const ActionsDAG::Node * fn = outputs[0];
    if (fn->type != ActionsDAG::ActionType::FUNCTION || fn->children.size() < 2)
        return false;

    /// Walk a node chain down to the first INPUT node.
    /// Only follows ALIAS nodes; stops at FUNCTION nodes to avoid treating
    /// expressions like st_expand_col(b, c) as a plain column reference.
    /// (If the predicate arg is wrapped in a computation, fall back to hash join.)
    auto find_input = [](const ActionsDAG::Node * node) -> const ActionsDAG::Node *
    {
        while (node)
        {
            if (node->type == ActionsDAG::ActionType::INPUT)
                return node;
            if (node->type != ActionsDAG::ActionType::ALIAS || node->children.empty())
                return nullptr;
            node = node->children[0];
        }
        return nullptr;
    };

    const auto * in0 = find_input(fn->children[0]);
    const auto * in1 = find_input(fn->children[1]);

    if (!in0 || !in1)
        return false;

    if (right_hdr.has(in0->result_name))
    {
        out_right_col = in0->result_name;
        out_left_col  = in1->result_name;
    }
    else if (right_hdr.has(in1->result_name))
    {
        out_right_col = in1->result_name;
        out_left_col  = in0->result_name;
    }
    else
    {
        return false;
    }

    /// Extract the constant distance value from the designated argument index.
    /// If it is not a compile-time constant we fall back to hash join (returning false)
    /// to avoid false negatives in the bbox pre-filter.
    out_bbox_expand = 0.0;
    if (expand_arg_index >= 0)
    {
        auto idx = static_cast<size_t>(expand_arg_index);
        if (idx >= fn->children.size())
            return false;
        const auto * dist_node = fn->children[idx];
        if (dist_node->type != ActionsDAG::ActionType::COLUMN
            || !dist_node->column
            || dist_node->column->size() == 0)
            return false;
        try
        {
            out_bbox_expand = dist_node->column->getFloat64(0);
        }
        catch (...)
        {
            return false;
        }
    }

    return true;
}

// ── IJoin implementation ──────────────────────────────────────────────────────

SpatialRTreeJoin::SpatialRTreeJoin(std::shared_ptr<TableJoin> table_join_, SharedHeader right_header_, double bbox_expand_)
    : table_join(std::move(table_join_))
    , right_header(std::move(right_header_))
    , bbox_expand(bbox_expand_)
{
    const auto & mixed = table_join->getMixedJoinExpression();
    if (!mixed)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "SpatialRTreeJoin: no mixed join expression");

    double ignored_expand = 0.0;
    if (!identifyGeomColumns(mixed, *right_header, left_geom_col, right_geom_col, -1, ignored_expand))
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "SpatialRTreeJoin: cannot identify geometry columns from spatial predicate DAG");

    /// The expression's single output column is used as the filter mask.
    filter_col_name = mixed->getActionsDAG().getOutputs()[0]->result_name;

    /// Pick up any non-spatial pre-filter from the planner (e.g. `b1.id < b2.id`
    /// extracted from ON clause alongside the spatial predicate).
    if (const auto & pre_expr = table_join->getPreSpatialFilterExpression())
    {
        pre_filter_expr = pre_expr;
        pre_filter_col_name = pre_expr->getActionsDAG().getOutputs()[0]->result_name;
    }
}

void SpatialRTreeJoin::initialize(const Block & left_sample_block)
{
    left_sample = left_sample_block.cloneEmpty();
}

bool SpatialRTreeJoin::addBlockToJoin(const Block & block, bool /*check_limits*/)
{
    const IColumn & geom_col = *block.getByName(right_geom_col).column;
    const size_t n = block.rows();

    /// Step 1: claim a block_idx under lock, then release immediately.
    /// This lets multiple build threads compute bboxes in parallel (step 2).
    size_t block_idx;
    {
        std::lock_guard lock(build_mutex);
        block_idx = right_blocks.size();
        right_blocks.push_back(block);
        total_right_rows  += n;
        total_right_bytes += block.allocatedBytes();
    }

    /// Step 2: compute bboxes without holding the lock.
    /// geom_col is part of the original block (const ref, caller keeps it alive),
    /// so reads here are safe even while other threads append to right_blocks.
    std::vector<std::pair<BGBox, RightPos>> local_entries(n);
    for (size_t row = 0; row < n; ++row)
    {
        local_entries[row] = {
            wkbBBox(geom_col.getDataAt(row)),
            RightPos{static_cast<UInt32>(block_idx), static_cast<UInt32>(row)}};
    }

    /// Step 3: bulk-append to shared pending_entries under lock.
    /// The insert may reallocate pending_entries — safe because local_entries is stable.
    {
        std::lock_guard lock(build_mutex);
        pending_entries.insert(
            pending_entries.end(),
            std::make_move_iterator(local_entries.begin()),
            std::make_move_iterator(local_entries.end()));
    }

    return true;
}

void SpatialRTreeJoin::setTotals(const Block & block)
{
    /// Multiple FillingRightJoinSideTransform instances call setTotals concurrently
    /// when supportParallelJoin() = true.  Guard the base-class write with a mutex.
    /// Skip empty blocks (synthesized when the right source has no WITH TOTALS).
    if (!block.empty())
    {
        std::lock_guard lock(totals_mutex);
        IJoin::setTotals(block);
    }
}

const Block & SpatialRTreeJoin::getTotals() const
{
    std::lock_guard lock(totals_mutex);
    return IJoin::getTotals();
}

void SpatialRTreeJoin::runPostBuildPhase()
{
    /// Bulk-load sub-trees in parallel using the packing (STR) algorithm.
    /// Called once by FillingRightJoinSideTransform (single thread) before any joinBlock(),
    /// so no locking is needed here or in joinBlock() when reading sub_trees.
    ///
    /// Splitting into K chunks and building K trees in parallel eliminates the long
    /// single-CPU period that precedes parallel probe when the right side is large (e.g.
    /// 60M trip points).  Each sub-tree covers a contiguous slice of the spatially-unsorted
    /// pending_entries, so query overhead is O(K × log(N/K)) instead of O(log N).
    ///
    /// For small right sides (≤ 200K entries) the build is already fast and the per-lookup
    /// cost of querying K trees instead of 1 dominates; use a single tree in that case.
    const size_t n = pending_entries.size();
    if (n == 0)
        return;

    static constexpr size_t kParallelThreshold = 200'000;
    const size_t num_parts = (n <= kParallelThreshold)
        ? 1
        : std::max(size_t{1}, std::min(static_cast<size_t>(std::thread::hardware_concurrency()), n));
    sub_trees.resize(num_parts);

    const size_t chunk = (n + num_parts - 1) / num_parts;
    std::vector<std::thread> threads;
    threads.reserve(num_parts);
    for (size_t t = 0; t < num_parts; ++t)
    {
        const size_t begin = t * chunk;
        const size_t end   = std::min(begin + chunk, n);
        threads.emplace_back([this, begin, end, t]
        {
            sub_trees[t] = RTree(pending_entries.begin() + begin,
                                  pending_entries.begin() + end);
        });
    }
    for (auto & th : threads)
        th.join();

    pending_entries.clear();
    pending_entries.shrink_to_fit();

    /// For RIGHT JOIN: allocate per-block matched byte arrays (all 0 = unmatched).
    if (table_join->kind() == JoinKind::Right)
    {
        right_matched_per_block.resize(right_blocks.size());
        for (size_t i = 0; i < right_blocks.size(); ++i)
            right_matched_per_block[i].assign(right_blocks[i].rows(), 0);
    }
}

JoinResultPtr SpatialRTreeJoin::joinBlock(Block left_block)
{
    const size_t left_rows = left_block.rows();
    const bool is_left_join  = table_join->kind() == JoinKind::Left;
    const bool is_right_join = table_join->kind() == JoinKind::Right;

    /// Build the output block schema from the columns the planner says are needed
    /// downstream (table_join->resultColumnsFromLeftTable() and columnsAddedByJoin).
    /// This correctly excludes geometry-only columns (e.g. z_boundary in Q10, which
    /// is 150 KB/row and would OOM if materialised for every match) while including
    /// them when they ARE used in SELECT (e.g. b1.geom in Q9's st_area(b1.geom)).
    auto buildOutputSchema = [&]() -> Block
    {
        Block schema;
        for (const auto & col_def : table_join->resultColumnsFromLeftTable())
        {
            if (left_block.has(col_def.name))
                schema.insert(left_block.getByName(col_def.name).cloneEmpty());
        }
        for (const auto & col_def : table_join->columnsAddedByJoin())
        {
            if (!schema.has(col_def.name))
                schema.insert({col_def.type->createColumn(), col_def.type, col_def.name});
        }
        return schema;
    };

    /// Append unmatched left rows (NULLs on right) to result for LEFT JOIN.
    auto appendUnmatched = [&](Block & result, const std::vector<bool> & left_matched)
    {
        std::vector<size_t> unmatched;
        for (size_t li = 0; li < left_rows; ++li)
            if (!left_matched[li])
                unmatched.push_back(li);
        if (unmatched.empty())
            return;
        const size_t n = unmatched.size();
        for (auto & dst_col : result)
        {
            auto mut = dst_col.column->assumeMutable();
            if (left_block.has(dst_col.name))
            {
                const auto & src = *left_block.getByName(dst_col.name).column;
                for (size_t li : unmatched)
                    mut->insertFrom(src, li);
            }
            else
            {
                for (size_t i = 0; i < n; ++i)
                    mut->insertDefault();
            }
            dst_col.column = std::move(mut);
        }
    };

    if (left_rows == 0)
        return IJoinResult::createFromBlock(buildOutputSchema());

    if (total_right_rows == 0)
    {
        Block result = buildOutputSchema();
        if (!is_left_join)
            return IJoinResult::createFromBlock(std::move(result));

        /// LEFT JOIN with no right rows: emit all left rows with NULL right cols.
        for (auto & dst_col : result)
        {
            auto mut = dst_col.column->assumeMutable();
            if (left_block.has(dst_col.name))
                mut->insertRangeFrom(*left_block.getByName(dst_col.name).column, 0, left_rows);
            else
                for (size_t i = 0; i < left_rows; ++i)
                    mut->insertDefault();
            dst_col.column = std::move(mut);
        }
        return IJoinResult::createFromBlock(std::move(result));
    }

    const IColumn & left_geom = *left_block.getByName(left_geom_col).column;

    // ─────────────────────────────────────────────────────────────────────────
    // Probe strategy: collect all bbox candidates across left rows, then
    // evaluate in grouped batches so WASM builds PreparedGeometry once per
    // unique geometry value.  Grouping by the side with fewer unique values
    // (group_by_right or group_by_left) minimises PreparedGeometry rebuilds to
    // unique_right or unique_left counts per joinBlock call — not per flush.
    //
    // Memory protection via lazy iteration: R-tree results are collected one
    // entry at a time.  If a single left row accumulates ≥ kLargeHit results
    // (e.g. a globally-scoped zone whose bbox covers the entire right dataset),
    // the shared candidates buffer is flushed and that row is streamed alone in
    // kChunkSize-sized batches with the left geometry as ColumnConst.  This
    // bounds peak memory to O(max(kMaxCandidates, kLargeHit) × entry_size).
    // ─────────────────────────────────────────────────────────────────────────

    Block output = buildOutputSchema();
    const size_t n_out_cols = output.columns();

    struct ColSource { bool from_left; String name; };
    std::vector<ColSource> col_sources;
    col_sources.reserve(n_out_cols);
    for (size_t i = 0; i < n_out_cols; ++i)
    {
        const auto & col_def = output.getByPosition(i);
        col_sources.push_back({left_block.has(col_def.name), col_def.name});
    }

    MutableColumns mutable_cols(n_out_cols);
    for (size_t i = 0; i < n_out_cols; ++i)
        mutable_cols[i] = IColumn::mutate(output.getByPosition(i).column);

    std::vector<bool> left_matched(is_left_join ? left_rows : 0, false);
    /// For RIGHT JOIN, right_matched_per_block[block][row] is marked with relaxed-atomic
    /// byte writes.  Multiple probe threads may race to set the same byte to 1, but since
    /// all writers write the same value the race is benign.  getNonJoinedBlocks() reads
    /// the bitmap only after all joinBlock() calls have returned (pipeline barrier).

    NameSet pre_filter_required;
    if (pre_filter_expr)
        for (const auto & col : pre_filter_expr->getRequiredColumns())
            pre_filter_required.insert(col);

    static constexpr size_t kChunkSize     = 65536;       // WASM batch size for large-row streaming
    static constexpr size_t kLargeHit      = 4'000'000;   // per-row hit limit before streaming mode
    static constexpr size_t kMaxCandidates = 8'000'000;   // shared buffer overflow guard

    struct Candidate { size_t left_row; UInt32 block_idx; UInt32 row_idx; };
    std::vector<Candidate> candidates;
    candidates.reserve(std::min(left_rows * 8, kMaxCandidates));

    // ── helpers ──────────────────────────────────────────────────────────────

    /// Apply pre_filter_expr to a block of candidates (in-place compaction).
    auto applyPreFilter = [&](std::vector<Candidate> & cands)
    {
        if (!pre_filter_expr || cands.empty())
            return;
        const size_t n = cands.size();

        Block pre_block;
        for (const auto & src_col : left_block)
        {
            if (!pre_filter_required.contains(src_col.name))
                continue;
            auto col = src_col.type->createColumn();
            col->reserve(n);
            for (const auto & c : cands)
                col->insertFrom(*src_col.column, c.left_row);
            pre_block.insert({std::move(col), src_col.type, src_col.name});
        }
        for (const auto & rcd : *right_header)
        {
            if (!pre_filter_required.contains(rcd.name) || pre_block.has(rcd.name))
                continue;
            auto col = rcd.type->createColumn();
            col->reserve(n);
            for (const auto & c : cands)
                col->insertFrom(*right_blocks[c.block_idx].getByName(rcd.name).column, c.row_idx);
            pre_block.insert({std::move(col), rcd.type, rcd.name});
        }
        pre_filter_expr->execute(pre_block);
        const auto & fc = *pre_block.getByName(pre_filter_col_name).column;
        size_t j = 0;
        for (size_t i = 0; i < n; ++i)
            if (fc.getBool(i))
                cands[j++] = cands[i];
        cands.resize(j);
    };

    /// Evaluate spatial predicate on cands and emit matched rows.
    /// Decides group_by_right vs group_by_left to pick the better ColumnConst side.
    auto evaluateAndEmit = [&](std::vector<Candidate> & cands)
    {
        if (cands.empty())
            return;

        applyPreFilter(cands);
        if (cands.empty())
            return;

        const size_t n = cands.size();

        // Count unique right rows to decide which side becomes ColumnConst.
        std::unordered_map<uint64_t, std::vector<size_t>> right_groups;
        right_groups.reserve(std::min(n, static_cast<size_t>(4096)));
        for (size_t ci = 0; ci < n; ++ci)
        {
            uint64_t key = static_cast<uint64_t>(cands[ci].block_idx) * 0x100000000ULL + cands[ci].row_idx;
            right_groups[key].push_back(ci);
        }
        const bool group_by_right = right_groups.size() <= left_rows;

        IColumn::Filter mask(n, 0);

        if (group_by_right)
        {
            for (const auto & [key, indices] : right_groups)
            {
                const size_t k = indices.size();
                const auto & c0 = cands[indices[0]];
                const Block & rb = right_blocks[c0.block_idx];

                Block pred_block;
                for (const auto & src_col : left_block)
                {
                    auto data = src_col.type->createColumn();
                    data->reserve(k);
                    for (size_t ci : indices)
                        data->insertFrom(*src_col.column, cands[ci].left_row);
                    pred_block.insert({std::move(data), src_col.type, src_col.name});
                }
                for (const auto & rcd : *right_header)
                {
                    if (pred_block.has(rcd.name))
                        continue;
                    ColumnPtr single = rb.getByName(rcd.name).column->cut(c0.row_idx, 1);
                    pred_block.insert({ColumnConst::create(std::move(single), k), rcd.type, rcd.name});
                }
                table_join->getMixedJoinExpression()->execute(pred_block);
                const auto & fc = *pred_block.getByName(filter_col_name).column;
                for (size_t j = 0; j < k; ++j)
                    mask[indices[j]] = fc.getBool(j) ? 1 : 0;
            }
        }
        else
        {
            std::unordered_map<size_t, std::vector<size_t>> left_groups;
            left_groups.reserve(left_rows);
            for (size_t ci = 0; ci < n; ++ci)
                left_groups[cands[ci].left_row].push_back(ci);

            for (const auto & [left_row, indices] : left_groups)
            {
                const size_t k = indices.size();
                Block pred_block;
                for (const auto & src_col : left_block)
                {
                    ColumnPtr single = src_col.column->cut(left_row, 1);
                    pred_block.insert({ColumnConst::create(std::move(single), k), src_col.type, src_col.name});
                }
                for (const auto & rcd : *right_header)
                {
                    if (pred_block.has(rcd.name))
                        continue;
                    auto data = rcd.type->createColumn();
                    data->reserve(k);
                    for (size_t ci : indices)
                        data->insertFrom(
                            *right_blocks[cands[ci].block_idx].getByName(rcd.name).column,
                            cands[ci].row_idx);
                    pred_block.insert({std::move(data), rcd.type, rcd.name});
                }
                table_join->getMixedJoinExpression()->execute(pred_block);
                const auto & fc = *pred_block.getByName(filter_col_name).column;
                for (size_t j = 0; j < k; ++j)
                    mask[indices[j]] = fc.getBool(j) ? 1 : 0;
            }
        }

        for (size_t ci = 0; ci < n; ++ci)
        {
            if (!mask[ci])
                continue;
            const auto & c = cands[ci];
            if (is_left_join)
                left_matched[c.left_row] = true;
            if (is_right_join)
                __atomic_store_n(&right_matched_per_block[c.block_idx][c.row_idx], '\1', __ATOMIC_RELAXED);
            for (size_t i = 0; i < n_out_cols; ++i)
            {
                if (col_sources[i].from_left)
                    mutable_cols[i]->insertFrom(*left_block.getByName(col_sources[i].name).column, c.left_row);
                else
                    mutable_cols[i]->insertFrom(
                        *right_blocks[c.block_idx].getByName(col_sources[i].name).column, c.row_idx);
            }
        }

        cands.clear();
    };

    /// Process a single left row's hit list in kChunkSize-sized batches.
    /// Used when one left row has ≥ kLargeHit R-tree results (large-row mode).
    /// Left row is ColumnConst per batch (PreparedGeometry for left geometry).
    auto processLargeRow = [&](size_t li, std::vector<std::pair<BGBox, RightPos>> & row_hits)
    {
        for (size_t h = 0; h < row_hits.size(); h += kChunkSize)
        {
            const size_t chunk_end = std::min(h + kChunkSize, row_hits.size());
            const size_t k = chunk_end - h;

            // Pre-filter
            std::vector<size_t> surviving;
            if (pre_filter_expr)
            {
                Block pre_block;
                for (const auto & src_col : left_block)
                {
                    if (!pre_filter_required.contains(src_col.name))
                        continue;
                    auto col = src_col.type->createColumn();
                    col->reserve(k);
                    for (size_t j = h; j < chunk_end; ++j)
                        col->insertFrom(*src_col.column, li);
                    pre_block.insert({std::move(col), src_col.type, src_col.name});
                }
                for (const auto & rcd : *right_header)
                {
                    if (!pre_filter_required.contains(rcd.name) || pre_block.has(rcd.name))
                        continue;
                    auto col = rcd.type->createColumn();
                    col->reserve(k);
                    for (size_t j = h; j < chunk_end; ++j)
                        col->insertFrom(*right_blocks[row_hits[j].second.block_idx].getByName(rcd.name).column, row_hits[j].second.row_idx);
                    pre_block.insert({std::move(col), rcd.type, rcd.name});
                }
                pre_filter_expr->execute(pre_block);
                const auto & pfc = *pre_block.getByName(pre_filter_col_name).column;
                surviving.reserve(k);
                for (size_t j = 0; j < k; ++j)
                    if (pfc.getBool(j))
                        surviving.push_back(h + j);
                if (surviving.empty())
                    continue;
            }

            const size_t eff_k = pre_filter_expr ? surviving.size() : k;
            Block pred_block;
            for (const auto & src_col : left_block)
            {
                ColumnPtr single = src_col.column->cut(li, 1);
                pred_block.insert({ColumnConst::create(std::move(single), eff_k), src_col.type, src_col.name});
            }
            for (const auto & rcd : *right_header)
            {
                if (pred_block.has(rcd.name))
                    continue;
                auto col = rcd.type->createColumn();
                col->reserve(eff_k);
                if (pre_filter_expr)
                    for (size_t idx : surviving)
                        col->insertFrom(*right_blocks[row_hits[idx].second.block_idx].getByName(rcd.name).column, row_hits[idx].second.row_idx);
                else
                    for (size_t j = h; j < chunk_end; ++j)
                        col->insertFrom(*right_blocks[row_hits[j].second.block_idx].getByName(rcd.name).column, row_hits[j].second.row_idx);
                pred_block.insert({std::move(col), rcd.type, rcd.name});
            }
            table_join->getMixedJoinExpression()->execute(pred_block);
            const auto & fc = *pred_block.getByName(filter_col_name).column;
            for (size_t j = 0; j < eff_k; ++j)
            {
                if (!fc.getBool(j))
                    continue;
                const size_t hit_idx = pre_filter_expr ? surviving[j] : (h + j);
                const RightPos & pos = row_hits[hit_idx].second;
                if (is_left_join)
                    left_matched[li] = true;
                if (is_right_join)
                    __atomic_store_n(&right_matched_per_block[pos.block_idx][pos.row_idx], '\1', __ATOMIC_RELAXED);
                for (size_t ci = 0; ci < n_out_cols; ++ci)
                {
                    if (col_sources[ci].from_left)
                        mutable_cols[ci]->insertFrom(*left_block.getByName(col_sources[ci].name).column, li);
                    else
                        mutable_cols[ci]->insertFrom(*right_blocks[pos.block_idx].getByName(col_sources[ci].name).column, pos.row_idx);
                }
            }
        }
    };

    // ── main probe loop ───────────────────────────────────────────────────────

    /// Per-left-row hit buffer.  Populated lazily one entry at a time so we can
    /// detect and handle large-hit-count rows before materialising them fully.
    std::vector<std::pair<BGBox, RightPos>> row_buf;
    row_buf.reserve(kChunkSize);

    for (size_t li = 0; li < left_rows; ++li)
    {
        row_buf.clear();
        bool is_large = false;

        BGBox bbox = wkbBBox(left_geom.getDataAt(li));
        if (bbox_expand > 0.0)
        {
            bbox = BGBox{
                BGPoint{boost::geometry::get<boost::geometry::min_corner, 0>(bbox) - bbox_expand,
                        boost::geometry::get<boost::geometry::min_corner, 1>(bbox) - bbox_expand},
                BGPoint{boost::geometry::get<boost::geometry::max_corner, 0>(bbox) + bbox_expand,
                        boost::geometry::get<boost::geometry::max_corner, 1>(bbox) + bbox_expand}};
        }

        for (const auto & sub_tree : sub_trees)
        {
            for (auto qit = sub_tree.qbegin(boost::geometry::index::intersects(bbox));
                 qit != sub_tree.qend(); ++qit)
            {
                row_buf.push_back(*qit);

                if (!is_large && row_buf.size() >= kLargeHit)
                {
                    /// This left row has hit the large-row threshold (e.g. a
                    /// globally-scoped zone whose bbox covers the entire right
                    /// dataset).  Flush the shared candidates buffer so other
                    /// rows' hits are not mixed in, then switch to streaming mode
                    /// where this row's hits are processed in kChunkSize batches
                    /// with the left geometry as ColumnConst.
                    evaluateAndEmit(candidates);
                    is_large = true;
                }

                if (is_large && row_buf.size() >= kChunkSize)
                {
                    processLargeRow(li, row_buf);
                    row_buf.clear();
                }
            }
        }

        if (is_large)
        {
            if (!row_buf.empty())
                processLargeRow(li, row_buf);
        }
        else if (!row_buf.empty())
        {
            /// Normal row: commit its hits to the shared candidate buffer so
            /// evaluateAndEmit can group across many left rows, calling WASM
            /// once per unique right (or left) geometry value.
            for (const auto & [box, pos] : row_buf)
                candidates.push_back({li, pos.block_idx, pos.row_idx});

            if (candidates.size() >= kMaxCandidates)
                evaluateAndEmit(candidates);
        }
    }
    evaluateAndEmit(candidates); // emit all accumulated candidates

    for (size_t i = 0; i < n_out_cols; ++i)
        output.getByPosition(i).column = std::move(mutable_cols[i]);

    if (is_left_join)
        appendUnmatched(output, left_matched);

    return IJoinResult::createFromBlock(std::move(output));
}

// ── Right-join non-joined blocks ──────────────────────────────────────────────

/// Returns a lazy stream of all right rows that were NOT matched during any
/// joinBlock() call.  For RIGHT JOIN semantics: every right row must appear
/// in output; unmatched right rows get NULL for all left-side columns.
IBlocksStreamPtr SpatialRTreeJoin::getNonJoinedBlocks(
    const Block & left_sample_block,
    const Block & result_sample_block,
    UInt64 max_block_size) const
{
    if (table_join->kind() != JoinKind::Right)
        return nullptr;

    struct UnmatchedRightStream final : IBlocksStream
    {
        const SpatialRTreeJoin & join;
        const Block left_sample;
        const Block result_sample;
        const UInt64 block_size;

        size_t block_idx = 0;
        size_t row_idx   = 0;

        UnmatchedRightStream(const SpatialRTreeJoin & j, Block ls, Block rs, UInt64 bs)
            : join(j), left_sample(std::move(ls)), result_sample(std::move(rs)), block_size(bs) {}

        Block nextImpl() override
        {
            MutableColumns cols;
            cols.reserve(result_sample.columns());
            for (const auto & cd : result_sample)
                cols.push_back(cd.column->cloneEmpty());

            size_t emitted = 0;
            while (block_idx < join.right_blocks.size() && emitted < block_size)
            {
                const Block & rb = join.right_blocks[block_idx];
                const auto & matched = join.right_matched_per_block[block_idx];
                while (row_idx < rb.rows() && emitted < block_size)
                {
                    if (!__atomic_load_n(&matched[row_idx], __ATOMIC_RELAXED))
                    {
                        for (size_t ci = 0; ci < result_sample.columns(); ++ci)
                        {
                            const auto & cd = result_sample.getByPosition(ci);
                            if (left_sample.has(cd.name))
                                cols[ci]->insertDefault(); // NULL for left cols
                            else
                                cols[ci]->insertFrom(*rb.getByName(cd.name).column, row_idx);
                        }
                        ++emitted;
                    }
                    ++row_idx;
                }
                if (row_idx >= rb.rows()) { ++block_idx; row_idx = 0; }
            }

            if (emitted == 0)
                return {};

            Block out = result_sample.cloneEmpty();
            for (size_t ci = 0; ci < out.columns(); ++ci)
                out.getByPosition(ci).column = std::move(cols[ci]);
            return out;
        }
    };

    return std::make_unique<UnmatchedRightStream>(*this, left_sample_block, result_sample_block, max_block_size);
}

}
