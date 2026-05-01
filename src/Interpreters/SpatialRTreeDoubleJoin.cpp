#include <Interpreters/SpatialRTreeDoubleJoin.h>

#include <unordered_map>

#include <Columns/ColumnConst.h>
#include <Common/Exception.h>
#include <Common/logger_useful.h>
#include <Common/Stopwatch.h>

namespace DB
{

namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

SpatialRTreeDoubleJoin::SpatialRTreeDoubleJoin(
    std::shared_ptr<TableJoin> table_join_,
    SharedHeader right_header_)
    : Base(table_join_, right_header_)
{
    if (!table_join_->isFusedSpatialJoin())
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "SpatialRTreeDoubleJoin: table_join has no fused spatial join fields");

    left_geom_col_1 = table_join_->fused_first_probe_left_col;

    double ignored = 0.0;
    String right_col;
    if (!Base::identifyGeomColumns(
            table_join_->getMixedJoinExpression(), *right_header_,
            left_geom_col_2, right_col, -1, ignored))
        throw Exception(ErrorCodes::LOGICAL_ERROR,
            "SpatialRTreeDoubleJoin: cannot identify geometry columns from spatial predicate");

    for (const auto & fc : table_join_->fused_first_probe_output_cols)
        first_probe_output_cols.push_back({fc.right_col, fc.first_probe_col, fc.type});
}

JoinResultPtr SpatialRTreeDoubleJoin::joinBlock(Block left_block)
{
    const size_t left_rows = left_block.rows();

    auto isFirstProbeCol = [&](const String & name) -> const FirstProbeOutputCol *
    {
        for (const auto & fc : first_probe_output_cols)
            if (name == fc.first_probe_col)
                return &fc;
        return nullptr;
    };

    auto buildOutputSchema = [&]() -> Block
    {
        Block schema;
        for (const auto & col_def : getTableJoin().resultColumnsFromLeftTable())
        {
            if (!left_block.has(col_def.name))
                continue;
            if (const auto * fp = isFirstProbeCol(col_def.name))
                schema.insert({fp->type->createColumn(), fp->type, col_def.name});
            else
                schema.insert(left_block.getByName(col_def.name).cloneEmpty());
        }
        for (const auto & col_def : getTableJoin().columnsAddedByJoin())
            if (!schema.has(col_def.name))
                schema.insert({col_def.type->createColumn(), col_def.type, col_def.name});
        for (const auto & fc : first_probe_output_cols)
            if (!schema.has(fc.first_probe_col))
                schema.insert({fc.type->createColumn(), fc.type, fc.first_probe_col});
        return schema;
    };

    if (left_rows == 0 || alwaysReturnsEmptySet())
        return IJoinResult::createFromBlock(buildOutputSchema());

    const IColumn & left_geom1 = *left_block.getByName(left_geom_col_1).column;
    const IColumn & left_geom2 = *left_block.getByName(left_geom_col_2).column;

    const auto & mixed        = getTableJoin().getMixedJoinExpression();
    const String & filter_col = mixed->getActionsDAG().getOutputs()[0]->result_name;

    static constexpr size_t kChunkSize = 65536;

    // Phase 1: R-tree probe — collect zone candidates for both geometries in one pass.
    struct ZoneCands
    {
        std::vector<size_t> probe1; // left row indices whose pickup bbox hits this zone
        std::vector<size_t> probe2; // left row indices whose dropoff bbox hits this zone
    };
    std::unordered_map<UInt64, ZoneCands> zone_map;
    zone_map.reserve(16384);

    Stopwatch sw_rtree;
    for (size_t li = 0; li < left_rows; ++li)
    {
        BGBox bbox1 = wkbBBox(left_geom1.getDataAt(li));
        BGBox bbox2 = wkbBBox(left_geom2.getDataAt(li));
        for (const auto & st : getSubTrees())
        {
            for (auto q = st.qbegin(boost::geometry::index::intersects(bbox1)); q != st.qend(); ++q)
            {
                UInt64 key = (static_cast<UInt64>(q->second.block_idx) << 32) | q->second.row_idx;
                zone_map[key].probe1.push_back(li);
            }
            for (auto q = st.qbegin(boost::geometry::index::intersects(bbox2)); q != st.qend(); ++q)
            {
                UInt64 key = (static_cast<UInt64>(q->second.block_idx) << 32) | q->second.row_idx;
                zone_map[key].probe2.push_back(li);
            }
        }
    }
    const double t_rtree = sw_rtree.elapsedSeconds();

    // Phase 2: For each zone, evaluate probe1 and probe2 in ONE WASM call.
    //
    // We concatenate probe1 and probe2 trip lists into a single batch, using a mixed
    // geometry column: pickup geometry for probe1 rows, dropoff geometry for probe2 rows.
    // This halves the WASM call count vs two separate evaluations and eliminates any
    // PrepGeomCache dependency — the PreparedGeometry is built once per zone per call.
    std::vector<std::vector<RightPos>> matched1(left_rows);
    std::vector<std::vector<RightPos>> matched2(left_rows);
    const bool mixed_geom = (left_geom_col_1 != left_geom_col_2);
    UInt64 ns_wasm = 0;

    for (auto & [zone_key, zone_cands] : zone_map)
    {
        const UInt32 block_idx = static_cast<UInt32>(zone_key >> 32);
        const UInt32 row_idx   = static_cast<UInt32>(zone_key);
        const RightPos zone_pos{block_idx, row_idx};

        const size_t n1    = zone_cands.probe1.size();
        const size_t n2    = zone_cands.probe2.size();
        const size_t total = n1 + n2;
        if (total == 0)
            continue;

        // Map combined index j → (source trip index, is_probe1).
        // j < n1: probe1 trip; j >= n1: probe2 trip.
        auto tripIdx = [&](size_t j) -> size_t
        {
            return j < n1 ? zone_cands.probe1[j] : zone_cands.probe2[j - n1];
        };

        for (size_t h = 0; h < total; h += kChunkSize)
        {
            const size_t chunk_end = std::min(h + kChunkSize, total);
            const size_t k         = chunk_end - h;

            Block pred_block;

            for (const auto & src_col : left_block)
            {
                auto col = src_col.type->createColumn();
                col->reserve(k);

                if (mixed_geom && src_col.name == left_geom_col_2)
                {
                    // Mixed geometry column: pickup geom for probe1 rows, dropoff for probe2 rows.
                    for (size_t j = h; j < chunk_end; ++j)
                    {
                        const size_t li = tripIdx(j);
                        col->insertFrom(j < n1 ? left_geom1 : left_geom2, li);
                    }
                }
                else
                {
                    for (size_t j = h; j < chunk_end; ++j)
                        col->insertFrom(*src_col.column, tripIdx(j));
                }

                pred_block.insert({std::move(col), src_col.type, src_col.name});
            }

            // Zone as ColumnConst.
            for (const auto & rcd : *getRightHeader())
            {
                if (pred_block.has(rcd.name))
                    continue;
                auto single = getRightBlock(block_idx).getByName(rcd.name).column->cut(row_idx, 1);
                pred_block.insert({ColumnConst::create(std::move(single), k), rcd.type, rcd.name});
            }

            const UInt64 t0 = clock_gettime_ns();
            mixed->execute(pred_block);
            ns_wasm += clock_gettime_ns() - t0;

            const auto & fc = *pred_block.getByName(filter_col).column;
            for (size_t j = 0; j < k; ++j)
            {
                if (!fc.getBool(j))
                    continue;
                const size_t global_j = h + j;
                const size_t li       = tripIdx(global_j);
                if (global_j < n1)
                    matched1[li].push_back(zone_pos);
                else
                    matched2[li].push_back(zone_pos);
            }
        }
    }

    LOG_TRACE(getLogger("SpatialRTreeDoubleJoin"),
        "joinBlock: rtree={:.3f}s wasm={:.3f}s zones={} left_rows={}",
        t_rtree, static_cast<double>(ns_wasm) / 1e9, zone_map.size(), left_rows);

    // Phase 3: Cross-product emit.
    Block output = buildOutputSchema();
    const size_t n_out_cols = output.columns();

    struct ColSource { bool from_left; String name; };
    std::vector<ColSource> col_sources;
    col_sources.reserve(n_out_cols);
    for (size_t i = 0; i < n_out_cols; ++i)
    {
        const auto & cd = output.getByPosition(i);
        col_sources.push_back({left_block.has(cd.name), cd.name});
    }

    MutableColumns mutable_cols(n_out_cols);
    for (size_t i = 0; i < n_out_cols; ++i)
        mutable_cols[i] = IColumn::mutate(output.getByPosition(i).column);

    for (size_t li = 0; li < left_rows; ++li)
    {
        if (matched1[li].empty() || matched2[li].empty())
            continue;

        for (const RightPos & pz : matched1[li])
        {
            for (const RightPos & dz : matched2[li])
            {
                for (size_t i = 0; i < n_out_cols; ++i)
                {
                    const auto & cs = col_sources[i];

                    bool is_first_probe = false;
                    for (const auto & fc : first_probe_output_cols)
                    {
                        if (cs.name == fc.first_probe_col)
                        {
                            mutable_cols[i]->insertFrom(
                                *getRightBlock(pz.block_idx).getByName(fc.right_col).column,
                                pz.row_idx);
                            is_first_probe = true;
                            break;
                        }
                    }
                    if (is_first_probe)
                        continue;

                    if (cs.from_left)
                        mutable_cols[i]->insertFrom(*left_block.getByName(cs.name).column, li);
                    else
                        mutable_cols[i]->insertFrom(
                            *getRightBlock(dz.block_idx).getByName(cs.name).column, dz.row_idx);
                }
            }
        }
    }

    for (size_t i = 0; i < n_out_cols; ++i)
        output.getByPosition(i).column = std::move(mutable_cols[i]);

    return IJoinResult::createFromBlock(std::move(output));
}

}
