#include <Interpreters/SpatialRTreeJoin.h>

#include <bit>
#include <cstring>
#include <limits>

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
    String & out_right_col)
{
    const auto & dag = expr->getActionsDAG();
    const auto & outputs = dag.getOutputs();

    if (outputs.size() != 1)
        return false;

    const ActionsDAG::Node * fn = outputs[0];
    if (fn->type != ActionsDAG::ActionType::FUNCTION || fn->children.size() != 2)
        return false;

    /// Walk a node chain (e.g. CAST wrappers) down to the first INPUT node.
    auto find_input = [](const ActionsDAG::Node * node) -> const ActionsDAG::Node *
    {
        while (node)
        {
            if (node->type == ActionsDAG::ActionType::INPUT)
                return node;
            if (node->children.empty())
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

    return true;
}

// ── IJoin implementation ──────────────────────────────────────────────────────

SpatialRTreeJoin::SpatialRTreeJoin(std::shared_ptr<TableJoin> table_join_, SharedHeader right_header_)
    : table_join(std::move(table_join_))
    , right_header(std::move(right_header_))
{
    const auto & mixed = table_join->getMixedJoinExpression();
    if (!mixed)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "SpatialRTreeJoin: no mixed join expression");

    if (!identifyGeomColumns(mixed, *right_header, left_geom_col, right_geom_col))
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "SpatialRTreeJoin: cannot identify geometry columns from spatial predicate DAG");

    /// The expression's single output column is used as the filter mask.
    filter_col_name = mixed->getActionsDAG().getOutputs()[0]->result_name;
}

void SpatialRTreeJoin::initialize(const Block & left_sample_block)
{
    left_sample = left_sample_block.cloneEmpty();
}

bool SpatialRTreeJoin::addBlockToJoin(const Block & block, bool /*check_limits*/)
{
    const size_t block_idx = right_blocks.size();
    right_blocks.push_back(block);

    const IColumn & geom_col = *block.getByName(right_geom_col).column;
    const size_t n = block.rows();

    for (size_t row = 0; row < n; ++row)
    {
        BGBox box = wkbBBox(geom_col.getDataAt(row));
        rtree.insert({box, RightPos{static_cast<UInt32>(block_idx), static_cast<UInt32>(row)}});
    }

    total_right_rows  += n;
    total_right_bytes += block.allocatedBytes();
    return true;
}

JoinResultPtr SpatialRTreeJoin::joinBlock(Block left_block)
{
    const size_t left_rows = left_block.rows();
    if (left_rows == 0 || total_right_rows == 0)
    {
        /// Return empty block with correct schema: left cols + right cols.
        Block empty = left_block.cloneEmpty();
        for (const auto & col : *right_header)
            if (!empty.has(col.name))
                empty.insert(col.cloneEmpty());
        return IJoinResult::createFromBlock(std::move(empty));
    }

    const IColumn & left_geom = *left_block.getByName(left_geom_col).column;

    /// Collect all bbox candidates across all left rows.
    struct Candidate { size_t left_row; UInt32 block_idx; UInt32 row_idx; };
    std::vector<Candidate> candidates;
    candidates.reserve(left_rows * 4); /// rough heuristic

    std::vector<std::pair<BGBox, RightPos>> hits;
    for (size_t li = 0; li < left_rows; ++li)
    {
        hits.clear();
        BGBox bbox = wkbBBox(left_geom.getDataAt(li));
        rtree.query(boost::geometry::index::intersects(bbox), std::back_inserter(hits));
        for (const auto & [box, pos] : hits)
            candidates.push_back({li, pos.block_idx, pos.row_idx});
    }

    if (candidates.empty())
    {
        Block empty = left_block.cloneEmpty();
        for (const auto & col : *right_header)
            if (!empty.has(col.name))
                empty.insert(col.cloneEmpty());
        return IJoinResult::createFromBlock(std::move(empty));
    }

    const size_t n_cand = candidates.size();

    /// Build a block with left columns (repeated) + right columns (one per candidate).
    Block candidate_block;

    /// Left columns
    for (const auto & src_col : left_block)
    {
        auto mutable_col = src_col.column->cloneEmpty();
        mutable_col->reserve(n_cand);
        for (const auto & c : candidates)
            mutable_col->insertFrom(*src_col.column, c.left_row);
        candidate_block.insert({std::move(mutable_col), src_col.type, src_col.name});
    }

    /// Right columns
    for (const auto & right_col_def : *right_header)
    {
        if (candidate_block.has(right_col_def.name))
            continue; /// already present (name collision with left — ignore for now)

        auto mutable_col = right_col_def.column->cloneEmpty();
        mutable_col->reserve(n_cand);
        for (const auto & c : candidates)
        {
            const Block & rb = right_blocks[c.block_idx];
            mutable_col->insertFrom(*rb.getByName(right_col_def.name).column, c.row_idx);
        }
        candidate_block.insert({std::move(mutable_col), right_col_def.type, right_col_def.name});
    }

    /// Apply exact spatial predicate (WASM call via ExpressionActions).
    table_join->getMixedJoinExpression()->execute(candidate_block);

    /// Extract UInt8 filter column produced by the predicate.
    const auto & filter_col_with_type = candidate_block.getByName(filter_col_name);
    const auto & filter_col = *filter_col_with_type.column;
    const size_t total = candidate_block.rows();

    IColumn::Filter mask(total);
    for (size_t i = 0; i < total; ++i)
        mask[i] = filter_col.getBool(i) ? 1 : 0;

    /// Remove the predicate output column — it is not part of the join schema.
    candidate_block.erase(filter_col_name);

    /// Apply filter to all remaining columns.
    for (auto & col : candidate_block)
        col.column = col.column->filter(mask, -1);

    return IJoinResult::createFromBlock(std::move(candidate_block));
}

}
