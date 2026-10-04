#include <Interpreters/SpatialRTreeJoin.h>

#include <algorithm>
#include <bit>
#include <cmath>
#include <cstring>
#include <functional>
#include <limits>
#include <optional>
#include <thread>

#include <base/scope_guard.h>
#include <Common/RadixSort.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Common/CurrentMetrics.h>
#include <Common/Exception.h>
#include <Common/ThreadPool.h>
#include <Common/threadPoolCallbackRunner.h>
#include <Common/logger_useful.h>
#include <Common/Stopwatch.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/ReadHelpers.h>

#include <boost/geometry/algorithms/intersects.hpp>

namespace CurrentMetrics
{
    extern const Metric SpatialJoinThreads;
    extern const Metric SpatialJoinThreadsActive;
    extern const Metric SpatialJoinThreadsScheduled;
}

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
    /// Fast path for the overwhelmingly common probe-side shape: a little-endian
    /// POINT (plain WKB, EWKB Z/M/ZM flag bits, or ISO 1001/2001/3001 codes).
    /// The generic scanner below allocates a read buffer and recurses through a
    /// std::function for every probe row; for a single point that is pure overhead.
    /// Anything this path is not certain about (big endian, SRID, wrong length,
    /// other types) falls through to the generic scanner, which stays authoritative.
    if (wkb.size() >= 21)
    {
        const auto * bytes = reinterpret_cast<const unsigned char *>(wkb.data());
        if (bytes[0] == 1) /// little endian
        {
            UInt32 type_word;
            memcpy(&type_word, bytes + 1, 4);

            if (!(type_word & 0x20000000u)) /// no SRID
            {
                bool has_z = (type_word & 0x80000000u) != 0;
                bool has_m = (type_word & 0x40000000u) != 0;
                UInt32 base_type = type_word & 0x0FFFFFFFu;

                if (base_type > 3000 && base_type < 4000)      { has_m = true;              base_type -= 3000; }
                else if (base_type > 2000 && base_type < 3000) { has_z = true; has_m = true; base_type -= 2000; }
                else if (base_type > 1000 && base_type < 2000) { has_z = true;              base_type -= 1000; }

                if (base_type == 1)
                {
                    const size_t needed = 5 + 16 + (has_z ? 8 : 0) + (has_m ? 8 : 0);
                    if (wkb.size() >= needed)
                    {
                        double x, y;
                        memcpy(&x, bytes + 5, 8);
                        memcpy(&y, bytes + 13, 8);
                        return BGBox{BGPoint{x, y}, BGPoint{x, y}};
                    }
                }
            }
        }
    }

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

namespace
{

/// Wrappers we may look through when resolving a predicate argument to the
/// column the bbox scanner will read.  Each either preserves the geometry
/// (the WKB parsers) or only changes nullability, so a bbox taken from the
/// wrapped column's raw bytes still bounds the value the predicate sees.
bool isGeometryPreservingWrapper(const String & name)
{
    static const std::unordered_set<String> wrappers = {
        "assumeNotNull",
        "readWKB",
        "readWKBPoint",
        "readWKBLineString",
        "readWKBPolygon",
        "readWKBMultiPoint",
        "readWKBMultiLineString",
        "readWKBMultiPolygon",
    };
    return wrappers.contains(name);
}

/// Resolve a predicate argument down to the INPUT column holding its WKB bytes.
/// Follows ALIAS links and single-argument geometry-preserving wrappers; stops at
/// anything else, because the bbox scanner reads the resolved column directly and
/// a computed value would not correspond to the argument.
const ActionsDAG::Node * findGeomInput(const ActionsDAG::Node * node)
{
    while (node)
    {
        if (node->type == ActionsDAG::ActionType::INPUT)
            return node;

        if (node->type == ActionsDAG::ActionType::ALIAS)
        {
            if (node->children.empty())
                return nullptr;
            node = node->children[0];
            continue;
        }

        if (node->type == ActionsDAG::ActionType::FUNCTION
            && node->children.size() == 1
            && isGeometryPreservingWrapper(node->function_base ? node->function_base->getName() : String{}))
        {
            node = node->children[0];
            continue;
        }

        return nullptr;
    }
    return nullptr;
}

/// wkbBBox reads bytes via IColumn::getDataAt, which only String-family columns
/// implement.  A natively-typed geo column (Point, Array(Tuple(Float64,Float64)), …)
/// would throw NOT_IMPLEMENTED deep inside the build phase.
bool isWKBBytesColumn(const DataTypePtr & type)
{
    return isString(removeNullable(removeLowCardinality(type)));
}

}

bool SpatialRTreeJoin::canUseSpatialPredicate(const ActionsDAG::Node * fn)
{
    if (!fn || fn->type != ActionsDAG::ActionType::FUNCTION || !fn->function_base || fn->children.size() < 2)
        return false;

    for (size_t i = 0; i < 2; ++i)
    {
        const auto * input = findGeomInput(fn->children[i]);
        if (!input || !isWKBBytesColumn(input->result_type))
            return false;
    }

    /// The bbox pre-filter widens by this distance, so it must be known at plan
    /// time; a non-constant would let the filter drop true matches.
    int expand_arg_index = fn->function_base->getSpatialExpandArg();
    if (expand_arg_index >= 0)
    {
        auto idx = static_cast<size_t>(expand_arg_index);
        if (idx >= fn->children.size())
            return false;
        const auto * dist_node = fn->children[idx];
        if (dist_node->type != ActionsDAG::ActionType::COLUMN || !dist_node->column)
            return false;
    }

    return true;
}

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
    if (!canUseSpatialPredicate(fn))
        return false;

    const auto * in0 = findGeomInput(fn->children[0]);
    const auto * in1 = findGeomInput(fn->children[1]);

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
        return false;

    /// Read the constant distance from the designated argument.
    /// canUseSpatialPredicate has already established that it is a constant column;
    /// only the numeric conversion can still fail.
    out_bbox_expand = 0.0;
    if (expand_arg_index >= 0)
    {
        try
        {
            out_bbox_expand = fn->children[static_cast<size_t>(expand_arg_index)]->column->getFloat64(0);
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

    /// Step 3: hand the entries over under lock; only the chunk pointer moves.
    {
        std::lock_guard lock(build_mutex);
        pending_chunks.push_back(std::move(local_entries));
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

void SpatialRTreeJoin::buildOccupancyGrid()
{
    grid_cells.clear();
    /// A large right side is built in parallel and probed by comparatively few rows;
    /// a serial grid pass over it would cost more than it saves.
    static constexpr size_t kMaxGridEntries = 200'000;
    if (pending_entries.size() > kMaxGridEntries)
        return;
    double min_x = std::numeric_limits<double>::infinity();
    double min_y = min_x;
    double max_x = -min_x;
    double max_y = -min_x;
    for (const auto & [box, pos] : pending_entries)
    {
        const double x0 = boost::geometry::get<boost::geometry::min_corner, 0>(box);
        const double y0 = boost::geometry::get<boost::geometry::min_corner, 1>(box);
        const double x1 = boost::geometry::get<boost::geometry::max_corner, 0>(box);
        const double y1 = boost::geometry::get<boost::geometry::max_corner, 1>(box);
        if (!std::isfinite(x0) || !std::isfinite(y0) || !std::isfinite(x1) || !std::isfinite(y1))
            continue; /// such an entry is never returned by an intersects query either
        min_x = std::min(min_x, x0);
        min_y = std::min(min_y, y0);
        max_x = std::max(max_x, x1);
        max_y = std::max(max_y, y1);
    }
    if (!(max_x > min_x) || !(max_y > min_y))
        return;

    const double side = static_cast<double>(kGridSide);
    grid_min_x = min_x;
    grid_min_y = min_y;
    grid_inv_dx = side / (max_x - min_x);
    grid_inv_dy = side / (max_y - min_y);

    auto cell = [&](double v, double lo, double inv)
    {
        const double c = std::floor((v - lo) * inv);
        return static_cast<size_t>(std::clamp(c, 0.0, side - 1));
    };

    /// Cap total marking work; a right side of huge boxes gains nothing from the grid.
    static constexpr size_t kMaxMarkedCells = 4 * kGridSide * kGridSide;
    size_t marked = 0;
    std::vector<UInt8> cells(kGridSide * kGridSide, 0);
    for (const auto & [box, pos] : pending_entries)
    {
        const double x0 = boost::geometry::get<boost::geometry::min_corner, 0>(box);
        const double y0 = boost::geometry::get<boost::geometry::min_corner, 1>(box);
        const double x1 = boost::geometry::get<boost::geometry::max_corner, 0>(box);
        const double y1 = boost::geometry::get<boost::geometry::max_corner, 1>(box);
        if (!std::isfinite(x0) || !std::isfinite(y0) || !std::isfinite(x1) || !std::isfinite(y1))
            continue;
        const size_t cx0 = cell(x0, min_x, grid_inv_dx);
        const size_t cx1 = cell(x1, min_x, grid_inv_dx);
        const size_t cy0 = cell(y0, min_y, grid_inv_dy);
        const size_t cy1 = cell(y1, min_y, grid_inv_dy);
        marked += (cx1 - cx0 + 1) * (cy1 - cy0 + 1);
        if (marked > kMaxMarkedCells)
            return;
        for (size_t cy = cy0; cy <= cy1; ++cy)
            std::fill(cells.begin() + cy * kGridSide + cx0, cells.begin() + cy * kGridSide + cx1 + 1, UInt8{1});
    }
    grid_cells = std::move(cells);
}

bool SpatialRTreeJoin::gridMayHit(const BGBox & box) const
{
    if (grid_cells.empty())
        return true;
    const double x0 = boost::geometry::get<boost::geometry::min_corner, 0>(box);
    const double y0 = boost::geometry::get<boost::geometry::min_corner, 1>(box);
    const double x1 = boost::geometry::get<boost::geometry::max_corner, 0>(box);
    const double y1 = boost::geometry::get<boost::geometry::max_corner, 1>(box);
    if (!std::isfinite(x0) || !std::isfinite(y0) || !std::isfinite(x1) || !std::isfinite(y1))
        return true;
    const double side = static_cast<double>(kGridSide);
    const double fx0 = std::floor((x0 - grid_min_x) * grid_inv_dx);
    const double fx1 = std::floor((x1 - grid_min_x) * grid_inv_dx);
    const double fy0 = std::floor((y0 - grid_min_y) * grid_inv_dy);
    const double fy1 = std::floor((y1 - grid_min_y) * grid_inv_dy);
    if (fx1 < 0 || fy1 < 0 || fx0 >= side || fy0 >= side)
        return false; /// entirely outside the right side's extent
    const size_t cx0 = static_cast<size_t>(std::max(fx0, 0.0));
    const size_t cx1 = static_cast<size_t>(std::min(fx1, side - 1));
    const size_t cy0 = static_cast<size_t>(std::max(fy0, 0.0));
    const size_t cy1 = static_cast<size_t>(std::min(fy1, side - 1));
    for (size_t cy = cy0; cy <= cy1; ++cy)
    {
        const UInt8 * row = grid_cells.data() + cy * kGridSide;
        for (size_t cx = cx0; cx <= cx1; ++cx)
            if (row[cx])
                return true;
    }
    return false;
}

void SpatialRTreeJoin::gatherPendingChunks()
{
    size_t total = pending_entries.size();
    std::vector<size_t> offsets;
    offsets.reserve(pending_chunks.size());
    for (const auto & chunk : pending_chunks)
    {
        offsets.push_back(total);
        total += chunk.size();
    }
    pending_entries.resize(total);

    const size_t num_threads = std::min<size_t>(
        std::max<size_t>(std::thread::hardware_concurrency(), 1), pending_chunks.size());
    std::atomic<size_t> next = 0;
    auto copy_chunks = [&]
    {
        for (size_t i = next++; i < pending_chunks.size(); i = next++)
        {
            std::copy(pending_chunks[i].begin(), pending_chunks[i].end(), pending_entries.begin() + offsets[i]);
            std::vector<std::pair<BGBox, RightPos>>().swap(pending_chunks[i]);
        }
    };
    std::vector<std::thread> threads;
    for (size_t t = 1; t < num_threads; ++t)
        threads.emplace_back(copy_chunks);
    copy_chunks();
    for (auto & thread : threads)
        thread.join();
    pending_chunks.clear();
}

namespace
{

/// Position of (x, y) along a Hilbert curve over a 2^16 x 2^16 grid.
UInt32 hilbertIndex(UInt32 x, UInt32 y)
{
    UInt32 a = x ^ y;
    UInt32 b = 0xFFFF ^ a;
    UInt32 c = 0xFFFF ^ (x | y);
    UInt32 d = x & (y ^ 0xFFFF);

    UInt32 A = a | (b >> 1);
    UInt32 B = (a >> 1) ^ a;
    UInt32 C = ((c >> 1) ^ (b & (d >> 1))) ^ c;
    UInt32 D = ((a & (c >> 1)) ^ (d >> 1)) ^ d;

    a = A; b = B; c = C; d = D;
    A = ((a & (a >> 2)) ^ (b & (b >> 2)));
    B = ((a & (b >> 2)) ^ (b & ((a ^ b) >> 2)));
    C ^= ((a & (c >> 2)) ^ (b & (d >> 2)));
    D ^= ((b & (c >> 2)) ^ ((a ^ b) & (d >> 2)));

    a = A; b = B; c = C; d = D;
    A = ((a & (a >> 4)) ^ (b & (b >> 4)));
    B = ((a & (b >> 4)) ^ (b & ((a ^ b) >> 4)));
    C ^= ((a & (c >> 4)) ^ (b & (d >> 4)));
    D ^= ((b & (c >> 4)) ^ ((a ^ b) & (d >> 4)));

    a = A; b = B; c = C; d = D;
    C ^= ((a & (c >> 8)) ^ (b & (d >> 8)));
    D ^= ((b & (c >> 8)) ^ ((a ^ b) & (d >> 8)));

    a = C ^ (C >> 1);
    b = D ^ (D >> 1);

    UInt32 i0 = x ^ y;
    UInt32 i1 = b | (0xFFFF ^ (i0 | a));

    i0 = (i0 | (i0 << 8)) & 0x00FF00FF;
    i0 = (i0 | (i0 << 4)) & 0x0F0F0F0F;
    i0 = (i0 | (i0 << 2)) & 0x33333333;
    i0 = (i0 | (i0 << 1)) & 0x55555555;

    i1 = (i1 | (i1 << 8)) & 0x00FF00FF;
    i1 = (i1 | (i1 << 4)) & 0x0F0F0F0F;
    i1 = (i1 | (i1 << 2)) & 0x33333333;
    i1 = (i1 | (i1 << 1)) & 0x55555555;

    return (i1 << 1) | i0;
}

/// Radix-sorts (key << 32 | payload) words by the 32-bit key only: half the passes of a
/// full 64-bit sort. Little-endian: the key is the upper half of the word.
struct HilbertOrderTraits
{
    using Element = UInt64;
    using Result = Element;
    using Key = UInt32;
    using CountType = UInt32;
    using KeyBits = UInt32;

    static constexpr size_t PART_SIZE_BITS = 8;

    using Transform = RadixSortIdentityTransform<KeyBits>;

    static Key & extractKey(Element & elem) { return reinterpret_cast<UInt32 *>(&elem)[1]; }
    static Result & extractResult(Element & elem) { return elem; }

};

}

SpatialRTreeJoin::RTree::RTree(std::span<const std::vector<Entry>> parts)
{
    size_t total = 0;
    for (const auto & part : parts)
        total += part.size();
    std::vector<const Entry *> sources;
    sources.reserve(total);
    for (const auto & part : parts)
        for (const auto & entry : part)
            sources.push_back(&entry);
    const size_t n = sources.size();
    if (n == 0)
        return;

    Rect extent{
        std::numeric_limits<double>::infinity(), std::numeric_limits<double>::infinity(),
        -std::numeric_limits<double>::infinity(), -std::numeric_limits<double>::infinity()};
    points_only = true;
    for (const auto * entry : sources)
    {
        const Rect r = Rect::of(entry->first);
        points_only = points_only && r.min_x == r.max_x && r.min_y == r.max_y;
        extent.min_x = std::min(extent.min_x, r.min_x);
        extent.min_y = std::min(extent.min_y, r.min_y);
        extent.max_x = std::max(extent.max_x, r.max_x);
        extent.max_y = std::max(extent.max_y, r.max_y);
    }

    /// Sort by the Hilbert position of each box centre. Non-finite boxes (never hit by a
    /// finite probe) get key 0; the order only affects locality, never correctness.
    const double width = extent.max_x - extent.min_x;
    const double height = extent.max_y - extent.min_y;
    const double scale_x = width > 0 && std::isfinite(width) ? 65535.0 / width : 0.0;
    const double scale_y = height > 0 && std::isfinite(height) ? 65535.0 / height : 0.0;
    auto grid = [](double v, double origin, double scale) -> UInt32
    {
        const double g = (v - origin) * scale;
        return g >= 0 && g <= 65535.0 ? static_cast<UInt32>(g) : 0;
    };
    /// Hilbert key in the high half, entry index in the low half: one radix sort of plain integers.
    std::vector<UInt64> order(n);
    for (size_t i = 0; i < n; ++i)
    {
        const Rect r = Rect::of(sources[i]->first);
        const UInt32 gx = grid((r.min_x + r.max_x) / 2, extent.min_x, scale_x);
        const UInt32 gy = grid((r.min_y + r.max_y) / 2, extent.min_y, scale_y);
        order[i] = (UInt64{hilbertIndex(gx, gy)} << 32) | i;
    }
    RadixSort<HilbertOrderTraits>::executeLSD(order.data(), n);
    positions.reserve(n);
    if (points_only)
        leaf_points.reserve(n);
    else
        leaf_rects.reserve(n);
    static constexpr size_t kPrefetchDistance = 16;
    for (size_t i = 0; i < n; ++i)
    {
        if (i + kPrefetchDistance < n)
            __builtin_prefetch(sources[static_cast<UInt32>(order[i + kPrefetchDistance])]);
        const Entry & entry = *sources[static_cast<UInt32>(order[i])];
        positions.push_back(entry.second);
        if (points_only)
            leaf_points.push_back({entry.first.min_corner().x(), entry.first.min_corner().y()});
        else
            leaf_rects.push_back(Rect::of(entry.first));
    }

    /// Bottom-up node boxes: each node covers kNodeSize consecutive children.
    level_offsets.push_back(0);
    size_t child_count = n;
    auto child_rect = [&](size_t level, size_t i)
    {
        if (level != 0)
            return nodes[level_offsets[level - 1] + i];
        if (points_only)
            return Rect{leaf_points[i].x, leaf_points[i].y, leaf_points[i].x, leaf_points[i].y};
        return leaf_rects[i];
    };
    for (size_t level = 0;; ++level)
    {
        const size_t node_count = (child_count + kNodeSize - 1) / kNodeSize;
        for (size_t node = 0; node < node_count; ++node)
        {
            Rect r = child_rect(level, node * kNodeSize);
            const size_t last = std::min(node * kNodeSize + kNodeSize, child_count);
            for (size_t i = node * kNodeSize + 1; i < last; ++i)
            {
                const Rect c = child_rect(level, i);
                r.min_x = std::min(r.min_x, c.min_x);
                r.min_y = std::min(r.min_y, c.min_y);
                r.max_x = std::max(r.max_x, c.max_x);
                r.max_y = std::max(r.max_y, c.max_y);
            }
            nodes.push_back(r);
        }
        level_offsets.push_back(nodes.size());
        if (node_count == 1)
            break;
        child_count = node_count;
    }
}

void SpatialRTreeJoin::runPostBuildPhase()
{
    /// Bulk-load sub-trees in parallel using the packing (STR) algorithm.
    /// Called once by FillingRightJoinSideTransform (single thread) before any joinBlock(),
    /// so no locking is needed here or in joinBlock() when reading sub_trees.
    ///
    /// Splitting into K chunks and building K trees in parallel eliminates the long
    /// single-CPU period that precedes parallel probe when the right side is large.
    ///
    /// For small right sides (≤ 200K entries) the build is already fast and the per-lookup
    /// cost of querying K trees instead of 1 dominates; use a single tree in that case.
    Stopwatch sw_build;
    static constexpr size_t kMaxPartitionedEntries = 1'000'000;
    size_t chunked_entries = 0;
    for (const auto & chunk : pending_chunks)
        chunked_entries += chunk.size();
    if (pending_entries.empty() && chunked_entries > kMaxPartitionedEntries)
    {
        buildSubTreesFromChunks(chunked_entries);
        LOG_TRACE(getLogger("SpatialRTreeJoin"), "runPostBuildPhase: entries={} trees={} build={:.3f}s",
            chunked_entries, sub_trees.size(), sw_build.elapsedSeconds());
        return;
    }
    gatherPendingChunks();
    const size_t n = pending_entries.size();
    if (n == 0)
        return;

    buildOccupancyGrid();

    static constexpr size_t kParallelThreshold = 200'000;
    const size_t num_parts = (n <= kParallelThreshold)
        ? 1
        : std::max(size_t{1}, std::min(static_cast<size_t>(std::thread::hardware_concurrency()), n));
    sub_trees.resize(num_parts);

    /// Up to kMaxPartitionedEntries, partition entries into spatially disjoint slices
    /// (kd bisection on box centres, alternating axes), so each sub-tree covers its own
    /// region: a probe is rejected at the root of every tree it cannot touch instead of
    /// descending into all of them. Above it, the serial top-level nth_element costs more
    /// than probing saves, so slices stay contiguous in arrival order.
    auto build_slice = [this](size_t part, size_t begin, size_t end)
    {
        const std::vector<RTree::Entry> slice(pending_entries.begin() + begin, pending_entries.begin() + end);
        sub_trees[part] = RTree(std::span(&slice, 1));
    };

    if (n > kMaxPartitionedEntries)
    {
        const size_t chunk = (n + num_parts - 1) / num_parts;
        std::vector<std::thread> threads;
        threads.reserve(num_parts);
        for (size_t t = 0; t < num_parts; ++t)
            threads.emplace_back(build_slice, t, t * chunk, std::min(t * chunk + chunk, n));
        for (auto & th : threads)
            th.join();
    }
    else
    {
        auto centre = [](const BGBox & box, int axis)
        {
            /// NaN would break nth_element's strict weak ordering; such boxes are never hit anyway.
            const double c = axis == 0
                ? boost::geometry::get<boost::geometry::min_corner, 0>(box) + boost::geometry::get<boost::geometry::max_corner, 0>(box)
                : boost::geometry::get<boost::geometry::min_corner, 1>(box) + boost::geometry::get<boost::geometry::max_corner, 1>(box);
            return std::isnan(c) ? std::numeric_limits<double>::infinity() : c;
        };
        /// Each leaf of the bisection builds its sub-tree on the thread that produced it.
        std::function<void(size_t, size_t, size_t, size_t, int)> partition_and_build
            = [&](size_t begin, size_t end, size_t first_part, size_t parts, int axis)
        {
            if (parts == 1)
            {
                build_slice(first_part, begin, end);
                return;
            }
            const size_t left_parts = parts / 2;
            const size_t mid = begin + (end - begin) * left_parts / parts;
            std::nth_element(
                pending_entries.begin() + begin, pending_entries.begin() + mid, pending_entries.begin() + end,
                [&](const auto & a, const auto & b) { return centre(a.first, axis) < centre(b.first, axis); });
            std::thread left([&, begin, mid, first_part, left_parts, axis]
                             { partition_and_build(begin, mid, first_part, left_parts, 1 - axis); });
            partition_and_build(mid, end, first_part + left_parts, parts - left_parts, 1 - axis);
            left.join();
        };
        partition_and_build(0, n, 0, num_parts, 0);
    }

    pending_entries.clear();
    pending_entries.shrink_to_fit();
    LOG_TRACE(getLogger("SpatialRTreeJoin"), "runPostBuildPhase: entries={} trees={} build={:.3f}s",
        n, sub_trees.size(), sw_build.elapsedSeconds());

    allocateRightMatched();
}

void SpatialRTreeJoin::allocateRightMatched()
{
    /// For RIGHT JOIN: allocate per-block matched byte arrays (all 0 = unmatched).
    if (table_join->kind() == JoinKind::Right)
    {
        right_matched_per_block.resize(right_blocks.size());
        for (size_t i = 0; i < right_blocks.size(); ++i)
            right_matched_per_block[i].assign(right_blocks[i].rows(), 0);
    }
}

void SpatialRTreeJoin::buildSubTreesFromChunks(size_t total_entries)
{
    /// Arrival-order slices need no global ordering, so each sub-tree is packed straight
    /// from its own run of chunks; the whole right side is never gathered into one vector.
    grid_cells.clear();
    const size_t num_parts = std::min<size_t>(
        std::max<size_t>(std::thread::hardware_concurrency(), 1), pending_chunks.size());
    sub_trees.resize(num_parts);

    std::vector<size_t> first_chunk(num_parts + 1, pending_chunks.size());
    first_chunk[0] = 0;
    size_t part = 0;
    size_t seen = 0;
    for (size_t i = 0; i < pending_chunks.size() && part + 1 < num_parts; ++i)
    {
        seen += pending_chunks[i].size();
        if (seen >= total_entries * (part + 1) / num_parts)
            first_chunk[++part] = i + 1;
    }
    for (size_t p = part + 1; p < num_parts; ++p)
        first_chunk[p] = pending_chunks.size();

    auto build_part = [&](size_t p)
    {
        const std::span<const std::vector<std::pair<BGBox, RightPos>>> chunks(
            pending_chunks.data() + first_chunk[p], first_chunk[p + 1] - first_chunk[p]);
        sub_trees[p] = RTree(chunks);
        for (size_t i = first_chunk[p]; i < first_chunk[p + 1]; ++i)
            std::vector<std::pair<BGBox, RightPos>>().swap(pending_chunks[i]);
    };
    std::vector<std::thread> threads;
    threads.reserve(num_parts);
    for (size_t p = 1; p < num_parts; ++p)
        threads.emplace_back(build_part, p);
    build_part(0);
    for (auto & thread : threads)
        thread.join();
    pending_chunks.clear();
    allocateRightMatched();
}

JoinResultPtr SpatialRTreeJoin::joinBlock(Block left_block)
{
    ++active_probes;
    SCOPE_EXIT({ --active_probes; });
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

    /// Column pointers resolved once: the per-candidate loops below would otherwise look every
    /// column up by name for every row they copy.
    std::unordered_map<String, std::vector<const IColumn *>> right_columns_by_name;
    for (const auto & rcd : *right_header)
    {
        auto & per_block = right_columns_by_name[rcd.name];
        per_block.reserve(right_blocks.size());
        for (const auto & rb : right_blocks)
            per_block.push_back(rb.has(rcd.name) ? rb.getByName(rcd.name).column.get() : nullptr);
    }
    std::vector<const IColumn *> out_left_columns(n_out_cols, nullptr);
    std::vector<const std::vector<const IColumn *> *> out_right_columns(n_out_cols, nullptr);
    for (size_t i = 0; i < n_out_cols; ++i)
    {
        if (col_sources[i].from_left)
            out_left_columns[i] = left_block.getByName(col_sources[i].name).column.get();
        else
            out_right_columns[i] = &right_columns_by_name.at(col_sources[i].name);
    }

    MutableColumns mutable_cols(n_out_cols);
    for (size_t i = 0; i < n_out_cols; ++i)
        mutable_cols[i] = IColumn::mutate(output.getByPosition(i).column);

    std::vector<bool> left_matched(is_left_join ? left_rows : 0, false);
    /// For RIGHT JOIN, right_matched_per_block[block][row] is marked with relaxed-atomic
    /// byte writes.  Multiple probe threads may race to set the same byte to 1, but since
    /// all writers write the same value the race is benign.  getNonJoinedBlocks() reads
    /// the bitmap only after all joinBlock() calls have returned (pipeline barrier).

    /// Only the columns the spatial predicate reads are copied into its input block; the rest
    /// of a candidate row is materialised once, at output, and only if it matched.
    NameSet predicate_required;
    for (const auto & name : table_join->getMixedJoinExpression()->getRequiredColumns())
        predicate_required.insert(name);

    NameSet pre_filter_required;
    if (pre_filter_expr)
        for (const auto & col : pre_filter_expr->getRequiredColumns())
            pre_filter_required.insert(col);

    static constexpr size_t kChunkSize     = 65536;       // WASM batch size for large-row streaming
    static constexpr size_t kLargeRowBatch = 16 * kChunkSize; // large-row hits buffered per parallel flush
    static constexpr size_t kLargeHit      = 4'000'000;   // per-row hit limit before streaming mode
    static constexpr size_t kMaxCandidates = 8'000'000;   // shared buffer overflow guard

    Stopwatch sw_join;
    std::atomic<UInt64> ns_join_wasm = 0;
    std::atomic<size_t> join_wasm_calls = 0;

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

    /// Runs `evaluate` for every task. Tasks are independent predicate calls writing disjoint
    /// slots of the mask. With `parallel` set they are spread over a shared pool from the start;
    /// otherwise the calling thread works through them alone, but recruits pool threads as soon
    /// as most other probe streams have finished, so the last busy stream is not left alone.
    auto runTasks = [&](const auto & tasks, bool parallel, auto && evaluate)
    {
        static constexpr size_t kMaxParallelTasks = 8;
        const size_t hardware_threads = std::max<size_t>(std::thread::hardware_concurrency(), 2);
        std::atomic<size_t> next = 0;
        auto worker = [&tasks, &evaluate, &next]
        {
            for (size_t i = next++; i < tasks.size(); i = next++)
                evaluate(tasks[i]);
        };
        std::optional<ThreadPoolCallbackRunnerLocal<void>> runner;
        auto recruit = [&]
        {
            const size_t remaining = tasks.size() - std::min(next.load(), tasks.size());
            const size_t helpers = std::min(kMaxParallelTasks, remaining) - (remaining > 0 ? 1 : 0);
            if (helpers == 0)
                return;
            runner.emplace(getGroupPool(), ThreadName::SPATIAL_JOIN);
            for (size_t t = 0; t < helpers; ++t)
                runner->enqueueAndKeepTrack(worker);
        };
        const size_t recruit_below = hardware_threads / 4;
        if (parallel)
            recruit();
        for (size_t i = next++; i < tasks.size(); i = next++)
        {
            evaluate(tasks[i]);
            if (!runner && active_probes.load(std::memory_order_relaxed) <= recruit_below)
                recruit();
        }
        if (runner)
            runner->waitForAllToFinishAndRethrowFirstError();
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

        // Candidates are appended in ascending left_row order, so each left row's candidates form one
        // contiguous run. When runs are long (few left rows, many hits each), grouping by left is the
        // obvious choice and the per-candidate hash of right rows below would only cost time.
        size_t left_runs = 0;
        bool left_sorted = true;
        for (size_t ci = 0; ci < n; ++ci)
        {
            if (ci == 0 || cands[ci].left_row != cands[ci - 1].left_row)
            {
                ++left_runs;
                if (ci != 0 && cands[ci].left_row < cands[ci - 1].left_row)
                    left_sorted = false;
            }
        }
        static constexpr size_t kMinAverageLeftRun = 4;
        const bool group_by_left_runs = left_sorted && left_runs * kMinAverageLeftRun <= n;

        // Count unique right rows to decide which side becomes ColumnConst.
        std::unordered_map<uint64_t, std::vector<size_t>> right_groups;
        if (!group_by_left_runs)
        {
            right_groups.reserve(std::min(n, static_cast<size_t>(4096)));
            for (size_t ci = 0; ci < n; ++ci)
            {
                uint64_t key = static_cast<uint64_t>(cands[ci].block_idx) * 0x100000000ULL + cands[ci].row_idx;
                right_groups[key].push_back(ci);
            }
        }
        const bool group_by_right = !group_by_left_runs && right_groups.size() <= left_rows;

        IColumn::Filter mask(n, 0);

        if (group_by_right)
        {
            std::vector<const std::vector<size_t> *> groups;
            groups.reserve(right_groups.size());
            for (const auto & [key, indices] : right_groups)
                groups.push_back(&indices);
            runTasks(groups, /*parallel=*/ false, [&](const std::vector<size_t> * group)
            {
                const auto & indices = *group;
                const size_t k = indices.size();
                const auto & c0 = cands[indices[0]];
                const Block & rb = right_blocks[c0.block_idx];

                Block pred_block;
                for (const auto & src_col : left_block)
                {
                    if (!predicate_required.contains(src_col.name))
                        continue;
                    auto data = src_col.type->createColumn();
                    data->reserve(k);
                    for (size_t ci : indices)
                        data->insertFrom(*src_col.column, cands[ci].left_row);
                    pred_block.insert({std::move(data), src_col.type, src_col.name});
                }
                for (const auto & rcd : *right_header)
                {
                    if (pred_block.has(rcd.name) || !predicate_required.contains(rcd.name))
                        continue;
                    ColumnPtr single = rb.getByName(rcd.name).column->cut(c0.row_idx, 1);
                    pred_block.insert({ColumnConst::create(std::move(single), k), rcd.type, rcd.name});
                }
                {
                    const UInt64 t0 = clock_gettime_ns();
                    table_join->getMixedJoinExpression()->execute(pred_block);
                    ns_join_wasm += clock_gettime_ns() - t0;
                    ++join_wasm_calls;
                }
                const auto & fc = *pred_block.getByName(filter_col_name).column;
                for (size_t j = 0; j < k; ++j)
                    mask[indices[j]] = fc.getBool(j) ? 1 : 0;
            });
        }
        else
        {
            std::vector<std::pair<size_t, std::vector<size_t>>> left_groups;
            if (group_by_left_runs)
            {
                left_groups.reserve(left_runs);
                for (size_t ci = 0; ci < n; ++ci)
                {
                    if (ci == 0 || cands[ci].left_row != cands[ci - 1].left_row)
                        left_groups.emplace_back(cands[ci].left_row, std::vector<size_t>{});
                    left_groups.back().second.push_back(ci);
                }
            }
            else
            {
                std::unordered_map<size_t, std::vector<size_t>> by_left_row;
                by_left_row.reserve(left_rows);
                for (size_t ci = 0; ci < n; ++ci)
                    by_left_row[cands[ci].left_row].push_back(ci);
                left_groups.assign(std::make_move_iterator(by_left_row.begin()), std::make_move_iterator(by_left_row.end()));
            }

            /// A left geometry whose box covers a dense part of the right side can collect
            /// millions of candidates; evaluated as one call it pins its probe stream long
            /// after the others have finished. Such groups are cut into slices that run on
            /// the pool, so the work of a single heavy row is shared by idle threads.
            static constexpr size_t kSliceRows = 32'768;
            static constexpr size_t kParallelGroupRows = 2 * kSliceRows;
            struct Slice { size_t left_row; const std::vector<size_t> * indices; size_t begin; size_t end; };
            std::vector<Slice> slices;
            slices.reserve(left_groups.size());
            bool has_heavy_group = false;
            for (const auto & [left_row, indices] : left_groups)
            {
                has_heavy_group |= indices.size() >= kParallelGroupRows;
                for (size_t begin = 0; begin < indices.size(); begin += kSliceRows)
                    slices.push_back({left_row, &indices, begin, std::min(begin + kSliceRows, indices.size())});
            }
            runTasks(slices, has_heavy_group, [&](const Slice & slice)
            {
                const size_t left_row = slice.left_row;
                const auto & indices = *slice.indices;
                const size_t k = slice.end - slice.begin;
                Block pred_block;
                for (const auto & src_col : left_block)
                {
                    if (!predicate_required.contains(src_col.name))
                        continue;
                    ColumnPtr single = src_col.column->cut(left_row, 1);
                    pred_block.insert({ColumnConst::create(std::move(single), k), src_col.type, src_col.name});
                }
                for (const auto & rcd : *right_header)
                {
                    if (pred_block.has(rcd.name) || !predicate_required.contains(rcd.name))
                        continue;
                    auto data = rcd.type->createColumn();
                    data->reserve(k);
                    const auto & source = right_columns_by_name.at(rcd.name);
                    for (size_t j = slice.begin; j < slice.end; ++j)
                        data->insertFrom(*source[cands[indices[j]].block_idx], cands[indices[j]].row_idx);
                    pred_block.insert({std::move(data), rcd.type, rcd.name});
                }
                {
                    const UInt64 t0 = clock_gettime_ns();
                    table_join->getMixedJoinExpression()->execute(pred_block);
                    ns_join_wasm += clock_gettime_ns() - t0;
                    ++join_wasm_calls;
                }
                const auto & fc = *pred_block.getByName(filter_col_name).column;
                for (size_t j = 0; j < k; ++j)
                    mask[indices[slice.begin + j]] = fc.getBool(j) ? 1 : 0;
            });
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
                if (out_left_columns[i])
                    mutable_cols[i]->insertFrom(*out_left_columns[i], c.left_row);
                else
                    mutable_cols[i]->insertFrom(*(*out_right_columns[i])[c.block_idx], c.row_idx);
            }
        }

        cands.clear();
    };

    /// Process a single left row's hit list in kChunkSize-sized batches.
    /// Used when one left row has ≥ kLargeHit R-tree results (large-row mode).
    /// Left row is ColumnConst per batch (PreparedGeometry for left geometry).
    /// Batches are independent predicate calls, so they run on the shared pool;
    /// matches are emitted afterwards in hit order.
    auto processLargeRow = [&](size_t li, std::vector<RightPos> & row_hits)
    {
        struct Batch { size_t begin; size_t end; };
        std::vector<Batch> batches;
        for (size_t h = 0; h < row_hits.size(); h += kChunkSize)
            batches.push_back({h, std::min(h + kChunkSize, row_hits.size())});

        std::vector<UInt8> mask(row_hits.size(), 0);
        runTasks(batches, batches.size() > 1, [&](const Batch & batch)
        {
            const size_t h = batch.begin;
            const size_t chunk_end = batch.end;
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
                    const auto & src_columns = right_columns_by_name.at(rcd.name);
                    auto col = rcd.type->createColumn();
                    col->reserve(k);
                    for (size_t j = h; j < chunk_end; ++j)
                        col->insertFrom(*src_columns[row_hits[j].block_idx], row_hits[j].row_idx);
                    pre_block.insert({std::move(col), rcd.type, rcd.name});
                }
                pre_filter_expr->execute(pre_block);
                const auto & pfc = *pre_block.getByName(pre_filter_col_name).column;
                surviving.reserve(k);
                for (size_t j = 0; j < k; ++j)
                    if (pfc.getBool(j))
                        surviving.push_back(h + j);
                if (surviving.empty())
                    return;
            }

            const size_t eff_k = pre_filter_expr ? surviving.size() : k;
            Block pred_block;
            for (const auto & src_col : left_block)
            {
                if (!predicate_required.contains(src_col.name))
                    continue;
                ColumnPtr single = src_col.column->cut(li, 1);
                pred_block.insert({ColumnConst::create(std::move(single), eff_k), src_col.type, src_col.name});
            }
            for (const auto & rcd : *right_header)
            {
                if (pred_block.has(rcd.name) || !predicate_required.contains(rcd.name))
                    continue;
                const auto & src_columns = right_columns_by_name.at(rcd.name);
                auto col = rcd.type->createColumn();
                col->reserve(eff_k);
                if (pre_filter_expr)
                    for (size_t idx : surviving)
                        col->insertFrom(*src_columns[row_hits[idx].block_idx], row_hits[idx].row_idx);
                else
                    for (size_t j = h; j < chunk_end; ++j)
                        col->insertFrom(*src_columns[row_hits[j].block_idx], row_hits[j].row_idx);
                pred_block.insert({std::move(col), rcd.type, rcd.name});
            }
            {
                const UInt64 t0 = clock_gettime_ns();
                table_join->getMixedJoinExpression()->execute(pred_block);
                ns_join_wasm += clock_gettime_ns() - t0;
                ++join_wasm_calls;
            }
            const auto & fc = *pred_block.getByName(filter_col_name).column;
            for (size_t j = 0; j < eff_k; ++j)
                mask[pre_filter_expr ? surviving[j] : (h + j)] = fc.getBool(j) ? 1 : 0;
        });

        for (size_t hit_idx = 0; hit_idx < row_hits.size(); ++hit_idx)
        {
            if (!mask[hit_idx])
                continue;
            const RightPos & pos = row_hits[hit_idx];
            if (is_left_join)
                left_matched[li] = true;
            if (is_right_join)
                __atomic_store_n(&right_matched_per_block[pos.block_idx][pos.row_idx], '\1', __ATOMIC_RELAXED);
            for (size_t ci = 0; ci < n_out_cols; ++ci)
            {
                if (col_sources[ci].from_left)
                    mutable_cols[ci]->insertFrom(*out_left_columns[ci], li);
                else
                    mutable_cols[ci]->insertFrom(*(*out_right_columns[ci])[pos.block_idx], pos.row_idx);
            }
        }
    };

    // ── main probe loop ───────────────────────────────────────────────────────

    /// Per-left-row hit buffer.  Populated lazily one entry at a time so we can
    /// detect and handle large-hit-count rows before materialising them fully.
    std::vector<RightPos> row_buf;
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

        if (!gridMayHit(bbox))
            continue;

        for (const auto & sub_tree : sub_trees)
        {
            sub_tree.query(bbox, [&](const RightPos & hit)
            {
                row_buf.push_back(hit);

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

                if (is_large && row_buf.size() >= kLargeRowBatch)
                {
                    processLargeRow(li, row_buf);
                    row_buf.clear();
                }
            });
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
            for (const auto & pos : row_buf)
                candidates.push_back({li, pos.block_idx, pos.row_idx});

            if (candidates.size() >= kMaxCandidates)
                evaluateAndEmit(candidates);
        }
    }
    evaluateAndEmit(candidates); // emit all accumulated candidates

    LOG_TRACE(getLogger("SpatialRTreeJoin"),
        "joinBlock: total={:.3f}s wasm={:.3f}s wasm_calls={} left_rows={}",
        sw_join.elapsedSeconds(), static_cast<double>(ns_join_wasm.load()) / 1e9,
        join_wasm_calls.load(), left_rows);

    for (size_t i = 0; i < n_out_cols; ++i)
        output.getByPosition(i).column = std::move(mutable_cols[i]);

    if (is_left_join)
        appendUnmatched(output, left_matched);

    return IJoinResult::createFromBlock(std::move(output));
}

SpatialRTreeJoin::~SpatialRTreeJoin() = default;

ThreadPool & SpatialRTreeJoin::getGroupPool()
{
    std::call_once(group_pool_once, [this]
    {
        group_pool = std::make_unique<ThreadPool>(
            CurrentMetrics::SpatialJoinThreads,
            CurrentMetrics::SpatialJoinThreadsActive,
            CurrentMetrics::SpatialJoinThreadsScheduled,
            std::thread::hardware_concurrency());
    });
    return *group_pool;
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
