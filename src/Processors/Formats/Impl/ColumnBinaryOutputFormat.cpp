#include <Processors/Formats/Impl/ColumnBinaryOutputFormat.h>

#include <Core/Block.h>
#include <Processors/Port.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>

#include <Columns/ColumnConst.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnSparse.h>

#include <Common/typeid_cast.h>
#include <Common/Exception.h>
#include <Formats/ColumnBinaryWire.h>

#include <limits>

namespace DB
{

namespace ErrorCodes
{
    extern const int INCORRECT_DATA;
}

// TODO(ColumnBinary settings): add a FormatSettings knob for diagnostics/benchmarking:
//   column_binary_disable_preallocation  — return std::nullopt here to fall through to
//     CH's normal heap-allocation path (eliminates the conservative-size scan entirely;
//     useful to measure the overhead of the two-phase layout vs. a plain WriteBuffer).

void ColumnBinaryOutputFormat::checkNumCols(size_t num_cols) const
{
    if (num_cols != header_->columns())
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "ColumnBinary: block has {} columns, expected {}",
            num_cols, header_->columns());
}

void ColumnBinaryOutputFormat::checkColumnStructure(size_t i, const IColumn & column) const
{
    // Mirror `ColumnBinaryInputFormat`'s read-side check exactly: the reader decodes each
    // column against `header_->getByPosition(i).type` and rejects a structural mismatch, so a
    // writer that only enforces the column *count* fails open on the public
    // `IOutputFormat::write` path - a formatter built with a `UInt64` header would happily
    // serialize a same-count `String` block, emitting a frame that disagrees with the
    // advertised sample header and that the matching reader then refuses. Require the same
    // exact schema the reader does, so the mismatch is caught before the frame is written
    // rather than after it is read back.
    // `COL_IS_CONST` legitimately serializes a `ColumnConst` wrapper, which never structurally
    // equals the plain column the declared type creates; compare what it wraps, as the reader
    // does on its side.
    const IColumn & actual = isColumnConst(column)
        ? static_cast<const ColumnConst &>(column).getDataColumn()
        : column;
    const auto & expected_type = header_->getByPosition(i).type;
    if (!actual.structureEquals(*expected_type->createColumn()))
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "ColumnBinary: column {} is {}, which does not match the declared type {}",
            i, actual.getName(), expected_type->getName());
}

bool ColumnBinaryOutputFormat::supportsRowRange(const Block & block) const
{
    // Without preallocation the caller has no size to work with, so there is nothing a row range
    // could be used for; report the plain whole-block contract instead.
    if (disable_preallocation_)
        return false;

    for (const auto & elem : block.getColumnsWithTypeAndName())
    {
        // A `ColumnConst` serializes its single data row whatever range is asked for, so it never
        // constrains range addressing.
        if (isColumnConst(*elem.column))
            continue;
        if (!ColumnBinaryWire::supportsRowOffset(*removeSpecialRepresentations(elem.column)))
            return false;
    }
    return true;
}

std::optional<uint64_t> ColumnBinaryOutputFormat::precomputeSerializedSize(const Block & block, size_t row_offset, size_t rows) const
{
    if (disable_preallocation_)
        return std::nullopt;

    if (rows == 0 || block.columns() == 0)
        return std::nullopt;

    // Mirror consume()'s exact-match requirement: otherwise the size probe and the write
    // could model a different number of columns, and the buffered WASM path would reserve a
    // guest buffer that consume() does not fill.
    checkNumCols(block.columns());

    checkRowCount(rows);

    const uint64_t hdr_desc_size = ColumnBinaryWire::FRAME_HEADER_BYTES + block.columns() * ColumnBinaryWire::COL_DESC_BYTES;
    uint64_t cursor = hdr_desc_size;

    for (size_t i = 0; i < block.columns(); ++i)
    {
        // Strip `Sparse` / `Replicated` wrappers exactly as `consume` does below (keeping the
        // const wrapper), so both passes model the same layout. `buildColDescriptor` has no
        // notion of them: a sparse `String` would miss the `ColumnString` branch and throw,
        // and a sparse fixed-width column would be mis-sized, since
        // `ColumnSparse::sizeOfValueIfFixed` reports the value plus offset width. Today the
        // only caller is the buffered WASM path, whose function does not override
        // `useDefaultImplementationForSparseColumns` / `...ForReplicatedColumns`, so
        // `IExecutableFunction` has already removed both before `executeImpl` runs - but the
        // two passes must not disagree about the frame size if that ever changes.
        const ColumnPtr & raw_ptr = block.getByPosition(i).column;
        bool is_const = isColumnConst(*raw_ptr);
        ColumnPtr stripped = is_const
            ? removeSpecialRepresentations(static_cast<const ColumnConst &>(*raw_ptr).getDataColumnPtr())
            : removeSpecialRepresentations(raw_ptr);
        const IColumn * actual = stripped.get();
        checkColumnStructure(i, *actual);
        bool is_nullable = typeid_cast<const ColumnNullable *>(actual) != nullptr;
        uint32_t col_rows = is_const ? 1u : static_cast<uint32_t>(rows);
        // A const column carries its one data row at index 0 regardless of the range asked for.
        uint32_t col_offset = is_const ? 0u : static_cast<uint32_t>(row_offset);

        ColumnBinaryWire::ColDescriptor desc{};
        cursor = ColumnBinaryWire::buildColDescriptor(
            actual, is_const, is_nullable, col_rows, cursor, desc, /* gaps */ nullptr, col_offset);
    }

    return cursor;
}

void ColumnBinaryOutputFormat::consume(Chunk chunk)
{
    if (!chunk)
        return;

    // See the matching check in precomputeSerializedSize: reject before the narrowing cast
    // rather than silently truncating/wrapping the row count.
    checkRowCount(chunk.getNumRows());

    // `ColumnBinary` is schema-driven: `ColumnBinaryInputFormat::checkNumCols` rejects any
    // frame whose `num_cols` differs from the schema. Clamping to the smaller of the two here
    // would fail open on the public `IOutputFormat::write` path - extra columns silently
    // dropped, missing columns emitting a frame that disagrees with the advertised sample
    // header (and that the matching reader then refuses) - so require the exact match the
    // reader does.
    checkNumCols(chunk.getNumColumns());

    size_t num_rows = chunk.getNumRows();
    writeFrame(chunk.detachColumns(), 0, num_rows);
}

void ColumnBinaryOutputFormat::consumeRowRange(const Block & block, size_t row_offset, size_t rows)
{
    // A layout `buildColDescriptor` cannot address by row (Array, Tuple, LowCardinality, ...) has
    // to be sliced out first; the base implementation does exactly that and comes back through
    // `consume`.
    if (!supportsRowRange(block))
    {
        IOutputFormat::consumeRowRange(block, row_offset, rows);
        return;
    }

    if (rows == 0)
        return;

    checkRowCount(rows);
    checkNumCols(block.columns());
    writeFrame(block.getColumns(), row_offset, rows);
}

void ColumnBinaryOutputFormat::checkRowCount(size_t rows) const
{
    // The frame header's num_rows field is a uint32_t and the descriptor arithmetic is only
    // overflow-safe below that, so reject rather than silently truncating the row count.
    if (rows >= std::numeric_limits<uint32_t>::max())
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "ColumnBinary: frame has {} rows, exceeding the maximum representable row count ({})",
            rows, std::numeric_limits<uint32_t>::max());
}

void ColumnBinaryOutputFormat::writeFrame(Columns columns, size_t row_offset, size_t rows)
{
    uint32_t num_rows = static_cast<uint32_t>(rows);
    uint32_t num_cols = static_cast<uint32_t>(columns.size());

    // Layout pass: build descriptors (compute offsets and total size).
    const uint64_t hdr_desc_size = ColumnBinaryWire::FRAME_HEADER_BYTES + num_cols * ColumnBinaryWire::COL_DESC_BYTES;
    uint64_t cursor = hdr_desc_size;

    // `expectMaterializedColumns` returns false for this format so that a top-level
    // `ColumnConst` survives into `COL_IS_CONST`, but that also skips the pipeline's
    // `MaterializingTransform` entirely, so `ColumnSparse` and `ColumnReplicated` wrappers reach
    // the writer unchanged. `buildColDescriptor` has no notion of them: a sparse `UInt64` falls
    // through to the fixed-width path, whose `writeColData` calls `IColumn::getRawData`, and that
    // throws `NOT_IMPLEMENTED` for `ColumnSparse`. Strip those wrappers here - recursively, so
    // nested columns inside `Tuple` / `Array` are covered too - while keeping the const wrapper.
    for (auto & col : columns)
    {
        if (const auto * const_col = typeid_cast<const ColumnConst *>(col.get()))
        {
            auto data = removeSpecialRepresentations(const_col->getDataColumnPtr());
            if (data.get() != const_col->getDataColumnPtr().get())
                col = ColumnConst::create(std::move(data), const_col->size());
        }
        else
            col = removeSpecialRepresentations(col);
    }

    std::vector<ColumnBinaryWire::ColDescriptor> descs(num_cols);
    std::vector<uint32_t> col_offsets(num_cols);
    ColumnBinaryWire::PaddingGaps gaps;
    for (uint32_t i = 0; i < num_cols; ++i)
    {
        const IColumn & raw_col = *columns[i];
        bool is_const = isColumnConst(raw_col);
        const IColumn * actual = is_const
            ? &static_cast<const ColumnConst &>(raw_col).getDataColumn()
            : &raw_col;
        checkColumnStructure(i, *actual);
        bool is_nullable = typeid_cast<const ColumnNullable *>(actual) != nullptr;
        uint32_t col_rows = is_const ? 1u : num_rows;
        // A const column carries its one data row at index 0 regardless of the range asked for.
        col_offsets[i] = is_const ? 0u : static_cast<uint32_t>(row_offset);
        cursor = ColumnBinaryWire::buildColDescriptor(
            actual, is_const, is_nullable, col_rows, cursor, descs[i], &gaps, col_offsets[i]);
    }

    // Get write destination: use the pre-allocated region in out when available,
    // otherwise fall back to a temporary buffer (e.g. when the caller did not
    // pre-allocate via precomputeSerializedSize, such as in tests or the legacy
    // IOutputFormat::write() compatibility path).
    std::vector<uint8_t> tmp_buf;
    uint8_t * buf = nullptr;
    bool use_prealloc = !disable_preallocation_ && out.available() >= cursor;
    if (!use_prealloc)
    {
        tmp_buf.resize(cursor);
        buf = tmp_buf.data();
    }
    else
    {
        // Unlike tmp_buf (std::vector::resize value-initializes to 0), the
        // WriteBuffer's internal buffer is not zeroed. Every byte a descriptor
        // points at is overwritten below, so the only bytes that would leak
        // uninitialized memory are the alignment padding gaps writeColData
        // never touches - and the layout pass recorded exactly those. Zeroing
        // them instead of the whole frame keeps this to a few bytes per column.
        buf = reinterpret_cast<uint8_t *>(out.position());
        for (const auto & [gap_offset, gap_size] : gaps)
            std::memset(buf + gap_offset, 0, gap_size);
    }

    // Write header and descriptor table.
    ColumnBinaryWire::writeFrameHeader(buf, num_rows, num_cols);
    std::memcpy(buf + ColumnBinaryWire::FRAME_HEADER_BYTES,
                descs.data(),
                num_cols * ColumnBinaryWire::COL_DESC_BYTES);

    // Write column data.
    std::span<uint8_t> buf_span{buf, cursor};
    for (uint32_t i = 0; i < num_cols; ++i)
    {
        const IColumn & raw_col = *columns[i];
        bool is_const = isColumnConst(raw_col);
        const IColumn * actual = is_const
            ? &static_cast<const ColumnConst &>(raw_col).getDataColumn()
            : &raw_col;
        bool is_nullable = typeid_cast<const ColumnNullable *>(actual) != nullptr;
        uint32_t col_rows = is_const ? 1u : num_rows;
        ColumnBinaryWire::writeColData(actual, is_nullable, col_rows, descs[i], buf_span, col_offsets[i]);
    }

    if (!use_prealloc)
        out.write(reinterpret_cast<char *>(buf), cursor);
    else
        out.position() += cursor;
}

ColumnBinaryOutputFormat::ColumnBinaryOutputFormat(WriteBuffer & out_, SharedHeader header,
                                                   bool disable_preallocation)
    : IOutputFormat(header, out_)
    , header_(header)
    , disable_preallocation_(disable_preallocation)
{
    // Reject unsupported signatures (nested Nullable/Variant, Map, >8-byte fixed-width
    // types) here so callers find out at format construction, not on the first block.
    for (const auto & col : header_->getColumnsWithTypeAndName())
        ColumnBinaryWire::validateColumnBinaryWireSupportedType(col.type);
}

void registerOutputFormatColumnBinary(FormatFactory & factory)
{
    factory.registerOutputFormat("ColumnBinary", [](
        WriteBuffer & buf,
        const Block & sample,
        const FormatSettings & format_settings,
        FormatFilterInfoPtr /*format_filter_info*/)
    {
        ColumnBinaryWire::checkColumnBinaryFormatIsAllowed(format_settings.column_binary.allow_experimental);
        return std::make_shared<ColumnBinaryOutputFormat>(
            buf,
            std::make_shared<const Block>(sample),
            format_settings.column_binary.disable_preallocation);
    });
    factory.markOutputFormatSupportsParallelFormatting("ColumnBinary");
    factory.markOutputFormatNotTTYFriendly("ColumnBinary");
    factory.setContentType("ColumnBinary", "application/octet-stream");
}

}
