#pragma once

#include <Processors/Formats/IOutputFormat.h>
#include <IO/WriteBuffer.h>

namespace DB
{

class FormatFactory;
class ColumnBinaryOutputFormat final : public IOutputFormat
{
public:
    ColumnBinaryOutputFormat(WriteBuffer & out_, SharedHeader header,
                             bool disable_preallocation = false);

    String getName() const override { return "ColumnBinary"; }

    bool expectMaterializedColumns() const override { return false; }
    bool supportsColumnSchema() const override { return true; }
    std::optional<uint64_t> precomputeSerializedSize(const Block & block, size_t row_offset, size_t rows) const override;
    bool supportsRowRange(const Block & block) const override;

    /// Enforce the exact column count the reader requires; see consume().
    void checkNumCols(size_t num_cols) const;

    /// Enforce the exact column type the reader requires; see checkColumnStructure().
    void checkColumnStructure(size_t i, const IColumn & column) const;

    /// Reject a row count the uint32_t frame header cannot represent.
    void checkRowCount(size_t rows) const;

protected:
    void consume(Chunk chunk) override;
    void consumeRowRange(const Block & block, size_t row_offset, size_t rows) override;

    /// Serializes rows [row_offset, row_offset + rows) of `columns` as one frame. Shared by the
    /// whole-block and row-range paths, which differ only in that offset.
    void writeFrame(Columns columns, size_t row_offset, size_t rows);
    void writePrefix() override {}

    SharedHeader header_;
    bool disable_preallocation_;
};

void registerOutputFormatColumnBinary(FormatFactory & factory);

}
