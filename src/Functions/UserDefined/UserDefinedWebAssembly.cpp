#include <Functions/UserDefined/UserDefinedWebAssembly.h>
#include <Formats/ColumnBinaryWire.h>
#include <Functions/UserDefined/UserDefinedWebAssemblyScriptAbi.h>
#include <Functions/UserDefined/UserDefinedWebAssemblyTypeHelpers.h>

#include <ranges>
#include <algorithm>
#include <base/hex.h>

#include <Columns/ColumnVector.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnVariant.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeVariant.h>

#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnTuple.h>

#include <AggregateFunctions/IAggregateFunction.h>
#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <Common/Arena.h>
#include <Common/CurrentThread.h>
#include <Common/ThreadStatus.h>
#include <IO/VarInt.h>

#include <Functions/IFunction.h>
#include <Functions/IFunctionAdaptors.h>

#include <Formats/FormatFactory.h>
#include <Formats/formatBlock.h>

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/WasmModuleManager.h>
#include <Interpreters/WebAssembly/HostApi.h>
#include <Interpreters/WebAssembly/WasmMemory.h>

#include <Parsers/ASTCreateWasmFunctionQuery.h>

#include <Interpreters/castColumn.h>
#include <IO/NullWriteBuffer.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromStringWithMemoryTracking.h>

#include <Processors/Chunk.h>
#include <Processors/Formats/IInputFormat.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Common/formatReadable.h>

#include <Common/NamePrompter.h>
#include <Common/PoolBase.h>
#include <fmt/ranges.h>
#include <Poco/String.h>
#include <Common/transformEndianness.h>
#include <base/extended_types.h>
#include <base/arithmeticOverflow.h>


#include <Common/ProfileEvents.h>
#include <Common/ElapsedTimeProfileEventIncrement.h>

namespace ProfileEvents
{
extern const Event WasmTotalExecuteMicroseconds;
extern const Event WasmSerializationMicroseconds;
extern const Event WasmDeserializationMicroseconds;
extern const Event WasmGuestExecuteMicroseconds;
extern const Event WasmInputBlockPrepMicroseconds;
extern const Event WasmResultAssemblyMicroseconds;
extern const Event WasmFormatSetupMicroseconds;
}


namespace DB
{

using namespace WebAssembly;
using namespace ColumnBinaryWire;

namespace Setting
{
extern const SettingsUInt64 webassembly_udf_max_fuel;
extern const SettingsUInt64 webassembly_udf_max_memory;
extern const SettingsUInt64 webassembly_udf_max_input_block_size;
extern const SettingsUInt64 webassembly_udf_max_instances;
extern const SettingsFloat webassembly_udf_input_split_memory_ratio;
}

namespace ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int LOGICAL_ERROR;
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int RESOURCE_NOT_FOUND;
extern const int TOO_LARGE_STRING_SIZE;
extern const int WASM_ERROR;
}

UserDefinedWebAssemblyFunction::UserDefinedWebAssemblyFunction(
    std::shared_ptr<WebAssembly::WasmModule> wasm_module_,
    const String & function_name_,
    const Strings & argument_names_,
    const DataTypes & arguments_,
    const DataTypePtr & result_type_,
    WebAssemblyFunctionSettings function_settings_,
    bool is_deterministic_)
    : function_name(function_name_)
    , argument_names(argument_names_)
    , arguments(arguments_)
    , result_type(result_type_)
    , wasm_module(wasm_module_)
    , settings(std::move(function_settings_))
    , is_deterministic(is_deterministic_)
{
}

class UserDefinedWebAssemblyFunctionSimple : public UserDefinedWebAssemblyFunction
{
public:
    template <typename... Args>
    explicit UserDefinedWebAssemblyFunctionSimple(Args &&... args) : UserDefinedWebAssemblyFunction(std::forward<Args>(args)...)
    {
        checkSignature();
    }

    /// Arguments and the result cross the boundary as WebAssembly values, so guest memory is
    /// never touched.
    bool requiresGuestLinearMemory() const override { return false; }

    bool serializesInputBlockToGuestMemory() const override { return false; }

    void checkSignature() const
    {
        auto function_declaration = wasm_module->getExport(function_name);

        const auto & wasm_argument_types = function_declaration.getArgumentTypes();
        if (wasm_argument_types.size() != arguments.size())
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "WebAssembly function '{}' expects {} arguments, but it's declared with {} arguments",
                function_name, wasm_argument_types.size(), arguments.size());
        }

        for (size_t i = 0; i < arguments.size(); ++i)
            checkDataTypeWithWasmValKind(arguments[i].get(), wasm_argument_types[i]);

        auto wasm_return_type = function_declaration.getReturnType();
        if (bool(result_type) != wasm_return_type.has_value())
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "WebAssembly function '{}' expects return type {}, but it's declared with {} return type",
                function_name,
                result_type ? result_type->getName() : "void",
                wasm_return_type ? toString(wasm_return_type.value()) : "void");
        }

        if (wasm_return_type)
            checkDataTypeWithWasmValKind(result_type.get(), wasm_return_type.value());
    }


    static void checkDataTypeWithWasmValKind(const IDataType * type, WasmValKind kind)
    {
        bool is_data_type_compatible = tryExecuteForNumericTypes(
            [type, kind]<typename T>() { return typeid_cast<const DataTypeNumber<T> *>(type) && wasmKindFor<T>() == kind; });
        if (!is_data_type_compatible)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "WebAssembly function expects type compatible with {}, but got {}",
                toString(kind),
                type->getName());
    }

    MutableColumnPtr
    executeOnBlock(WebAssembly::WasmCompartment * compartment, const Block & block, ContextPtr, size_t num_rows, StopToken stop_token) const override
    {
        ProfileEventTimeIncrement<Microseconds> timer_execute(ProfileEvents::WasmTotalExecuteMicroseconds);

        auto get_column_element = []<typename T>(const IColumn * column, size_t row_idx, WasmVal & val)
        {
            if (auto * column_typed = checkAndGetColumn<ColumnVector<T>>(column))
            {
                val = static_cast<typename WasmStorageType<T>::Type>(column_typed->getElement(row_idx));
                return true;
            }
            return false;
        };

        MutableColumnPtr result_column = result_type->createColumn();
        auto invoke_and_set_column = [&]<typename T>(const VectorWithMemoryTracking<WasmVal> & args)
        {
            if (auto * column_typed = typeid_cast<ColumnVector<T> *>(result_column.get()))
            {
                auto value = compartment->invoke<typename WasmStorageType<T>::Type>(function_name, args, stop_token);
                column_typed->insertValue(static_cast<T>(value));
                return true;
            }
            return false;
        };

        size_t num_columns = block.columns();
        VectorWithMemoryTracking<WasmVal> wasm_args(num_columns);
        for (size_t row_idx = 0; row_idx < num_rows; ++row_idx)
        {
            for (size_t col_idx = 0; col_idx < num_columns; ++col_idx)
            {
                const auto & column = block.getByPosition(col_idx);
                if (!tryExecuteForNumericTypes(get_column_element, column.column.get(), row_idx, wasm_args[col_idx]))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot convert {} to WebAssembly type", column.type->getName());
            }

            if (!tryExecuteForNumericTypes(invoke_and_set_column, wasm_args))
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Cannot get value of type {} from result of WebAssembly function {}",
                    result_column->getName(),
                    function_name);
        }

        return result_column;
    }
};

struct WasmBuffer
{
    WasmPtr ptr;
    WasmSizeT size;
};

static_assert(sizeof(WasmBuffer) == 8, "WasmBuffer size must be 8 bytes");
static_assert(alignof(WasmBuffer) == 4, "WasmBuffer alignment must be 4 bytes");

class WasmMemoryManagerV01 final : public WasmMemoryManager
{
public:
    constexpr static std::string_view allocate_function_name = "clickhouse_create_buffer";
    constexpr static std::string_view deallocate_function_name = "clickhouse_destroy_buffer";

    static WasmFunctionDeclaration allocateFunctionDeclaration() { return {"", allocate_function_name, {WasmValKind::I32}, WasmValKind::I32}; }
    static WasmFunctionDeclaration deallocateFunctionDeclaration() { return {"", deallocate_function_name, {WasmValKind::I32}, std::nullopt}; }

    explicit WasmMemoryManagerV01(WasmCompartment * compartment_, StopToken stop_token_)
        : compartment(compartment_)
        , stop_token(stop_token_)
    {
    }

    WasmPtr createBuffer(WasmSizeT size) const override { return compartment->invoke<WasmPtr>(allocate_function_name, {size}, stop_token); }
    void destroyBuffer(WasmPtr handle) const override { compartment->invoke<void>(deallocate_function_name, {handle}, stop_token); }

    std::span<uint8_t> getMemoryView(WasmPtr handle) const override
    {
        if (handle == 0)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Wasm buffer is nullptr");

        auto raw_buffer_span = compartment->getMemory(handle, sizeof(WasmBuffer));
        const auto * raw_buffer_ptr = raw_buffer_span.data();
        auto ptr = loadFromWasmMemory<WasmPtr>(raw_buffer_ptr);
        auto size = loadFromWasmMemory<WasmSizeT>(raw_buffer_ptr + sizeof(WasmPtr));

        if (size > 0 && ptr == 0)
            throw Exception(ErrorCodes::WASM_ERROR,
                "WebAssembly buffer returned null data pointer with size {}", size);

        return compartment->getMemory(ptr, size);
    }

private:
    WasmCompartment * compartment;
    StopToken stop_token;
};

class UserDefinedWebAssemblyFunctionBufferedV1 : public UserDefinedWebAssemblyFunction
{
public:
    template <typename... Args>
    explicit UserDefinedWebAssemblyFunctionBufferedV1(Args &&... args) : UserDefinedWebAssemblyFunction(std::forward<Args>(args)...)
    {
        checkSignature();
        serialization_format = settings.getValue("serialization_format").safeGet<String>();
        Block input_header;
        for (size_t i = 0; i < arguments.size(); ++i)
        {
            String col_name = !argument_names[i].empty() ? argument_names[i] : fmt::format("arg{}", i);
            input_header.insert(ColumnWithTypeAndName(arguments[i], col_name));
        }
        // Validate the argument and result types eagerly, at declaration time, instead of
        // deferring to the first call. For `ColumnBinary` this is the same check its output
        // format runs in its constructor, done directly rather than by building that format:
        // building it would also demand `allow_experimental_column_binary_format`, and whether
        // the experimental wire may be used belongs to the query that calls the function, not
        // to the statement that declares it. Every other format is probed by construction,
        // which is also what rejects a serialization format that does not exist.
        if (serialization_format == "ColumnBinary")
        {
            for (const auto & column : input_header)
                validateColumnBinaryWireSupportedType(column.type);
            validateColumnBinaryWireSupportedType(result_type);
        }
        else
        {
            probe_format = FormatFactory::instance().getOutputFormatWithDefaultSettings(
                serialization_format, probe_null_wb, input_header);
        }
    }

    /// The input block is serialized into a buffer the guest allocates, and the result read
    /// back from guest memory.
    bool requiresGuestLinearMemory() const override { return true; }

    bool serializesInputBlockToGuestMemory() const override { return true; }

    void checkFunction(const WasmFunctionDeclaration & expected) const
    {
        checkFunctionDeclarationMatches(wasm_module->getExport(expected.getName()), expected);
    }

    void checkSignature() const
    {
        checkFunction(WasmFunctionDeclaration("", function_name, {WasmValKind::I32, WasmValKind::I32}, WasmValKind::I32));
        checkFunction(WasmMemoryManagerV01::allocateFunctionDeclaration());
        checkFunction(WasmMemoryManagerV01::deallocateFunctionDeclaration());
    }

    /// Reads the whole result out of `input_format` by driving it directly, without building a
    /// `QueryPipeline` and a `PullingPipelineExecutor` around it. `FormatFactory::getInput` returns a
    /// single `IInputFormat` source with no transforms attached, so a pipeline would add nothing here
    /// beyond its own construction cost, which is substantial relative to deserializing one small
    /// in-memory frame: it dominated the WASM read-back path in profiles. `IInputFormat::generate`
    /// yields an empty chunk at end of input, exactly as `ISource::tryGenerate` (and therefore
    /// `ISource::work`) interprets it, so this loop reproduces the source's own driving logic,
    /// including the trailing `onFinish`. No input format overrides `tryGenerate`, so nothing else
    /// can be interposed between `work` and `generate`.
    static void readSingleBlock(IInputFormat & input_format, Block & result_block)
    {
        Chunk result_chunk;
        while (true)
        {
            Chunk chunk = input_format.generate();
            bool has_data = static_cast<bool>(chunk);

            if (chunk && chunk.getNumColumns() != result_block.columns())
                throw Exception(
                    ErrorCodes::WASM_ERROR,
                    "Different number of columns in result chunks, expected {}, got {}",
                    result_block.dumpStructure(),
                    chunk.dumpStructure());

            if (!result_chunk)
                result_chunk = std::move(chunk);
            else if (chunk)
            {
                // `Chunk::append` concatenates with `insertRangeFrom`, which is not const-safe, and
                // `ColumnBinary` preserves top-level const, so a multi-frame result can legitimately
                // contain const chunks. A const destination would only grow its row count and repeat
                // the first frame's value for every later frame; a const source would reach
                // `insertRangeFrom`'s `assert_cast`, which is a plain `static_cast` in release
                // builds. Materialize both sides before concatenating. The single-chunk case above
                // is untouched, so a result that is const end to end still stays const.
                convertToFullIfConst(result_chunk);
                convertToFullIfConst(chunk);
                result_chunk.append(chunk);
            }

            if (!has_data)
                break;
        }

        input_format.onFinish();

        if (result_chunk.getNumColumns() != result_block.columns())
            throw Exception(
                ErrorCodes::WASM_ERROR,
                "WebAssembly function returned a result with {} columns, expected {}",
                result_chunk.getNumColumns(), result_block.columns());

        result_block.setColumns(result_chunk.detachColumns());
    }

    MutableColumnPtr
    executeOnBlock(WebAssembly::WasmCompartment * compartment, const Block & block, ContextPtr context, size_t num_rows, StopToken stop_token) const override
    {
        ProfileEventTimeIncrement<Microseconds> timer_execute(ProfileEvents::WasmTotalExecuteMicroseconds);

        if (num_rows == 0)
            return result_type->createColumn();
        if (num_rows >= std::numeric_limits<WasmSizeT>::max())
            throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "Too large number of rows: {}", num_rows);

        auto wmm = std::make_unique<WasmMemoryManagerV01>(compartment, stop_token);

        // Build the format settings and the empty sample header once per call. `getFormatSettings`
        // reads several hundred settings and allocates for every string-valued one, and it used to
        // run three times per invocation (probe, real output format, input format), with
        // `block.cloneEmpty()` running twice on top of that. They are query-invariant, so hoisting
        // them changes nothing about which settings apply while removing the repeated work.
        std::optional<FormatSettings> format_settings_holder;
        Block empty_header;
        {
            ProfileEventTimeIncrement<Microseconds> timer_format_setup(ProfileEvents::WasmFormatSetupMicroseconds);
            format_settings_holder.emplace(getFormatSettings(context));
            empty_header = block.cloneEmpty();
        }
        const FormatSettings & format_settings = *format_settings_holder;

        WasmMemoryGuard wasm_data = nullptr;
        if (!block.empty())
        {
            ProfileEventTimeIncrement<Microseconds> timer_serialize(ProfileEvents::WasmSerializationMicroseconds);

            // Build the probe from the query's actual Context rather than reusing probe_format
            // (built once at construction with default FormatSettings, kept only for its early
            // validation side effect): otherwise this precompute/allocate fast path would
            // ignore per-query settings like column_binary_disable_preallocation while the real
            // `out` format below picks them up from context, and the two could disagree on
            // whether or how to serialize. A local NullWriteBuffer (not the probe_null_wb
            // member) avoids a data race if this const method is called concurrently for the
            // same instance.
            NullWriteBuffer local_probe_wb;
            auto probe = context->getOutputFormat(serialization_format, local_probe_wb, empty_header, format_settings);
            std::optional<uint64_t> precomputed = probe->precomputeSerializedSize(block, num_rows);

            if (precomputed)
            {
                wasm_data = allocateInWasmMemory(wmm.get(), *precomputed);
                auto wasm_mem = wasm_data.getMemoryView();
                // Same defensive check as the fallback branch below: a buggy clickhouse_create_buffer
                // implementation in the WASM module could return a handle to a smaller buffer than
                // requested. Without this check, WriteBufferFromPointer below would be constructed
                // with the *requested* size (*precomputed) rather than the actual buffer size, and
                // out->write(block) could write past the end of the real guest buffer.
                if (wasm_mem.size() != *precomputed)
                    throw Exception(ErrorCodes::WASM_ERROR,
                        "Cannot allocate WASM buffer of size {}, got {}. "
                        "Maybe '{}' function implementation in WebAssembly module is incorrect",
                        *precomputed, wasm_mem.size(), WasmMemoryManagerV01::allocate_function_name);
                WriteBufferFromPointer wb(reinterpret_cast<char *>(wasm_mem.data()), *precomputed);
                auto out = context->getOutputFormat(serialization_format, wb, empty_header, format_settings);
                // write()+finalize() instead of formatBlock(): formatBlock calls flush()
                // which triggers out.next() — fatal for WriteBufferFromPointer.
                // auto_flush defaults to false so neither write() nor finalize() flush.
                out->write(block);
                out->finalize();
                wb.cancel();
            }
            else
            {
                // Fallback: serialize into a CH-side String, then copy into WASM memory.
                // WriteBufferForWasmMemory (zero-copy path) cannot be used here because it
                // invokes clickhouse_create_buffer in the WASM compartment during construction,
                // which crashes during constant-folding dry-run (executeImplDryRun).
                StringWithMemoryTracking input_data;
                {
                    WriteBufferFromStringWithMemoryTracking buf(input_data);
                    auto out = context->getOutputFormat(serialization_format, buf, empty_header, format_settings);
                    formatBlock(out, block);
                }
                wasm_data = allocateInWasmMemory(wmm.get(), input_data.size());
                auto wasm_mem = wasm_data.getMemoryView();
                if (wasm_mem.size() != input_data.size())
                    throw Exception(ErrorCodes::WASM_ERROR,
                        "Cannot allocate WASM buffer of size {}, got {}",
                        input_data.size(), wasm_mem.size());
                std::copy(input_data.data(), input_data.data() + input_data.size(), wasm_mem.begin());
            }
        }

        auto result_ptr = compartment->invoke<WasmPtr>(function_name, {wasm_data.getHandle(), static_cast<WasmSizeT>(num_rows)}, stop_token);
        if (result_ptr == 0)
            throw Exception(ErrorCodes::WASM_ERROR, "WebAssembly function '{}' returned nullptr", function_name);

        WasmMemoryGuard result(wmm.get(), result_ptr);
        auto result_data = result.getMemoryView();
        ReadBufferFromMemory inbuf(result_data.data(), result_data.size());

        ProfileEventTimeIncrement<Microseconds> timer_deserialize(ProfileEvents::WasmDeserializationMicroseconds);

        Block result_header({ColumnWithTypeAndName(result_type->createColumn(), result_type, "result")});

        auto input_format = context->getInputFormat(
            serialization_format, inbuf, result_header, /* max_block_size */ DBMS_DEFAULT_BUFFER_SIZE,
            format_settings);
        readSingleBlock(*input_format, result_header);

        if (result_header.columns() != 1 || result_header.rows() != num_rows)
            throw Exception(
                ErrorCodes::WASM_ERROR,
                "Unexpected result column structure: {} returned from WebAssembly function '{}'",
                result_header.dumpStructure(),
                function_name);

        auto result_columns = result_header.mutateColumns();
        return std::move(result_columns[0]);
    }

private:
    String serialization_format;
    NullWriteBuffer probe_null_wb;
    OutputFormatPtr probe_format;
};

// ─────────────────────────────────────────────────────────────────────────────
// COLUMNAR_V1 ABI
//
// Wire format (all offsets are byte offsets from the buffer start):
//
//   BufHeader (8 bytes): num_rows:u32, num_cols:u32
//   ColDescriptor[num_cols] (40 bytes each):
//     type:u64, null_offset:u64, offsets_offset:u64, data_offset:u64, data_size:u64
//   Data blocks at the described offsets.
//
//   type bits: ColType (0-6) | COL_IS_NULLABLE (0x20) | COL_IS_CONST (0x80)
//
//   COL_BYTES  (0): start-based u64 offsets[rows+1] + chars (no null terminators)
//   COL_FIXED8 (1): u8[rows]
//   COL_FIXED16(2): u16[rows]
//   COL_FIXED64(4): u64/f64[rows]
//   Any type | COL_IS_NULLABLE: null_map[rows] at null_offset, then column data
//
// The WASM export is <function_name>_col(i32 buf_handle, i32 num_rows) -> i32.
// The caller (CH) allocates the input buffer with clickhouse_create_buffer,
// fills it, then invokes the function.  The function returns a handle to an
// output buffer (same layout, 1 column) which CH reads and frees.
// ─────────────────────────────────────────────────────────────────────────────

class UserDefinedWebAssemblyFunctionColumnarV1 : public UserDefinedWebAssemblyFunction
{
public:
    template <typename... Args>
    explicit UserDefinedWebAssemblyFunctionColumnarV1(Args &&... args)
        : UserDefinedWebAssemblyFunction(std::forward<Args>(args)...)
    {
        // WASM export name matches the registered function name directly
        col_function_name = function_name;
        checkSignature();
        // Reject unsupported argument/result signatures at CREATE FUNCTION time rather
        // than on the first call: see validateColumnBinaryWireSupportedType for the exact list.
        // COLUMNAR_V1 writes the same frame as the `ColumnBinary` wire, so it supports exactly
        // the types that wire supports.
        for (const auto & arg : arguments)
            validateColumnBinaryWireSupportedType(arg);
        validateColumnBinaryWireSupportedType(result_type);
    }

    bool requiresGuestLinearMemory() const override { return true; }

    bool serializesInputBlockToGuestMemory() const override { return true; }

    // Direct columnar execution — bypasses RowBinary batching.
    // Called from FunctionUserDefinedWasm::executeImpl() for ColumnarV1 functions.
    MutableColumnPtr executeColumnar(
        WebAssembly::WasmCompartment * compartment,
        const ColumnsWithTypeAndName & cols,
        size_t input_rows_count,
        ContextPtr,
        StopToken stop_token) const
    {
        ProfileEventTimeIncrement<Microseconds> timer(ProfileEvents::WasmTotalExecuteMicroseconds);

        if (input_rows_count == 0)
            return result_type->createColumn();

        if (input_rows_count >= std::numeric_limits<uint32_t>::max())
            throw Exception(ErrorCodes::TOO_LARGE_STRING_SIZE, "Too large number of rows: {}", input_rows_count);

        // ── Build the columnar input buffer ──────────────────────────────────
        const uint32_t num_cols = static_cast<uint32_t>(cols.size());

        std::vector<ColDescriptor> descs; // STYLE_CHECK_ALLOW_STD_CONTAINERS
        std::vector<const IColumn *> inner_cols; // STYLE_CHECK_ALLOW_STD_CONTAINERS
        std::vector<bool> is_nullable_flags; // STYLE_CHECK_ALLOW_STD_CONTAINERS
        std::vector<uint32_t> row_counts; // STYLE_CHECK_ALLOW_STD_CONTAINERS
        std::vector<ColumnPtr> casted_columns; // STYLE_CHECK_ALLOW_STD_CONTAINERS -- keeps casted columns alive through writeColData below

        const uint64_t total_buf_size = buildColumnarLayout(
            cols, input_rows_count, /* declared_positions */ {},
            descs, inner_cols, is_nullable_flags, row_counts, casted_columns);

        // ── Allocate buffer in WASM memory ───────────────────────────────────
        {
            auto wmm = std::make_unique<WasmMemoryManagerV01>(compartment, stop_token);
            WasmMemoryGuard wasm_input = nullptr;

            // Scope the serialization timer to the allocate-and-write phase only. It must
            // not extend over `compartment->invoke` below (guest execution, which is neither
            // serialization nor host work) nor over the read-back block (already counted by
            // `WasmDeserializationMicroseconds`); otherwise `WasmSerializationMicroseconds`
            // reports very nearly the whole of `WasmTotalExecuteMicroseconds` and double
            // counts the deserialization time, making the COLUMNAR_V1 profile unreadable and
            // incomparable with the BUFFERED_V1 path, whose timers are already disjoint.
            {
                ProfileEventTimeIncrement<Microseconds> timer_ser(ProfileEvents::WasmSerializationMicroseconds);

                wasm_input = allocateInWasmMemory(wmm.get(), total_buf_size);
                auto wasm_mem = wasm_input.getMemoryView();
                // Same defensive check as the buffered path's fallback branch: a buggy
                // clickhouse_create_buffer implementation in the WASM module could return a
                // handle to a smaller buffer than requested. Without it, the header/descriptor
                // memcpys and writeColData below would write past the end of the real guest
                // buffer instead of throwing.
                if (wasm_mem.size() != total_buf_size)
                    throw Exception(ErrorCodes::WASM_ERROR,
                        "Cannot allocate WASM buffer of size {}, got {}. "
                        "Maybe '{}' function implementation in WebAssembly module is incorrect",
                        total_buf_size, wasm_mem.size(), WasmMemoryManagerV01::allocate_function_name);

                // Write header. The frame is the one `ColumnBinaryWire.h` defines - magic,
                // version and reserved word ahead of the row and column counts - so that a guest
                // reads the same header whichever of the two registration paths reached it.
                writeFrameHeader(wasm_mem.data(), static_cast<uint32_t>(input_rows_count), num_cols);

                // Write descriptors
                for (uint32_t ci = 0; ci < num_cols; ++ci)
                    std::memcpy(wasm_mem.data() + FRAME_HEADER_BYTES + ci * COL_DESC_BYTES,
                                &descs[ci], COL_DESC_BYTES);

                // Write column data
                for (uint32_t ci = 0; ci < num_cols; ++ci)
                    writeColData(inner_cols[ci], is_nullable_flags[ci], row_counts[ci],
                                 descs[ci], wasm_mem);
            }

            // ── Invoke WASM ──────────────────────────────────────────────────
            auto result_ptr = compartment->invoke<WasmPtr>(
                col_function_name,
                {wasm_input.getHandle(), static_cast<WasmSizeT>(input_rows_count)},
                stop_token);

            if (result_ptr == 0)
                throw Exception(ErrorCodes::WASM_ERROR,
                    "COLUMNAR_V1 function '{}' returned nullptr", col_function_name);

            WasmMemoryGuard result_guard(wmm.get(), result_ptr);

            // ── Read output ──────────────────────────────────────────────────
            {
                ProfileEventTimeIncrement<Microseconds> timer_de(ProfileEvents::WasmDeserializationMicroseconds);
                auto out_view = result_guard.getMemoryView();
                return readColumnarOutput(
                    {out_view.data(), out_view.size()},
                    result_type,
                    input_rows_count);
            }
        }
    }

    // executeOnBlock is required by the base class but unused for ColumnarV1
    // (FunctionUserDefinedWasm calls executeColumnar directly).
    MutableColumnPtr executeOnBlock(
        WebAssembly::WasmCompartment * compartment,
        const Block & block,
        ContextPtr context,
        size_t num_rows,
        StopToken stop_token) const override
    {
        ColumnsWithTypeAndName args;
        args.reserve(block.columns());
        for (size_t i = 0; i < block.columns(); ++i)
            args.push_back(block.getByPosition(i));
        return executeColumnar(compartment, args, num_rows, context, stop_token);
    }

    /// The exact bytes a call carrying `[start_idx, start_idx + length)` puts in guest memory.
    ///
    /// This ABI builds its buffer here rather than through a FormatFactory output, so what a
    /// `serialization_format` would measure is not what the guest is handed - that setting names
    /// the wire of the BUFFERED_V1 path and this ABI ignores it. The layout pass below is the very
    /// one `executeColumnar` sizes its allocation with, so the measurement and the allocation
    /// cannot disagree.
    ///
    /// `declared_positions` says where each entry of `cols` sits in the declared argument list; it
    /// is empty when `cols` is that list in order. The cast to the declared type is what the size
    /// depends on, and the declared type is found by an argument's declared position, not by where
    /// it happens to land in a subset.
    size_t measureColumnarBytes(
        const ColumnsWithTypeAndName & cols,
        size_t start_idx,
        size_t length,
        const std::vector<size_t> & declared_positions = {}) const // STYLE_CHECK_ALLOW_STD_CONTAINERS
    {
        ColumnsWithTypeAndName batch;
        batch.reserve(cols.size());
        for (const auto & col : cols)
        {
            /// A `ColumnConst` is one stored row broadcast to the batch and stays compact on this
            /// wire, so it is passed through whole: cutting it would only rebuild the same const.
            bool whole_column = isColumnConst(*col.column) || (start_idx == 0 && length == col.column->size());
            batch.emplace_back(whole_column ? col.column : col.column->cut(start_idx, length), col.type, col.name);
        }

        std::vector<ColDescriptor> descs; // STYLE_CHECK_ALLOW_STD_CONTAINERS
        std::vector<const IColumn *> inner_cols; // STYLE_CHECK_ALLOW_STD_CONTAINERS
        std::vector<bool> is_nullable_flags; // STYLE_CHECK_ALLOW_STD_CONTAINERS
        std::vector<uint32_t> row_counts; // STYLE_CHECK_ALLOW_STD_CONTAINERS
        std::vector<ColumnPtr> casted_columns; // STYLE_CHECK_ALLOW_STD_CONTAINERS

        return buildColumnarLayout(
            batch, length, declared_positions, descs, inner_cols, is_nullable_flags, row_counts, casted_columns);
    }

private:
    /// Fills the per-column descriptors of a COLUMNAR_V1 frame and returns the frame's total size.
    /// `casted_columns` owns whatever the cast below produced and must outlive `inner_cols`, which
    /// points into it.
    uint64_t buildColumnarLayout(
        const ColumnsWithTypeAndName & cols,
        size_t input_rows_count,
        const std::vector<size_t> & declared_positions, // STYLE_CHECK_ALLOW_STD_CONTAINERS
        std::vector<ColDescriptor> & descs, // STYLE_CHECK_ALLOW_STD_CONTAINERS
        std::vector<const IColumn *> & inner_cols, // STYLE_CHECK_ALLOW_STD_CONTAINERS
        std::vector<bool> & is_nullable_flags, // STYLE_CHECK_ALLOW_STD_CONTAINERS
        std::vector<uint32_t> & row_counts, // STYLE_CHECK_ALLOW_STD_CONTAINERS
        std::vector<ColumnPtr> & casted_columns) const // STYLE_CHECK_ALLOW_STD_CONTAINERS
    {
        const uint32_t num_cols = static_cast<uint32_t>(cols.size());
        uint64_t cursor = FRAME_HEADER_BYTES + num_cols * COL_DESC_BYTES;

        descs.assign(num_cols, ColDescriptor{});
        inner_cols.assign(num_cols, nullptr);
        is_nullable_flags.assign(num_cols, false);
        row_counts.assign(num_cols, 0);
        casted_columns.assign(num_cols, nullptr);

        for (uint32_t ci = 0; ci < num_cols; ++ci)
        {
            // Cast to the declared argument type: getReturnTypeImpl accepts numeric
            // coercions (i32->i64, int->float, ...), but the wire only encodes a coarse
            // width class (COL_FIXED8/16/32/64), not the declared width/signedness. Without
            // this cast, a declared UInt64 argument passed as an actual UInt8 would still
            // serialize as 1 byte, and the guest's get_u64-style reader would read past it.
            //
            // getReturnTypeImpl also accepts a genuinely Nullable(T) argument against a
            // plain declared T (COLUMNAR_V1 derives is_nullable from the runtime column
            // below, so it round-trips this correctly) -- but casting straight to the
            // non-nullable declared type here would insert NULLs into an ordinary column
            // and throw on the first real NULL. Cast to Nullable(declared type) instead
            // whenever the actual argument is nullable, to fix the width/coercion while
            // still preserving the null map for the is_nullable detection below.
            const DataTypePtr & declared_arg_type = arguments[declared_positions.empty() ? ci : declared_positions[ci]];
            DataTypePtr target_type = (cols[ci].type->isNullable() && !declared_arg_type->isNullable())
                ? makeNullable(declared_arg_type)
                : declared_arg_type;
            casted_columns[ci] = cols[ci].type->equals(*target_type)
                ? cols[ci].column
                : castColumn(cols[ci], target_type);
            const IColumn * col = casted_columns[ci].get();
            bool is_const = false;

            if (const auto * cc = typeid_cast<const ColumnConst *>(col))
            {
                col = &cc->getDataColumn();
                is_const = true;
            }

            bool is_nullable = typeid_cast<const ColumnNullable *>(col) != nullptr;
            uint32_t nrows = is_const ? 1u : static_cast<uint32_t>(input_rows_count);

            is_nullable_flags[ci] = is_nullable;
            inner_cols[ci] = col;
            row_counts[ci] = nrows;

            cursor = buildColDescriptor(col, is_const, is_nullable, nrows, cursor, descs[ci]);
        }

        return cursor;
    }

    void checkSignature() const
    {
        auto decl = wasm_module->getExport(col_function_name);
        WasmFunctionDeclaration expected("", col_function_name,
            {WasmValKind::I32, WasmValKind::I32}, WasmValKind::I32);
        checkFunctionDeclarationMatches(decl, expected);
        // Also require clickhouse_create_buffer / clickhouse_destroy_buffer
        checkFunctionDeclarationMatches(
            wasm_module->getExport(WasmMemoryManagerV01::allocate_function_name),
            WasmMemoryManagerV01::allocateFunctionDeclaration());
        checkFunctionDeclarationMatches(
            wasm_module->getExport(WasmMemoryManagerV01::deallocate_function_name),
            WasmMemoryManagerV01::deallocateFunctionDeclaration());
    }

    String col_function_name;
};

std::unique_ptr<UserDefinedWebAssemblyFunction> UserDefinedWebAssemblyFunction::create(
    std::shared_ptr<WebAssembly::WasmModule> wasm_module_,
    const String & function_name_,
    const Strings & argument_names_,
    const DataTypes & arguments_,
    const DataTypePtr & result_type_,
    WasmAbiVersion abi_type,
    WebAssemblyFunctionSettings function_settings,
    bool is_deterministic_)
{
    switch (abi_type)
    {
        case WasmAbiVersion::RowDirect:
            return std::make_unique<UserDefinedWebAssemblyFunctionSimple>(
                wasm_module_, function_name_, argument_names_, arguments_, result_type_, std::move(function_settings), is_deterministic_);
        case WasmAbiVersion::BufferedV1:
            return std::make_unique<UserDefinedWebAssemblyFunctionBufferedV1>(
                wasm_module_, function_name_, argument_names_, arguments_, result_type_, std::move(function_settings), is_deterministic_);
        case WasmAbiVersion::AssemblyScript:
            return createUserDefinedWebAssemblyFunctionAssemblyScript(
                wasm_module_, function_name_, argument_names_, arguments_, result_type_, std::move(function_settings), is_deterministic_);
        case WasmAbiVersion::ColumnarV1:
            return std::make_unique<UserDefinedWebAssemblyFunctionColumnarV1>(
                wasm_module_, function_name_, argument_names_, arguments_, result_type_, std::move(function_settings), is_deterministic_);
    }
    throw Exception(
        ErrorCodes::LOGICAL_ERROR, "Unknown WebAssembly ABI version: {}", std::to_underlying(abi_type));
}

String toString(WasmAbiVersion abi_type)
{
    switch (abi_type)
    {
        case WasmAbiVersion::RowDirect:
            return "ROW_DIRECT";
        case WasmAbiVersion::BufferedV1:
            return "BUFFERED_V1";
        case WasmAbiVersion::AssemblyScript:
            return "ASSEMBLYSCRIPT";
        case WasmAbiVersion::ColumnarV1:
            return "COLUMNAR_V1";
    }
    throw Exception(
        ErrorCodes::LOGICAL_ERROR, "Unknown WebAssembly ABI version: {}", std::to_underlying(abi_type));
}

WasmAbiVersion getWasmAbiFromString(const String & str)
{
    for (auto abi_type : {WasmAbiVersion::RowDirect, WasmAbiVersion::BufferedV1, WasmAbiVersion::AssemblyScript, WasmAbiVersion::ColumnarV1})
        if (Poco::toUpper(str) == toString(abi_type))
            return abi_type;

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown WebAssembly ABI version: '{}'", str);
}

class WasmCompartmentPool final : private PoolBase<WebAssembly::WasmCompartment>
{
public:
    using Base = PoolBase<WasmCompartment>;
    using Object = Base::Object;
    using ObjectPtr = Base::ObjectPtr;

    explicit WasmCompartmentPool(
        unsigned limit,
        std::shared_ptr<WebAssembly::WasmModule> wasm_module_,
        WebAssembly::WasmModule::Config module_cfg_,
        StopToken stop_token_,
        std::optional<String> init_function_name_ = std::nullopt)
        : Base(limit, getLogger("WasmCompartmentPool"))
        , wasm_module(std::move(wasm_module_))
        , module_cfg(std::move(module_cfg_))
        , stop_token(std::move(stop_token_))
        , init_function_name(std::move(init_function_name_))
    {
        LOG_DEBUG(log, "WasmCompartmentPool created with limit: {}", limit);
    }

    Entry acquire() { return get(-1); }

protected:
    ObjectPtr allocObject() override
    {
        LOG_DEBUG(log, "Allocating new WasmCompartment");
        auto compartment = wasm_module->instantiate(module_cfg, stop_token);
        if (init_function_name)
            compartment->invoke<void>(*init_function_name, {}, stop_token);
        return compartment;
    }

private:
    std::shared_ptr<WebAssembly::WasmModule> wasm_module;
    WebAssembly::WasmModule::Config module_cfg;

    std::mutex acquire_mutex;
    StopToken stop_token;
    std::optional<String> init_function_name;
};


/// Returns "clickhouse_module_init" if the module exports it, nullopt otherwise.
static std::optional<String> tryGetModuleInitFn(const std::shared_ptr<WebAssembly::WasmModule> & module)
{
    try
    {
        module->getExport("clickhouse_module_init");
        return "clickhouse_module_init";
    }
    catch (...)
    {
        return std::nullopt;
    }
}

static WebAssembly::WasmModule::Config getWasmModuleConfig(ContextPtr context, WebAssembly::FuelMode fuel_mode)
{
    WebAssembly::WasmModule::Config cfg(fuel_mode);

    UInt64 max_fuel = context->getSettingsRef()[Setting::webassembly_udf_max_fuel];
    if (common::mulOverflow(max_fuel, 1024, cfg.fuel_limit))
        cfg.fuel_limit = std::numeric_limits<UInt64>::max();

    cfg.memory_limit = context->getSettingsRef()[Setting::webassembly_udf_max_memory];

    return cfg;
}

static bool computePreserveConstColumns(const ContextPtr & context, const std::shared_ptr<UserDefinedWebAssemblyFunction> & udf)
{
    const String fmt = udf->getSettings().getValue("serialization_format").safeGet<String>();
    StringWithMemoryTracking dummy_buf;
    WriteBufferFromStringWithMemoryTracking dummy_writer(dummy_buf);
    Block sample_block;
    size_t arg_idx = 0;
    for (const auto & arg : udf->getArguments())
        sample_block.insert(ColumnWithTypeAndName(arg->createColumn(), arg, "arg" + std::to_string(arg_idx++)));
    auto format = context->getOutputFormat(fmt, dummy_writer, sample_block);
    return !format->expectMaterializedColumns() || format->supportsColumnSchema();
}

class FunctionUserDefinedWasm final : public IFunction
{
public:
    FunctionUserDefinedWasm(String function_name_, std::shared_ptr<UserDefinedWebAssemblyFunction> udf_, ContextPtr context_)
        : user_defined_function(std::move(udf_))
        , wasm_module(user_defined_function->getModule())
        , function_name(std::move(function_name_))
        , argument_names(user_defined_function->getArgumentNames())
        , context(std::move(context_))
        , preserve_const_columns(computePreserveConstColumns(context, user_defined_function))
        , interrupt_source()
        , compartment_pool(
              static_cast<UInt32>(context->getSettingsRef()[Setting::webassembly_udf_max_instances]),
              wasm_module,
              getWasmModuleConfig(context, user_defined_function->getSettings().getFuelMode()),
              interrupt_source.get_token())
    {
        const size_t configured_memory_limit = context->getSettingsRef()[Setting::webassembly_udf_max_memory];
        if (configured_memory_limit != 0)
            module_memory_limit = configured_memory_limit;
        serialization_format = user_defined_function->getSettings().getValue("serialization_format").safeGet<String>();
    }

    String getName() const override { return function_name; }
    bool isVariadic() const override { return false; }
    bool isDeterministic() const override { return user_defined_function->getIsDeterministic(); }
    const DataTypes & getArgumentTypes() const { return user_defined_function->getArguments(); }
    bool isSpatialPredicate() const override
    {
        auto val = user_defined_function->getSettings().getValue("is_spatial_predicate");
        if (val.getType() == Field::Types::Bool)
            return val.safeGet<bool>();
        return val.safeGet<UInt64>() != 0;
    }
    int getSpatialExpandArg() const override
    {
        auto val = user_defined_function->getSettings().getValue("spatial_expand_arg");
        if (val.isNull()) return -1;
        auto idx = val.safeGet<Int64>();
        return idx < 0 ? -1 : static_cast<int>(idx);
    }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /* arguments */) const override { return false; }
    size_t getNumberOfArguments() const override { return user_defined_function->getArguments().size(); }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto & expected_arguments = user_defined_function->getArguments();
        if (arguments.size() != expected_arguments.size())
            throw Exception(
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Number of arguments doesn't match: passed {}, should be {}",
                arguments.size(),
                expected_arguments.size());

        for (size_t i = 0; i < arguments.size(); ++i)
        {
            if (arguments[i]->equals(*expected_arguments[i]))
                continue;

            /// When useDefaultImplementationForNulls() returns false (non-nullable return
            /// types such as Array), CH passes Nullable-wrapped argument types.
            /// Strip Nullable and retry the exact-match / coercion checks below — but only
            /// for COLUMNAR_V1: its executeColumnar derives is_nullable from the actual
            /// runtime column, so a genuinely-Nullable argument against a non-nullable
            /// declared parameter still round-trips correctly. BUFFERED_V1's
            /// getArgumentsBlock instead casts the column down to the declared
            /// (non-nullable) type before serialization, which would silently drop or fail
            /// on real NULL values, so that path must not accept this relaxation at all —
            /// neither for an exact type match nor for a numeric coercion.
            bool allow_nullable_relaxation
                = dynamic_cast<const UserDefinedWebAssemblyFunctionColumnarV1 *>(user_defined_function.get()) != nullptr;
            if (allow_nullable_relaxation)
            {
                const DataTypePtr & stripped = removeNullable(arguments[i]);
                if (stripped->equals(*expected_arguments[i]))
                    continue;

                /// Allow implicit coercions: same kind, i32→i64, any int→any float, f32→f64.
                auto actual_kind = wasmKindForDataType(stripped.get());
                auto expected_kind = wasmKindForDataType(expected_arguments[i].get());
                if (actual_kind && expected_kind && canCoerce(*actual_kind, *expected_kind))
                    continue;
            }
            else
            {
                auto actual_kind = wasmKindForDataType(arguments[i].get());
                auto expected_kind = wasmKindForDataType(expected_arguments[i].get());
                if (actual_kind && expected_kind && canCoerce(*actual_kind, *expected_kind))
                    continue;
            }

            auto get_type_names = std::views::transform([](const auto & arg) { return arg->getName(); });
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Illegal type of arguments, expected ({}), got ({})",
                fmt::join(expected_arguments | get_type_names, ", "),
                fmt::join(arguments | get_type_names, ", "));
        }
        return user_defined_function->getResultType();
    }

    /// When the function is deterministic, returning true here causes the framework to
    /// call executeImpl with a single-row block and wrap the result in ColumnConst.
    /// That ColumnConst is then recognised by the Analyzer's constant-folding check
    /// (isColumnConst(*column) in resolveFunction.cpp). Without this, executeImpl
    /// returns a plain ColumnVector which the Analyzer does not fold.
    bool useDefaultImplementationForConstants() const override { return user_defined_function->getIsDeterministic(); }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {}; }

    bool isSuitableForConstantFolding() const override { return user_defined_function->getIsDeterministic(); }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /* result_type */, size_t input_rows_count) const override
    {
        /// Memory grows in whole pages and the limiter refuses a growth crossing the cap, so a
        /// `webassembly_udf_max_memory` below one page leaves the guest unable to hold anything.
        /// Checked here rather than at instantiation, which does not know the ABI and would also
        /// reject a function that never touches the memory.
        /// An empty block allocates nothing in the guest, so a memory it could never use does not
        /// make the call impossible.
        if (input_rows_count > 0 && module_memory_limit && *module_memory_limit < WebAssembly::WASM_PAGE_SIZE
            && user_defined_function->requiresGuestLinearMemory())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "WebAssembly memory limit is {} bytes, which is less than a single {} byte page",
                *module_memory_limit,
                WebAssembly::WASM_PAGE_SIZE);

        auto compartment_entry = compartment_pool.acquire();
        auto * compartment_ptr = &(*compartment_entry);

        // Coerce actual columns to Variant when the function declares a Variant parameter but
        // received a member type (e.g. Point passed to Geometry).  Lets callers skip the
        // explicit CAST(x, 'Geometry').
        // When the input type lacks a custom name (e.g. bare Array(Tuple) not cast to Ring),
        // we fall back to structural matching against variant members. Note: structurally
        // ambiguous types (Array(Array(Tuple)) matches both Polygon and MultiLineString) require
        // the caller to use an explicit geo type cast; without one the first alphabetical match wins.
        const auto & declared = user_defined_function->getArguments();
        ColumnsWithTypeAndName coerced = arguments;
        for (size_t i = 0; i < coerced.size() && i < declared.size(); ++i)
        {
            const auto * variant_type = typeid_cast<const DataTypeVariant *>(declared[i].get());
            if (!variant_type || coerced[i].type->equals(*declared[i]))
                continue;

            if (!coerced[i].type->hasCustomName())
            {
                for (const auto & v : variant_type->getVariants())
                {
                    if (coerced[i].type->equals(*v))
                    {
                        coerced[i].column = castColumn(coerced[i], v);
                        coerced[i].type = v;
                        break;
                    }
                }
            }

            coerced[i].column = castColumn(coerced[i], declared[i]);
            coerced[i].type = declared[i];
        }

        // `ColumnBinary`: bypass RowBinary batching, pass columns directly (ColumnConst stays const).
        try
        {
            // COLUMNAR_V1: bypass RowBinary batching, pass columns directly (ColumnConst stays
            // const) — but still apply the same webassembly_udf_max_input_block_size /
            // guest-memory-budget splitting as the buffered path below, or a single large
            // batch can still build an oversized guest buffer.
            if (const auto * cv1 = dynamic_cast<const UserDefinedWebAssemblyFunctionColumnarV1 *>(user_defined_function.get()))
            {
                auto stop_token = interrupt_source.get_token();
                auto expected_col = user_defined_function->getResultType()->createColumn();
                MutableColumnPtr result_column = user_defined_function->getResultType()->createColumn();

                auto flush_columnar_batch = [&](size_t batch_start, size_t end_idx)
                {
                    if (end_idx <= batch_start)
                        return;
                    size_t batch_size = end_idx - batch_start;
                    ColumnsWithTypeAndName batch_cols;
                    {
                        ProfileEventTimeIncrement<Microseconds> timer_prep(ProfileEvents::WasmInputBlockPrepMicroseconds);
                        batch_cols.reserve(arguments.size());
                        for (const auto & arg : arguments)
                        {
                            /// cut() materializes a copy of the whole range; when the batch already spans
                            /// the entire column there is nothing to slice, so pass the column through.
                            bool whole_column = batch_start == 0 && batch_size == arg.column->size();
                            batch_cols.emplace_back(
                                whole_column ? arg.column : arg.column->cut(batch_start, batch_size), arg.type, arg.name);
                        }
                    }
                    auto result = cv1->executeColumnar(compartment_ptr, batch_cols, batch_size, context, stop_token);
                    ProfileEventTimeIncrement<Microseconds> timer_assembly(ProfileEvents::WasmResultAssemblyMicroseconds);
                    // A guest that set COL_IS_CONST legitimately returns a ColumnConst; structureEquals
                    // only holds between two ColumnConst instances, so compare the unwrapped nested
                    // column against expected_col instead of rejecting every valid const result.
                    const IColumn * result_for_check = result.get();
                    if (const auto * result_const = typeid_cast<const ColumnConst *>(result_for_check))
                        result_for_check = &result_const->getDataColumn();
                    if (!result_for_check->structureEquals(*expected_col))
                        throw Exception(ErrorCodes::WASM_ERROR,
                            "COLUMNAR_V1: returned column structure {} does not match declared type {}",
                            result->dumpStructure(),
                            user_defined_function->getResultType()->getName());
                    // A ColumnConst batch result must be materialized before it's accumulated:
                    // ColumnConst::insertRangeFrom only bumps the row count, it doesn't copy in
                    // the source's actual value, so concatenating a later (possibly different)
                    // batch into a ColumnConst accumulator would silently keep repeating the
                    // first batch's value for every row appended afterwards.
                    result = IColumn::mutate(result->convertToFullColumnIfConst());
                    if (result_column->empty())
                        result_column = result->assumeMutable();
                    else
                        result_column->insertRangeFrom(*result, 0, result->size());
                };

                const size_t fixed_block_size = context->getSettingsRef()[Setting::webassembly_udf_max_input_block_size];
                if (fixed_block_size > 0)
                {
                    for (size_t start = 0; start < input_rows_count; start += fixed_block_size)
                        flush_columnar_batch(start, std::min(start + fixed_block_size, input_rows_count));
                    return result_column;
                }

                // Sized against the same budget as the buffered path below, and by the same walk,
                // so that both ABIs hand the guest calls of comparable size. What differs is only
                // how a candidate is measured: this frame is built here rather than by an output
                // format, so the walk is given the layout pass that sizes the real allocation.
                const std::optional<size_t> budget = getInputBudget(compartment_ptr, fixed_block_size);
                if (!budget)
                {
                    flush_columnar_batch(0, input_rows_count);
                    return result_column;
                }

                const BatchMeasurer measure_columnar
                    = [cv1](const ColumnsWithTypeAndName & batch_cols, size_t start_idx, size_t length,
                            const std::vector<size_t> & declared_positions) // STYLE_CHECK_ALLOW_STD_CONTAINERS
                    { return cv1->measureColumnarBytes(batch_cols, start_idx, length, declared_positions); };

                size_t batch_start = 0;
                while (batch_start < input_rows_count)
                {
                    const size_t batch_rows
                        = chooseBatchRows(arguments, batch_start, input_rows_count - batch_start, *budget, measure_columnar);
                    flush_columnar_batch(batch_start, batch_start + batch_rows);
                    batch_start += batch_rows;
                }
                return result_column;
            }

            return execute(compartment_ptr, arguments, input_rows_count);
        }
        catch (...)
        {
            /// A trapped/faulted compartment may have leftovers, half-allocated buffers,
            /// or otherwise inconsistent guest state. Drop it so the pool recreates it.
            compartment_entry.expire();
            throw;
        }
    }

    ColumnPtr executeImplDryRun(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        /// Deterministic functions must actually run during dry-run so the Analyzer can constant-fold them.
        /// Non-deterministic functions return defaults to avoid WASM execution at query-analysis time.
        if (user_defined_function->getIsDeterministic())
            return executeImpl(arguments, result_type, input_rows_count);

        MutableColumnPtr result_column = user_defined_function->getResultType()->createColumn();
        result_column->insertManyDefaults(input_rows_count);
        return result_column;
    }

    void cancelExecution() const override
    {
        interrupt_source.request_stop();
    }

private:
    /// The size one call's serialized input is grown up to, empty when the input is not split by
    /// its size. A batch is never taken below a single row: splitting only decides how many rows
    /// share a call, so a row too large for the guest's memory fails inside its allocator, and no
    /// budget can rescue it.
    std::optional<size_t> getInputBudget(WebAssembly::WasmCompartment * compartment, size_t fixed_block_size) const
    {
        /// Read before the range is checked, because a value out of range is only rejected where
        /// a batch size is actually decided, but a zero has to be honoured everywhere.
        const Float64 memory_ratio = static_cast<Float64>(context->getSettingsRef()[Setting::webassembly_udf_input_split_memory_ratio].value);

        /// A zero budget is the opt-out: with no part of the memory set aside for a call's input
        /// there is nothing to size a batch against, so a zero `webassembly_udf_max_input_block_size`
        /// keeps its original meaning of one call per pipeline block.
        if (memory_ratio == 0.0)
            return {};

        /// An ABI that ships no serialized input block into guest memory has no size for the
        /// memory to bound and nothing to measure - neither one passing its arguments as
        /// WebAssembly values, whose compartment may well hold nothing at all because a module
        /// declaring `memory 0 0` stays callable this way, nor `ASSEMBLYSCRIPT`, which builds one
        /// object per row and would otherwise be bounded by a `serialization_format` it ignores.
        if (!user_defined_function->serializesInputBlockToGuestMemory())
            return {};

        /// An explicit block size caps the rows per call instead of splitting by size.
        if (fixed_block_size > 0)
            return {};

        /// The ratio only sizes a batch past this point, so an out-of-range value is only rejected
        /// past this point: a query that pins the rows per call never uses it and must not be
        /// failed by it.
        if (!(memory_ratio > 0.0 && memory_ratio <= 1.0))
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Setting `webassembly_udf_input_split_memory_ratio` must be at least 0 and at most 1, got {}", memory_ratio);

        /// Budget a batch against a fraction of the memory the module starts with, leaving the
        /// rest for its own working set beside the input buffer. The declared initial size is
        /// what the basis must be: the current size moves with `memory.grow` and never shrinks,
        /// and compartments are pooled, so a basis taken from it would depend on which instance a
        /// worker picked up and on what earlier blocks made it grow. Identical blocks would then
        /// reach the guest in different batches, which it observes through the row count.
        ///
        /// The ceiling is no basis either, even though it is stable: a guest allocator usually
        /// serves the input out of a heap far smaller than the maximum the memory may reach, so
        /// budgeting against the ceiling proposes batches the guest cannot allocate.
        ///
        /// A module declared as `memory 0 N` starts with no pages, so the initial size alone
        /// would be zero and would disable splitting; such a memory falls back to the ceiling,
        /// which the guest can still grow into and which is equally the same for every instance.
        const std::optional<size_t> initial_memory = compartment->getInitialLinearMemorySize();
        const std::optional<size_t> budget_basis = initial_memory.value_or(0) > 0 ? initial_memory : compartment->getMaxLinearMemorySize();
        if (!budget_basis)
            return {};
        return static_cast<size_t>(static_cast<Float64>(*budget_basis) * memory_ratio);
    }

    /// How a candidate batch is priced. The walk below is the same for every ABI; what a call
    /// costs is not, because an ABI that builds its own buffer does not put on the wire what an
    /// output format would write. Such an ABI hands its own layout pass in here.
    using BatchMeasurer = std::function<size_t(
        const ColumnsWithTypeAndName &, size_t, size_t, const std::vector<size_t> &)>; // STYLE_CHECK_ALLOW_STD_CONTAINERS

    /// The exact number of bytes one call carrying `[start, start + length)` puts on the wire.
    ///
    /// The batch is measured whole rather than assembled out of per-row measurements. A row has
    /// no cost of its own under a block-scoped wire: `ColumnBinary` writes a frame header, a
    /// descriptor per column and one `COL_LOWCARD` dictionary per batch, and `BuffersWriter`
    /// runs `NativeWriter::writeData` once per block, which emits a fresh `LowCardinality`
    /// dictionary and the `Dynamic` / `Variant` structure prefixes for whatever rows the block
    /// holds. Summing one-row probes charges every row a whole frame and a whole dictionary,
    /// which over-prices such a batch by more than an order of magnitude, and no fixed per-write
    /// subtraction can remove state whose size depends on which rows the batch carries.
    ///
    /// What comes back here is the stream the guest is really handed - framing, wrapping and
    /// shared state included - so the budget below is compared against the actual size rather
    /// than against a bound on it.
    size_t measureBatchBytes(
        const ColumnsWithTypeAndName & arguments,
        size_t start_idx,
        size_t length,
        const std::vector<size_t> & declared_positions = {}) const
    {
        auto block = getArgumentsBlock(arguments, start_idx, length, declared_positions);
        NullWriteBuffer measure_buf;
        auto measure_out
            = context->getOutputFormat(serialization_format, measure_buf, block.cloneEmpty());

        /// `ColumnBinary` states the size of a block without writing it. This is the very
        /// primitive `executeOnBlock` sizes the guest buffer with, so the measurement and the
        /// allocation cannot disagree, and it is exact for the whole block being measured.
        if (auto precomputed = measure_out->precomputeSerializedSize(block, length))
            return *precomputed;

        measure_out->write(block);
        measure_out->finalize();
        return measure_buf.count();
    }

    /// The bytes a batch pays whatever its row count.
    ///
    /// A `ColumnConst` argument is one stored row broadcast to the batch, and a wire that carries
    /// constness writes that row once, so the argument costs the same at one row as at a whole
    /// block. Measuring the const arguments on their own prices exactly that part - and prices a
    /// merely wide first row, which is a row like any other and does shrink out of a batch, at
    /// nothing. On a wire that does not carry constness `getArgumentsBlock` materializes the
    /// argument, and what comes back is the cost of its one row, which is the truth there.
    size_t measureConstArgumentBytes(
        const ColumnsWithTypeAndName & arguments, size_t start_idx, const BatchMeasurer & measure) const
    {
        ColumnsWithTypeAndName const_arguments;
        std::vector<size_t> declared_positions;
        for (size_t i = 0; i < arguments.size(); ++i)
        {
            if (arguments[i].column && isColumnConst(*arguments[i].column))
            {
                const_arguments.push_back(arguments[i]);
                declared_positions.push_back(i);
            }
        }

        if (const_arguments.empty())
            return 0;
        return measure(const_arguments, start_idx, 1, declared_positions);
    }

    /// How many rows the call starting at `start_idx` should carry, out of `remaining`.
    ///
    /// The cost of a batch is monotone in its row count - adding a row can only grow the payload,
    /// and can only grow a per-batch dictionary - so "the rows that fit the budget" is a prefix and
    /// can be bracketed. Each probe measures a candidate exactly, keeps the largest candidate known
    /// to fit and the smallest known to overflow, and picks the next candidate inside that bracket,
    /// so the bracket shrinks on every step and the walk ends on the real boundary rather than on
    /// the first prefix that looked full enough.
    ///
    /// The next candidate follows the marginal cost of a row, taken as the slope between the last
    /// two measurements, not the average bytes per row of the candidate. The average carries the
    /// batch-wide part of the payload - framing, a `LowCardinality` dictionary, `Dynamic` and
    /// `Variant` structure prefixes, and any single wide row already in the prefix - which is paid
    /// once and does not grow with the rows added next. Dividing by it prices every further row at
    /// the cost of the whole prefix, so a block whose first row is far wider than the rest would be
    /// handed to the guest one row per call while hundreds of its rows still fit.
    ///
    /// Every candidate is bounded by rows already measured: the walk starts at one row and grows by
    /// a bounded factor per probe. Nothing is carried over from a previous batch or block, because a
    /// row count only means something for rows of a known width - a count fitted by narrow rows
    /// would have the next batch materialize that many wide rows before any measurement justified
    /// it, recreating the oversized call the split exists to avoid.
    ///
    /// `measure` prices a candidate. It defaults to the wire the buffered path writes; an ABI that
    /// builds its own guest buffer passes the layout pass that sizes that buffer instead, so what
    /// the walk compares against the budget is what the guest is really handed.
    size_t chooseBatchRows(
        const ColumnsWithTypeAndName & arguments,
        size_t start_idx,
        size_t remaining,
        size_t budget,
        const BatchMeasurer & measure) const
    {
        /// A function without arguments is handed no input buffer, so no size bounds its calls.
        if (arguments.empty())
            return remaining;

        static constexpr size_t max_probes = 16;
        /// A probe may only ask for this many times the rows the previous probe measured. The
        /// extrapolated count is read off a prefix, and a prefix of narrow rows says nothing about
        /// wider rows later in the block, so growth is paid for by rows already materialized.
        /// Reaching any batch size still costs a logarithmic number of probes.
        static constexpr size_t max_growth_per_probe = 4;

        /// Probe upwards from a single row, rather than downwards from the whole block. A probe
        /// serializes the candidate, and for a wire that does not carry constness a `ColumnConst`
        /// argument is materialized to do it, so a first probe of the whole block would expand
        /// exactly the input the splitting exists to rescue.
        size_t candidate = 1;
        size_t largest_fitting = 0;
        size_t smallest_overflowing = remaining + 1;

        /// The previous measurement, so the next candidate can be read off a slope. There is no
        /// previous measurement while `previous_rows` is zero.
        size_t previous_rows = 0;
        size_t previous_bytes = 0;

        for (size_t probe = 0; probe < max_probes; ++probe)
        {
            const size_t measured = measure(arguments, start_idx, candidate, {});
            if (measured <= budget)
            {
                largest_fitting = candidate;
                if (candidate == remaining)
                    break;
            }
            else
            {
                smallest_overflowing = candidate;
                if (candidate == 1)
                {
                    /// When what does not fit is the part of the batch that no row count changes,
                    /// no row count brings the call inside the budget: splitting then re-pays the
                    /// same bytes once per call and takes from the guest whatever it amortizes
                    /// across a call. Hand it the whole block instead - exceeding the budget once
                    /// beats exceeding it on every call of a one-row split.
                    if (measureConstArgumentBytes(arguments, start_idx, measure) >= budget)
                        return remaining;
                    /// Otherwise it is the row itself that does not fit, and it is still passed on
                    /// its own: whether the guest can hold it is for its allocator to say.
                    break;
                }
            }

            /// The boundary is known exactly once the bracket has nothing left between its ends.
            if (largest_fitting + 1 >= smallest_overflowing)
                break;

            /// An empty payload gives no slope to follow, so nothing bounds the batch but the block.
            if (measured == 0)
            {
                candidate = remaining;
                continue;
            }

            /// The marginal bytes a row adds. With one measurement in hand the average is all there
            /// is; it over-states the marginal cost, so the step it proposes is an undershoot, and
            /// the clamp below still moves the walk on by a row, which buys the second measurement
            /// the slope needs.
            Float64 bytes_per_row = static_cast<Float64>(measured) / static_cast<Float64>(candidate);
            if (previous_rows != 0 && candidate != previous_rows)
            {
                const Float64 slope = (static_cast<Float64>(measured) - static_cast<Float64>(previous_bytes))
                    / (static_cast<Float64>(candidate) - static_cast<Float64>(previous_rows));
                if (slope > 0.0)
                    bytes_per_row = slope;
            }
            previous_rows = candidate;
            previous_bytes = measured;

            const Float64 target = static_cast<Float64>(candidate)
                + (static_cast<Float64>(budget) - static_cast<Float64>(measured)) / bytes_per_row;

            size_t next = 1;
            if (target >= static_cast<Float64>(remaining))
                next = remaining;
            else if (target > 1.0)
                next = static_cast<size_t>(target);

            if (next > candidate)
                next = std::min(next, candidate * max_growth_per_probe);
            /// The bracket both keeps the candidate meaningful and guarantees progress: a candidate
            /// that fits raises the lower end past itself, one that overflows lowers the upper end
            /// below itself, and the check above leaves at least one row between the ends.
            candidate = std::clamp(next, largest_fitting + 1, smallest_overflowing - 1);
        }

        return std::max<size_t>(largest_fitting, 1);
    }

    void appendBatchResult(MutableColumnPtr & result_column, MutableColumnPtr batch_column) const
    {
        /// Under a const-preserving wire a guest may legitimately return `COL_IS_CONST`, which
        /// `ColumnBinaryInputFormat` decodes as a `ColumnConst`. `structureEquals` only holds
        /// between two `ColumnConst`s, so compare the unwrapped nested column rather than
        /// rejecting every valid const result.
        const IColumn * batch_for_check = batch_column.get();
        if (const auto * batch_const = typeid_cast<const ColumnConst *>(batch_for_check))
            batch_for_check = &batch_const->getDataColumn();
        if (!result_column->structureEquals(*batch_for_check))
            throw Exception(
                ErrorCodes::WASM_ERROR,
                "Different column types in result blocks: {} and {}",
                result_column->dumpStructure(),
                batch_column->dumpStructure());

        /// A `ColumnConst` batch result must be materialized before it is accumulated:
        /// `ColumnConst::insertRangeFrom` only bumps the row count without copying the source's
        /// value, so a const accumulator would keep repeating the first batch's value for every
        /// row appended afterwards.
        batch_column = IColumn::mutate(batch_column->convertToFullColumnIfConst());
        if (result_column->empty())
            result_column = std::move(batch_column);
        else
            result_column->insertRangeFrom(*batch_column, 0, batch_column->size());
    }

    ColumnPtr execute(WebAssembly::WasmCompartment * compartment, const ColumnsWithTypeAndName & arguments, size_t input_rows_count) const
    {
        /// A module whose linear memory is bounded at zero bytes can hold no input at all, whatever
        /// the batching is. This is reported before any measurement, because a function without
        /// arguments has no row to attribute the failure to and would otherwise fail inside the
        /// guest allocator.
        if (input_rows_count > 0 && user_defined_function->requiresGuestLinearMemory()
            && compartment->getMaxLinearMemorySize() == 0)
            throw Exception(ErrorCodes::WASM_ERROR,
                "The maximum linear memory of the module is 0 bytes, so it cannot hold the input of the function");

        MutableColumnPtr result_column = user_defined_function->getResultType()->createColumn();

        const size_t fixed_block_size = context->getSettingsRef()[Setting::webassembly_udf_max_input_block_size];
        const std::optional<size_t> budget = getInputBudget(compartment, fixed_block_size);

        size_t batch_start = 0;
        auto flush_batch = [&](size_t end_idx)
        {
            if (end_idx <= batch_start)
                return;
            const size_t batch_size = end_idx - batch_start;
            Block block;
            {
                ProfileEventTimeIncrement<Microseconds> timer_prep(ProfileEvents::WasmInputBlockPrepMicroseconds);
                block = getArgumentsBlock(arguments, batch_start, batch_size);
            }
            auto stop_token = interrupt_source.get_token();
            auto batch_result = user_defined_function->executeOnBlock(compartment, block, context, batch_size, stop_token);
            {
                ProfileEventTimeIncrement<Microseconds> timer_assembly(ProfileEvents::WasmResultAssemblyMicroseconds);
                appendBatchResult(result_column, std::move(batch_result));
            }
            batch_start = end_idx;
        };

        if (budget)
        {
            /// Take the rows a call can hold, measure the call, and start the next one where
            /// it ended. A stride derived from an average row size cannot bound a skewed block:
            /// one huge row among many tiny ones would still share a call with its neighbours.
            const BatchMeasurer measure_wire = [this](
                const ColumnsWithTypeAndName & batch_cols, size_t start_idx, size_t length,
                const std::vector<size_t> & declared_positions) // STYLE_CHECK_ALLOW_STD_CONTAINERS
            { return measureBatchBytes(batch_cols, start_idx, length, declared_positions); };

            while (batch_start < input_rows_count)
                flush_batch(
                    batch_start + chooseBatchRows(arguments, batch_start, input_rows_count - batch_start, *budget, measure_wire));
        }
        else if (fixed_block_size > 0)
        {
            for (size_t row = fixed_block_size; row < input_rows_count; row += fixed_block_size)
                flush_batch(row);
        }

        flush_batch(input_rows_count);
        return result_column;
    }

    /// `declared_positions` says where each entry of `arguments` sits in the function's declared
    /// argument list. It is empty when `arguments` is that list in order, and is given only when a
    /// caller passes a subset of the arguments - the declared type and name of an argument are
    /// looked up by its declared position, not by where it happens to land in `arguments`.
    Block getArgumentsBlock(
        const ColumnsWithTypeAndName & arguments,
        size_t start_idx,
        size_t length,
        const std::vector<size_t> & declared_positions = {}) const
    {
        const auto & declared_arguments = user_defined_function->getArguments();
        Block arguments_block;
        for (size_t i = 0; i < arguments.size(); ++i)
        {
            const size_t declared_idx = declared_positions.empty() ? i : declared_positions[i];
            /// Cut first, materialize second: `ColumnConst::cut` is O(1), while materializing
            /// the whole block first would make the per-row measurement O(rows^2). A wire that
            /// encodes constness itself keeps the wrapper instead of materializing at all.
            ColumnPtr column = arguments[i].column->cut(start_idx, length);
            if (!preserve_const_columns)
                column = column->convertToFullColumnIfConst();
            String column_name = declared_idx < argument_names.size() && !argument_names[declared_idx].empty()
                ? argument_names[declared_idx]
                : arguments[i].name;
            /// Cast to the declared type so serialization uses the correct width.
            /// Without this, e.g. Int8 passed to an Int32 parameter would be serialized
            /// as 1 byte by RowBinary instead of 4, causing the WASM module to read garbage.
            /// `ColumnBinary`'s descriptor only encodes a coarse width class (`COL_FIXED8/16/32/64`),
            /// not exact signedness - a `UInt8(255)` and an `Int8(-1)` both serialize to the same
            /// single `0xff` byte, so a guest reading a declared `Int32` has no way to tell them
            /// apart. Always cast here regardless of format until the wire carries real logical
            /// type and signedness information.
            const DataTypePtr & declared_type = declared_arguments[declared_idx];
            if (!arguments[i].type->equals(*declared_type))
                column = castColumn(ColumnWithTypeAndName(column, arguments[i].type, column_name), declared_type);
            arguments_block.insert(ColumnWithTypeAndName(column, declared_type, column_name));
        }
        return arguments_block;
    }

    std::shared_ptr<UserDefinedWebAssemblyFunction> user_defined_function;
    std::shared_ptr<WebAssembly::WasmModule> wasm_module;
    String function_name;
    Strings argument_names;
    ContextPtr context;
    /// Whether the configured wire keeps a top-level `ColumnConst` compact instead of
    /// materializing it - `ColumnBinary`'s `COL_IS_CONST`. Driven off the format's own
    /// capabilities rather than its name: `Buffers` exposes a native serialization but
    /// `NativeWriter::writeData` calls `convertToFullColumnIfConst` before writing, so it is
    /// not const-preserving.
    bool preserve_const_columns;

    String serialization_format;

    /// Configured `webassembly_udf_max_memory` in bytes, empty when the host caps nothing.
    std::optional<size_t> module_memory_limit;

    mutable StopSource interrupt_source;
    mutable WasmCompartmentPool compartment_pool;
};

/// Aggregate function wrapper for WASM UDFs with is_aggregate=1.
///
/// Argument rows are accumulated per group as a singly linked list of arena nodes, each
/// holding one row with all arguments serialized back to back. A per-group MutableColumns
/// would be simpler, but every ColumnString there allocates a 4 KiB PODArray for its chars
/// and another for its offsets, so a query with millions of small groups pays several KiB
/// per group no matter how few rows it holds.
///
/// Results are produced a whole batch of groups at a time (see insertResultIntoBatch): the
/// accumulated rows are flattened into one Array(T) column per argument, with one array
/// element per group, and handed to the underlying WASM function in a single call. Calling
/// it once per group instead makes the compartment acquire and block marshalling dominate
/// everything else once the group count reaches the millions.
class AggregateFunctionUserDefinedWasm final
    : public IAggregateFunctionHelper<AggregateFunctionUserDefinedWasm>
{
public:
    AggregateFunctionUserDefinedWasm(
        String function_name_,
        std::shared_ptr<UserDefinedWebAssemblyFunction> wasm_function_,
        DataTypes original_arg_types_,
        ContextPtr context_)
        : IAggregateFunctionHelper<AggregateFunctionUserDefinedWasm>(original_arg_types_, {}, wasm_function_->getResultType())
        , function_name(std::move(function_name_))
        , wasm_function(std::move(wasm_function_))
        , original_arg_types(std::move(original_arg_types_))
        , context(std::move(context_))
        , interrupt_source()
        , compartment_pool(
              static_cast<UInt32>(context->getSettingsRef()[Setting::webassembly_udf_max_instances]),
              wasm_function->getModule(),
              getWasmModuleConfig(context, wasm_function->getSettings().getFuelMode()),
              interrupt_source.get_token(),
              tryGetModuleInitFn(wasm_function->getModule()))
    {
        arg_encodings.reserve(original_arg_types.size());
        fixed_value_sizes.reserve(original_arg_types.size());
        for (const auto & type : original_arg_types)
        {
            auto sample = type->createColumn();
            if (sample->isFixedAndContiguous())
            {
                arg_encodings.push_back(ArgEncoding::FixedWidth);
                fixed_value_sizes.push_back(sample->sizeOfValueIfFixed());
            }
            else if (typeid_cast<const ColumnString *>(sample.get()) != nullptr)
            {
                arg_encodings.push_back(ArgEncoding::VariableWidth);
                fixed_value_sizes.push_back(0);
            }
            else
            {
                arg_encodings.push_back(ArgEncoding::Serialized);
                fixed_value_sizes.push_back(0);
            }
        }
    }

    String getName() const override { return function_name; }
    bool allocatesMemoryInArena() const override { return true; }
    bool hasTrivialDestructor() const override { return true; }

    size_t sizeOfData() const override { return sizeof(State); }
    size_t alignOfData() const override { return alignof(State); }

    void create(AggregateDataPtr __restrict place) const override
    {
        new (place) State();
    }

    void destroy(AggregateDataPtr __restrict place) const noexcept override
    {
        /// Everything the state owns lives in the arena and dies with it.
        reinterpret_cast<State *>(place)->~State();
    }

    void add(AggregateDataPtr __restrict place, const IColumn ** columns, size_t row_num, Arena * arena) const override
    {
        appendRow(*reinterpret_cast<State *>(place), columns, row_num, arena);
    }

    void addBatchSinglePlace(
        size_t row_begin, size_t row_end,
        AggregateDataPtr __restrict place,
        const IColumn ** columns,
        Arena * arena,
        ssize_t if_argument_pos) const override
    {
        auto & state = *reinterpret_cast<State *>(place);
        if (if_argument_pos >= 0)
        {
            const auto & filter = assert_cast<const ColumnUInt8 &>(*columns[if_argument_pos]);
            for (size_t row = row_begin; row < row_end; ++row)
                if (filter.getElement(row))
                    appendRow(state, columns, row, arena);
            return;
        }
        for (size_t row = row_begin; row < row_end; ++row)
            appendRow(state, columns, row, arena);
    }

    void mergeImpl(AggregateDataPtr __restrict place, ConstAggregateDataPtr rhs, Arena * arena) const override
    {
        auto & state = *reinterpret_cast<State *>(place);
        const auto & rhs_state = *reinterpret_cast<const State *>(rhs);

        /// The two states may live in different arenas, so the bytes have to be copied
        /// rather than the lists spliced.
        for (const Node * node = rhs_state.head; node != nullptr; node = node->next)
        {
            char * copy = arena->alloc(node->size);
            memcpy(copy, node->data, node->size);
            pushNode(state, copy, node->size, arena);
        }
    }

    void serialize(ConstAggregateDataPtr __restrict place, WriteBuffer & buf, std::optional<size_t> /*version*/) const override
    {
        const auto * state = reinterpret_cast<const State *>(place);
        auto columns = materializeRows(&state, 1);
        writeVarUInt(state->num_rows, buf);
        for (size_t i = 0; i < columns.size(); ++i)
        {
            auto serialization = original_arg_types[i]->getDefaultSerialization();
            serialization->serializeBinaryBulk(*columns[i], buf, 0, state->num_rows);
        }
    }

    void deserialize(AggregateDataPtr __restrict place, ReadBuffer & buf, std::optional<size_t> /*version*/, Arena * arena) const override
    {
        auto & state = *reinterpret_cast<State *>(place);
        size_t num_rows = 0;
        readVarUInt(num_rows, buf);

        MutableColumns columns;
        columns.reserve(original_arg_types.size());
        for (const auto & type : original_arg_types)
            columns.push_back(type->createColumn());
        for (size_t i = 0; i < columns.size(); ++i)
        {
            auto serialization = original_arg_types[i]->getDefaultSerialization();
            serialization->deserializeBinaryBulk(*columns[i], buf, num_rows, /*avg_value_size_hint=*/0);
        }

        std::vector<const IColumn *> raw;
        raw.reserve(columns.size());
        for (const auto & column : columns)
            raw.push_back(column.get());
        for (size_t row = 0; row < num_rows; ++row)
            appendRow(state, raw.data(), row, arena);
    }

    void insertResultInto(AggregateDataPtr __restrict place, IColumn & to, Arena *) const override
    {
        const auto * state = reinterpret_cast<const State *>(place);
        executeOnGroups(&state, 1, to);
    }

    void insertResultIntoBatch(
        size_t row_begin,
        size_t row_end,
        AggregateDataPtr * places,
        size_t place_offset,
        IColumn & to,
        Arena *) const override
    {
        std::vector<const State *> states;
        states.reserve(row_end - row_begin);
        for (size_t i = row_begin; i < row_end; ++i)
            states.push_back(reinterpret_cast<const State *>(places[i] + place_offset));

        try
        {
            executeOnGroups(states.data(), states.size(), to);
        }
        catch (...)
        {
            /// Nothing was inserted, so every place in the range is still ours to destroy.
            for (size_t i = row_begin; i < row_end; ++i)
                destroy(places[i] + place_offset);
            throw;
        }

        for (size_t i = row_begin; i < row_end; ++i)
            destroyUpToState(places[i] + place_offset);
    }

private:
    /// One accumulated row: all arguments serialized back to back, immediately after the node.
    enum class ArgEncoding : uint8_t
    {
        FixedWidth,
        VariableWidth,
        Serialized,
    };

    struct Node
    {
        Node * next;
        const char * data;
        size_t size;
    };

    struct State
    {
        Node * head = nullptr;
        Node * tail = nullptr;
        size_t num_rows = 0;
    };

    void pushNode(State & state, const char * data, size_t size, Arena * arena) const
    {
        auto * node = reinterpret_cast<Node *>(arena->alignedAlloc(sizeof(Node), alignof(Node)));
        node->next = nullptr;
        node->data = data;
        node->size = size;

        if (state.tail != nullptr)
            state.tail->next = node;
        else
            state.head = node;
        state.tail = node;
        ++state.num_rows;
    }

    void appendRow(State & state, const IColumn ** columns, size_t row_num, Arena * arena) const
    {
        const auto settings = IColumn::SerializationSettings::createForAggregationState();
        const char * begin = nullptr;
        size_t size = 0;
        for (size_t i = 0; i < arg_encodings.size(); ++i)
        {
            switch (arg_encodings[i])
            {
                case ArgEncoding::FixedWidth:
                {
                    const size_t value_size = fixed_value_sizes[i];
                    memcpy(arena->allocContinue(value_size, begin), columns[i]->getDataAt(row_num).data(), value_size);
                    size += value_size;
                    break;
                }
                case ArgEncoding::VariableWidth:
                {
                    const auto value = columns[i]->getDataAt(row_num);
                    const auto value_size = static_cast<UInt32>(value.size());
                    char * pos = arena->allocContinue(sizeof(value_size) + value.size(), begin);
                    memcpy(pos, &value_size, sizeof(value_size));
                    memcpy(pos + sizeof(value_size), value.data(), value.size());
                    size += sizeof(value_size) + value.size();
                    break;
                }
                case ArgEncoding::Serialized:
                    size += columns[i]->serializeValueIntoArena(row_num, *arena, begin, &settings).size();
                    break;
            }
        }
        pushNode(state, begin, size, arena);
    }

    /// Flatten the rows of `num_groups` consecutive states into one column per argument.
    MutableColumns materializeRows(const State * const * states, size_t num_groups) const
    {
        auto settings = IColumn::SerializationSettings::createForAggregationState();

        size_t total_rows = 0;
        for (size_t g = 0; g < num_groups; ++g)
            total_rows += states[g]->num_rows;

        MutableColumns columns;
        columns.reserve(original_arg_types.size());
        for (const auto & type : original_arg_types)
        {
            columns.push_back(type->createColumn());
            columns.back()->reserve(total_rows);
        }

        for (size_t g = 0; g < num_groups; ++g)
        {
            for (const Node * node = states[g]->head; node != nullptr; node = node->next)
            {
                const char * cursor = node->data;
                const char * row_end = node->data + node->size;
                for (size_t i = 0; i < columns.size(); ++i)
                {
                    switch (arg_encodings[i])
                    {
                        case ArgEncoding::FixedWidth:
                            columns[i]->insertData(cursor, fixed_value_sizes[i]);
                            cursor += fixed_value_sizes[i];
                            break;
                        case ArgEncoding::VariableWidth:
                        {
                            UInt32 value_size = 0;
                            memcpy(&value_size, cursor, sizeof(value_size));
                            cursor += sizeof(value_size);
                            columns[i]->insertData(cursor, value_size);
                            cursor += value_size;
                            break;
                        }
                        case ArgEncoding::Serialized:
                        {
                            ReadBufferFromMemory in(cursor, row_end - cursor);
                            columns[i]->deserializeAndInsertFromArena(in, &settings);
                            cursor += in.count();
                            break;
                        }
                    }
                }
            }
        }
        return columns;
    }

    /// One WASM call for the whole batch: each argument becomes an Array(T) column with one
    /// array element per group.
    void executeOnGroups(const State * const * states, size_t num_groups, IColumn & to) const
    {
        auto columns = materializeRows(states, num_groups);

        PaddedPODArray<UInt64> offsets;
        offsets.reserve(num_groups);
        UInt64 total = 0;
        for (size_t g = 0; g < num_groups; ++g)
        {
            total += states[g]->num_rows;
            offsets.push_back(total);
        }

        Block block;
        for (size_t i = 0; i < original_arg_types.size(); ++i)
        {
            auto offsets_col = ColumnArray::ColumnOffsets::create();
            offsets_col->getData().assign(offsets);
            auto array_col = ColumnArray::create(std::move(columns[i]), std::move(offsets_col));
            auto array_type = std::make_shared<DataTypeArray>(original_arg_types[i]);
            block.insert(ColumnWithTypeAndName(std::move(array_col), array_type, "arg" + std::to_string(i)));
        }

        auto compartment_entry = compartment_pool.acquire();
        StopSource stop_source;
        auto result_col = wasm_function->executeOnBlock(&(*compartment_entry), block, context, num_groups, stop_source.get_token());

        if (result_col->size() != num_groups)
            throw Exception(ErrorCodes::WASM_ERROR,
                "WASM aggregate function '{}' returned {} rows, expected {}",
                function_name, result_col->size(), num_groups);

        to.insertRangeFrom(*result_col, 0, num_groups);
    }

    String function_name;
    std::shared_ptr<UserDefinedWebAssemblyFunction> wasm_function;
    DataTypes original_arg_types;
    std::vector<ArgEncoding> arg_encodings;
    std::vector<size_t> fixed_value_sizes;
    ContextPtr context;
    mutable StopSource interrupt_source;
    mutable WasmCompartmentPool compartment_pool;
};

/// Returns true if `actual` argument types are compatible with `expected` for WASM dispatch.
/// Mirrors the per-argument matching logic in FunctionUserDefinedWasm::getReturnTypeImpl.
static bool typesMatchOverload(const DataTypes & actual, const DataTypes & expected)
{
    if (actual.size() != expected.size())
        return false;
    for (size_t i = 0; i < actual.size(); ++i)
    {
        if (actual[i]->equals(*expected[i]))
            continue;
        const DataTypePtr & stripped = removeNullable(actual[i]);
        if (stripped->equals(*expected[i]))
            continue;
        auto actual_kind   = wasmKindForDataType(stripped.get());
        auto expected_kind = wasmKindForDataType(expected[i].get());
        if (actual_kind && expected_kind && *actual_kind == *expected_kind)
            continue;
        // Allow CH geo types (Point, LineString, …) to satisfy a Geometry (Variant) parameter.
        // Constant-folded geo literals arrive as bare structural types (e.g. Tuple(Float64,Float64))
        // without the custom name, so we also accept structural matches against variant members.
        if (const auto * variant_type = typeid_cast<const DataTypeVariant *>(expected[i].get()))
        {
            if (variant_type->tryGetVariantDiscriminator(stripped->getName()).has_value())
                continue;
            bool structural_match = false;
            for (const auto & v : variant_type->getVariants())
                if (stripped->equals(*v)) { structural_match = true; break; }
            if (structural_match)
                continue;
        }
        return false;
    }
    return true;
}

/// Overload resolver for WASM functions registered under the same SQL name with different
/// argument type signatures.  At planning time it picks the first overload whose declared
/// argument types match the call-site types; the selected FunctionBase carries exactly that
/// one overload into executeImpl, so no re-selection is needed at execution time.
class WasmOverloadResolver : public IFunctionOverloadResolver
{
public:
    WasmOverloadResolver(
        String name_,
        std::vector<std::shared_ptr<FunctionUserDefinedWasm>> overloads_,
        ContextPtr context_)
        : name(std::move(name_))
        , overloads(std::move(overloads_))
        , context(std::move(context_))
    {}

    String getName() const override { return name; }
    bool isVariadic() const override { return false; }
    size_t getNumberOfArguments() const override
    {
        return overloads.empty() ? 0 : overloads.front()->getNumberOfArguments();
    }

    bool isDeterministic() const override
    {
        return overloads.empty() || overloads.front()->isDeterministic();
    }
    bool useDefaultImplementationForNulls() const override
    {
        return overloads.empty() || overloads.front()->useDefaultImplementationForNulls();
    }
    bool isSpatialPredicate() const override
    {
        return !overloads.empty() && overloads.front()->isSpatialPredicate();
    }

    // Disable CH's automatic Variant-expansion so that a Geometry argument reaches the
    // overload that explicitly declares Geometry rather than being fanned out over each
    // Variant alternative (Point, LineString, …) before we get a chance to match.
    bool useDefaultImplementationForVariant() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        for (const auto & fn : overloads)
        {
            if (typesMatchOverload(arguments, fn->getArgumentTypes()))
                return fn->getReturnTypeImpl(arguments);
        }
        auto get_names = std::views::transform([](const auto & t) { return t->getName(); });
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "No matching overload of '{}' for argument types ({})",
            name,
            fmt::join(arguments | get_names, ", "));
    }

    FunctionBasePtr buildImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type) const override
    {
        DataTypes types;
        types.reserve(arguments.size());
        for (const auto & a : arguments)
            types.push_back(a.type);

        for (const auto & fn : overloads)
        {
            if (typesMatchOverload(types, fn->getArgumentTypes()))
                return std::make_unique<FunctionToFunctionBaseAdaptor>(fn, types, result_type);
        }
        throw Exception(
            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
            "No matching overload of '{}' during build",
            name);
    }

private:
    String name;
    std::vector<std::shared_ptr<FunctionUserDefinedWasm>> overloads;
    ContextPtr context;
};

UserDefinedWebAssemblyFunctionFactory::RegisteredFunction
UserDefinedWebAssemblyFunctionFactory::prepareFunction(ASTPtr create_function_query, WasmModuleManager & module_manager) const
{
    auto * create_query = typeid_cast<ASTCreateWasmFunctionQuery *>(create_function_query.get());
    if (!create_query)
        throw Exception(
            ErrorCodes::LOGICAL_ERROR,
            "Expected definition of WebAssembly function, got {}",
            create_function_query ? create_function_query->formatForErrorMessage() : "nullptr");

    auto function_def = create_query->validateAndGetDefinition();
    auto fuel_mode = function_def.settings.getFuelMode();
    auto [wasm_module, module_hash] = module_manager.getModule(function_def.module_name, fuel_mode);
    transformEndianness<std::endian::big>(module_hash);
    String module_hash_str = getHexUIntLowercase(module_hash);
    if (function_def.module_hash.empty())
    {
        create_query->setModuleHash(module_hash_str);
    }
    else if (function_def.module_hash != module_hash_str)
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "WebAssembly module '{}' digest mismatch, expected {}, got {}",
            function_def.module_name,
            module_hash_str,
            function_def.module_hash);
    }

    const auto & internal_function_name
        = function_def.source_function_name.empty() ? function_def.function_name : function_def.source_function_name;

    const bool is_aggregate = function_def.settings.isAggregate();

    if (is_aggregate && function_def.abi_version == WasmAbiVersion::RowDirect)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "SETTINGS is_aggregate = 1 is not supported with ABI ROW_DIRECT: array arguments "
            "cannot be passed as scalar WASM values. Use ABI BUFFERED_V1 instead.");

    /// For aggregate functions, the WASM export receives Array(T) arguments — one array per declared
    /// argument type containing all accumulated rows for the group.
    /// If the declared type is already Array(T), use T as the accumulator element type and keep
    /// the WASM signature as Array(T) (no double-wrapping).
    /// If the declared type is scalar T, wrap to Array(T) for the WASM signature.
    DataTypes wasm_arg_types = function_def.argument_types;
    DataTypes accumulator_arg_types;
    if (is_aggregate)
    {
        accumulator_arg_types.reserve(wasm_arg_types.size());
        for (auto & type : wasm_arg_types)
        {
            if (const auto * array_type = typeid_cast<const DataTypeArray *>(type.get()))
            {
                accumulator_arg_types.push_back(array_type->getNestedType());
                /// type is already Array(T) — leave wasm_arg_types entry unchanged
            }
            else
            {
                accumulator_arg_types.push_back(type);
                type = std::make_shared<DataTypeArray>(type);
            }
        }
    }

    std::shared_ptr<UserDefinedWebAssemblyFunction> wasm_func = UserDefinedWebAssemblyFunction::create(
        wasm_module,
        internal_function_name,
        function_def.argument_names,
        function_def.argument_types,
        function_def.result_type,
        function_def.abi_version,
        function_def.settings,
        function_def.is_deterministic);

    return RegisteredFunction{
        function_def.function_name,
        std::move(wasm_func),
        function_def.argument_types,
        std::move(create_function_query),
        is_aggregate,
        accumulator_arg_types};
}

std::shared_ptr<UserDefinedWebAssemblyFunction>
UserDefinedWebAssemblyFunctionFactory::addOrReplace(ASTPtr create_function_query, WasmModuleManager & module_manager)
{
    auto registered_function = prepareFunction(std::move(create_function_query), module_manager);
    auto wasm_func = registered_function.function;
    addOrReplace(std::move(registered_function));
    return wasm_func;
}

void UserDefinedWebAssemblyFunctionFactory::addOrReplace(RegisteredFunction registered_function)
{
    std::unique_lock lock(registry_mutex);
    auto & entries = registry[registered_function.sql_name];
    // Replace an existing overload with the same argument types; otherwise add a new one.
    for (auto & entry : entries)
    {
        if (typesMatchOverload(registered_function.original_arg_types, entry.original_arg_types))
        {
            entry = RegistryEntry{
                std::move(registered_function.function),
                std::move(registered_function.original_arg_types),
                std::move(registered_function.create_query),
                registered_function.is_aggregate,
                std::move(registered_function.accumulator_arg_types)};
            return;
        }
    }
    const bool is_aggregate = registered_function.is_aggregate;
    const String sql_name = registered_function.sql_name;
    DataTypes accumulator_arg_types = registered_function.accumulator_arg_types;
    entries.push_back(RegistryEntry{
        std::move(registered_function.function),
        std::move(registered_function.original_arg_types),
        std::move(registered_function.create_query),
        is_aggregate,
        std::move(registered_function.accumulator_arg_types)});

    if (is_aggregate)
        registerAggregateName(sql_name, accumulator_arg_types);
}

void UserDefinedWebAssemblyFunctionFactory::registerAggregateName(
    const String & sql_name, const DataTypes & accumulator_arg_types)
{
    if (!aggregate_names.insert(sql_name).second)
        return;
    if (AggregateFunctionFactory::instance().hasNameOrAlias(sql_name))
        return;

    /// The creator resolves the WASM function on every call rather than capturing it, so the
    /// registration stays valid across DROP + CREATE OR REPLACE of the underlying function.
    AggregateFunctionCreator wasm_aggregate_creator =
        {[sql_name, accumulator_arg_types](const String &, const DataTypes &, const Array &, const Settings *) -> AggregateFunctionPtr
         {
             ContextPtr ctx;
             if (CurrentThread::isInitialized())
                 ctx = CurrentThread::get().tryGetQueryContext();
             return UserDefinedWebAssemblyFunctionFactory::instance().getAggregate(sql_name, accumulator_arg_types, ctx);
         }};
    AggregateFunctionFactory::instance().registerFunction(
        sql_name,
        AggregateFunctionWithProperties{wasm_aggregate_creator, FunctionDocumentation{}, AggregateFunctionProperties{}});
}

bool UserDefinedWebAssemblyFunctionFactory::ownsAggregateName(const String & function_name) const
{
    std::shared_lock lock(registry_mutex);
    return aggregate_names.contains(function_name);
}

void UserDefinedWebAssemblyFunctionFactory::replaceAll(VectorWithMemoryTracking<RegisteredFunction> registered_functions)
{
    UnorderedMapWithMemoryTracking<String, std::vector<RegistryEntry>> new_registry;
    std::vector<std::pair<String, DataTypes>> aggregates;
    for (auto & registered_function : registered_functions)
    {
        if (registered_function.is_aggregate)
            aggregates.emplace_back(registered_function.sql_name, registered_function.accumulator_arg_types);
        new_registry[registered_function.sql_name].push_back(RegistryEntry{
            std::move(registered_function.function),
            std::move(registered_function.original_arg_types),
            std::move(registered_function.create_query),
            registered_function.is_aggregate,
            std::move(registered_function.accumulator_arg_types)});
    }

    std::unique_lock lock(registry_mutex);
    registry = std::move(new_registry);
    /// Functions restored from storage at startup are registered here too; skipping this left
    /// them callable only as scalar functions.
    for (const auto & [sql_name, accumulator_arg_types] : aggregates)
        registerAggregateName(sql_name, accumulator_arg_types);
}

bool UserDefinedWebAssemblyFunctionFactory::has(const String & function_name) const
{
    std::shared_lock lock(registry_mutex);
    auto it = registry.find(function_name);
    return it != registry.end() && !it->second.empty();
}

bool UserDefinedWebAssemblyFunctionFactory::hasOverload(const String & function_name, const DataTypes & arg_types) const
{
    std::shared_lock lock(registry_mutex);
    auto it = registry.find(function_name);
    if (it == registry.end() || it->second.empty())
        return false;
    for (const auto & entry : it->second)
        if (typesMatchOverload(arg_types, entry.original_arg_types))
            return true;
    return false;
}

std::shared_ptr<UserDefinedWebAssemblyFunction> UserDefinedWebAssemblyFunctionFactory::getFunction(const String & function_name) const
{
    std::shared_lock lock(registry_mutex);
    auto it = registry.find(function_name);
    if (it == registry.end() || it->second.empty())
        return nullptr;
    return it->second.front().function;
}

void UserDefinedWebAssemblyFunctionFactory::checkWebAssemblyIsAvailable(const ContextPtr & context)
{
    /// `getWasmModuleManager` always throws `SUPPORT_IS_DISABLED` here, and it is the single place that
    /// words the difference between the engine being turned off and being absent from the build.
    if (!context->hasWasmModuleManager())
        context->getWasmModuleManager();
}

FunctionOverloadResolverPtr UserDefinedWebAssemblyFunctionFactory::get(const String & function_name, ContextPtr context)
{
    std::vector<std::shared_ptr<FunctionUserDefinedWasm>> overload_fns;
    {
        std::shared_lock lock(registry_mutex);
        auto it = registry.find(function_name);
        if (it == registry.end() || it->second.empty())
            throw Exception(
                ErrorCodes::RESOURCE_NOT_FOUND,
                "WebAssembly function '{}' not found in [{}]",
                function_name,
                fmt::join(registry | std::views::transform([](const auto & pair) { return pair.first; }), ", "));
        for (const auto & entry : it->second)
            overload_fns.push_back(std::make_shared<FunctionUserDefinedWasm>(function_name, entry.function, context));
    }

    if (overload_fns.size() == 1)
        return std::make_unique<FunctionToOverloadResolverAdaptor>(std::move(overload_fns.front()));
    return std::make_unique<WasmOverloadResolver>(function_name, std::move(overload_fns), std::move(context));
}

FunctionOverloadResolverPtr UserDefinedWebAssemblyFunctionFactory::tryGet(const String & function_name, ContextPtr context)
{
    std::shared_ptr<UserDefinedWebAssemblyFunction> wasm_func = nullptr;
    {
        std::shared_lock lock(registry_mutex);
        auto it = registry.find(function_name);
        if (it == registry.end())
            return nullptr;
        wasm_func = it->second.front().function;
    }

    auto executable_function = std::make_shared<FunctionUserDefinedWasm>(function_name, std::move(wasm_func), std::move(context));
    return std::make_unique<FunctionToOverloadResolverAdaptor>(std::move(executable_function));
}

bool UserDefinedWebAssemblyFunctionFactory::isAggregate(const String & function_name) const
{
    std::shared_lock lock(registry_mutex);
    auto it = registry.find(function_name);
    return it != registry.end() && !it->second.empty() && it->second.front().is_aggregate;
}

AggregateFunctionPtr UserDefinedWebAssemblyFunctionFactory::getAggregate(
    const String & function_name, const DataTypes & arg_types, ContextPtr context) const
{
    std::shared_ptr<UserDefinedWebAssemblyFunction> wasm_func;
    DataTypes original_arg_types;
    {
        std::shared_lock lock(registry_mutex);
        auto it = registry.find(function_name);
        if (it == registry.end() || it->second.empty())
            throw Exception(
                ErrorCodes::RESOURCE_NOT_FOUND,
                "WebAssembly aggregate function '{}' not found",
                function_name);

        const RegistryEntry * match = nullptr;
        for (const auto & entry : it->second)
        {
            if (!entry.is_aggregate)
                continue;
            if (arg_types.empty() || typesMatchOverload(arg_types, entry.accumulator_arg_types))
            {
                match = &entry;
                break;
            }
        }
        if (!match)
            match = &it->second.front(); // fallback: first aggregate entry
        if (!match->is_aggregate)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "WebAssembly function '{}' is not an aggregate function",
                function_name);
        wasm_func = match->function;
        original_arg_types = match->accumulator_arg_types;
    }
    return std::make_shared<AggregateFunctionUserDefinedWasm>(
        function_name, std::move(wasm_func), std::move(original_arg_types), std::move(context));
}


bool UserDefinedWebAssemblyFunctionFactory::dropIfExists(const String & function_name)
{
    // Backward compatibility: call the new method with empty argument types
    return dropIfExists(function_name, {});
}

bool UserDefinedWebAssemblyFunctionFactory::dropIfExists(const String & function_name, const Strings & argument_type_names)
{
    std::unique_lock lock(registry_mutex);
    auto it = registry.find(function_name);
    if (it == registry.end())
        return false;

    auto & entries = it->second;

    if (argument_type_names.empty())
    {
        /// Drop all overloads
        registry.erase(it);
        return true;
    }

    /// Drop only the overload matching the signature
    auto new_end = std::remove_if(entries.begin(), entries.end(),
        [&argument_type_names](const RegistryEntry & entry)
    {
        if (entry.original_arg_types.size() != argument_type_names.size())
            return false;
        for (size_t i = 0; i < argument_type_names.size(); ++i)
            if (entry.original_arg_types[i]->getName() != argument_type_names[i])
                return false;
        return true;
    });

    bool removed = (new_end != entries.end());
    if (removed)
    {
        entries.erase(new_end, entries.end());
        if (entries.empty())
            registry.erase(it);
    }
    return removed;
}

VectorWithMemoryTracking<UserDefinedWebAssemblyFunctionFactory::RegisteredFunction> UserDefinedWebAssemblyFunctionFactory::getAllFunctions() const
{
    std::shared_lock lock(registry_mutex);
    VectorWithMemoryTracking<RegisteredFunction> result;
    for (const auto & [sql_name, entries] : registry)
        for (const auto & entry : entries)
            result.push_back(RegisteredFunction{
                .sql_name = sql_name,
                .function = entry.function,
                .original_arg_types = entry.original_arg_types,
                .create_query = entry.create_query,
                .is_aggregate = entry.is_aggregate,
                .accumulator_arg_types = entry.accumulator_arg_types});
    return result;
}



UserDefinedWebAssemblyFunctionFactory & UserDefinedWebAssemblyFunctionFactory::instance()
{
    static UserDefinedWebAssemblyFunctionFactory factory;
    return factory;
}

// ─────────────────────────────────────────────────────────────────────────────
// WASM chain executor
//
// Executes a validated SOURCE → XFORM* → SINK chain in a single WASM call via
// clickhouse_chain_execute.
//
// Chain buffer layout (passed to WASM):
//   [n_funcs: u32][cstr name_0]...[cstr name_n-1][pad to 8B][`ColumnBinary` frame]
// ─────────────────────────────────────────────────────────────────────────────

class FunctionUserDefinedWasmChain : public IFunction
{
public:
    FunctionUserDefinedWasmChain(
        String name_,
        Strings fn_names_,
        std::shared_ptr<WebAssembly::WasmModule> wasm_module_,
        DataTypes source_arg_types_,
        DataTypePtr result_type_,
        std::vector<std::vector<Field>> fn_scalar_values_,
        std::vector<DataTypes>          fn_scalar_types_,
        ContextPtr context_,
        WebAssembly::FuelMode fuel_mode_)
        : name(std::move(name_))
        , fn_names(std::move(fn_names_))
        , wasm_module(std::move(wasm_module_))
        , source_arg_types(std::move(source_arg_types_))
        , result_type(std::move(result_type_))
        , fn_scalar_values(std::move(fn_scalar_values_))
        , fn_scalar_types(std::move(fn_scalar_types_))
        , context(std::move(context_))
        , fuel_mode(fuel_mode_)
        , compartment_pool(
              static_cast<UInt32>(context->getSettingsRef()[Setting::webassembly_udf_max_instances]),
              wasm_module,
              getWasmModuleConfig(context, fuel_mode),
              interrupt_source.get_token(),
              tryGetModuleInitFn(wasm_module))
    {
    }

    String getName() const override { return name; }
    bool isVariadic() const override { return false; }
    bool isDeterministic() const override { return false; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }
    size_t getNumberOfArguments() const override { return source_arg_types.size(); }

    DataTypePtr getReturnTypeImpl(const DataTypes &) const override { return result_type; }

    bool useDefaultImplementationForNulls() const override
    {
        return result_type->canBeInsideNullable();
    }

    ColumnPtr executeImpl(
        const ColumnsWithTypeAndName & arguments,
        const DataTypePtr & /*result_type*/,
        size_t input_rows_count) const override
    {
        ProfileEventTimeIncrement<Microseconds> timer(ProfileEvents::WasmTotalExecuteMicroseconds);

        if (input_rows_count == 0)
            return result_type->createColumn();

        auto compartment_entry = compartment_pool.acquire();
        auto * compartment_ptr = &(*compartment_entry);
        auto stop_token = interrupt_source.get_token();

        auto wmm = std::make_unique<WasmMemoryManagerV01>(compartment_ptr, stop_token);

        // ── Build chain descriptor buffer: [n_funcs: u32][cstr names...] ────
        const uint32_t n_funcs = static_cast<uint32_t>(fn_names.size());
        std::vector<uint8_t> chain_bytes;
        chain_bytes.resize(4);
        std::memcpy(chain_bytes.data(), &n_funcs, 4);
        for (const auto & fn : fn_names)
        {
            chain_bytes.insert(chain_bytes.end(),
                reinterpret_cast<const uint8_t *>(fn.c_str()),
                reinterpret_cast<const uint8_t *>(fn.c_str()) + fn.size() + 1);
        }

        // ── Build the `ColumnBinary` row frame ───────────────────────────────
        // Scalar constants are appended after the source geometry columns as
        // COL_IS_CONST columns (1 stored row each).  The WASM side reads them
        // by column index after consuming the source geometry columns.
        const uint32_t num_src_cols = static_cast<uint32_t>(arguments.size());

        // Materialise scalar constants into 1-row mutable columns.
        uint32_t total_scalar_cols = 0;
        for (const auto & sv : fn_scalar_values)
            total_scalar_cols += static_cast<uint32_t>(sv.size());

        std::vector<MutableColumnPtr> scalar_col_storage;
        scalar_col_storage.reserve(total_scalar_cols);
        for (size_t fi = 0; fi < fn_scalar_values.size(); ++fi)
            for (size_t si = 0; si < fn_scalar_values[fi].size(); ++si)
            {
                auto mut = fn_scalar_types[fi][si]->createColumn();
                mut->insert(fn_scalar_values[fi][si]);
                scalar_col_storage.push_back(std::move(mut));
            }

        const uint32_t num_cols = num_src_cols + total_scalar_cols;
        uint64_t col_cursor = FRAME_HEADER_BYTES + num_cols * COL_DESC_BYTES;

        std::vector<ColDescriptor> descs(num_cols);
        std::vector<const IColumn *> inner_cols(num_cols);
        std::vector<bool> is_nullable_flags(num_cols, false);
        std::vector<uint32_t> row_counts(num_cols);

        for (uint32_t ci = 0; ci < num_src_cols; ++ci)
        {
            const IColumn * col = arguments[ci].column.get();
            bool is_const = false;
            if (const auto * cc = typeid_cast<const ColumnConst *>(col))
            {
                col = &cc->getDataColumn();
                is_const = true;
            }
            bool is_nullable = typeid_cast<const ColumnNullable *>(col) != nullptr;
            uint32_t nrows = is_const ? 1u : static_cast<uint32_t>(input_rows_count);
            is_nullable_flags[ci] = is_nullable;
            inner_cols[ci]        = col;
            row_counts[ci]        = nrows;
            col_cursor = buildColDescriptor(col, is_const, is_nullable, nrows, col_cursor, descs[ci]);
        }

        for (uint32_t si = 0; si < total_scalar_cols; ++si)
        {
            uint32_t ci = num_src_cols + si;
            const IColumn * col = scalar_col_storage[si].get();
            inner_cols[ci]  = col;
            row_counts[ci]  = 1u;
            col_cursor = buildColDescriptor(col, /*is_const=*/true, /*is_nullable=*/false, 1u, col_cursor, descs[ci]);
        }

        {
            // Stopped explicitly before the guest call below, so that marshalling is not
            // credited with the execution time. The buffers have to stay alive across both,
            // which is why the timer is optional instead of scoped to a block.
            std::optional<ProfileEventTimeIncrement<Microseconds>> timer_ser;
            timer_ser.emplace(ProfileEvents::WasmSerializationMicroseconds);

            // Allocate two separate WASM buffers.
            WasmMemoryGuard wasm_chain = allocateInWasmMemory(wmm.get(), static_cast<uint32_t>(chain_bytes.size()));
            std::memcpy(wasm_chain.getMemoryView().data(), chain_bytes.data(), chain_bytes.size());

            WasmMemoryGuard wasm_row = allocateInWasmMemory(wmm.get(), col_cursor);
            auto row_mem = wasm_row.getMemoryView();

            writeFrameHeader(row_mem.data(), static_cast<uint32_t>(input_rows_count), num_cols);

            for (uint32_t ci = 0; ci < num_cols; ++ci)
                std::memcpy(row_mem.data() + FRAME_HEADER_BYTES + ci * COL_DESC_BYTES,
                            &descs[ci], COL_DESC_BYTES);

            for (uint32_t ci = 0; ci < num_cols; ++ci)
                writeColData(inner_cols[ci], is_nullable_flags[ci], row_counts[ci], descs[ci], row_mem);

            timer_ser.reset();

            // ── Invoke clickhouse_chain_execute(chain_buf, row_buf, n) ───────
            auto result_ptr = compartment_ptr->invoke<WasmPtr>(
                "clickhouse_chain_execute",
                {wasm_chain.getHandle(), wasm_row.getHandle(), static_cast<WasmSizeT>(input_rows_count)},
                stop_token);

            if (result_ptr == 0)
                throw Exception(ErrorCodes::WASM_ERROR, "clickhouse_chain_execute returned nullptr");

            WasmMemoryGuard result_guard(wmm.get(), result_ptr);

            {
                ProfileEventTimeIncrement<Microseconds> timer_de(ProfileEvents::WasmDeserializationMicroseconds);
                auto out_view = result_guard.getMemoryView();
                return readColumnarOutput(
                    {out_view.data(), out_view.size()},
                    result_type,
                    input_rows_count);
            }
        }
    }

    void cancelExecution() const override { interrupt_source.request_stop(); }

private:
    String                                    name;
    Strings                                   fn_names;
    std::shared_ptr<WebAssembly::WasmModule>  wasm_module;
    DataTypes                                 source_arg_types;
    DataTypePtr                               result_type;
    std::vector<std::vector<Field>>           fn_scalar_values;
    std::vector<DataTypes>                    fn_scalar_types;
    ContextPtr                                context;
    WebAssembly::FuelMode                     fuel_mode;
    mutable StopSource                        interrupt_source;
    mutable WasmCompartmentPool               compartment_pool;
};

FunctionOverloadResolverPtr createWasmChainResolver(
    Strings fn_names,
    std::shared_ptr<WebAssembly::WasmModule> wasm_module,
    DataTypes source_arg_types,
    DataTypePtr result_type,
    std::vector<std::vector<Field>> fn_scalar_values,
    std::vector<DataTypes>          fn_scalar_types,
    ContextPtr context,
    WebAssembly::FuelMode fuel_mode)
{
    // Synthetic name for logging/explain; not registered in any factory.
    String chain_name = "__wasm_chain";
    for (const auto & n : fn_names)
    {
        chain_name += '_';
        chain_name += n;
    }

    auto fn = std::make_shared<FunctionUserDefinedWasmChain>(
        std::move(chain_name),
        std::move(fn_names),
        std::move(wasm_module),
        std::move(source_arg_types),
        std::move(result_type),
        std::move(fn_scalar_values),
        std::move(fn_scalar_types),
        std::move(context),
        fuel_mode);
    return std::make_unique<FunctionToOverloadResolverAdaptor>(std::move(fn));
}

struct WebAssemblyFunctionSettingsConstraits : public IHints<>
{
    struct SettingDefinition
    {
        explicit SettingDefinition(std::function<void(std::string_view, Field &)> normalize_and_check_, Field default_value_)
            : default_value(std::move(default_value_)), normalize_and_check(std::move(normalize_and_check_))
        {
            chassert(normalize_and_check);
        }

        Field default_value;
        std::function<void(std::string_view, Field &)> normalize_and_check;
    };

    struct SettingStringFromSet
    {
        SettingDefinition withDefault(String default_value) const
        {
            return SettingDefinition(
                [values_ = this->values](std::string_view name, Field & value) // NOLINT
                {
                    if (value.getType() != Field::Types::String)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected String, got '{}'", value.getTypeName());
                    if (!values_.contains(value.safeGet<String>()))
                        throw Exception(
                            ErrorCodes::BAD_ARGUMENTS,
                            "Unexpected value '{}' for setting '{}', expected one of: {}",
                            value.safeGet<String>(),
                            name,
                            fmt::join(values_, ", "));
                },
                Field(default_value));
        }
        UnorderedSetWithMemoryTracking<String> values;
    };

    struct SettingBool
    {
        SettingDefinition withDefault(bool default_value) const
        {
            return SettingDefinition(
                [](std::string_view name, Field & value)
                {
                    if (value.getType() == Field::Types::Bool)
                        return;

                    if (value.getType() == Field::Types::UInt64)
                    {
                        UInt64 u = value.safeGet<UInt64>();
                        if (u != 0 && u != 1)
                            throw Exception(
                                ErrorCodes::BAD_ARGUMENTS,
                                "Setting '{}' must be 0/1 or false/true, got {}",
                                name,
                                u);
                        value = Field(static_cast<bool>(u));
                        return;
                    }

                    throw Exception(
                        ErrorCodes::BAD_ARGUMENTS,
                        "Setting '{}' must be a boolean, got {}",
                        name,
                        value.getTypeName());
                },
                Field(default_value));
        }
    };

    struct SettingInt64
    {
        SettingDefinition withDefault(Int64 default_value) const
        {
            return SettingDefinition(
                [](std::string_view name, const Field & value)
                {
                    if (value.getType() != Field::Types::Int64 && value.getType() != Field::Types::UInt64)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected an integer for setting '{}'", name);
                },
                Field(default_value));
        }
    };

    const UnorderedMapWithMemoryTracking<String, SettingDefinition> settings_def = {
        /// Serialization format for input/output data for ABI what uses serialization
        {"serialization_format", SettingStringFromSet{{"MsgPack", "JSONEachRow", "CSV", "TSV", "TSVRaw", "RowBinary", "Buffers", "ColumnBinary"}}.withDefault("MsgPack")},
        {"webassembly_udf_enable_fuel", SettingBool{}.withDefault(true)},
        /// Whether bbox-disjoint pruning is safe for this function (see IFunctionBase::isSpatialPredicate).
        {"is_spatial_predicate", SettingBool{}.withDefault(false)},
        /// For distance predicates (e.g. st_dwithin): 0-based index of the constant distance
        /// argument. SpatialRTreeJoin expands the R-tree query bbox by this amount.
        /// -1 (default) means no expansion.
        {"spatial_expand_arg", SettingInt64{}.withDefault(-1)},
        /// Registers the WASM function as an aggregate function: arguments are received
        /// as Array(T) accumulating all rows in the group instead of per-row scalars.
        {"is_aggregate", SettingBool{}.withDefault(false)},
    };

    VectorWithMemoryTracking<String> getAllRegisteredNames() const override
    {
        VectorWithMemoryTracking<String> result;
        result.reserve(settings_def.size());
        for (const auto & [name, _] : settings_def)
            result.push_back(name);
        return result;
    }

    void normalizeAndCheck(const String & name, Field & value) const
    {
        auto it = settings_def.find(name);
        if (it == settings_def.end())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown setting name: '{}'{}", name, getHintsMessage(name));
        it->second.normalize_and_check(name, value);
    }

    Field getDefault(const String & name) const
    {
        auto it = settings_def.find(name);
        if (it == settings_def.end())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown setting name: '{}'{}", name, getHintsMessage(name));
        return it->second.default_value;
    }

    static const WebAssemblyFunctionSettingsConstraits & instance()
    {
        static WebAssemblyFunctionSettingsConstraits instance;
        return instance;
    }
};

void WebAssemblyFunctionSettings::trySet(const String & name, Field value)
{
    WebAssemblyFunctionSettingsConstraits::instance().normalizeAndCheck(name, value);
    settings.emplace(name, std::move(value));
}

Field WebAssemblyFunctionSettings::getValue(const String & name) const
{
    auto it = settings.find(name);
    if (it == settings.end())
        return WebAssemblyFunctionSettingsConstraits::instance().getDefault(name);
    return it->second;
}

bool WebAssemblyFunctionSettings::isFuelEnabled() const
{
    return getValue("webassembly_udf_enable_fuel").safeGet<bool>();
}

WebAssembly::FuelMode WebAssemblyFunctionSettings::getFuelMode() const
{
    return isFuelEnabled() ? WebAssembly::FuelMode::Enabled : WebAssembly::FuelMode::Disabled;
}

bool WebAssemblyFunctionSettings::isAggregate() const
{
    return getValue("is_aggregate").safeGet<bool>();
}


}
