#pragma once

#include <Core/Block.h>
#include <Core/Field.h>
#include <Core/ColumnsWithTypeAndName.h>

#include <AggregateFunctions/IAggregateFunction.h>

#include <DataTypes/IDataType.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/WebAssembly/WasmEngine.h>

#include <Parsers/IAST_fwd.h>

#include <Common/SharedMutex.h>
#include <Common/StopToken.h>
#include <AggregateFunctions/IAggregateFunction_fwd.h>
#include <Common/UnorderedMapWithMemoryTracking.h>
#include <Common/UnorderedSetWithMemoryTracking.h>
#include <Common/VectorWithMemoryTracking.h>

namespace DB
{

class IFunctionOverloadResolver;
using FunctionOverloadResolverPtr = std::shared_ptr<IFunctionOverloadResolver>;

enum class WasmAbiVersion : uint8_t
{
    RowDirect,
    BufferedV1,
    AssemblyScript,
};

String toString(WasmAbiVersion abi_type);
WasmAbiVersion getWasmAbiFromString(const String & str);

class WebAssemblyFunctionSettings
{
public:
    void trySet(const String & name, Field value);
    Field getValue(const String & name) const;
    bool isFuelEnabled() const;
    WebAssembly::FuelMode getFuelMode() const;
    bool isAggregate() const;

private:
    UnorderedMapWithMemoryTracking<String, Field> settings;
};

class UserDefinedWebAssemblyFunction
{
public:
    virtual MutableColumnPtr executeOnBlock(WebAssembly::WasmCompartment * compartment, const Block & block, ContextPtr context, size_t num_rows, StopToken stop_token) const = 0;

    /// True when a call has to place data in, or read data from, the guest's linear memory. Such a
    /// function cannot run in a compartment whose memory can never hold a single page, while one
    /// passing its arguments as WebAssembly values is indifferent to the memory configuration.
    virtual bool requiresGuestLinearMemory() const = 0;

    /// True when a call serializes the whole input block into the guest's linear memory through
    /// `serialization_format`, so what the memory has to hold is the serialized size of a batch.
    /// An ABI that hands the guest one row at a time - `ASSEMBLYSCRIPT` builds a separate object
    /// per row and never reads `serialization_format` - writes no such block, and sizing its
    /// input by a serialization it does not perform would bound it by bytes it never places
    /// there.
    virtual bool serializesInputBlockToGuestMemory() const = 0;

    virtual ~UserDefinedWebAssemblyFunction() = default;

    static std::unique_ptr<UserDefinedWebAssemblyFunction> create(
        std::shared_ptr<WebAssembly::WasmModule> wasm_module_,
        const String & function_name_,
        const Strings & argument_names_,
        const DataTypes & arguments_,
        const DataTypePtr & result_type_,
        WasmAbiVersion abi_type,
        WebAssemblyFunctionSettings function_settings_,
        bool is_deterministic_ = false);

    const String & getInternalFunctionName() const { return function_name; }
    const DataTypes & getArguments() const { return arguments; }
    const Strings & getArgumentNames() const { return argument_names; }
    const DataTypePtr & getResultType() const { return result_type; }
    std::shared_ptr<WebAssembly::WasmModule> getModule() const { return wasm_module; }
    const WebAssemblyFunctionSettings & getSettings() const { return settings; }
    bool getIsDeterministic() const { return is_deterministic; }

protected:

    UserDefinedWebAssemblyFunction(
        std::shared_ptr<WebAssembly::WasmModule> wasm_module_,
        const String & function_name_,
        const Strings & argument_names_,
        const DataTypes & arguments_,
        const DataTypePtr & result_type_,
        WebAssemblyFunctionSettings function_settings_,
        bool is_deterministic_ = false);

    String function_name;
    Strings argument_names;
    DataTypes arguments;
    DataTypePtr result_type;

    std::shared_ptr<WebAssembly::WasmModule> wasm_module;

    WebAssemblyFunctionSettings settings;
    bool is_deterministic = false;
};

class WasmModuleManager;

/// Build a FunctionOverloadResolver that executes a WASM function chain in a
/// single WASM call via clickhouse_chain_execute.
///
/// fn_names: function names in SOURCE → SINK order.
/// source_arg_types: declared argument types of the SOURCE function.
/// result_type: return type of the SINK function.
/// wasm_module: the shared WasmModule (all functions must be from the same module).
/// fn_scalar_values / fn_scalar_types: per-function scalar constants (indexed same as
///   fn_names); SOURCE and SINK have empty inner vectors. XFORMs may carry compile-time
///   constant args (e.g. the radius in st_buffer) that are appended to row_buf as
///   COL_IS_CONST columns so the WASM side can read them without extra protocol fields.
FunctionOverloadResolverPtr createWasmChainResolver(
    Strings fn_names,
    std::shared_ptr<WebAssembly::WasmModule> wasm_module,
    DataTypes source_arg_types,
    DataTypePtr result_type,
    std::vector<std::vector<Field>> fn_scalar_values,
    std::vector<DataTypes>          fn_scalar_types,
    ContextPtr context,
    WebAssembly::FuelMode fuel_mode);

class UserDefinedWebAssemblyFunctionFactory
{
public:
    struct RegisteredFunction
    {
        String sql_name;
        std::shared_ptr<UserDefinedWebAssemblyFunction> function;
        DataTypes original_arg_types; /// declared types as written in CREATE FUNCTION
        ASTPtr create_query;
        bool is_aggregate = false;
        /// For is_aggregate=1 entries: element types used by the accumulator.
        /// When declared type is already Array(T), this holds T (not Array(T)).
        /// When declared type is scalar T, this equals original_arg_types.
        /// Empty for non-aggregate entries.
        DataTypes accumulator_arg_types;
    };

    RegisteredFunction prepareFunction(ASTPtr create_function_query, WasmModuleManager & module_manager) const;
    std::shared_ptr<UserDefinedWebAssemblyFunction> addOrReplace(ASTPtr create_function_query, WasmModuleManager & module_manager);
    void addOrReplace(RegisteredFunction registered_function);
    void replaceAll(VectorWithMemoryTracking<RegisteredFunction> registered_functions);

    bool has(const String & function_name) const;
    FunctionOverloadResolverPtr get(const String & function_name, ContextPtr context);

    /// Fail close before resolving a name that is stored as a WebAssembly UDF.
    /// A `CREATE FUNCTION ... LANGUAGE WASM` definition lives in SQL object storage and outlives the engine
    /// that can run it: the server may be restarted with `allow_experimental_webassembly_udf` turned off, or
    /// on a build that has no WebAssembly engine at all. The definition is then still stored while this
    /// registry is empty, and without this check the name resolves to `UNKNOWN_FUNCTION` or to an
    /// empty-registry `RESOURCE_NOT_FOUND` instead of reporting that WebAssembly support is unavailable.
    static void checkWebAssemblyIsAvailable(const ContextPtr & context);
    /// Returns nullptr if the function is not registered. Useful for non-throwing rewrite-candidate checks.
    FunctionOverloadResolverPtr tryGet(const String & function_name, ContextPtr context);

    /// Returns the underlying WASM function object, or nullptr if not found.
    /// Used by WasmChainFusionPass to inspect module identity and signatures.
    std::shared_ptr<UserDefinedWebAssemblyFunction> getFunction(const String & function_name) const;

    /// Returns true if the function is an aggregate function (is_aggregate=1).
    bool isAggregate(const String & function_name) const;

    /// Returns true if this factory registered `function_name` in AggregateFunctionFactory.
    ///
    /// That registration is a trampoline which resolves the WASM function lazily, so it is made
    /// once and never withdrawn: AggregateFunctionFactory has no unregister API. Callers that
    /// treat an AggregateFunctionFactory hit as "this is a built-in, hands off" must consult
    /// this first, or DROP and CREATE OR REPLACE of a WASM aggregate both become impossible
    /// after the first drop.
    bool ownsAggregateName(const String & function_name) const;

    /// Returns an AggregateFunctionPtr for use in the query analyzer.
    /// arg_types must match the declared (non-Array) argument types.
    AggregateFunctionPtr getAggregate(const String & function_name, const DataTypes & arg_types, ContextPtr context) const;

    /// Returns true if function was removed.
    /// Empty argument_type_names = drop all overloads; non-empty = drop specific overload by signature.
    bool dropIfExists(const String & function_name, const Strings & argument_type_names);

    /// Backward compatibility for scripts that call dropIfExists without argument_type_names.
    bool dropIfExists(const String & function_name);

    /// Returns true if an overload with the exact argument-type signature exists.
    bool hasOverload(const String & function_name, const DataTypes & arg_types) const;

    /// Returns all registered WASM functions with their metadata for introspection (e.g. system.functions).
    VectorWithMemoryTracking<RegisteredFunction> getAllFunctions() const;


    static UserDefinedWebAssemblyFunctionFactory & instance();
private:
    struct RegistryEntry
    {
        std::shared_ptr<UserDefinedWebAssemblyFunction> function;
        DataTypes original_arg_types;     /// declared types as written in CREATE FUNCTION
        ASTPtr create_query;
        bool is_aggregate = false;
        /// For is_aggregate=1 entries: element types used by the accumulator.
        /// When declared type is already Array(T), this holds T (not Array(T)).
        /// When declared type is scalar T, this equals original_arg_types.
        /// Empty for non-aggregate entries.
        DataTypes accumulator_arg_types;
    };

    /// Registers `sql_name` in AggregateFunctionFactory unless that was already done.
    /// Caller must hold registry_mutex.
    void registerAggregateName(const String & sql_name, const DataTypes & accumulator_arg_types);

    mutable DB::SharedMutex registry_mutex;
    UnorderedMapWithMemoryTracking<String, std::vector<RegistryEntry>> registry;
    /// Names handed to AggregateFunctionFactory. Outlives `registry` entries by design; see
    /// ownsAggregateName().
    std::unordered_set<String> aggregate_names;
};

}
