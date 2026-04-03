#pragma once

#include <Core/Block.h>

#include <DataTypes/IDataType.h>
#include <Interpreters/Context_fwd.h>
#include <Interpreters/WebAssembly/WasmEngine.h>

#include <Parsers/IAST_fwd.h>

#include <Common/SharedMutex.h>
#include <Common/StopToken.h>
#include <AggregateFunctions/IAggregateFunction_fwd.h>

namespace DB
{

class IFunctionOverloadResolver;
using FunctionOverloadResolverPtr = std::shared_ptr<IFunctionOverloadResolver>;

enum class WasmAbiVersion : uint8_t
{
    RowDirect,
    BufferedV1,
    ColumnarV1,
};

String toString(WasmAbiVersion abi_type);
WasmAbiVersion getWasmAbiFromString(const String & str);

class WebAssemblyFunctionSettings
{
public:
    void trySet(const String & name, Field value);
    Field getValue(const String & name) const;
    bool isAggregate() const;

private:
    std::unordered_map<String, Field> settings;
};

class UserDefinedWebAssemblyFunction
{
public:
    virtual MutableColumnPtr executeOnBlock(WebAssembly::WasmCompartment * compartment, const Block & block, ContextPtr context, size_t num_rows, StopToken stop_token) const = 0;

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

class UserDefinedWebAssemblyFunctionFactory
{
public:
    struct RegisteredFunction
    {
        String sql_name;
        std::shared_ptr<UserDefinedWebAssemblyFunction> function;
        ASTPtr create_query;
        bool is_aggregate = false;
    };

    std::shared_ptr<UserDefinedWebAssemblyFunction> addOrReplace(ASTPtr create_function_query, WasmModuleManager & module_manager);

    bool has(const String & function_name) const;
    FunctionOverloadResolverPtr get(const String & function_name, ContextPtr context);

    /// Returns true if the function is an aggregate function (is_aggregate=1).
    bool isAggregate(const String & function_name) const;

    /// Returns an AggregateFunctionPtr for use in the query analyzer.
    /// arg_types must match the declared (non-Array) argument types.
    AggregateFunctionPtr getAggregate(const String & function_name, const DataTypes & arg_types, ContextPtr context) const;

    /// Returns true if function was removed
    bool dropIfExists(const String & function_name);

    /// Returns all registered WASM functions with their metadata for introspection (e.g. system.functions).
    std::vector<RegisteredFunction> getAllFunctions() const;

    static UserDefinedWebAssemblyFunctionFactory & instance();
private:
    struct RegistryEntry
    {
        std::shared_ptr<UserDefinedWebAssemblyFunction> function;
        DataTypes original_arg_types; /// declared types before Array wrapping for aggregates
        ASTPtr create_query;
        bool is_aggregate = false;
    };

    mutable DB::SharedMutex registry_mutex;
    std::unordered_map<String, RegistryEntry> registry;
};

}
