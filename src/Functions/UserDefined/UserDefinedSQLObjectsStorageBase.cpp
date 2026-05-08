#include <Functions/UserDefined/UserDefinedSQLObjectsStorageBase.h>

#include <boost/container/flat_set.hpp>

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/NormalizeSelectWithUnionQueryVisitor.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Functions/UserDefined/UserDefinedWebAssembly.h>
#include <Parsers/ASTCreateSQLFunctionQuery.h>
#include <Parsers/ASTCreateWasmFunctionQuery.h>
#include <Common/quoteString.h>

#include <optional>

namespace DB
{
namespace Setting
{
    extern const SettingsSetOperationMode union_default_mode;
}

namespace ErrorCodes
{
    extern const int FUNCTION_ALREADY_EXISTS;
    extern const int UNKNOWN_FUNCTION;
}

namespace
{
    std::optional<UserDefinedWebAssemblyFunctionFactory::RegisteredFunction> prepareWasmFunction(
        const String & function_name,
        ASTPtr create_query,
        const ContextPtr & context)
    {
        if (!create_query->as<ASTCreateWasmFunctionQuery>())
            return std::nullopt;

        /// Startup can load persisted `CREATE FUNCTION ... LANGUAGE WASM` definitions before
        /// `WasmModuleManager` is initialized.
        /// Keep the `AST` in storage and let `loadFunctions` synchronize the runtime registry later.
        if (!context->hasWasmModuleManager())
            return std::nullopt;

        try
        {
            return UserDefinedWebAssemblyFunctionFactory::instance().prepareFunction(std::move(create_query), context->getWasmModuleManager());
        }
        catch (Exception & exception)
        {
            exception.addMessage(fmt::format("while loading user defined function {}", backQuote(function_name)));
            throw;
        }
    }
}

Strings UserDefinedSQLObjectsStorageBase::extractSignatureFromAST(const IAST & ast)
{
    Strings sig;
    if (const auto * wasm_query = ast.as<ASTCreateWasmFunctionQuery>())
    {
        const auto & def = wasm_query->validateAndGetDefinition();
        for (const auto & arg_type : def.argument_types)
            sig.push_back(arg_type->getName());
    }
    return sig;
}

UserDefinedSQLObjectsStorageBase::UserDefinedSQLObjectsStorageBase(ContextPtr global_context_)
    : WithContext(global_context_)
{}

ASTPtr UserDefinedSQLObjectsStorageBase::get(const String & object_name) const
{
    std::lock_guard lock(mutex);
    auto it = object_overloads_map.find(object_name);
    if (it == object_overloads_map.end() || it->second.empty())
        throw Exception(ErrorCodes::UNKNOWN_FUNCTION,
            "The object name '{}' is not saved",
            object_name);
    return it->second.begin()->second;
}

ASTPtr UserDefinedSQLObjectsStorageBase::tryGet(const std::string & object_name) const
{
    std::lock_guard lock(mutex);
    auto it = object_overloads_map.find(object_name);
    if (it == object_overloads_map.end() || it->second.empty())
        return nullptr;
    return it->second.begin()->second;
}

bool UserDefinedSQLObjectsStorageBase::has(const String & object_name) const
{
    return tryGet(object_name) != nullptr;
}

VectorWithMemoryTracking<String> UserDefinedSQLObjectsStorageBase::getAllObjectNames() const
{
    VectorWithMemoryTracking<String> names;

    std::lock_guard lock(mutex);
    names.reserve(object_overloads_map.size());
    for (const auto & [name, _] : object_overloads_map)
        names.emplace_back(name);
    return names;
}

bool UserDefinedSQLObjectsStorageBase::empty() const
{
    std::lock_guard lock(mutex);
    return object_overloads_map.empty();
}

bool UserDefinedSQLObjectsStorageBase::storeObject(
    const ContextPtr & current_context,
    UserDefinedSQLObjectType object_type,
    const String & object_name,
    ASTPtr create_object_query,
    bool throw_if_exists,
    bool replace_if_exists,
    const Settings & settings)
{
    std::lock_guard lock{mutex};
    auto sig = extractSignatureFromAST(*create_object_query);
    auto outer_it = object_overloads_map.find(object_name);
    if (outer_it != object_overloads_map.end())
    {
        auto inner_it = outer_it->second.find(sig);
        if (inner_it != outer_it->second.end())
        {
            if (throw_if_exists)
                throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS,
                    "User-defined object '{}' already exists", object_name);
            if (!replace_if_exists)
                return false;
        }
    }

    bool stored = storeObjectImpl(
        current_context,
        object_type,
        object_name,
        create_object_query,
        throw_if_exists,
        replace_if_exists,
        settings);

    if (stored)
        object_overloads_map[object_name][sig] = create_object_query;

    return stored;
}

bool UserDefinedSQLObjectsStorageBase::removeObject(
        const ContextPtr & current_context,
        UserDefinedSQLObjectType object_type,
        const String & object_name,
        const Strings & argument_type_names,
        bool throw_if_not_exists)
{
    std::lock_guard lock(mutex);

    bool removed = removeObjectImpl(
        current_context,
        object_type,
        object_name,
        argument_type_names,
        throw_if_not_exists);

    if (removed)
    {
        if (argument_type_names.empty())
        {
            object_overloads_map.erase(object_name);
        }
        else
        {
            auto outer_it = object_overloads_map.find(object_name);
            if (outer_it != object_overloads_map.end())
            {
                outer_it->second.erase(argument_type_names);
                if (outer_it->second.empty())
                    object_overloads_map.erase(outer_it);
            }
        }
    }

    return removed;
}

std::unique_lock<std::recursive_mutex> UserDefinedSQLObjectsStorageBase::getLock() const
{
    return std::unique_lock{mutex};
}

void UserDefinedSQLObjectsStorageBase::setAllObjects(const VectorWithMemoryTracking<std::pair<String, ASTPtr>> & new_objects)
{
    UnorderedMapWithMemoryTracking<String, std::map<Strings, ASTPtr>> new_map;
    VectorWithMemoryTracking<UserDefinedWebAssemblyFunctionFactory::RegisteredFunction> wasm_functions;

    for (const auto & [function_name, create_query] : new_objects)
    {
        auto normalized_query = normalizeCreateFunctionQuery(*create_query, getContext());
        if (auto wasm_function = prepareWasmFunction(function_name, normalized_query, getContext()))
            wasm_functions.push_back(std::move(*wasm_function));
        auto sig = extractSignatureFromAST(*normalized_query);
        new_map[function_name][sig] = std::move(normalized_query);
    }

    {
        std::lock_guard lock(mutex);
        object_overloads_map = std::move(new_map);
    }

    UserDefinedWebAssemblyFunctionFactory::instance().replaceAll(std::move(wasm_functions));
}

VectorWithMemoryTracking<std::pair<String, ASTPtr>> UserDefinedSQLObjectsStorageBase::getAllObjects() const
{
    std::lock_guard lock{mutex};
    VectorWithMemoryTracking<std::pair<String, ASTPtr>> all_objects;
    for (const auto & [name, overloads] : object_overloads_map)
        for (const auto & [sig, ast] : overloads)
            all_objects.emplace_back(name, ast);
    return all_objects;
}

void UserDefinedSQLObjectsStorageBase::setObject(const String & object_name, const IAST & create_object_query)
{
    auto normalized_query = normalizeCreateFunctionQuery(create_object_query, getContext());
    auto wasm_function = prepareWasmFunction(object_name, normalized_query, getContext());
    auto sig = extractSignatureFromAST(*normalized_query);

    {
        std::lock_guard lock(mutex);
        object_overloads_map[object_name][sig] = std::move(normalized_query);
    }

    if (wasm_function)
        UserDefinedWebAssemblyFunctionFactory::instance().addOrReplace(std::move(*wasm_function));
    else
        UserDefinedWebAssemblyFunctionFactory::instance().dropIfExists(object_name);
}

void UserDefinedSQLObjectsStorageBase::removeObject(const String & object_name)
{
    {
        std::lock_guard lock(mutex);
        object_overloads_map.erase(object_name);
    }

    UserDefinedWebAssemblyFunctionFactory::instance().dropIfExists(object_name);
}

void UserDefinedSQLObjectsStorageBase::removeAllObjectsExcept(const Strings & object_names_to_keep)
{
    boost::container::flat_set<std::string_view> names_set_to_keep{object_names_to_keep.begin(), object_names_to_keep.end()};

    {
        std::lock_guard lock(mutex);
        for (auto it = object_overloads_map.begin(); it != object_overloads_map.end();)
        {
            auto current = it++;
            if (!names_set_to_keep.contains(current->first))
                object_overloads_map.erase(current);
        }
    }

    for (const auto & registered_function : UserDefinedWebAssemblyFunctionFactory::instance().getAllFunctions())
    {
        if (!names_set_to_keep.contains(registered_function.sql_name))
            UserDefinedWebAssemblyFunctionFactory::instance().dropIfExists(registered_function.sql_name);
    }
}

}
