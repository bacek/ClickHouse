#include <Functions/UserDefined/UserDefinedSQLObjectsStorageBase.h>

#include <boost/container/flat_set.hpp>

#include <Core/Settings.h>
#include <Interpreters/Context.h>
#include <Interpreters/FunctionNameNormalizer.h>
#include <Interpreters/NormalizeSelectWithUnionQueryVisitor.h>
#include <Functions/UserDefined/UserDefinedSQLFunctionFactory.h>
#include <Parsers/ASTCreateSQLFunctionQuery.h>
#include <Parsers/ASTCreateWasmFunctionQuery.h>

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
    : global_context(std::move(global_context_))
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

std::vector<std::string> UserDefinedSQLObjectsStorageBase::getAllObjectNames() const
{
    std::vector<std::string> names;
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

void UserDefinedSQLObjectsStorageBase::setAllObjects(const std::vector<std::pair<String, ASTPtr>> & new_objects)
{
    std::unordered_map<String, std::map<Strings, ASTPtr>> new_map;
    for (const auto & [function_name, create_query] : new_objects)
    {
        auto normalized = normalizeCreateFunctionQuery(*create_query, global_context);
        auto sig = extractSignatureFromAST(*normalized);
        new_map[function_name][sig] = normalized;
    }
    std::lock_guard lock(mutex);
    object_overloads_map = std::move(new_map);
}

std::vector<std::pair<String, ASTPtr>> UserDefinedSQLObjectsStorageBase::getAllObjects() const
{
    std::lock_guard lock{mutex};
    std::vector<std::pair<String, ASTPtr>> all_objects;
    for (const auto & [name, overloads] : object_overloads_map)
        for (const auto & [sig, ast] : overloads)
            all_objects.emplace_back(name, ast);
    return all_objects;
}

void UserDefinedSQLObjectsStorageBase::setObject(const String & object_name, const IAST & create_object_query)
{
    std::lock_guard lock(mutex);
    auto normalized = normalizeCreateFunctionQuery(create_object_query, global_context);
    auto sig = extractSignatureFromAST(*normalized);
    object_overloads_map[object_name][sig] = normalized;
}

void UserDefinedSQLObjectsStorageBase::removeObject(const String & object_name)
{
    std::lock_guard lock(mutex);
    object_overloads_map.erase(object_name);
}

void UserDefinedSQLObjectsStorageBase::removeAllObjectsExcept(const Strings & object_names_to_keep)
{
    boost::container::flat_set<std::string_view> names_set_to_keep{object_names_to_keep.begin(), object_names_to_keep.end()};
    std::lock_guard lock(mutex);
    for (auto it = object_overloads_map.begin(); it != object_overloads_map.end();)
    {
        auto current = it++;
        if (!names_set_to_keep.contains(current->first))
            object_overloads_map.erase(current);
    }
}

}
