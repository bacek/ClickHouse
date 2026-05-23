#include <Functions/UserDefined/UserDefinedSQLObjectsDiskStorage.h>

#include <Functions/UserDefined/UserDefinedSQLObjectType.h>
#include <Functions/UserDefined/UserDefinedSQLObjectsStorageBase.h>

#include <Common/StringUtils.h>
#include <Common/atomicRename.h>
#include <Common/escapeForFileName.h>
#include <Common/logger_useful.h>
#include <Common/quoteString.h>

#include <Core/Settings.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>

#include <Interpreters/Context.h>

#include <Parsers/ASTCreateSQLFunctionQuery.h>
#include <Parsers/ASTCreateWasmFunctionQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTNameTypePair.h>
#include <Parsers/IAST.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserCreateFunctionQuery.h>
#include <Parsers/TokenIterator.h>

#include <Poco/DirectoryIterator.h>

#include <filesystem>

namespace fs = std::filesystem;


namespace DB
{
namespace Setting
{
    extern const SettingsBool fsync_metadata;
    extern const SettingsUInt64 max_parser_backtracks;
    extern const SettingsUInt64 max_parser_depth;
}

namespace ErrorCodes
{
    extern const int DIRECTORY_DOESNT_EXIST;
    extern const int FUNCTION_ALREADY_EXISTS;
    extern const int UNKNOWN_FUNCTION;
}


namespace
{
    /// Converts a path to an absolute path and append it with a separator.
    String makeDirectoryPathCanonical(const String & directory_path)
    {
        auto canonical_directory_path = std::filesystem::weakly_canonical(directory_path);
        if (canonical_directory_path.has_filename())
            canonical_directory_path += std::filesystem::path::preferred_separator;
        return canonical_directory_path;
    }
}

UserDefinedSQLObjectsDiskStorage::UserDefinedSQLObjectsDiskStorage(const ContextPtr & global_context_, const String & dir_path_)
    : UserDefinedSQLObjectsStorageBase(global_context_)
    , dir_path{makeDirectoryPathCanonical(dir_path_)}
    , log{getLogger("UserDefinedSQLObjectsLoaderFromDisk")}
{
}


static std::vector<ASTPtr> parseAllCreateFunctionStatements(const String & content, const ContextPtr & global_context, bool warn_on_truncation = true)
{
    std::vector<ASTPtr> result;

    if (content.empty())
        return result;

    ParserCreateFunctionQuery parser;
    Tokens tokens{content.data(), content.data() + content.size()};
    IParser::Pos pos(
        TokenIterator(tokens),
        static_cast<uint32_t>(global_context->getSettingsRef()[Setting::max_parser_depth]),
        static_cast<uint32_t>(global_context->getSettingsRef()[Setting::max_parser_backtracks]));
    Expected expected;
    ASTPtr ast;

    while (parser.parse(pos, ast, expected))
    {
        result.push_back(ast);
        expected = Expected();
        ast = nullptr;
    }

    if (warn_on_truncation && pos.get().type != TokenType::EndOfStream)
        tryLogCurrentException("UserDefinedSQLObjectsDiskStorage", "Trailing content after parsing user-defined function statements");

    return result;
}

static String extractFunctionName(const ASTPtr & ast)
{
    if (auto * wasm_query = ast->as<ASTCreateWasmFunctionQuery>())
        return wasm_query->getFunctionName();
    if (auto * sql_query = ast->as<ASTCreateSQLFunctionQuery>())
    {
        if (auto * func = sql_query->function_core->as<ASTFunction>())
            return func->name;
    }
    return {};
}

static Strings extractArgumentTypeNames(const ASTPtr & ast)
{
    Strings types;
    if (auto * wasm_query = ast->as<ASTCreateWasmFunctionQuery>())
    {
        const auto & def = wasm_query->validateAndGetDefinition();
        for (const auto & arg_type : def.argument_types)
            types.push_back(arg_type->getName());
    }
    else if (auto * sql_query = ast->as<ASTCreateSQLFunctionQuery>())
    {
        if (auto * func = sql_query->function_core->as<ASTFunction>())
        {
            const auto & args = func->arguments->children;
            for (size_t i = 0; i + 1 < args.size(); ++i)
            {
                const auto * pair = args[i]->as<ASTNameTypePair>();
                if (pair)
                    types.push_back(pair->type->as<ASTIdentifier &>().name());
                else
                    types.push_back(args[i]->as<ASTIdentifier &>().name());
            }
        }
    }
    return types;
}

static bool argumentsMatch(const Strings & query_types, const Strings & drop_types)
{
    if (query_types.size() != drop_types.size())
        return false;
    for (size_t i = 0; i < query_types.size(); ++i)
        if (query_types[i] != drop_types[i])
            return false;
    return true;
}


ASTPtr UserDefinedSQLObjectsDiskStorage::tryLoadObject(UserDefinedSQLObjectType object_type, const String & object_name)
{
    return tryLoadObject(object_type, object_name, getFilePath(object_type, object_name), /* check_file_exists= */ true);
}


ASTPtr UserDefinedSQLObjectsDiskStorage::tryLoadObject(UserDefinedSQLObjectType object_type, const String & object_name, const String & path, bool check_file_exists)
{
    LOG_DEBUG(log, "Loading user defined object {} from file {}", backQuote(object_name), path);

    try
    {
        if (check_file_exists && !fs::exists(path))
            return nullptr;

        /// There is .sql file with user defined object creation statement(s).
        ReadBufferFromFile in(path);

        String file_content;
        readStringUntilEOF(file_content, in);

        if (object_type != UserDefinedSQLObjectType::Function)
            return nullptr;

        auto asts = parseAllCreateFunctionStatements(file_content, getContext());
        return asts.empty() ? nullptr : asts.back();
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("while loading user defined SQL object {} from path {}", backQuote(object_name), path));
        return nullptr;
    }
}


void UserDefinedSQLObjectsDiskStorage::loadObjects()
{
    if (!objects_loaded)
        loadObjectsImpl();
}


void UserDefinedSQLObjectsDiskStorage::reloadObjects()
{
    loadObjectsImpl();
}


void UserDefinedSQLObjectsDiskStorage::loadObjectsImpl()
{
    LOG_INFO(log, "Loading user defined objects from {}", dir_path);

    if (!std::filesystem::exists(dir_path))
    {
        LOG_DEBUG(log, "The directory for user defined objects ({}) does not exist: nothing to load", dir_path);
        return;
    }

    VectorWithMemoryTracking<std::pair<String, ASTPtr>> function_names_and_queries;

    Poco::DirectoryIterator dir_end;
    for (Poco::DirectoryIterator it(dir_path); it != dir_end; ++it)
    {
        if (it->isDirectory())
            continue;

        const String & file_name = it.name();
        if (!startsWith(file_name, "function_") || !endsWith(file_name, ".sql"))
            continue;

        size_t prefix_length = strlen("function_");
        size_t suffix_length = strlen(".sql");
        String file_function_name = unescapeForFileName(file_name.substr(prefix_length, file_name.length() - prefix_length - suffix_length));

        if (file_function_name.empty())
            continue;

        String file_path = dir_path + it.name();
        ReadBufferFromFile in(file_path);
        String file_content;
        readStringUntilEOF(file_content, in);

        auto asts = parseAllCreateFunctionStatements(file_content, getContext());
        for (const auto & ast : asts)
        {
            String name = extractFunctionName(ast);
            if (!name.empty())
                function_names_and_queries.emplace_back(name, ast);
        }
    }

    setAllObjects(function_names_and_queries);
    objects_loaded = true;

    LOG_DEBUG(log, "User defined objects loaded");
}


void UserDefinedSQLObjectsDiskStorage::reloadObject(UserDefinedSQLObjectType object_type, const String & object_name)
{
    String file_path = getFilePath(object_type, object_name);
    if (!fs::exists(file_path))
    {
        UserDefinedSQLObjectsStorageBase::removeObject(object_name);
        return;
    }

    String file_content;
    try
    {
        ReadBufferFromFile in(file_path);
        readStringUntilEOF(file_content, in);
    }
    catch (...)
    {
        tryLogCurrentException(log, fmt::format("while reading file {}", file_path));
        return;
    }

    if (object_type != UserDefinedSQLObjectType::Function)
        return;

    auto asts = parseAllCreateFunctionStatements(file_content, getContext());
    if (asts.empty())
    {
        UserDefinedSQLObjectsStorageBase::removeObject(object_name);
        return;
    }

    // Update in-memory state only — file on disk is the source of truth
    UserDefinedSQLObjectsStorageBase::removeObject(object_name);
    for (const auto & ast : asts)
    {
        String name = extractFunctionName(ast);
        if (!name.empty())
            UserDefinedSQLObjectsStorageBase::setObject(name, *ast);
    }
}


void UserDefinedSQLObjectsDiskStorage::createDirectory()
{
    std::error_code create_dir_error_code;
    fs::create_directories(dir_path, create_dir_error_code);
    if (!fs::exists(dir_path) || !fs::is_directory(dir_path) || create_dir_error_code)
        throw Exception(ErrorCodes::DIRECTORY_DOESNT_EXIST, "Couldn't create directory {} reason: '{}'",
                        dir_path, create_dir_error_code.message());
}


bool UserDefinedSQLObjectsDiskStorage::storeObjectImpl(
    const ContextPtr & /*current_context*/,
    UserDefinedSQLObjectType object_type,
    const String & object_name,
    ASTPtr create_object_query,
    bool throw_if_exists,
    bool replace_if_exists,
    const Settings & settings)
{
    createDirectory();
    String file_path = getFilePath(object_type, object_name);
    LOG_DEBUG(log, "Storing user-defined object {} to file {}", backQuote(object_name), file_path);

    WriteBufferFromOwnString create_statement_buf;
    IAST::FormatSettings format_settings(/*one_line=*/false);
    create_object_query->format(create_statement_buf, format_settings);
    writeChar('\n', create_statement_buf);
    String create_statement = create_statement_buf.str();

    String existing_content;
    bool statement_in_file = false;

    if (fs::exists(file_path))
    {
        ReadBufferFromFile existing(file_path);
        readStringUntilEOF(existing_content, existing);

        /// Check if the exact same CREATE statement already exists in the file.
        /// For overloads (e.g. WASM functions with different signatures), the file
        /// may contain multiple CREATE statements — only throw if this exact one
        /// is already present.
        statement_in_file = (existing_content.find(create_statement) != String::npos);
        if (statement_in_file)
        {
            if (throw_if_exists)
                throw Exception(ErrorCodes::FUNCTION_ALREADY_EXISTS, "User-defined function '{}' already exists", object_name);

            if (!replace_if_exists)
                return false;

            /// replace_if_exists: strip the old statement before writing the replacement.
            auto old_asts = parseAllCreateFunctionStatements(existing_content, getContext());
            std::vector<ASTPtr> remaining;
            IAST::FormatSettings fmt_settings(/*one_line=*/false);
            for (const auto & old_ast : old_asts)
            {
                WriteBufferFromOwnString buf;
                old_ast->format(buf, fmt_settings);
                writeChar('\n', buf);
                if (buf.str() != create_statement)
                    remaining.push_back(old_ast);
            }
            if (remaining.empty())
            {
                /// All matching statements were replaced — file is now empty.
                existing_content.clear();
            }
            else
            {
                WriteBufferFromOwnString buf;
                for (const auto & ast : remaining)
                {
                    ast->format(buf, fmt_settings);
                    writeChar('\n', buf);
                }
                existing_content = buf.str();
            }
        }
    }

    /// New overload (not already in file): always append.
    String combined_content = existing_content + "\n" + create_statement;
    String temp_file_path = file_path + ".tmp";

    try
    {
        WriteBufferFromFile out(temp_file_path, combined_content.size());
        writeString(combined_content, out);
        out.next();
        if (settings[Setting::fsync_metadata])
            out.sync();
        out.close();

        fs::rename(temp_file_path, file_path);
    }
    catch (...)
    {
        fs::remove(temp_file_path);
        throw;
    }

    LOG_TRACE(log, "Object {} stored", backQuote(object_name));
    return true;
}


bool UserDefinedSQLObjectsDiskStorage::removeObjectImpl(
    const ContextPtr & /*current_context*/,
    UserDefinedSQLObjectType object_type,
    const String & object_name,
    const Strings & argument_type_names,
    bool throw_if_not_exists)
{
    String file_path = getFilePath(object_type, object_name);
    LOG_DEBUG(log, "Removing user defined object {} stored in file {}", backQuote(object_name), file_path);

    if (!fs::exists(file_path))
    {
        if (throw_if_not_exists)
            throw Exception(ErrorCodes::UNKNOWN_FUNCTION, "User-defined function '{}' doesn't exist", object_name);
        return false;
    }

    if (object_type != UserDefinedSQLObjectType::Function)
    {
        fs::remove(file_path);
        return true;
    }

    String file_content;
    {
        ReadBufferFromFile in(file_path);
        readStringUntilEOF(file_content, in);
    }

    auto asts = parseAllCreateFunctionStatements(file_content, getContext());
    std::vector<ASTPtr> remaining;
    bool found = false;
    bool overload_found = false;
    for (const auto & ast : asts)
    {
        String name = extractFunctionName(ast);
        if (name != object_name)
        {
            remaining.push_back(ast);
            continue;
        }
        found = true;

        /// If dropping all overloads, remove this statement.
        /// If dropping a specific overload, only remove if signature matches.
        if (argument_type_names.empty())
        {
            overload_found = true;
            continue;
        }

        auto query_types = extractArgumentTypeNames(ast);
        if (argumentsMatch(query_types, argument_type_names))
        {
            overload_found = true;
            continue;
        }

        remaining.push_back(ast);
    }

    if (!found)
    {
        if (throw_if_not_exists)
            throw Exception(ErrorCodes::UNKNOWN_FUNCTION, "User-defined function '{}' doesn't exist", object_name);
        return false;
    }

    if (argument_type_names.empty() && !found)
    {
        if (throw_if_not_exists)
            throw Exception(ErrorCodes::UNKNOWN_FUNCTION, "User-defined function '{}' doesn't exist", object_name);
        return false;
    }
    if (!argument_type_names.empty() && !overload_found)
    {
        if (throw_if_not_exists)
            throw Exception(ErrorCodes::UNKNOWN_FUNCTION, "User-defined function '{}' doesn't exist", object_name);
        return false;
    }

    if (overload_found && remaining.empty())
    {
        fs::remove(file_path);
    }
    else if (overload_found && !remaining.empty())
    {
        /// Rewrite remaining overloads to a temp file and atomically rename.
        String temp_file_path = file_path + ".tmp";
        String combined_content;
        {
            WriteBufferFromOwnString buf;
            IAST::FormatSettings format_settings(/*one_line=*/false);
            for (const auto & ast : remaining)
            {
                ast->format(buf, format_settings);
                writeChar('\n', buf);
            }
            combined_content = buf.str();
        }

        try
        {
            WriteBufferFromFile out(temp_file_path, combined_content.size());
            writeString(combined_content, out);
            out.next();
            out.close();

            fs::rename(temp_file_path, file_path);
        }
        catch (...)
        {
            fs::remove(temp_file_path);
            throw;
        }
    }

    LOG_TRACE(log, "Object {} removed", backQuote(object_name));
    return true;
}


String UserDefinedSQLObjectsDiskStorage::getFilePath(UserDefinedSQLObjectType object_type, const String & object_name) const
{
    String file_path;
    switch (object_type)
    {
        case UserDefinedSQLObjectType::Function:
        {
            file_path = dir_path + "function_" + escapeForFileName(object_name) + ".sql";
            break;
        }
    }
    return file_path;
}

}
