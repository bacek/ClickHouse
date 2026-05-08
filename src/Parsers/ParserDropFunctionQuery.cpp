#include <Parsers/ASTDropFunctionQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserDropFunctionQuery.h>

#include <Parsers/Lexer.h>

namespace DB
{

bool ParserDropFunctionQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    ParserKeyword s_drop(Keyword::DROP);
    ParserKeyword s_function(Keyword::FUNCTION);
    ParserKeyword s_if_exists(Keyword::IF_EXISTS);
    ParserIdentifier function_name_p;
    ParserIdentifier type_name_p;

    String cluster_str;
    bool if_exists = false;

    ASTPtr function_name;

    if (!s_drop.ignore(pos, expected))
        return false;

    if (!s_function.ignore(pos, expected))
        return false;

    if (s_if_exists.ignore(pos, expected))
        if_exists = true;

    if (!function_name_p.parse(pos, function_name, expected))
        return false;

    auto drop_function_query = make_intrusive<ASTDropFunctionQuery>();
    drop_function_query->function_name = function_name->as<ASTIdentifier &>().name();
    drop_function_query->if_exists = if_exists;

    // Parse optional type signature: (Type1, Type2, ...)
    ParserToken open_bracket(TokenType::OpeningRoundBracket);
    ASTPtr dummy;
    if (open_bracket.parse(pos, dummy, expected))
    {
        while (true)
        {
            ASTPtr type_ast;
            if (!type_name_p.parse(pos, type_ast, expected))
                break;
            drop_function_query->argument_type_names.push_back(type_ast->as<ASTIdentifier &>().name());

            ParserToken comma(TokenType::Comma);
            if (comma.parse(pos, dummy, expected))
                continue;
            break;
        }
        ParserToken close_bracket(TokenType::ClosingRoundBracket);
        close_bracket.parse(pos, dummy, expected);
    }

    ParserKeyword s_on(Keyword::ON);
    if (s_on.ignore(pos, expected))
    {
        String cluster_str_inner;
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str_inner, expected))
            return false;
        cluster_str = std::move(cluster_str_inner);
    }

    drop_function_query->cluster = std::move(cluster_str);

    node = drop_function_query;
    return true;
}

}
