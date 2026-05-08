#pragma once

#include <Parsers/IAST.h>
#include <Parsers/ASTQueryWithOnCluster.h>
#include <Core/Types.h>


namespace DB
{

class ASTDropFunctionQuery : public IAST, public ASTQueryWithOnCluster
{
public:
    String function_name;
    Strings argument_type_names;    /// Empty = drop all overloads; non-empty = drop specific overload by signature

    bool if_exists = false;

    String getID(char) const override { return "DropFunctionQuery"; }

    ASTPtr clone() const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const WithoutOnClusterASTRewriteParams &) const override { return removeOnCluster<ASTDropFunctionQuery>(clone()); }

    QueryKind getQueryKind() const override { return QueryKind::Drop; }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & s, FormatState & state, FormatStateStacked frame) const override;
};

}
