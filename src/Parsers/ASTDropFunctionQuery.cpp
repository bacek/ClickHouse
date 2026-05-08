#include <Parsers/ASTDropFunctionQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB
{

ASTPtr ASTDropFunctionQuery::clone() const
{
    return make_intrusive<ASTDropFunctionQuery>(*this);
}

void ASTDropFunctionQuery::formatImpl(WriteBuffer & ostr, const IAST::FormatSettings & settings, IAST::FormatState &, IAST::FormatStateStacked) const
{
    ostr << "DROP FUNCTION ";

    if (if_exists)
        ostr << "IF EXISTS ";

    ostr << backQuoteIfNeed(function_name);
    if (!argument_type_names.empty())
    {
        ostr << "(";
        for (size_t i = 0; i < argument_type_names.size(); ++i)
        {
            if (i > 0)
                ostr << ", ";
            ostr << argument_type_names[i];
        }
        ostr << ")";
    }
    formatOnCluster(ostr, settings);
}

}
