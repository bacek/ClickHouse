#include <Analyzer/Passes/SpatialPredicateJoinPass.h>

#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/Utils.h>
#include <Functions/logical.h>

namespace DB
{

namespace
{

bool findInTableExpression(const QueryTreeNodePtr & source, const QueryTreeNodePtr & table_expression)
{
    if (!source)
        return true;
    if (source->isEqual(*table_expression))
        return true;
    if (const auto * join_node = table_expression->as<JoinNode>())
        return findInTableExpression(source, join_node->getLeftTableExpression())
            || findInTableExpression(source, join_node->getRightTableExpression());
    return false;
}

/// Split a WHERE node into: spatial predicates referencing both table sides,
/// and everything else. A "spatial predicate" is a resolved FunctionNode with
/// isSpatialPredicate() == true whose column arguments come from two different tables.
void extractSpatialConditions(
    const QueryTreeNodePtr & node,
    const QueryTreeNodePtr & left_table,
    const QueryTreeNodePtr & right_table,
    QueryTreeNodes & spatial_out,
    QueryTreeNodes & other_out)
{
    if (!node)
        return;

    auto * func = node->as<FunctionNode>();
    if (!func)
    {
        other_out.push_back(node);
        return;
    }

    /// Flatten AND
    if (func->getFunctionName() == "and")
    {
        for (const auto & arg : func->getArguments().getNodes())
            extractSpatialConditions(arg, left_table, right_table, spatial_out, other_out);
        return;
    }

    /// Check if this is a resolved spatial predicate
    if (!func->isResolved())
    {
        other_out.push_back(node);
        return;
    }

    auto fn_base = func->getFunctionOrThrow();
    if (!fn_base->isSpatialPredicate())
    {
        other_out.push_back(node);
        return;
    }

    /// Verify that among the column arguments, at least one comes from each table.
    bool touches_left = false;
    bool touches_right = false;

    for (const auto & arg : func->getArguments().getNodes())
    {
        auto [src, _] = getExpressionSource(arg);
        if (!src)
            continue;
        if (findInTableExpression(src, left_table))
            touches_left = true;
        if (findInTableExpression(src, right_table))
            touches_right = true;
    }

    if (touches_left && touches_right)
        spatial_out.push_back(node);
    else
        other_out.push_back(node);
}

class SpatialPredicateJoinVisitor : public InDepthQueryTreeVisitorWithContext<SpatialPredicateJoinVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<SpatialPredicateJoinVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        auto * query_node = node->as<QueryNode>();
        if (!query_node)
            return;

        auto & where_node = query_node->getWhere();
        if (!where_node)
            return;

        auto & join_tree_node = query_node->getJoinTree();
        if (!join_tree_node || join_tree_node->getNodeType() != QueryTreeNodeType::CROSS_JOIN)
            return;

        auto & cross_join = join_tree_node->as<CrossJoinNode &>();
        const auto & tables = cross_join.getTableExpressions();

        /// Only handle the 2-table case for now.
        if (tables.size() != 2)
            return;

        const auto & left_table  = tables[0];
        const auto & right_table = tables[1];

        QueryTreeNodes spatial_conditions;
        QueryTreeNodes other_conditions;
        extractSpatialConditions(where_node, left_table, right_table, spatial_conditions, other_conditions);

        /// Need exactly one spatial predicate to convert cleanly.
        if (spatial_conditions.size() != 1)
            return;

        /// Build: left INNER JOIN right ON spatial_pred
        const auto & join_types = cross_join.getJoinTypes();
        JoinLocality locality = join_types.empty() ? JoinLocality::Unspecified : join_types[0].locality;

        auto join_node = std::make_shared<JoinNode>(
            left_table,
            right_table,
            /*join_expression=*/nullptr,
            locality,
            JoinStrictness::Unspecified,
            JoinKind::Cross,
            /*is_using=*/false);

        join_node->crossToInner(spatial_conditions[0]);

        query_node->getJoinTree() = std::move(join_node);

        /// Rebuild WHERE from remaining conditions.
        if (other_conditions.empty())
        {
            where_node = nullptr;
        }
        else if (other_conditions.size() == 1)
        {
            where_node = other_conditions[0];
        }
        else
        {
            auto and_func = std::make_shared<FunctionNode>("and");
            and_func->markAsOperator();
            for (const auto & cond : other_conditions)
                and_func->getArguments().getNodes().push_back(cond);
            const auto & fn = createInternalFunctionAndOverloadResolver();
            and_func->resolveAsFunction(fn->build(and_func->getArgumentColumns()));
            where_node = std::move(and_func);
        }
    }
};

} // namespace

void SpatialPredicateJoinPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    SpatialPredicateJoinVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
