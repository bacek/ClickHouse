#include <Analyzer/Passes/SpatialJoinFusePass.h>

#include <Analyzer/ColumnNode.h>
#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>
#include <Analyzer/JoinNode.h>
#include <Analyzer/QueryNode.h>
#include <Analyzer/TableFunctionNode.h>
#include <Analyzer/TableNode.h>

namespace DB
{

namespace
{

/// Returns true if two table expressions scan the same physical data:
///   - TableNode:         same storage UUID
///   - TableFunctionNode: same function name AND same argument values
/// Aliases are ignored.
bool samePhysicalData(const QueryTreeNodePtr & lhs, const QueryTreeNodePtr & rhs)
{
    if (lhs.get() == rhs.get())
        return true;
    if (!lhs || !rhs)
        return false;
    if (lhs->getNodeType() != rhs->getNodeType())
        return false;

    if (const auto * lt = lhs->as<TableNode>())
    {
        const auto & rt = rhs->as<TableNode &>();
        return lt->getStorageID().uuid == rt.getStorageID().uuid
            && lt->getStorageID().uuid != UUIDHelpers::Nil;
    }

    if (const auto * lf = lhs->as<TableFunctionNode>())
    {
        const auto & rf = rhs->as<TableFunctionNode &>();
        if (lf->getTableFunctionName() != rf.getTableFunctionName())
            return false;
        const auto & largs = lf->getArguments().getNodes();
        const auto & rargs = rf.getArguments().getNodes();
        if (largs.size() != rargs.size())
            return false;
        for (size_t i = 0; i < largs.size(); ++i)
            if (!largs[i]->isEqual(*rargs[i], IQueryTreeNode::CompareOptions{.compare_aliases = false}))
                return false;
        return true;
    }

    return false;
}

/// Walk a spatial FunctionNode's arguments to find the one whose ColumnNode source
/// matches probe_table.  Returns the physical column name, or empty string if not found.
String findProbeColName(const FunctionNode & func, const QueryTreeNodePtr & probe_table)
{
    for (const auto & arg : func.getArguments().getNodes())
    {
        const auto * col = arg->as<ColumnNode>();
        if (!col)
            continue;
        auto src = col->getColumnSourceOrNull();
        if (src && src->isEqual(*probe_table))
            return col->getColumnName();
    }
    return {};
}

/// Returns true if node is a resolved FunctionNode with isSpatialPredicate() == true.
bool isSpatialPredFunc(const QueryTreeNodePtr & node)
{
    const auto * func = node->as<FunctionNode>();
    if (!func || !func->isResolved())
        return false;
    auto fn_base = func->getFunctionOrThrow();
    return fn_base && fn_base->isSpatialPredicate();
}

class SpatialJoinFuseVisitor : public InDepthQueryTreeVisitorWithContext<SpatialJoinFuseVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<SpatialJoinFuseVisitor>;
    using Base::Base;

    void enterImpl(QueryTreeNodePtr & node)
    {
        auto * query_node = node->as<QueryNode>();
        if (!query_node)
            return;

        auto & join_tree = query_node->getJoinTree();
        if (!join_tree || join_tree->getNodeType() != QueryTreeNodeType::JOIN)
            return;

        auto & outer_join = join_tree->as<JoinNode &>();

        if (outer_join.getKind() != JoinKind::Inner || !outer_join.isOnJoinExpression())
            return;

        auto & outer_left = outer_join.getLeftTableExpression();
        if (!outer_left || outer_left->getNodeType() != QueryTreeNodeType::JOIN)
            return;

        auto & inner_join = outer_left->as<JoinNode &>();
        if (inner_join.getKind() != JoinKind::Inner || !inner_join.isOnJoinExpression())
            return;

        /// Both ON clauses must be spatial predicates.
        if (!isSpatialPredFunc(inner_join.getJoinExpression()))
            return;
        if (!isSpatialPredFunc(outer_join.getJoinExpression()))
            return;

        /// The repeated dimension table appears as both inner_join.right and outer_join.right.
        /// The probe table appears once (inner_join.left or inner_join.right).
        ///
        /// Handle both orderings:
        ///   FROM probe JOIN dim ON ... JOIN dim ON ...  → dim on RIGHT of inner join
        ///   FROM dim JOIN probe ON ... JOIN dim ON ...  → dim on LEFT of inner join
        auto & outer_dim = outer_join.getRightTableExpression();

        QueryTreeNodePtr * inner_dim_ptr  = nullptr;
        QueryTreeNodePtr * probe_ptr      = nullptr;

        if (samePhysicalData(inner_join.getRightTableExpression(), outer_dim))
        {
            inner_dim_ptr = &inner_join.getRightTableExpression();
            probe_ptr     = &inner_join.getLeftTableExpression();
        }
        else if (samePhysicalData(inner_join.getLeftTableExpression(), outer_dim))
        {
            inner_dim_ptr = &inner_join.getLeftTableExpression();
            probe_ptr     = &inner_join.getRightTableExpression();
        }
        else
            return;

        auto & inner_dim = *inner_dim_ptr;
        auto & probe     = *probe_ptr;

        /// Find which arg of spatial1 comes from the probe table.
        const auto & spatial1_func = inner_join.getJoinExpression()->as<FunctionNode &>();
        String first_probe_col = findProbeColName(spatial1_func, probe);
        if (first_probe_col.empty())
            return;

        /// Verify spatial2 also references a column from the probe table.
        const auto & spatial2_func = outer_join.getJoinExpression()->as<FunctionNode &>();
        if (findProbeColName(spatial2_func, probe).empty())
            return;

        /// Annotate the join pair.
        inner_join.is_fused_child = true;
        inner_join.fused_probe_on_right = (probe_ptr == &inner_join.getRightTableExpression());
        outer_join.fused_spatial_info = JoinNode::FusedSpatialInfo{
            .first_probe_col_name    = first_probe_col,
            .probe_table_node        = probe,
            .inner_dim_table_node    = inner_dim,
        };
    }
};

} // namespace

void SpatialJoinFusePass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    SpatialJoinFuseVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

}
