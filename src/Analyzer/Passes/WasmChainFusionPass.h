#pragma once

#include <Analyzer/IQueryTreePass.h>

namespace DB
{

/** Fuse a chain of WASM UDF calls — e.g. st_length(st_makeline(a, b)) — into a
  * single WASM invocation via clickhouse_chain_execute, eliminating intermediate
  * WKB serialization between steps.
  *
  * Opt-in requirements (both must hold):
  *   1. All functions in the chain belong to the same WASM module.
  *   2. The module exports clickhouse_can_chain_execute, which returns 1 for
  *      the specific sequence of function names.
  *
  * Pattern matched (SOURCE → XFORM* → SINK):
  *   sink_fn(xform_fn*(source_fn(col1, col2, ...)))
  *
  * Rewritten to a single synthetic chain function call that takes the SOURCE
  * function's arguments and returns the SINK function's result type.
  */
class WasmChainFusionPass final : public IQueryTreePass
{
public:
    String getName() override { return "WasmChainFusionPass"; }

    String getDescription() override
    {
        return "Fuse WASM UDF chains into a single clickhouse_chain_execute call";
    }

    void run(QueryTreeNodePtr & query_tree_node, ContextPtr context) override;
};

}
