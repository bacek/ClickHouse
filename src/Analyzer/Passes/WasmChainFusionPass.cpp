#include <Analyzer/Passes/WasmChainFusionPass.h>

#include <cstring>

#include <DataTypes/DataTypeNullable.h>

#include <Analyzer/FunctionNode.h>
#include <Analyzer/InDepthQueryTreeVisitor.h>

#include <Functions/IFunctionAdaptors.h>
#include <Functions/UserDefined/UserDefinedWebAssembly.h>

#include <Interpreters/Context.h>
#include <Interpreters/WebAssembly/WasmEngine.h>
#include <Interpreters/WebAssembly/WasmMemory.h>
#include <Interpreters/WebAssembly/WasmTypes.h>

#include <Common/StopToken.h>
#include <Common/logger_useful.h>
#include <fmt/ranges.h>

namespace DB
{

namespace
{

using namespace WebAssembly;

/// Try to retrieve the WASM function object for a resolved function node.
/// Returns nullptr if the node is not a registered WASM UDF.
std::shared_ptr<UserDefinedWebAssemblyFunction> tryGetWasmFunction(const FunctionNode * fn)
{
    if (!fn || !fn->isResolved())
        return nullptr;
    return UserDefinedWebAssemblyFunctionFactory::instance().getFunction(fn->getFunctionName());
}

/// Returns true when the module exports clickhouse_can_chain_execute.
/// Exceptions from getExport() (export not found) are caught and silenced.
bool moduleSupportsChaining(const WasmModule & module)
{
    try
    {
        module.getExport("clickhouse_can_chain_execute");
        return true;
    }
    catch (...)
    {
        return false;
    }
}

/// Instantiate a short-lived compartment, build a names buffer in WASM memory,
/// call clickhouse_can_chain_execute, and return true if the chain is valid.
bool validateChainViaWasm(WasmModule & module, const Strings & fn_names)
{
    WasmModule::Config cfg;
    cfg.memory_limit = 64u * 1024u * 1024u;
    cfg.fuel_limit   = 1u << 24;

    auto compartment = module.instantiate(cfg);

    StopSource stop_source;
    StopToken  stop_token = stop_source.get_token();

    // Build names buffer: [cstr name_0][cstr name_1]...[cstr name_n-1]
    std::vector<uint8_t> names_bytes;
    for (const auto & name : fn_names)
        names_bytes.insert(names_bytes.end(),
            reinterpret_cast<const uint8_t *>(name.c_str()),
            reinterpret_cast<const uint8_t *>(name.c_str()) + name.size() + 1);

    // Allocate buffer in WASM via clickhouse_create_buffer.
    WasmPtr wasm_handle = compartment->invoke<WasmPtr>(
        "clickhouse_create_buffer",
        {static_cast<WasmSizeT>(names_bytes.size())},
        stop_token);

    if (wasm_handle == 0)
        return false;

    // raw_buffer in WASM is std::vector<uint8_t>; its first two fields are
    // {data_ptr: u32, size: u32}. Follow data_ptr to write the names.
    constexpr size_t wasm_buffer_struct_size = 8;
    auto struct_view = compartment->getMemory(wasm_handle, wasm_buffer_struct_size);

    WasmPtr data_ptr = 0;
    WasmSizeT data_size = 0;
    std::memcpy(&data_ptr,  struct_view.data(),     sizeof(WasmPtr));
    std::memcpy(&data_size, struct_view.data() + 4, sizeof(WasmSizeT));

    if (data_size < static_cast<WasmSizeT>(names_bytes.size()))
    {
        compartment->invoke<void>("clickhouse_destroy_buffer", {wasm_handle}, stop_token);
        return false;
    }

    auto data_view = compartment->getMemory(data_ptr, static_cast<WasmSizeT>(names_bytes.size()));
    std::memcpy(data_view.data(), names_bytes.data(), names_bytes.size());

    const uint32_t n = static_cast<uint32_t>(fn_names.size());
    // clickhouse_can_chain_execute returns i32 (0 or 1). WasmVal uses uint32_t for i32.
    WasmPtr result = compartment->invoke<WasmPtr>(
        "clickhouse_can_chain_execute",
        {wasm_handle, static_cast<WasmSizeT>(n)},
        stop_token);

    compartment->invoke<void>("clickhouse_destroy_buffer", {wasm_handle}, stop_token);
    return result == 1u;
}

/// Describes a candidate chain that can be fused.
struct ChainCandidate
{
    Strings                          fn_names;          // SOURCE → SINK order
    std::shared_ptr<WasmModule>      wasm_module;
    QueryTreeNodes                   source_args;       // arguments of the SOURCE function
    DataTypes                        source_arg_types;
    DataTypePtr                      result_type;       // return type of the SINK function
};

/// Walk inward from `node` collecting a chain of WASM functions from the same module.
///
/// Rules:
///   - `node` (outermost) is the SINK candidate: must have exactly 1 argument that is
///     a WASM UDF from the same module.
///   - Each intermediate function (XFORM) must also have exactly 1 argument leading
///     to the next function in the chain.
///   - The innermost function (SOURCE) may have any number of arguments (those are
///     the actual column inputs to the chain).
///
/// Returns a ChainCandidate on success, nullopt when no multi-step chain exists.
std::optional<ChainCandidate> tryCollectChain(QueryTreeNodePtr & node)
{
    auto * sink_fn = node->as<FunctionNode>();
    auto sink_wasm = tryGetWasmFunction(sink_fn);
    if (!sink_wasm)
        return std::nullopt;

    auto & sink_args = sink_fn->getArguments().getNodes();
    if (sink_args.size() != 1)
        return std::nullopt;

    auto * inner_fn = sink_args[0]->as<FunctionNode>();
    if (!inner_fn)
        return std::nullopt;

    auto inner_wasm = tryGetWasmFunction(inner_fn);
    if (!inner_wasm || inner_wasm->getModule().get() != sink_wasm->getModule().get())
        return std::nullopt;

    // Build the chain walking inward from the SINK.
    // We collect function names in SINK→SOURCE order, then reverse.
    Strings fn_names;
    fn_names.push_back(sink_fn->getFunctionName());
    fn_names.push_back(inner_fn->getFunctionName());

    FunctionNode * source_fn = inner_fn;

    // Keep walking if inner has exactly 1 arg that is also a WASM UDF from same module.
    while (true)
    {
        auto & cur_args = source_fn->getArguments().getNodes();
        if (cur_args.size() != 1)
            break; // source_fn is the SOURCE (multiple or zero args)

        auto * next_fn = cur_args[0]->as<FunctionNode>();
        if (!next_fn)
            break;

        auto next_wasm = tryGetWasmFunction(next_fn);
        if (!next_wasm || next_wasm->getModule().get() != sink_wasm->getModule().get())
            break;

        fn_names.push_back(next_fn->getFunctionName());
        source_fn = next_fn;
    }

    // Reverse from SINK→SOURCE to SOURCE→SINK.
    std::reverse(fn_names.begin(), fn_names.end());

    ChainCandidate candidate;
    candidate.fn_names    = std::move(fn_names);
    candidate.wasm_module = sink_wasm->getModule();
    // Use the base (non-nullable) result type so the chain function can rely on
    // useDefaultImplementationForNulls() for external null propagation.  The sink's
    // query-tree result type may be Nullable(T) if it is called with a nullable
    // argument (e.g. Nullable(String) from a nullable-returning source), but the
    // chain's WASM sink always emits a plain COL_FIXED* / COL_BYTES result.
    candidate.result_type = removeNullable(sink_fn->getResultType());

    auto source_wasm = tryGetWasmFunction(source_fn);
    if (!source_wasm)
        return std::nullopt;
    candidate.source_arg_types = source_wasm->getArguments();

    for (const auto & arg : source_fn->getArguments().getNodes())
        candidate.source_args.push_back(arg);

    return candidate;
}

class WasmChainFusionVisitor : public InDepthQueryTreeVisitorWithContext<WasmChainFusionVisitor>
{
public:
    using Base = InDepthQueryTreeVisitorWithContext<WasmChainFusionVisitor>;
    using Base::Base;

    // leaveImpl = post-order (children first) so inner WASM nodes are visited before outer.
    void leaveImpl(QueryTreeNodePtr & node)
    {
        auto * fn = node->as<FunctionNode>();
        if (!fn)
            return;

        auto candidate = tryCollectChain(node);
        if (!candidate)
        {
            LOG_TRACE(
                getLogger("WasmChainFusionPass"),
                "No chain at function '{}'",
                fn->getFunctionName());
            return;
        }

        LOG_DEBUG(
            getLogger("WasmChainFusionPass"),
            "Chain candidate: [{}]",
            fmt::join(candidate->fn_names, " -> "));

        if (!moduleSupportsChaining(*candidate->wasm_module))
        {
            LOG_DEBUG(getLogger("WasmChainFusionPass"),
                "Module does not export clickhouse_can_chain_execute, skipping chain [{}]",
                fmt::join(candidate->fn_names, " -> "));
            return;
        }

        if (!validateChainViaWasm(*candidate->wasm_module, candidate->fn_names))
        {
            LOG_DEBUG(getLogger("WasmChainFusionPass"),
                "clickhouse_can_chain_execute rejected chain [{}]",
                fmt::join(candidate->fn_names, " -> "));
            return;
        }

        LOG_DEBUG(getLogger("WasmChainFusionPass"),
            "Fusing WASM chain [{}]",
            fmt::join(candidate->fn_names, " -> "));

        auto resolver = createWasmChainResolver(
            std::move(candidate->fn_names),
            std::move(candidate->wasm_module),
            std::move(candidate->source_arg_types),
            std::move(candidate->result_type),
            getContext());

        auto original_alias = node->getAlias();

        auto new_fn = std::make_shared<FunctionNode>("__wasm_chain");
        new_fn->getArguments().getNodes() = std::move(candidate->source_args);
        new_fn->resolveAsFunction(resolver->build(new_fn->getArgumentColumns()));

        if (!original_alias.empty())
            new_fn->setAlias(original_alias);

        node = std::move(new_fn);
    }
};

} // namespace

void WasmChainFusionPass::run(QueryTreeNodePtr & query_tree_node, ContextPtr context)
{
    WasmChainFusionVisitor visitor(std::move(context));
    visitor.visit(query_tree_node);
}

} // namespace DB
