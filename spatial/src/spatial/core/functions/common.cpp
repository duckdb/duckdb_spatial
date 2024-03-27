#include "spatial/common.hpp"
#include "spatial/core/functions/common.hpp"

namespace spatial {

namespace core {

GeometryFunctionLocalState::GeometryFunctionLocalState(ClientContext &context) : arena(BufferAllocator::Get(context)) {
}

unique_ptr<FunctionLocalState>
GeometryFunctionLocalState::Init(ExpressionState &state, const BoundFunctionExpression &expr, FunctionData *bind_data) {
	return make_uniq<GeometryFunctionLocalState>(state.GetContext());
}

unique_ptr<FunctionLocalState> GeometryFunctionLocalState::InitCast(CastLocalStateParameters &parameters) {
	return make_uniq<GeometryFunctionLocalState>(*parameters.context.get());
}

GeometryFunctionLocalState &GeometryFunctionLocalState::ResetAndGet(CastParameters &parameters) {
	auto &local_state = parameters.local_state->Cast<GeometryFunctionLocalState>();
	local_state.arena.Reset();
	return local_state;
}

GeometryFunctionLocalState &GeometryFunctionLocalState::ResetAndGet(ExpressionState &state) {
	auto &local_state = ExecuteFunctionState::GetFunctionState(state)->Cast<GeometryFunctionLocalState>();
	local_state.arena.Reset();
	return local_state;
}

} // namespace core

} // namespace spatial
