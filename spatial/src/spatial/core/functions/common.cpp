#include "spatial/common.hpp"
#include "spatial/core/functions/common.hpp"

namespace spatial {

namespace core {

GeometryFunctionLocalState::GeometryFunctionLocalState(ClientContext &context)
	: factory(BufferAllocator::Get(context)) {

}

unique_ptr<FunctionLocalState> GeometryFunctionLocalState::Init(
    ExpressionState &state, const BoundFunctionExpression &expr, FunctionData *bind_data) {
	return make_unique<GeometryFunctionLocalState>(state.GetContext());
}

unique_ptr<FunctionLocalState> GeometryFunctionLocalState::InitCast(ClientContext &context) {
	return make_unique<GeometryFunctionLocalState>(context);
}

GeometryFunctionLocalState &GeometryFunctionLocalState::ResetAndGet(CastParameters &parameters) {
	auto &local_state = (GeometryFunctionLocalState &)*parameters.local_state;
	local_state.factory.allocator.Reset();
	return local_state;
}

GeometryFunctionLocalState &GeometryFunctionLocalState::ResetAndGet(ExpressionState &state) {
	auto &local_state = (GeometryFunctionLocalState &)*ExecuteFunctionState::GetFunctionState(state);
	local_state.factory.allocator.Reset();
	return local_state;
}

} // namespace core

} // namespace spatial
