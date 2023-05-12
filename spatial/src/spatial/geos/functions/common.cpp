#include "spatial/common.hpp"
#include "spatial/geos/functions/common.hpp"

namespace spatial {

namespace geos {

using namespace spatial::core;

GEOSFunctionLocalState::GEOSFunctionLocalState(ClientContext &context) : ctx(), factory(BufferAllocator::Get(context)) {
}

unique_ptr<FunctionLocalState> GEOSFunctionLocalState::Init(ExpressionState &state, const BoundFunctionExpression &expr,
                                                            FunctionData *bind_data) {
	return make_uniq<GEOSFunctionLocalState>(state.GetContext());
}

unique_ptr<FunctionLocalState> GEOSFunctionLocalState::InitCast(CastLocalStateParameters &parameters) {
	return make_uniq<GEOSFunctionLocalState>(*parameters.context.get());
}

GEOSFunctionLocalState &GEOSFunctionLocalState::ResetAndGet(CastParameters &parameters) {
	auto &local_state = (GEOSFunctionLocalState &)*parameters.local_state;
	local_state.factory.allocator.Reset();
	return local_state;
}

GEOSFunctionLocalState &GEOSFunctionLocalState::ResetAndGet(ExpressionState &state) {
	auto &local_state = (GEOSFunctionLocalState &)*ExecuteFunctionState::GetFunctionState(state);
	local_state.factory.allocator.Reset();
	return local_state;
}

} // namespace geos

} // namespace spatial