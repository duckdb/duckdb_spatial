#include "spatial/common.hpp"
#include "spatial/geos/functions/common.hpp"

namespace spatial {

namespace geos {

using namespace spatial::core;

GEOSFunctionLocalState::GEOSFunctionLocalState(ClientContext &context) : ctx(), arena(BufferAllocator::Get(context)) {
	// TODO: Set GEOS error handler
	// GEOSContext_setErrorMessageHandler_r()
}

unique_ptr<FunctionLocalState> GEOSFunctionLocalState::Init(ExpressionState &state, const BoundFunctionExpression &expr,
                                                            FunctionData *bind_data) {
	return make_uniq<GEOSFunctionLocalState>(state.GetContext());
}

unique_ptr<FunctionLocalState> GEOSFunctionLocalState::InitCast(CastLocalStateParameters &parameters) {
	return make_uniq<GEOSFunctionLocalState>(*parameters.context);
}

GEOSFunctionLocalState &GEOSFunctionLocalState::ResetAndGet(CastParameters &parameters) {
	auto &local_state = parameters.local_state->Cast<GEOSFunctionLocalState>();
	local_state.arena.Reset();
	return local_state;
}

GEOSFunctionLocalState &GEOSFunctionLocalState::ResetAndGet(ExpressionState &state) {
	auto &local_state = ExecuteFunctionState::GetFunctionState(state)->Cast<GEOSFunctionLocalState>();
	local_state.arena.Reset();
	return local_state;
}

} // namespace geos

} // namespace spatial