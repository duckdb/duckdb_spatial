#pragma once
#include "spatial/common.hpp"
#include "spatial/core/geometry/geometry_factory.hpp"

namespace spatial {

namespace core {

struct GeometryFunctionLocalState : FunctionLocalState {
public:
	GeometryFactory factory;

public:
	explicit GeometryFunctionLocalState(ClientContext &context);
	static unique_ptr<FunctionLocalState> Init(ExpressionState &state, const BoundFunctionExpression &expr,
	                                           FunctionData *bind_data);
	static unique_ptr<FunctionLocalState> InitCast(CastLocalStateParameters &context);
	static GeometryFunctionLocalState &ResetAndGet(ExpressionState &state);
	static GeometryFunctionLocalState &ResetAndGet(CastParameters &parameters);
};

} // namespace core

} // namespace spatial