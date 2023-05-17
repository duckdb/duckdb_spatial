#pragma once
#include "spatial/common.hpp"

namespace spatial {

namespace core {

struct CoreCastFunctions {
public:
	static void Register(ClientContext &context) {
		RegisterVarcharCasts(context);
		RegisterDimensionalCasts(context);
		RegisterGeometryCasts(context);
		RegisterWKBCasts(context);
	}

private:
	static void RegisterVarcharCasts(ClientContext &context);
	static void RegisterDimensionalCasts(ClientContext &context);
	static void RegisterGeometryCasts(ClientContext &context);
	static void RegisterWKBCasts(ClientContext &context);
};

} // namespace core

} // namespace spatial