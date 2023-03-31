#pragma once
#include "spatial/common.hpp"

namespace spatial {

namespace core {

struct CoreCastFunctions {
public:
	static void Register(ClientContext &context) {
		RegisterDimensionalCasts(context);
		RegisterGeometryCasts(context);
	}
private:
	static void RegisterDimensionalCasts(ClientContext &context);
	static void RegisterGeometryCasts(ClientContext &context);
};

} // namespace core

} // namespace spatial