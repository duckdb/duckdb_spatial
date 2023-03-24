#pragma once
#include "geo/common.hpp"

namespace geo {

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

} // namespace geo