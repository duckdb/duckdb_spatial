#pragma once
#include "geo/common.hpp"

namespace geo {

namespace geos {

struct GeosScalarFunctions {
	static void Register(ClientContext &context);
};

} // namespace geos

} // namespace geo