#pragma once
#include "geo/common.hpp"

namespace geo {

namespace geos {

struct GeosAggregateFunctions {
	static void Register(ClientContext &context);
};

} // namespace geos

} // namespace geo