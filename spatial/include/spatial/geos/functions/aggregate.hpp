#pragma once
#include "spatial/common.hpp"

namespace spatial {

namespace geos {

struct GeosAggregateFunctions {
	static void Register(DatabaseInstance &db);
};

} // namespace geos

} // namespace spatial