#pragma once
#include "spatial/common.hpp"

namespace spatial {

namespace geos {

struct GeosCastFunctions {
	static void Register(DatabaseInstance &db);
};

} // namespace geos

} // namespace spatial