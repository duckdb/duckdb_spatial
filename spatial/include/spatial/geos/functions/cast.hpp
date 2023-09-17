#pragma once
#include "spatial/common.hpp"

namespace spatial {

namespace geos {

struct GeosCastFunctions {
	static void Register(DatabaseInstance &instance);
};

} // namespace geos

} // namespace spatial