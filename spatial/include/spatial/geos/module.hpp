#pragma once
#include "spatial/common.hpp"

namespace spatial {

namespace geos {

struct GeosModule {
public:
	static void Register(DatabaseInstance &instance);
};

} // namespace geos

} // namespace spatial