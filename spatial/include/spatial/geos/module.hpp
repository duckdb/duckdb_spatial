#pragma once
#include "spatial/common.hpp"

namespace spatial {

namespace geos {

struct GeosModule {
public:
	static void Register(ClientContext &context);
};

} // namespace geos

} // namespace spatial