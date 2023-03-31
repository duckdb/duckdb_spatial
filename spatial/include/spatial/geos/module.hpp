#pragma once
#include "spatial/common.hpp"

namespace spatial {

namespace geos {

struct GeosModule {
public:
	static void Register(ClientContext &context);
};

} // namespace spatials

} // namespace spatial