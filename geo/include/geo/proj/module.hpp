#include "geo/common.hpp"

#pragma once

namespace geo {

namespace proj {

struct ProjModule {
public:
	static void Register(ClientContext &context);
};

} // namespace proj

} // namespace geo