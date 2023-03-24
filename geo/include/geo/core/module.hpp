#pragma once
#include "geo/common.hpp"

#include "geo/core/geometry/geometry.hpp"

namespace geo {

namespace core {

struct CoreModule {
public:
	static void Register(ClientContext &context);
};

} // namespace core

} // namespace geo