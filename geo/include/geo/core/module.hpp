#include "geo/common.hpp"

#pragma once

namespace geo {

namespace core {

struct CoreModule {
public:
	static void Register(ClientContext &context);
};

} // namespace gdal

} // namespace geo