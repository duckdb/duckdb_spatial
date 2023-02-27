#include "geo/common.hpp"

#pragma once

namespace geo {

namespace gdal {

struct GdalModule {
public:
	static void Register(ClientContext &context);
};

} // namespace gdal

} // namespace geo