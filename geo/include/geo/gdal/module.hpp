#pragma once
#include "geo/common.hpp"

namespace geo {

namespace gdal {

struct GdalModule {
public:
	static void Register(ClientContext &context);
};

} // namespace gdal

} // namespace geo