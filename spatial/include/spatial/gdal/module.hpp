#pragma once
#include "spatial/common.hpp"

namespace spatial {

namespace gdal {

struct GdalModule {
public:
	static void Register(ClientContext &context);
};

} // namespace gdal

} // namespace spatial