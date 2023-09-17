#pragma once
#include "spatial/common.hpp"

namespace spatial {

namespace gdal {

struct GdalModule {
public:
	static void Register(DatabaseInstance &instance);
};

} // namespace gdal

} // namespace spatial