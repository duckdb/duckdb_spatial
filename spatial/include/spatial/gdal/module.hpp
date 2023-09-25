#pragma once
#include "spatial/common.hpp"

namespace spatial {

namespace gdal {

struct GdalModule {
public:
	static void Register(DatabaseInstance &db);
};

} // namespace gdal

} // namespace spatial