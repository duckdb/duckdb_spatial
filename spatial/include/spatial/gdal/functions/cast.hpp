#pragma once
#include "spatial/common.hpp"

namespace spatial {

namespace gdal {

struct GdalCastFunctions {
public:
	static void Register(DatabaseInstance &db);
};

} // namespace gdal

} // namespace spatial
