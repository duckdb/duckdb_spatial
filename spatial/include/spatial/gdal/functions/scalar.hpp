#pragma once
#include "spatial/common.hpp"

namespace spatial {

namespace gdal {

struct GdalScalarFunctions {
public:
	static void Register(DatabaseInstance &db) {
		RegisterStGetSRID(db);
	}

private:
	// ST_SRID
	static void RegisterStGetSRID(DatabaseInstance &db);
};

} // namespace gdal

} // namespace spatial
