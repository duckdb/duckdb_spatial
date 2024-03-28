#pragma once
#include "spatial/common.hpp"

namespace spatial {

namespace gdal {

struct GdalScalarFunctions {
public:
	static void Register(DatabaseInstance &db) {
		RegisterStGetSRID(db);
		RegisterStGetGeometry(db);
	}

private:
	// ST_SRID
	static void RegisterStGetSRID(DatabaseInstance &db);

	// ST_GetGeometry
	static void RegisterStGetGeometry(DatabaseInstance &db);
};

} // namespace gdal

} // namespace spatial
