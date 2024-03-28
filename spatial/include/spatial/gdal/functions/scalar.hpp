#pragma once
#include "spatial/common.hpp"

namespace spatial {

namespace gdal {

struct GdalScalarFunctions {
public:
	static void Register(DatabaseInstance &db) {
		RegisterStGetSRID(db);
		RegisterStGetGeometry(db);
		RegisterStGetMetadata(db);
	}

private:
	// ST_SRID
	static void RegisterStGetSRID(DatabaseInstance &db);

	// ST_GetGeometry
	static void RegisterStGetGeometry(DatabaseInstance &db);

	// Raster Accessors:
	// ST_Width, ST_Height, ST_NumBands,
	// ST_UpperLeftX, ST_UpperLeftY, ST_ScaleX, ST_ScaleY, ST_SkewX, ST_SkewY,
	// ST_PixelWidth, ST_PixelHeight
	static void RegisterStGetMetadata(DatabaseInstance &db);
};

} // namespace gdal

} // namespace spatial
