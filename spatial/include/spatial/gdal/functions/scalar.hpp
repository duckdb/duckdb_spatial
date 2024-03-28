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
		RegisterStGetHasNoBand(db);
		RegisterStBandPixelType(db);
		RegisterStBandPixelTypeName(db);
		RegisterStBandNoDataValue(db);
		RegisterStBandColorInterp(db);
		RegisterStBandColorInterpName(db);
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

	// ST_HasNoBand
	static void RegisterStGetHasNoBand(DatabaseInstance &db);

	// ST_GetBandPixelType
	static void RegisterStBandPixelType(DatabaseInstance &db);

	// ST_GetBandPixelTypeName
	static void RegisterStBandPixelTypeName(DatabaseInstance &db);

	// ST_GetBandNoDataValue
	static void RegisterStBandNoDataValue(DatabaseInstance &db);

	// ST_GetBandColorInterp
	static void RegisterStBandColorInterp(DatabaseInstance &db);

	// ST_GetBandColorInterpName
	static void RegisterStBandColorInterpName(DatabaseInstance &db);
};

} // namespace gdal

} // namespace spatial
