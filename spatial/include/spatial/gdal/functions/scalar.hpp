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
		RegisterStRasterToWorldCoord(db);
		RegisterStWorldToRasterCoord(db);
		RegisterStGetValue(db);
		RegisterStRasterFromFile(db);
		RegisterStRasterAsFile(db);
		RegisterStRasterWarp(db);
		RegisterStRasterClip(db);
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

	// ST_RasterToWorldCoord[XY]
	static void RegisterStRasterToWorldCoord(DatabaseInstance &db);

	// ST_WorldToRasterCoord[XY]
	static void RegisterStWorldToRasterCoord(DatabaseInstance &db);

	// ST_Value
	static void RegisterStGetValue(DatabaseInstance &db);

	// ST_RasterFromFile
	static void RegisterStRasterFromFile(DatabaseInstance &db);

	// ST_RasterAsFile
	static void RegisterStRasterAsFile(DatabaseInstance &db);

	// ST_RasterWarp
	static void RegisterStRasterWarp(DatabaseInstance &db);

	// ST_RasterClip
	static void RegisterStRasterClip(DatabaseInstance &db);
};

} // namespace gdal

} // namespace spatial
