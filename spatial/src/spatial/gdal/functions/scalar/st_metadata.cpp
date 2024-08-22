#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/gdal/functions/scalar.hpp"
#include "spatial/gdal/raster/raster.hpp"

#include "gdal_priv.h"

using namespace spatial::core;

namespace spatial {

namespace gdal {

//------------------------------------------------------------------------------
// Raster Accessors
//------------------------------------------------------------------------------

static void RasterGetWidthFunction(DataChunk &args, ExpressionState &state, Vector &result) {

	UnaryExecutor::Execute<uintptr_t, int32_t>(args.data[0], result, args.size(), [&](uintptr_t input) {
		GDALDataset *dataset = reinterpret_cast<GDALDataset *>(input);
		return dataset->GetRasterXSize();
	});
}

static void RasterGetHeightFunction(DataChunk &args, ExpressionState &state, Vector &result) {

	UnaryExecutor::Execute<uintptr_t, int32_t>(args.data[0], result, args.size(), [&](uintptr_t input) {
		GDALDataset *dataset = reinterpret_cast<GDALDataset *>(input);
		return dataset->GetRasterYSize();
	});
}

static void RasterGetNumBandsFunction(DataChunk &args, ExpressionState &state, Vector &result) {

	UnaryExecutor::Execute<uintptr_t, int32_t>(args.data[0], result, args.size(), [&](uintptr_t input) {
		GDALDataset *dataset = reinterpret_cast<GDALDataset *>(input);
		return dataset->GetRasterCount();
	});
}

static void RasterGetGeoTransformItemFunction(DataChunk &args, ExpressionState &state, Vector &result,
                                              int32_t gt_index) {

	UnaryExecutor::Execute<uintptr_t, double>(args.data[0], result, args.size(), [&](uintptr_t input) {
		Raster raster(reinterpret_cast<GDALDataset *>(input));
		double gt[6] = {0};
		raster.GetGeoTransform(gt);
		return gt[gt_index];
	});
}

static void RasterGetUpperLeftX(DataChunk &args, ExpressionState &state, Vector &result) {
	RasterGetGeoTransformItemFunction(args, state, result, 0);
}
static void RasterGetUpperLeftY(DataChunk &args, ExpressionState &state, Vector &result) {
	RasterGetGeoTransformItemFunction(args, state, result, 3);
}
static void RasterGetScaleX(DataChunk &args, ExpressionState &state, Vector &result) {
	RasterGetGeoTransformItemFunction(args, state, result, 1);
}
static void RasterGetScaleY(DataChunk &args, ExpressionState &state, Vector &result) {
	RasterGetGeoTransformItemFunction(args, state, result, 5);
}
static void RasterGetSkewX(DataChunk &args, ExpressionState &state, Vector &result) {
	RasterGetGeoTransformItemFunction(args, state, result, 2);
}
static void RasterGetSkewY(DataChunk &args, ExpressionState &state, Vector &result) {
	RasterGetGeoTransformItemFunction(args, state, result, 4);
}

static void RasterGetPixelWidthFunction(DataChunk &args, ExpressionState &state, Vector &result) {

	UnaryExecutor::Execute<uintptr_t, double>(args.data[0], result, args.size(), [&](uintptr_t input) {
		Raster raster(reinterpret_cast<GDALDataset *>(input));
		double gt[6] = {0};
		raster.GetGeoTransform(gt);
		return sqrt(gt[1] * gt[1] + gt[4] * gt[4]);
	});
}

static void RasterGetPixelHeightFunction(DataChunk &args, ExpressionState &state, Vector &result) {

	UnaryExecutor::Execute<uintptr_t, double>(args.data[0], result, args.size(), [&](uintptr_t input) {
		Raster raster(reinterpret_cast<GDALDataset *>(input));
		double gt[6] = {0};
		raster.GetGeoTransform(gt);
		return sqrt(gt[5] * gt[5] + gt[2] * gt[2]);
	});
}

//------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------

static constexpr const char *DOC_DESCRIPTION_01 = R"(
	Returns the width of the raster in pixels.
)";

static constexpr const char *DOC_EXAMPLE_01 = R"(
	SELECT ST_Width(raster) FROM './test/data/mosaic/SCL.tif-land-clip00.tiff';
)";

static constexpr DocTag DOC_TAGS_01[] = {{"ext", "spatial"}, {"category", "property"}};

static constexpr const char *DOC_DESCRIPTION_02 = R"(
	Returns the height of the raster in pixels.
)";

static constexpr const char *DOC_EXAMPLE_02 = R"(
	SELECT ST_Height(raster) FROM './test/data/mosaic/SCL.tif-land-clip00.tiff';
)";

static constexpr DocTag DOC_TAGS_02[] = {{"ext", "spatial"}, {"category", "property"}};

static constexpr const char *DOC_DESCRIPTION_03 = R"(
	Returns the number of bands in the raster.
)";

static constexpr const char *DOC_EXAMPLE_03 = R"(
	SELECT ST_NumBands(raster) FROM './test/data/mosaic/SCL.tif-land-clip00.tiff';
)";

static constexpr DocTag DOC_TAGS_03[] = {{"ext", "spatial"}, {"category", "property"}};

static constexpr const char *DOC_DESCRIPTION_04 = R"(
	Returns the upper left X coordinate of raster in projected spatial reference.
)";

static constexpr const char *DOC_EXAMPLE_04 = R"(
	SELECT ST_UpperLeftX(raster) FROM './test/data/mosaic/SCL.tif-land-clip00.tiff';
)";

static constexpr DocTag DOC_TAGS_04[] = {{"ext", "spatial"}, {"category", "property"}};

static constexpr const char *DOC_DESCRIPTION_05 = R"(
	Returns the upper left Y coordinate of raster in projected spatial reference.
)";

static constexpr const char *DOC_EXAMPLE_05 = R"(
	SELECT ST_UpperLeftY(raster) FROM './test/data/mosaic/SCL.tif-land-clip00.tiff';
)";

static constexpr DocTag DOC_TAGS_05[] = {{"ext", "spatial"}, {"category", "property"}};

static constexpr const char *DOC_DESCRIPTION_06 = R"(
	Returns the X component of the pixel width in units of coordinate reference system.
	Refer to [World File](https://en.wikipedia.org/wiki/World_file) for more details.
)";

static constexpr const char *DOC_EXAMPLE_06 = R"(
	SELECT ST_ScaleX(raster) FROM './test/data/mosaic/SCL.tif-land-clip00.tiff';
)";

static constexpr DocTag DOC_TAGS_06[] = {{"ext", "spatial"}, {"category", "property"}};

static constexpr const char *DOC_DESCRIPTION_07 = R"(
	Returns the Y component of the pixel width in units of coordinate reference system.
	Refer to [World File](https://en.wikipedia.org/wiki/World_file) for more details.
)";

static constexpr const char *DOC_EXAMPLE_07 = R"(
	SELECT ST_ScaleY(raster) FROM './test/data/mosaic/SCL.tif-land-clip00.tiff';
)";

static constexpr DocTag DOC_TAGS_07[] = {{"ext", "spatial"}, {"category", "property"}};

static constexpr const char *DOC_DESCRIPTION_08 = R"(
	Returns the georeference X skew (or rotation parameter).
	Refer to [World File](https://en.wikipedia.org/wiki/World_file) for more details.
)";

static constexpr const char *DOC_EXAMPLE_08 = R"(
	SELECT ST_SkewX(raster) FROM './test/data/mosaic/SCL.tif-land-clip00.tiff';
)";

static constexpr DocTag DOC_TAGS_08[] = {{"ext", "spatial"}, {"category", "property"}};

static constexpr const char *DOC_DESCRIPTION_09 = R"(
	Returns the georeference Y skew (or rotation parameter).
	Refer to [World File](https://en.wikipedia.org/wiki/World_file) for more details.
)";

static constexpr const char *DOC_EXAMPLE_09 = R"(
	SELECT ST_SkewY(raster) FROM './test/data/mosaic/SCL.tif-land-clip00.tiff';
)";

static constexpr DocTag DOC_TAGS_09[] = {{"ext", "spatial"}, {"category", "property"}};

static constexpr const char *DOC_DESCRIPTION_10 = R"(
	Returns the width of a pixel in geometric units of the spatial reference system.
	In the common case where there is no skew, the pixel width is just the scale ratio between geometric coordinates and raster pixels.
)";

static constexpr const char *DOC_EXAMPLE_10 = R"(
	SELECT ST_PixelWidth(raster) FROM './test/data/mosaic/SCL.tif-land-clip00.tiff';
)";

static constexpr DocTag DOC_TAGS_10[] = {{"ext", "spatial"}, {"category", "property"}};

static constexpr const char *DOC_DESCRIPTION_11 = R"(
	Returns the height of a pixel in geometric units of the spatial reference system.
	In the common case where there is no skew, the pixel height is just the scale ratio between geometric coordinates and raster pixels.
)";

static constexpr const char *DOC_EXAMPLE_11 = R"(
	SELECT ST_PixelHeight(raster) FROM './test/data/mosaic/SCL.tif-land-clip00.tiff';
)";

static constexpr DocTag DOC_TAGS_11[] = {{"ext", "spatial"}, {"category", "property"}};

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------

void GdalScalarFunctions::RegisterStGetMetadata(DatabaseInstance &db) {

	ScalarFunctionSet set_01("ST_Width");
	set_01.AddFunction(ScalarFunction({GeoTypes::RASTER()}, LogicalType::INTEGER, RasterGetWidthFunction));
	ExtensionUtil::RegisterFunction(db, set_01);

	DocUtil::AddDocumentation(db, "ST_Width", DOC_DESCRIPTION_01, DOC_EXAMPLE_01, DOC_TAGS_01);

	ScalarFunctionSet set_02("ST_Height");
	set_02.AddFunction(ScalarFunction({GeoTypes::RASTER()}, LogicalType::INTEGER, RasterGetHeightFunction));
	ExtensionUtil::RegisterFunction(db, set_02);

	DocUtil::AddDocumentation(db, "ST_Height", DOC_DESCRIPTION_02, DOC_EXAMPLE_02, DOC_TAGS_02);

	ScalarFunctionSet set_03("ST_NumBands");
	set_03.AddFunction(ScalarFunction({GeoTypes::RASTER()}, LogicalType::INTEGER, RasterGetNumBandsFunction));
	ExtensionUtil::RegisterFunction(db, set_03);

	DocUtil::AddDocumentation(db, "ST_NumBands", DOC_DESCRIPTION_03, DOC_EXAMPLE_03, DOC_TAGS_03);

	ScalarFunctionSet set_04("ST_UpperLeftX");
	set_04.AddFunction(ScalarFunction({GeoTypes::RASTER()}, LogicalType::DOUBLE, RasterGetUpperLeftX));
	ExtensionUtil::RegisterFunction(db, set_04);

	DocUtil::AddDocumentation(db, "ST_UpperLeftX", DOC_DESCRIPTION_04, DOC_EXAMPLE_04, DOC_TAGS_04);

	ScalarFunctionSet set_05("ST_UpperLeftY");
	set_05.AddFunction(ScalarFunction({GeoTypes::RASTER()}, LogicalType::DOUBLE, RasterGetUpperLeftY));
	ExtensionUtil::RegisterFunction(db, set_05);

	DocUtil::AddDocumentation(db, "ST_UpperLeftY", DOC_DESCRIPTION_05, DOC_EXAMPLE_05, DOC_TAGS_05);

	ScalarFunctionSet set_06("ST_ScaleX");
	set_06.AddFunction(ScalarFunction({GeoTypes::RASTER()}, LogicalType::DOUBLE, RasterGetScaleX));
	ExtensionUtil::RegisterFunction(db, set_06);

	DocUtil::AddDocumentation(db, "ST_ScaleX", DOC_DESCRIPTION_06, DOC_EXAMPLE_06, DOC_TAGS_06);

	ScalarFunctionSet set_07("ST_ScaleY");
	set_07.AddFunction(ScalarFunction({GeoTypes::RASTER()}, LogicalType::DOUBLE, RasterGetScaleY));
	ExtensionUtil::RegisterFunction(db, set_07);

	DocUtil::AddDocumentation(db, "ST_ScaleY", DOC_DESCRIPTION_07, DOC_EXAMPLE_07, DOC_TAGS_07);

	ScalarFunctionSet set_08("ST_SkewX");
	set_08.AddFunction(ScalarFunction({GeoTypes::RASTER()}, LogicalType::DOUBLE, RasterGetSkewX));
	ExtensionUtil::RegisterFunction(db, set_08);

	DocUtil::AddDocumentation(db, "ST_SkewX", DOC_DESCRIPTION_08, DOC_EXAMPLE_08, DOC_TAGS_08);

	ScalarFunctionSet set_09("ST_SkewY");
	set_09.AddFunction(ScalarFunction({GeoTypes::RASTER()}, LogicalType::DOUBLE, RasterGetSkewY));
	ExtensionUtil::RegisterFunction(db, set_09);

	DocUtil::AddDocumentation(db, "ST_SkewY", DOC_DESCRIPTION_09, DOC_EXAMPLE_09, DOC_TAGS_09);

	ScalarFunctionSet set_10("ST_PixelWidth");
	set_10.AddFunction(ScalarFunction({GeoTypes::RASTER()}, LogicalType::DOUBLE, RasterGetPixelWidthFunction));
	ExtensionUtil::RegisterFunction(db, set_10);

	DocUtil::AddDocumentation(db, "ST_PixelWidth", DOC_DESCRIPTION_10, DOC_EXAMPLE_10, DOC_TAGS_10);

	ScalarFunctionSet set_11("ST_PixelHeight");
	set_11.AddFunction(ScalarFunction({GeoTypes::RASTER()}, LogicalType::DOUBLE, RasterGetPixelHeightFunction));
	ExtensionUtil::RegisterFunction(db, set_11);

	DocUtil::AddDocumentation(db, "ST_PixelHeight", DOC_DESCRIPTION_11, DOC_EXAMPLE_11, DOC_TAGS_11);
}

} // namespace gdal

} // namespace spatial
