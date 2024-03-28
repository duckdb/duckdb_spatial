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

static void RasterGetGeoTransformItemFunction(DataChunk &args, ExpressionState &state, Vector &result, int32_t gt_index) {

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

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------

void GdalScalarFunctions::RegisterStGetMetadata(DatabaseInstance &db) {

	ScalarFunctionSet set_01("ST_Width");
	set_01.AddFunction(ScalarFunction({GeoTypes::RASTER()}, LogicalType::INTEGER, RasterGetWidthFunction));
	ExtensionUtil::RegisterFunction(db, set_01);

	ScalarFunctionSet set_02("ST_Height");
	set_02.AddFunction(ScalarFunction({GeoTypes::RASTER()}, LogicalType::INTEGER, RasterGetHeightFunction));
	ExtensionUtil::RegisterFunction(db, set_02);

	ScalarFunctionSet set_03("ST_NumBands");
	set_03.AddFunction(ScalarFunction({GeoTypes::RASTER()}, LogicalType::INTEGER, RasterGetNumBandsFunction));
	ExtensionUtil::RegisterFunction(db, set_03);

	ScalarFunctionSet set_04("ST_UpperLeftX");
	set_04.AddFunction(ScalarFunction({GeoTypes::RASTER()}, LogicalType::DOUBLE, RasterGetUpperLeftX));
	ExtensionUtil::RegisterFunction(db, set_04);

	ScalarFunctionSet set_05("ST_UpperLeftY");
	set_05.AddFunction(ScalarFunction({GeoTypes::RASTER()}, LogicalType::DOUBLE, RasterGetUpperLeftY));
	ExtensionUtil::RegisterFunction(db, set_05);

	ScalarFunctionSet set_06("ST_ScaleX");
	set_06.AddFunction(ScalarFunction({GeoTypes::RASTER()}, LogicalType::DOUBLE, RasterGetScaleX));
	ExtensionUtil::RegisterFunction(db, set_06);

	ScalarFunctionSet set_07("ST_ScaleY");
	set_07.AddFunction(ScalarFunction({GeoTypes::RASTER()}, LogicalType::DOUBLE, RasterGetScaleY));
	ExtensionUtil::RegisterFunction(db, set_07);

	ScalarFunctionSet set_08("ST_SkewX");
	set_08.AddFunction(ScalarFunction({GeoTypes::RASTER()}, LogicalType::DOUBLE, RasterGetSkewX));
	ExtensionUtil::RegisterFunction(db, set_08);

	ScalarFunctionSet set_09("ST_SkewY");
	set_09.AddFunction(ScalarFunction({GeoTypes::RASTER()}, LogicalType::DOUBLE, RasterGetSkewY));
	ExtensionUtil::RegisterFunction(db, set_09);

	ScalarFunctionSet set_10("ST_PixelWidth");
	set_10.AddFunction(ScalarFunction({GeoTypes::RASTER()}, LogicalType::DOUBLE, RasterGetPixelWidthFunction));
	ExtensionUtil::RegisterFunction(db, set_10);

	ScalarFunctionSet set_11("ST_PixelHeight");
	set_11.AddFunction(ScalarFunction({GeoTypes::RASTER()}, LogicalType::DOUBLE, RasterGetPixelHeightFunction));
	ExtensionUtil::RegisterFunction(db, set_11);
}

} // namespace gdal

} // namespace spatial
