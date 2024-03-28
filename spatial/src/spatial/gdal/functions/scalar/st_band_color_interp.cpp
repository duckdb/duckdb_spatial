#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/gdal/types.hpp"
#include "spatial/gdal/functions/scalar.hpp"

#include "gdal_priv.h"

using namespace spatial::core;

namespace spatial {

namespace gdal {

//------------------------------------------------------------------------------
// ST_GetBandColorInterp
//------------------------------------------------------------------------------

static void RasterGetColorInterpFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 2);

	BinaryExecutor::Execute<uintptr_t, int32_t, int32_t>(args.data[0], args.data[1], result, args.size(), [&](uintptr_t input, int32_t band_num) {
		GDALDataset *dataset = reinterpret_cast<GDALDataset *>(input);

		if (band_num < 1) {
			throw InvalidInputException("BandNum must be greater than 0");
		}
		if (dataset->GetRasterCount() < band_num) {
			throw InvalidInputException("Dataset only has %d RasterBands", dataset->GetRasterCount());
		}
		GDALRasterBand *raster_band = dataset->GetRasterBand(band_num);
		return (int32_t)raster_band->GetColorInterpretation();
	});
}

//------------------------------------------------------------------------------
// ST_GetBandColorInterpName
//------------------------------------------------------------------------------

static void RasterGetColorInterpNameFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 2);

	BinaryExecutor::Execute<uintptr_t, int32_t, string_t>(args.data[0], args.data[1], result, args.size(), [&](uintptr_t input, int32_t band_num) {
		GDALDataset *dataset = reinterpret_cast<GDALDataset *>(input);

		if (band_num < 1) {
			throw InvalidInputException("BandNum must be greater than 0");
		}
		if (dataset->GetRasterCount() < band_num) {
			throw InvalidInputException("Dataset only has %d RasterBands", dataset->GetRasterCount());
		}
		GDALRasterBand *raster_band = dataset->GetRasterBand(band_num);
		return GetColorInterpName((ColorInterp)raster_band->GetColorInterpretation());
	});
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------

void GdalScalarFunctions::RegisterStBandColorInterp(DatabaseInstance &db) {

	ScalarFunctionSet set("ST_GetBandColorInterp");
	set.AddFunction(ScalarFunction({GeoTypes::RASTER(), LogicalType::INTEGER}, LogicalType::INTEGER, RasterGetColorInterpFunction));
	ExtensionUtil::RegisterFunction(db, set);
}

void GdalScalarFunctions::RegisterStBandColorInterpName(DatabaseInstance &db) {

	ScalarFunctionSet set("ST_GetBandColorInterpName");
	set.AddFunction(ScalarFunction({GeoTypes::RASTER(), LogicalType::INTEGER}, LogicalType::VARCHAR, RasterGetColorInterpNameFunction));
	ExtensionUtil::RegisterFunction(db, set);
}

} // namespace gdal

} // namespace spatial
