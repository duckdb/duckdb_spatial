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
// ST_GetBandNoDataValue
//------------------------------------------------------------------------------

static void RasterGetBandNoDataFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.data.size() == 2);

	BinaryExecutor::Execute<uintptr_t, int32_t, double_t>(args.data[0], args.data[1], result, args.size(), [&](uintptr_t input, int32_t band_num) {
		GDALDataset *dataset = reinterpret_cast<GDALDataset *>(input);

		if (band_num < 1) {
			throw InvalidInputException("BandNum must be greater than 0");
		}
		if (dataset->GetRasterCount() < band_num) {
			throw InvalidInputException("Dataset only has %d RasterBands", dataset->GetRasterCount());
		}
		GDALRasterBand *raster_band = dataset->GetRasterBand(band_num);
		return raster_band->GetNoDataValue();
	});
}

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------

void GdalScalarFunctions::RegisterStBandNoDataValue(DatabaseInstance &db) {

	ScalarFunctionSet set("ST_GetBandNoDataValue");
	set.AddFunction(ScalarFunction({GeoTypes::RASTER(), LogicalType::INTEGER}, LogicalType::DOUBLE, RasterGetBandNoDataFunction));
	ExtensionUtil::RegisterFunction(db, set);
}

} // namespace gdal

} // namespace spatial
