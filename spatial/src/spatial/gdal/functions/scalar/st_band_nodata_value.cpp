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

	BinaryExecutor::Execute<uintptr_t, int32_t, double_t>(
	    args.data[0], args.data[1], result, args.size(), [&](uintptr_t input, int32_t band_num) {
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

//------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------

static constexpr const char *DOC_DESCRIPTION = R"(
	Returns the NODATA value of a band in the raster.
)";

static constexpr const char *DOC_EXAMPLE = R"(
	SELECT ST_GetBandNoDataValue(raster, 1) FROM './test/data/mosaic/SCL.tif-land-clip00.tiff';
)";

static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "property"}};

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------

void GdalScalarFunctions::RegisterStBandNoDataValue(DatabaseInstance &db) {

	ScalarFunctionSet set("ST_GetBandNoDataValue");
	set.AddFunction(
	    ScalarFunction({GeoTypes::RASTER(), LogicalType::INTEGER}, LogicalType::DOUBLE, RasterGetBandNoDataFunction));
	ExtensionUtil::RegisterFunction(db, set);

	DocUtil::AddDocumentation(db, "ST_GetBandNoDataValue", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
}

} // namespace gdal

} // namespace spatial
