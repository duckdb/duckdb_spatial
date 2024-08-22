#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/gdal/functions/scalar.hpp"
#include "spatial/gdal/raster/raster.hpp"

using namespace spatial::core;

namespace spatial {

namespace gdal {

//------------------------------------------------------------------------------
// ST_SRID
//------------------------------------------------------------------------------

static void RasterGetSridFunction(DataChunk &args, ExpressionState &state, Vector &result) {

	UnaryExecutor::Execute<uintptr_t, int32_t>(args.data[0], result, args.size(), [&](uintptr_t input) {
		Raster raster(reinterpret_cast<GDALDataset *>(input));
		return raster.GetSrid();
	});
}

//------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------

static constexpr const char *DOC_DESCRIPTION = R"(
	Returns the spatial reference identifier (EPSG code) of the raster.
	Refer to [EPSG](https://spatialreference.org/ref/epsg/) for more details.
)";

static constexpr const char *DOC_EXAMPLE = R"(
	SELECT ST_SRID(raster) FROM './test/data/mosaic/SCL.tif-land-clip00.tiff';
)";

static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "property"}};

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------

void GdalScalarFunctions::RegisterStGetSRID(DatabaseInstance &db) {

	ScalarFunctionSet set("ST_SRID");
	set.AddFunction(ScalarFunction({GeoTypes::RASTER()}, LogicalType::INTEGER, RasterGetSridFunction));

	ExtensionUtil::RegisterFunction(db, set);

	DocUtil::AddDocumentation(db, "ST_SRID", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
}

} // namespace gdal

} // namespace spatial
