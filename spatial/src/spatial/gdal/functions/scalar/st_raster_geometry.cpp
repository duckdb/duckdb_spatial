#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "spatial/common.hpp"
#include "spatial/core/types.hpp"
#include "spatial/core/functions/common.hpp"
#include "spatial/core/geometry/geometry.hpp"
#include "spatial/gdal/functions/scalar.hpp"
#include "spatial/gdal/raster/raster.hpp"

#include "gdal_priv.h"

using namespace spatial::core;

namespace spatial {

namespace gdal {

//------------------------------------------------------------------------------
// ST_GetGeometry
//------------------------------------------------------------------------------

static void RasterGetGeometryFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &lstate = GeometryFunctionLocalState::ResetAndGet(state);

	UnaryExecutor::Execute<uintptr_t, geometry_t>(args.data[0], result, args.size(), [&](uintptr_t input) {
		Raster raster(reinterpret_cast<GDALDataset *>(input));
		return Geometry::Serialize(raster.GetGeometry(lstate.arena), result);
	});
}

//------------------------------------------------------------------------
// Documentation
//------------------------------------------------------------------------

static constexpr const char *DOC_DESCRIPTION = R"(
	Returns the polygon representation of the extent of the raster.
)";

static constexpr const char *DOC_EXAMPLE = R"(
	SELECT ST_GetGeometry(raster) FROM './test/data/mosaic/SCL.tif-land-clip00.tiff';
)";

static constexpr DocTag DOC_TAGS[] = {{"ext", "spatial"}, {"category", "property"}};

//------------------------------------------------------------------------------
// Register functions
//------------------------------------------------------------------------------

void GdalScalarFunctions::RegisterStGetGeometry(DatabaseInstance &db) {

	ScalarFunctionSet set("ST_GetGeometry");
	set.AddFunction(ScalarFunction({GeoTypes::RASTER()}, GeoTypes::GEOMETRY(), RasterGetGeometryFunction, nullptr,
	                               nullptr, nullptr, GeometryFunctionLocalState::Init));

	ExtensionUtil::RegisterFunction(db, set);

	DocUtil::AddDocumentation(db, "ST_GetGeometry", DOC_DESCRIPTION, DOC_EXAMPLE, DOC_TAGS);
}

} // namespace gdal

} // namespace spatial
